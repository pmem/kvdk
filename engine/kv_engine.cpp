/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kv_engine.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <dirent.h>
#include <future>
#include <libpmem.h>
#include <limits>
#include <mutex>
#include <sys/mman.h>
#include <thread>

#include "configs.hpp"
#include "dram_allocator.hpp"
#include "hash_list.hpp"
#include "kvdk/engine.hpp"
#include "skiplist.hpp"
#include "structures.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {
// use buffer to acc nt-write
thread_local std::string write_buffer;
static const int buffer_size = 1024 * 1024;
constexpr uint64_t kMaxWriteBatchSize = (1 << 20);

void PendingBatch::PersistProcessing(
    void *target, const std::vector<uint64_t> &entry_offsets) {
  pmem_memcpy_persist((char *)target + sizeof(PendingBatch),
                      entry_offsets.data(), entry_offsets.size() * 8);
  stage = Stage::Processing;
  num_kv = entry_offsets.size();
  pmem_memcpy_persist(target, this, sizeof(PendingBatch));
}

void PendingBatch::PersistStage(Stage s) {
  pmem_memcpy_persist(&stage, &s, sizeof(Stage));
}

KVEngine::KVEngine() {}

KVEngine::~KVEngine() {
  closing_ = true;
  GlobalLogger.Info("Closing instance ... \n");
  GlobalLogger.Info("Waiting bg threads exit ... \n");
  for (auto &t : bg_threads_) {
    t.join();
  }
  GlobalLogger.Info("Instance closed\n");
}

Status KVEngine::Open(const std::string &name, Engine **engine_ptr,
                      const Configs &configs) {
  KVEngine *engine = new KVEngine;
  Status s = engine->Init(name, configs);
  if (s == Status::Ok) {
    *engine_ptr = engine;
  }
  return s;
}

void KVEngine::BackgroundWork() {
  while (!closing_) {
    usleep(configs_.background_work_interval * 1000000);
    pmem_allocator_->BackgroundWork();
  }
}

Status KVEngine::Init(const std::string &name, const Configs &configs) {
  write_thread.id = 0;
  dir_ = format_dir_path(name);
  pending_batch_dir_ = dir_ + "pending_batch_files/";
  int res = create_dir_if_missing(dir_);
  if (res != 0) {
    GlobalLogger.Error("Create engine dir %s error\n", dir_.c_str());
    return Status::IOError;
  }

  res = create_dir_if_missing(pending_batch_dir_);
  if (res != 0) {
    GlobalLogger.Error("Create pending batch files dir %s error\n",
                       pending_batch_dir_.c_str());
    return Status::IOError;
  }

  db_file_ = db_file_name();
  configs_ = configs;
  Status s = PersistOrRecoverImmutableConfigs();
  if (s != Status::Ok) {
    return s;
  }
  pmem_allocator_.reset(new PMEMAllocator(
      db_file_, configs_.pmem_file_size, configs_.pmem_segment_blocks,
      configs_.pmem_block_size, configs_.max_write_threads));

  thread_res_.resize(configs_.max_write_threads);
  thread_manager_.reset(new ThreadManager(configs_.max_write_threads));
  hash_table_.reset(new HashTable(configs_.hash_bucket_num,
                                  configs_.hash_bucket_size,
                                  configs_.num_buckets_per_slot,
                                  pmem_allocator_, configs_.max_write_threads));
  ts_on_startup_ = get_cpu_tsc();
  s = Recovery();
  write_thread.id = -1;
  bg_threads_.emplace_back(&KVEngine::BackgroundWork, this);
  return s;
}

std::shared_ptr<Iterator>
KVEngine::NewSortedIterator(const pmem::obj::string_view collection) {
  Skiplist *skiplist;
  Status s = SearchOrInitSkiplist(collection, &skiplist, false);

  return s == Status::Ok
             ? std::make_shared<SortedIterator>(skiplist, pmem_allocator_)
             : nullptr;
}

Status KVEngine::MaybeInitWriteThread() {
  return thread_manager_->MaybeInitThread(write_thread);
}

Status KVEngine::RestoreData(uint64_t thread_id) try {
  write_thread.id = thread_id;

  SizedSpaceEntry segment_recovering;
  bool fetch = false;
  uint64_t cnt = 0;
  while (true) {
    if (segment_recovering.size == 0) {
      fetch = true;
    }
    if (fetch) {
      if (!pmem_allocator_->FreeAndFetchSegment(&segment_recovering)) {
        break;
      }
      fetch = false;
    }

    // Only DataEntry part is loaded from PMem for recovering
    DataEntry data_entry_recovering;

    void *pmp_record_recovering =
        pmem_allocator_->offset2addr(segment_recovering.space_entry.offset);

    memcpy(&data_entry_recovering, pmp_record_recovering, sizeof(DataEntry));

    // reach the of of this segment
    if (data_entry_recovering.header.b_size == 0) {
      fetch = true;
      continue;
    }

    segment_recovering.size -= data_entry_recovering.header.b_size;
    segment_recovering.space_entry.offset +=
        data_entry_recovering.header.b_size;

    uint32_t checksum{0};
    switch (data_entry_recovering.type) {
    case DataEntryType::SortedDataRecord:
    case DataEntryType::SortedDeleteRecord:
    case DataEntryType::SortedHeaderRecord:
    case DataEntryType::StringDataRecord:
    case DataEntryType::StringDeleteRecord: {
      checksum = _GetChecksumForSkiplistOrHashRecord_(
          &data_entry_recovering,
          static_cast<DataEntry *>(pmp_record_recovering));
      break;
    }
    case DataEntryType::Padding:
    case DataEntryType::Empty: {
      data_entry_recovering.type = DataEntryType::Padding;
      break;
    }
    default: {
      std::string msg{
          "Corrupted Record met when recovering. It has invalid type."};
      msg.append("Record type: ");
      msg.append(std::to_string(data_entry_recovering.type));
      msg.append("\n");
      GlobalLogger.Error(msg.data());
      // Report Corrupted Record, but still release it and continues
      data_entry_recovering.type = DataEntryType::Padding;
    }
    }

    if (data_entry_recovering.header.checksum != checksum) {
      DataEntryType type_padding = DataEntryType::Padding;
      pmem_memcpy(&static_cast<DataEntry *>(pmp_record_recovering)->type,
                  &type_padding, sizeof(DataEntryType), PMEM_F_MEM_NONTEMPORAL);
      data_entry_recovering.type = DataEntryType::Padding;
    }
    if (data_entry_recovering.type == DataEntryType::Padding) {
      pmem_allocator_->Free(
          SizedSpaceEntry(pmem_allocator_->addr2offset(pmp_record_recovering),
                          data_entry_recovering.header.b_size,
                          data_entry_recovering.timestamp));
      continue;
    }

    cnt++;

    auto ts_recovering = data_entry_recovering.timestamp;
    if (ts_recovering > thread_res_[thread_id].newest_restored_ts) {
      thread_res_[thread_id].newest_restored_ts = ts_recovering;
    }

    switch (data_entry_recovering.type) {
    case DataEntryType::SortedDataRecord:
    case DataEntryType::SortedDeleteRecord:
    case DataEntryType::SortedHeaderRecord:
    case DataEntryType::StringDataRecord:
    case DataEntryType::StringDeleteRecord: {
      Status s = _RestoreSkiplistOrHashRecord_(
          &data_entry_recovering,
          static_cast<DataEntry *>(pmp_record_recovering));
      if (s != Status::Ok)
        return s;
      break;
    }
    default: {
      std::string msg{
          "Invalid Record type when recovering. Trying restoring record. "};
      msg.append("Record type: ");
      msg.append(std::to_string(data_entry_recovering.type));
      msg.append("\n");
      throw std::runtime_error{msg};
    }
    }
  }
  write_thread.id = -1;
  restored_.fetch_add(cnt);
  return Status::Ok;
} catch (const std::runtime_error &e) {
  GlobalLogger.Error(e.what());
  return Status::NotSupported;
}

uint32_t
KVEngine::_GetChecksumForSkiplistOrHashRecord_(DataEntry *recovering_data_entry,
                                               DataEntry *pmem_data_entry) {
  bool dl_entry = recovering_data_entry->type & (DLDataEntryType);
  uint32_t checksum = dl_entry ? ((DLDataEntry *)pmem_data_entry)->Checksum()
                               : pmem_data_entry->Checksum();
  return checksum;
}

Status KVEngine::_RestoreSkiplistOrHashRecord_(DataEntry *recovering_data_entry,
                                               DataEntry *pmem_data_entry) {
  char existing_data_entry_buffer[sizeof(DLDataEntry)];
  DataEntry *existing_data_entry = (DataEntry *)existing_data_entry_buffer;
  HashEntry hash_entry;
  HashEntry *entry_base = nullptr;
  bool dl_entry = recovering_data_entry->type & (DLDataEntryType);

  pmem::obj::string_view pmem_key =
      dl_entry ? ((DLDataEntry *)pmem_data_entry)->Key()
               : pmem_data_entry->Key();
  std::string key(pmem_key.data(), pmem_key.size());

  if (recovering_data_entry->type == SortedHeaderRecord) {
    uint64_t id;
    memcpy_8(&id, ((DLDataEntry *)pmem_data_entry)->Value().data());
    Skiplist *skiplist;
    {
      std::lock_guard<std::mutex> lg(list_mu_);
      skiplists_.push_back(
          std::make_shared<Skiplist>((DLDataEntry *)pmem_data_entry, key, id,
                                     pmem_allocator_, hash_table_));
      skiplist = skiplists_.back().get();
    }
    compare_excange_if_larger(list_id_, id + 1);

    // Here key is the collection name
    auto hint = hash_table_->GetHint(key);
    std::lock_guard<SpinMutex> lg(*hint.spin);
    Status s = hash_table_->Search(hint, key, SortedHeaderRecord, &hash_entry,
                                   existing_data_entry, &entry_base,
                                   HashTable::SearchPurpose::Recover);
    if (s == Status::MemoryOverflow) {
      return s;
    }
    assert(s == Status::NotFound);
    hash_table_->Insert(hint, entry_base, SortedHeaderRecord,
                        (uint64_t)skiplist, HashOffsetType::Skiplist);
  } else {
    auto hint = hash_table_->GetHint(key);
    std::lock_guard<SpinMutex> lg(*hint.spin);
    Status s =
        hash_table_->Search(hint, key,
                            dl_entry ? (SortedDataRecord | SortedDeleteRecord)
                                     : (StringDeleteRecord | StringDataRecord),
                            &hash_entry, existing_data_entry, &entry_base,
                            HashTable::SearchPurpose::Recover);
    if (s == Status::MemoryOverflow) {
      return s;
    }
    bool found = s == Status::Ok;
    bool should_insert = true;
    if (found) {
      if (existing_data_entry->timestamp >= recovering_data_entry->timestamp) {
        should_insert = false;
      }
    }

    if (should_insert) {
      uint64_t new_hash_offset;
      uint64_t old_data_offset;
      if (dl_entry) {
        SkiplistNode *dram_node;
        if (!found) {
          auto height = Skiplist::RandomHeight();
          if (height > 0) {
            dram_node = SkiplistNode::NewNode(
                Skiplist::UserKey(key), (DLDataEntry *)pmem_data_entry, height);
            if (dram_node == nullptr) {
              GlobalLogger.Error("Memory overflow in recovery\n");
              return Status::MemoryOverflow;
            }
            new_hash_offset = (uint64_t)dram_node;
          } else {
            new_hash_offset = pmem_allocator_->addr2offset(pmem_data_entry);
          }
          // Hash entry won't be reused during data recovering so we don't
          // neet to check status here
          hash_table_->Insert(hint, entry_base, recovering_data_entry->type,
                              new_hash_offset,
                              height > 0 ? HashOffsetType::SkiplistNode
                                         : HashOffsetType::DLDataEntry);
        } else {
          if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
            dram_node = (SkiplistNode *)hash_entry.offset;
            old_data_offset =
                pmem_allocator_->addr2offset(dram_node->data_entry);
            dram_node->data_entry = (DLDataEntry *)pmem_data_entry;
            entry_base->header.data_type = recovering_data_entry->type;
          } else {
            old_data_offset = hash_entry.offset;
            hash_table_->Insert(hint, entry_base, recovering_data_entry->type,
                                pmem_allocator_->addr2offset(pmem_data_entry),
                                HashOffsetType::DLDataEntry);
          }
          pmem_allocator_->Free(SizedSpaceEntry(
              old_data_offset, existing_data_entry->header.b_size,
              existing_data_entry->timestamp));
        }
      } else {
        new_hash_offset = pmem_allocator_->addr2offset(pmem_data_entry);
        bool free_space =
            entry_base->header.status == HashEntryStatus::Updating;
        hash_table_->Insert(hint, entry_base, recovering_data_entry->type,
                            new_hash_offset, HashOffsetType::DataEntry);
        if (free_space) {
          old_data_offset = hash_entry.offset;
          pmem_allocator_->Free(SizedSpaceEntry(
              old_data_offset, existing_data_entry->header.b_size,
              existing_data_entry->timestamp));
        }
      }
      // If a delete record is the only existing data entry of a key, then we
      // can reuse the hash entry and free the data entry
      entry_base->header.status =
          (!found && (recovering_data_entry->type & DeleteDataEntryType)
               ? HashEntryStatus::Clean
               : HashEntryStatus::Normal);
    } else {
      pmem_allocator_->Free(
          SizedSpaceEntry(pmem_allocator_->addr2offset(pmem_data_entry),
                          recovering_data_entry->header.b_size,
                          recovering_data_entry->timestamp));
    }
  }
  return Status::Ok;
}

Status
KVEngine::SearchOrInitPersistentList(const pmem::obj::string_view &collection,
                                     PersistentList **list, bool init,
                                     uint16_t header_type) {
  auto hint = hash_table_->GetHint(collection);
  HashEntry hash_entry;
  HashEntry *entry_base = nullptr;
  Status s =
      hash_table_->Search(hint, collection, header_type, &hash_entry, nullptr,
                          &entry_base, HashTable::SearchPurpose::Read);
  if (s == Status::NotFound) {
    if (init) {
      DLDataEntry existing_data_entry;
      std::lock_guard<SpinMutex> lg(*hint.spin);
      // Since we do the first search without lock, we need to check again
      entry_base = nullptr;
      s = hash_table_->Search(hint, collection, header_type, &hash_entry,
                              &existing_data_entry, &entry_base,
                              HashTable::SearchPurpose::Write);
      if (s == Status::MemoryOverflow) {
        return s;
      }
      if (s == Status::NotFound) {
        uint32_t request_size =
            sizeof(DLDataEntry) + collection.size() + 8 /* id */;
        SizedSpaceEntry sized_space_entry =
            pmem_allocator_->Allocate(request_size);
        if (sized_space_entry.size == 0) {
          return Status::PmemOverflow;
        }
        char *block_base =
            pmem_allocator_->offset2addr(sized_space_entry.space_entry.offset);
        DLDataEntry data_entry(0, sized_space_entry.size, get_timestamp(),
                               header_type, collection.size(), 8,
                               kNullPmemOffset, kNullPmemOffset);
        uint64_t id = list_id_.fetch_add(1);
        PersistDataEntry(block_base, &data_entry, collection,
                         pmem::obj::string_view((char *)&id, 8), header_type);
        {
          std::lock_guard<std::mutex> lg(list_mu_);
          switch (header_type) {
          case SortedHeaderRecord:
            skiplists_.push_back(std::make_shared<Skiplist>(
                (DLDataEntry *)block_base,
                std::string(collection.data(), collection.size()), id,
                pmem_allocator_, hash_table_));
            *list = skiplists_.back().get();
            break;
          case HashListHeaderRecord:
            hashlists_.push_back(std::make_shared<HashList>());
            *list = hashlists_.back().get();
            break;
          default:
            return Status::NotSupported;
          }
        }
        auto entry_base_status = entry_base->header.status;
        hash_table_->Insert(hint, entry_base, header_type, (uint64_t)(*list),
                            HashOffsetType::Skiplist);
        if (entry_base_status == HashEntryStatus::Updating) {
          pmem_allocator_->Free(SizedSpaceEntry(
              hash_entry.offset, existing_data_entry.header.b_size,
              existing_data_entry.timestamp));
        } else if (entry_base_status == HashEntryStatus::BeingReused) {
          pmem_allocator_->DelayFree(SizedSpaceEntry(
              hash_entry.offset, existing_data_entry.header.b_size,
              existing_data_entry.timestamp));
        }
        return Status::Ok;
      }
    }
  }

  if (s == Status::Ok) {
    *list = (PersistentList *)hash_entry.offset;
  }

  return s;
}

Status KVEngine::PersistOrRecoverImmutableConfigs() {
  size_t mapped_len;
  int is_pmem;
  ImmutableConfigs *configs = (ImmutableConfigs *)pmem_map_file(
      config_file_name().c_str(), sizeof(ImmutableConfigs), PMEM_FILE_CREATE,
      0666, &mapped_len, &is_pmem);
  if (configs == nullptr || !is_pmem ||
      mapped_len != sizeof(ImmutableConfigs)) {
    GlobalLogger.Error("Open immutable configs file error %s\n",
                       !is_pmem ? (dir_ + "is not a valid pmem path").c_str()
                                : "");
    return Status::IOError;
  }
  if (configs->Valid()) {
    configs->AssignImmutableConfigs(configs_);
  }

  Status s = CheckConfigs(configs_);
  if (s == Status::Ok) {
    configs->PersistImmutableConfigs(configs_);
  }
  return s;
}

Status KVEngine::RestorePendingBatch() {
  DIR *dir;
  dirent *ent;
  uint64_t persisted_pending_file_size =
      kMaxWriteBatchSize * 8 /* offsets */ + sizeof(PendingBatch);
  size_t mapped_len;
  int is_pmem;

  // Iterate all pending batch files and rollback unfinished batch writes
  if ((dir = opendir(pending_batch_dir_.c_str())) != nullptr) {
    while ((ent = readdir(dir)) != nullptr) {
      std::string file_name = std::string(ent->d_name);
      if (file_name != "." && file_name != "..") {
        uint64_t id = std::stoul(file_name);
        std::string pending_batch_file = persisted_pending_block_file(id);

        PendingBatch *pending_batch = (PendingBatch *)pmem_map_file(
            pending_batch_file.c_str(), persisted_pending_file_size,
            PMEM_FILE_EXCL, 0666, &mapped_len, &is_pmem);

        if (pending_batch != nullptr) {
          if (!is_pmem || mapped_len != persisted_pending_file_size) {
            GlobalLogger.Error("Map persisted pending batch file %s failed\n",
                               pending_batch_file.c_str());
            return Status::IOError;
          }

          if (pending_batch->Unfinished()) {
            uint64_t *invalid_offsets = (uint64_t *)(pending_batch + 1);
            for (uint32_t i = 0; i < pending_batch->num_kv; i++) {
              DataEntry *data_entry =
                  (DataEntry *)pmem_allocator_->offset2addr(invalid_offsets[i]);
              if (data_entry->timestamp == pending_batch->timestamp) {
                data_entry->type = Padding;
                pmem_persist(&data_entry->type, 8);
              }
            }
            pending_batch->PersistStage(PendingBatch::Stage::Done);
          }

          if (id < configs_.max_write_threads) {
            thread_res_[id].persisted_pending_batch = pending_batch;
          } else {
            remove(pending_batch_file.c_str());
          }
        }
      }
    }
  } else {
    GlobalLogger.Error("Open persisted pending batch dir %s failed\n",
                       pending_batch_dir_.c_str());
    return Status::IOError;
  }

  return Status::Ok;
}

Status KVEngine::Recovery() {
  auto s = RestorePendingBatch();
  if (s != Status::Ok) {
    return s;
  }

  std::vector<std::future<Status>> fs;
  for (uint32_t i = 0; i < configs_.max_write_threads; i++) {
    fs.push_back(std::async(&KVEngine::RestoreData, this, i));
  }

  for (auto &f : fs) {
    Status s = f.get();
    if (s != Status::Ok) {
      return s;
    }
  }

  fs.clear();
  for (auto s : skiplists_) {
    fs.push_back(std::async(&Skiplist::Rebuild, s));
  }

  for (auto &f : fs) {
    Status s = f.get();
    if (s != Status::Ok) {
      return s;
    }
  }

  GlobalLogger.Info("In restoring: iterated %lu records\n", restored_.load());

  if (restored_.load() == 0) {
    if (configs_.populate_pmem_space) {
      pmem_allocator_->PopulateSpace();
    }
  } else {
    for (auto &ts : thread_res_) {
      if (ts.newest_restored_ts > newest_version_on_startup_) {
        newest_version_on_startup_ = ts.newest_restored_ts;
      }
    }
  }

  return Status::Ok;
}

Status KVEngine::HashGetImpl(const pmem::obj::string_view &key,
                             std::string *value, uint16_t type_mask) {
  std::unique_ptr<DataEntry> data_entry(
      type_mask & DLDataEntryType ? new DLDataEntry : new DataEntry);
  HashEntry hash_entry;
  HashEntry *entry_base = nullptr;
  bool is_found =
      hash_table_->Search(hash_table_->GetHint(key), key, type_mask,
                          &hash_entry, data_entry.get(), &entry_base,
                          HashTable::SearchPurpose::Read) == Status::Ok;
  if (!is_found || (hash_entry.header.data_type & DeleteDataEntryType)) {
    return Status::NotFound;
  }

  char *block_base = nullptr;
  if (hash_entry.header.data_type & (StringDataRecord | HashListDataRecord)) {
    block_base = pmem_allocator_->offset2addr(hash_entry.offset);
  } else if (hash_entry.header.data_type == SortedDataRecord) {
    if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
      SkiplistNode *dram_node = (SkiplistNode *)hash_entry.offset;
      block_base = (char *)dram_node->data_entry;
    } else {
      assert(hash_entry.header.offset_type == HashOffsetType::DLDataEntry);
      block_base = pmem_allocator_->offset2addr(hash_entry.offset);
    }
  } else {
    return Status::NotSupported;
  }

  while (1) {
    value->assign(block_base + key.size() +
                      data_entry_size(hash_entry.header.data_type),
                  data_entry->v_size);

    std::atomic_thread_fence(std::memory_order_acquire);
    // Double check for lock free read
    // TODO double check for skiplist get
    if (__glibc_likely(
            (hash_entry.header.data_type & DLDataEntryType) ||
            (hash_entry.offset == entry_base->offset &&
             hash_entry.header.key_prefix == entry_base->header.key_prefix)))
      break;
    if (entry_base->header.key_prefix != hash_entry.header.key_prefix ||
        entry_base->header.data_type == DeleteDataEntryType) {
      value->clear();
      return Status::NotFound;
    }
    HashEntry::CopyOffset(&hash_entry, entry_base);
    block_base = pmem_allocator_->offset2addr(hash_entry.offset);

    memcpy(data_entry.get(), block_base, sizeof(DataEntry));
  }
  return Status::Ok;
}

Status KVEngine::Get(const pmem::obj::string_view key, std::string *value) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  return HashGetImpl(key, value, StringDataEntryType);
}

Status KVEngine::Delete(const pmem::obj::string_view key) {
  Status s = MaybeInitWriteThread();

  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  return HashSetImpl(key, "", StringDeleteRecord);
}

inline void KVEngine::PersistDataEntry(char *block_base, DataEntry *data_entry,
                                       const pmem::obj::string_view &key,
                                       const pmem::obj::string_view &value,
                                       uint16_t type) {
  char *data_cpy_target;
  auto entry_size = data_entry_size(type);
  bool with_buffer = entry_size + key.size() + value.size() <= buffer_size;
  if (with_buffer) {
    if (write_buffer.empty()) {
      write_buffer.resize(buffer_size);
    }
    data_cpy_target = &write_buffer[0];
  } else {
    data_cpy_target = block_base;
  }
  memcpy(data_cpy_target, data_entry, entry_size);
  memcpy(data_cpy_target + entry_size, key.data(), key.size());
  memcpy(data_cpy_target + entry_size + key.size(), value.data(), value.size());
  if (type & DLDataEntryType) {
    DLDataEntry *entry_with_data = ((DLDataEntry *)data_cpy_target);
    entry_with_data->header.checksum = entry_with_data->Checksum();
  } else {
    DataEntry *entry_with_data = ((DataEntry *)data_cpy_target);
    entry_with_data->header.checksum = entry_with_data->Checksum();
  }
  if (with_buffer) {
    pmem_memcpy(block_base, data_cpy_target,
                entry_size + key.size() + value.size(), PMEM_F_MEM_NONTEMPORAL);
  } else {
    pmem_flush(block_base, entry_size + key.size() + value.size());
  }
  pmem_drain();
}

Status KVEngine::SSetImpl(Skiplist *skiplist,
                          const pmem::obj::string_view &user_key,
                          const pmem::obj::string_view &value, uint16_t dt) {
  uint64_t id = skiplist->id();
  std::string collection_key(PersistentList::ListKey(user_key, id));
  if (!CheckKeySize(collection_key) || !CheckValueSize(value)) {
    return Status::InvalidDataSize;
  }

  HashEntry hash_entry;
  DLDataEntry data_entry;
  SkiplistNode *dram_node = nullptr;
  uint64_t old_entry_offset;
  bool is_delete = (dt & (DeleteDataEntryType));

  auto request_size =
      value.size() + collection_key.size() + sizeof(DLDataEntry);
  SizedSpaceEntry sized_space_entry;
  if (!(dt & (DeleteDataEntryType))) {
    sized_space_entry = pmem_allocator_->Allocate(request_size);
    if (sized_space_entry.size == 0) {
      return Status::PmemOverflow;
    }
  }
  while (1) {
    HashEntry *entry_base = nullptr;
    auto hint = hash_table_->GetHint(collection_key);
    std::lock_guard<SpinMutex> lg(*hint.spin);
    Status s = hash_table_->Search(
        hint, collection_key, SortedDataRecord | SortedDeleteRecord,
        &hash_entry, &data_entry, &entry_base, HashTable::SearchPurpose::Write);
    if (s == Status ::MemoryOverflow) {
      return s;
    }
    bool found = s == Status::Ok;
    if (dt & DeleteDataEntryType) {
      if (found && entry_base->header.data_type != dt) {
        if (sized_space_entry.size == 0) {
          sized_space_entry = pmem_allocator_->Allocate(request_size);
        }
        if (sized_space_entry.size == 0) {
          return Status::PmemOverflow;
        }
      } else {
        return Status::Ok;
      }
    }

    char *block_base =
        pmem_allocator_->offset2addr(sized_space_entry.space_entry.offset);
    uint64_t new_ts = get_timestamp();

    if (found && new_ts < data_entry.timestamp) {
      pmem_allocator_->Free(sized_space_entry);
      return Status::Ok;
    }

    std::vector<SpinMutex *> spins;
    thread_local Skiplist::Splice splice;
    if (!skiplist->FindAndLockWritePos(&splice, user_key, hint, spins,
                                       found ? &data_entry : nullptr)) {
      continue;
    }

    uint64_t prev_offset = pmem_allocator_->addr2offset(splice.prev_data_entry);
    uint64_t next_offset = pmem_allocator_->addr2offset(splice.next_data_entry);

    DLDataEntry write_entry(0, sized_space_entry.size, new_ts, dt,
                            collection_key.size(), value.size(), prev_offset,
                            next_offset);
    PersistDataEntry(block_base, &write_entry, collection_key, value, dt);

    if (found) {
      if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
        dram_node = (SkiplistNode *)(hash_entry.offset);
        old_entry_offset = pmem_allocator_->addr2offset(dram_node->data_entry);
      } else {
        assert(hash_entry.header.offset_type == HashOffsetType::DLDataEntry);
        old_entry_offset = hash_entry.offset;
      }
    }

    dram_node = (SkiplistNode *)skiplist->InsertDataEntry(
        &splice, (DLDataEntry *)block_base, user_key, dram_node, found);

    if (!found) {
      uint64_t offset = (dram_node == nullptr)
                            ? pmem_allocator_->addr2offset(block_base)
                            : (uint64_t)dram_node;
      auto entry_base_status = entry_base->header.status;
      hash_table_->Insert(hint, entry_base, dt, offset,
                          dram_node ? HashOffsetType::SkiplistNode
                                    : HashOffsetType::DLDataEntry);
      if (entry_base_status == HashEntryStatus::Updating) {
        pmem_allocator_->Free(SizedSpaceEntry(
            hash_entry.offset, data_entry.header.b_size, data_entry.timestamp));
      } else if (entry_base_status == HashEntryStatus::BeingReused) {
        pmem_allocator_->DelayFree(SizedSpaceEntry(
            hash_entry.offset, data_entry.header.b_size, data_entry.timestamp));
      }
    } else {
      // update a height 0 dram node
      if (dram_node == nullptr) {
        hash_table_->Insert(hint, entry_base, dt,
                            pmem_allocator_->addr2offset(block_base),
                            HashOffsetType::DLDataEntry);
      } else {
        entry_base->header.data_type = dt;
      }
      pmem_allocator_->Free(SizedSpaceEntry(
          old_entry_offset, data_entry.header.b_size, data_entry.timestamp));
    }

    for (auto &m : spins) {
      m->unlock();
    }
    break;
  }
  return Status::Ok;
}

Status KVEngine::SSet(const pmem::obj::string_view collection,
                      const pmem::obj::string_view user_key,
                      const pmem::obj::string_view value) {
  Status s = MaybeInitWriteThread();
  if (s != Status::Ok) {
    return s;
  }

  Skiplist *skiplist = nullptr;
  s = SearchOrInitSkiplist(collection, &skiplist, true);
  if (s != Status::Ok) {
    return s;
  }

  return SSetImpl(skiplist, user_key, value, SortedDataRecord);
}

Status KVEngine::CheckConfigs(const Configs &configs) {
  auto is_2pown = [](uint64_t n) { return (n > 0) && (n & (n - 1)) == 0; };

  if (configs.pmem_block_size < 16) {
    GlobalLogger.Error("pmem block size too small\n");
    return Status::InvalidConfiguration;
  }

  if (configs.pmem_segment_blocks * configs.pmem_block_size < 1024) {
    GlobalLogger.Error("pmem segment size too small\n");
    return Status::InvalidConfiguration;
  }

  if (configs.pmem_file_size % configs.pmem_block_size != 0) {
    GlobalLogger.Error(
        "pmem file size should align to pmem block size (%d bytes)\n",
        configs.pmem_block_size);
    return Status::InvalidConfiguration;
  }

  auto sz_segment = configs.pmem_block_size * configs.pmem_segment_blocks;
  if (configs.pmem_file_size % sz_segment != 0) {
    GlobalLogger.Error("pmem file size should align to segment "
                       "size(pmem_segment_blocks*pmem_block_size) (%d bytes)\n",
                       configs.pmem_block_size);
    return Status::InvalidConfiguration;
  }

  if (configs.pmem_segment_blocks * configs.pmem_block_size *
          configs.max_write_threads >
      configs.pmem_file_size) {
    GlobalLogger.Error(
        "pmem file too small, should larger than pmem_segment_blocks * "
        "pmem_block_size * max_write_threads\n");
    return Status::InvalidConfiguration;
  }

  if (configs.hash_bucket_size < sizeof(HashEntry) + 8) {
    GlobalLogger.Error("hash bucket too small\n");
    return Status::InvalidConfiguration;
  }

  if (!is_2pown(configs.hash_bucket_num) ||
      !is_2pown(configs.num_buckets_per_slot)) {
    GlobalLogger.Error(
        "hash_bucket_num and num_buckets_per_slot should be 2^n\n");
    return Status::InvalidConfiguration;
  }

  if (configs.hash_bucket_num >= ((uint64_t)1 << 32)) {
    GlobalLogger.Error("too many hash buckets\n");
    return Status::InvalidConfiguration;
  }

  if (configs.num_buckets_per_slot > configs.hash_bucket_num) {
    GlobalLogger.Error(
        "num_buckets_per_slot should less than hash_bucket_num\n");
    return Status::InvalidConfiguration;
  }

  return Status::Ok;
}

Status KVEngine::SDelete(const pmem::obj::string_view collection,
                         const pmem::obj::string_view user_key) {
  Status s = MaybeInitWriteThread();
  if (s != Status::Ok) {
    return s;
  }

  Skiplist *skiplist = nullptr;
  s = SearchOrInitSkiplist(collection, &skiplist, false);
  if (s != Status::Ok) {
    return s == Status::NotFound ? Status::Ok : s;
  }

  return SSetImpl(skiplist, user_key, "", SortedDeleteRecord);
}

Status KVEngine::MaybeInitPendingBatchFile() {
  if (thread_res_[write_thread.id].persisted_pending_batch == nullptr) {
    int is_pmem;
    size_t mapped_len;
    uint64_t persisted_pending_file_size =
        kMaxWriteBatchSize * 8 + sizeof(PendingBatch);
    if ((thread_res_[write_thread.id].persisted_pending_batch =
             (PendingBatch *)pmem_map_file(
                 persisted_pending_block_file(write_thread.id).c_str(),
                 persisted_pending_file_size, PMEM_FILE_CREATE, 0666,
                 &mapped_len, &is_pmem)) == nullptr ||
        !is_pmem || mapped_len != persisted_pending_file_size) {
      return Status::MapError;
    }
  }
  return Status::Ok;
}

Status KVEngine::BatchWrite(const WriteBatch &write_batch) {
  if (write_batch.Size() > kMaxWriteBatchSize) {
    return Status::BatchOverflow;
  }

  Status s = MaybeInitWriteThread();
  if (s != Status::Ok) {
    return s;
  }

  s = MaybeInitPendingBatchFile();
  if (s != Status::Ok) {
    return s;
  }

  uint64_t ts = get_timestamp();

  std::vector<BatchWriteHint> batch_hints(write_batch.Size());
  std::vector<uint64_t> space_entry_offsets(write_batch.Size());
  for (size_t i = 0; i < write_batch.Size(); i++) {
    auto &kv = write_batch.kvs[i];
    uint32_t requested_size =
        kv.key.size() + kv.value.size() + sizeof(DataEntry);
    batch_hints[i].sized_space_entry =
        pmem_allocator_->Allocate(requested_size);
    if (batch_hints[i].sized_space_entry.size == 0) {
      return s;
    }

    batch_hints[i].ts = ts;
    space_entry_offsets[i] =
        batch_hints[i].sized_space_entry.space_entry.offset *
        configs_.pmem_block_size;
  }

  PendingBatch pending_batch(PendingBatch::Stage::Processing,
                             write_batch.Size(), ts);
  pending_batch.PersistProcessing(
      thread_res_[write_thread.id].persisted_pending_batch,
      space_entry_offsets);

  for (size_t i = 0; i < write_batch.Size(); i++) {
    auto &kv = write_batch.kvs[i];
    s = HashSetImpl(kv.key, kv.value, kv.type, &batch_hints[i]);

    if (s != Status::Ok) {
      return s;
    }
  }

  pending_batch.PersistStage(PendingBatch::Stage::Done);
  return s;
}

Status KVEngine::SGet(const pmem::obj::string_view collection,
                      const pmem::obj::string_view user_key,
                      std::string *value) {
  Skiplist *skiplist = nullptr;
  Status s = SearchOrInitSkiplist(collection, &skiplist, false);
  if (s != Status::Ok) {
    return s;
  }
  assert(skiplist);
  uint64_t id = skiplist->id();
  std::string skiplist_key(PersistentList::ListKey(user_key, id));
  return HashGetImpl(skiplist_key, value,
                     SortedDataRecord | SortedDeleteRecord);
}

Status KVEngine::HashSetImpl(const pmem::obj::string_view &key,
                             const pmem::obj::string_view &value, uint16_t dt,
                             BatchWriteHint *batch_hint) {
  DataEntry data_entry;
  HashEntry hash_entry;
  HashEntry *entry_base = nullptr;
  uint32_t v_size = value.size();

  uint32_t requested_size = v_size + key.size() + sizeof(DataEntry);
  char *block_base = nullptr;
  SizedSpaceEntry sized_space_entry;

  if (batch_hint == nullptr) {
    if (dt != StringDeleteRecord) {
      sized_space_entry = pmem_allocator_->Allocate(requested_size);
      if (sized_space_entry.size == 0) {
        return Status::PmemOverflow;
      }
    }
  } else {
    sized_space_entry = batch_hint->sized_space_entry;
  }

  {
    auto hint = hash_table_->GetHint(key);
    std::lock_guard<SpinMutex> lg(*hint.spin);
    Status s = hash_table_->Search(
        hint, key, StringDeleteRecord | StringDataRecord, &hash_entry,
        &data_entry, &entry_base, HashTable::SearchPurpose::Write);
    if (s == Status::MemoryOverflow) {
      return s;
    }
    bool found = s == Status::Ok;
    if (dt == StringDeleteRecord && batch_hint == nullptr) {
      if (found && entry_base->header.data_type != StringDeleteRecord) {
        sized_space_entry = pmem_allocator_->Allocate(requested_size);
        if (sized_space_entry.size == 0) {
          return Status::PmemOverflow;
        }
      } else {
        return Status::Ok;
      }
    }

    block_base =
        pmem_allocator_->offset2addr(sized_space_entry.space_entry.offset);

    uint64_t new_ts = batch_hint ? batch_hint->ts : get_timestamp();
    if (found && new_ts < data_entry.timestamp) {
      if (sized_space_entry.size > 0) {
        pmem_allocator_->Free(sized_space_entry);
      }
      return Status::Ok;
    }

    DataEntry write_entry(0, sized_space_entry.size, new_ts, dt, key.size(),
                          v_size);
    PersistDataEntry(block_base, &write_entry, key, value, dt);

    auto entry_base_status = entry_base->header.status;
    hash_table_->Insert(hint, entry_base, dt,
                        sized_space_entry.space_entry.offset,
                        HashOffsetType::DataEntry);
    if (entry_base_status == HashEntryStatus::Updating) {
      pmem_allocator_->Free(SizedSpaceEntry(
          hash_entry.offset, data_entry.header.b_size, data_entry.timestamp));
    } else if (entry_base_status == HashEntryStatus::BeingReused) {
      pmem_allocator_->DelayFree(SizedSpaceEntry(
          hash_entry.offset, data_entry.header.b_size, data_entry.timestamp));
    }
  }

  return Status::Ok;
}

Status KVEngine::Set(const pmem::obj::string_view key,
                     const pmem::obj::string_view value) {
  Status s = MaybeInitWriteThread();
  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(key) || !CheckValueSize(value)) {
    return Status::InvalidDataSize;
  }
  return HashSetImpl(key, value, StringDataRecord);
}
} // namespace KVDK_NAMESPACE
