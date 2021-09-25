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
thread_local std::string thread_data_buffer;
static const int kDataBufferSize = 1024 * 1024;
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
  // Only DataEntry part is loaded from PMem for recovering
  DataEntry cached_recovering_data_entry;
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

    void *recovering_pmem_data_entry =
        pmem_allocator_->offset2addr(segment_recovering.space_entry.offset);

    memcpy(&cached_recovering_data_entry, recovering_pmem_data_entry,
           sizeof(DataEntry));

    // reach the of of this segment
    // or the segment is empty
    if (cached_recovering_data_entry.header.b_size == 0) {
      fetch = true;
      continue;
    }

    segment_recovering.size -= cached_recovering_data_entry.header.b_size;
    segment_recovering.space_entry.offset +=
        cached_recovering_data_entry.header.b_size;

    switch (cached_recovering_data_entry.type) {
    case DataEntryType::SortedDataRecord:
    case DataEntryType::SortedDeleteRecord:
    case DataEntryType::SortedHeaderRecord:
    case DataEntryType::StringDataRecord:
    case DataEntryType::StringDeleteRecord: 
    case DataEntryType::DlistRecord:
    case DataEntryType::DlistHeadRecord:
    case DataEntryType::DlistTailRecord:
    case DataEntryType::DlistDataRecord:
    case DataEntryType::DlistDeleteRecord:
    {
      uint32_t checksum = CalculateChecksum(
          static_cast<DataEntry *>(recovering_pmem_data_entry));
      if (cached_recovering_data_entry.header.checksum != checksum) {
        // Checksum dismatch, mark PMem SizedSpaceEntry as padding to Free
        // Otherwise the Restore will continue normally
        DataEntryType type_padding = DataEntryType::Padding;
        pmem_memcpy(&static_cast<DataEntry *>(recovering_pmem_data_entry)->type,
                    &type_padding, sizeof(DataEntryType),
                    PMEM_F_MEM_NONTEMPORAL);
        cached_recovering_data_entry.type = DataEntryType::Padding;
      }
      break;
    }
    case DataEntryType::Padding:
    case DataEntryType::Empty: {
      cached_recovering_data_entry.type = DataEntryType::Padding;
      break;
    }
    default: {
      std::string msg{
          "Corrupted Record met when recovering. It has invalid type."};
      msg.append("Record type: ");
      msg.append(std::to_string(cached_recovering_data_entry.type));
      msg.append("\n");
      GlobalLogger.Error(msg.data());
      // Report Corrupted Record, but still release it and continues
      cached_recovering_data_entry.type = DataEntryType::Padding;
      break;
    }
    }
    // When met records with invalid checksum 
    // or the space is padding, empty or with corrupted DataEntry
    // Free the space and fetch another
    if (cached_recovering_data_entry.type == DataEntryType::Padding)
    {
      pmem_allocator_->Free(SizedSpaceEntry(
          pmem_allocator_->addr2offset(recovering_pmem_data_entry),
          cached_recovering_data_entry.header.b_size,
          cached_recovering_data_entry.timestamp));
      continue;
    }
    
    // DataEntry has valid type and Checksum is correct
    // Continue to restore the Record
    cnt++;

    auto ts_recovering = cached_recovering_data_entry.timestamp;
    if (ts_recovering > thread_res_[thread_id].newest_restored_ts) {
      thread_res_[thread_id].newest_restored_ts = ts_recovering;
    }

    Status s;
    switch (cached_recovering_data_entry.type) {
    case DataEntryType::SortedDataRecord:
    case DataEntryType::SortedDeleteRecord: {
      s = RestoreSortedRecord(
          static_cast<DLDataEntry *>(recovering_pmem_data_entry),
          &cached_recovering_data_entry);
      break;
    }
    case DataEntryType::SortedHeaderRecord: {
      s = RestoreSkiplist(
          static_cast<DLDataEntry *>(recovering_pmem_data_entry),
          &cached_recovering_data_entry);
      break;
    }
    case DataEntryType::StringDataRecord:
    case DataEntryType::StringDeleteRecord: {
      s = RestoreStringRecord(
          static_cast<DataEntry *>(recovering_pmem_data_entry),
          &cached_recovering_data_entry);
      break;
    }
    case DataEntryType::DlistRecord:
    case DataEntryType::DlistHeadRecord:
    case DataEntryType::DlistTailRecord:
    case DataEntryType::DlistDataRecord:
    case DataEntryType::DlistDeleteRecord:
    {
      s = RestoreDlistRecords(recovering_pmem_data_entry, cached_recovering_data_entry);
      break;
    }
    default: {
      std::string msg{
          "Invalid Record type when recovering. Trying restoring record. "};
      msg.append("Record type: ");
      msg.append(std::to_string(cached_recovering_data_entry.type));
      msg.append("\n");
      throw std::runtime_error{msg};
    }
    }
    if (s != Status::Ok) {
      write_thread.id = -1;
      return s;
    }
  }
  write_thread.id = -1;
  restored_.fetch_add(cnt);
  return Status::Ok;
} catch (const std::runtime_error &e) {
  write_thread.id = -1;
  GlobalLogger.Error(e.what());
  return Status::NotSupported;
}

uint32_t KVEngine::CalculateChecksum(DataEntry *data_entry) {
  uint32_t checksum;
  switch (data_entry->type)
  {
  case DataEntryType::StringDataRecord:
  case DataEntryType::StringDeleteRecord:
  {
    checksum = data_entry->Checksum();
    break;
  }
  case DataEntryType::SortedDataRecord:
  case DataEntryType::SortedDeleteRecord:
  case DataEntryType::SortedHeaderRecord:
  {
    checksum = static_cast<DLDataEntry*>(data_entry)->Checksum();
    break;
  }
  case DataEntryType::DlistDataRecord:
  case DataEntryType::DlistDeleteRecord:
  case DataEntryType::DlistRecord:
  case DataEntryType::DlistHeadRecord:
  case DataEntryType::DlistTailRecord:
  {
    checksum = UnorderedCollection::CheckSum(static_cast<DLDataEntry*>(data_entry));
    break;
  }
  default:
  {
    assert(false && "Unsupported type in CalculateChecksum()!");
    break;
  }
  }
  return checksum;
}

Status KVEngine::RestoreSkiplist(DLDataEntry *pmem_data_entry, DataEntry *) {
  assert(pmem_data_entry->type == SortedHeaderRecord);
  pmem::obj::string_view pmem_key = pmem_data_entry->Key();
  std::string key(string_view_2_string(pmem_key));
  thread_local DLDataEntry existing_data_entry;
  thread_local HashEntry hash_entry;
  HashEntry *entry_base = nullptr;

  uint64_t id;
  memcpy_8(&id, ((DLDataEntry *)pmem_data_entry)->Value().data());
  Skiplist *skiplist;
  {
    std::lock_guard<std::mutex> lg(list_mu_);
    skiplists_.push_back(std::make_shared<Skiplist>(
        (DLDataEntry *)pmem_data_entry, key, id, pmem_allocator_, hash_table_));
    skiplist = skiplists_.back().get();
  }
  compare_excange_if_larger(list_id_, id + 1);

  // Here key is the collection name
  auto hint = hash_table_->GetHint(key);
  std::lock_guard<SpinMutex> lg(*hint.spin);
  Status s = hash_table_->Search(hint, key, SortedHeaderRecord, &hash_entry,
                                 &existing_data_entry, &entry_base,
                                 HashTable::SearchPurpose::Recover);
  if (s == Status::MemoryOverflow) {
    return s;
  }
  assert(s == Status::NotFound);
  hash_table_->Insert(hint, entry_base, SortedHeaderRecord, (uint64_t)skiplist,
                      HashOffsetType::Skiplist);
  return Status::Ok;
}

Status KVEngine::RestoreStringRecord(DataEntry *pmem_data_entry,
                                     DataEntry *cached_meta) {
  assert(pmem_data_entry->type & StringDataEntryType);
  pmem::obj::string_view pmem_key = pmem_data_entry->Key();
  std::string key(string_view_2_string(pmem_key));
  thread_local DataEntry existing_data_entry;
  thread_local HashEntry hash_entry;
  HashEntry *entry_base = nullptr;

  auto hint = hash_table_->GetHint(key);
  std::lock_guard<SpinMutex> lg(*hint.spin);
  Status s = hash_table_->Search(hint, key, StringDataEntryType, &hash_entry,
                                 &existing_data_entry, &entry_base,
                                 HashTable::SearchPurpose::Recover);

  if (s == Status::MemoryOverflow) {
    return s;
  }

  bool found = (s == Status::Ok);
  if (found && existing_data_entry.timestamp >= cached_meta->timestamp) {
    pmem_allocator_->Free(
        SizedSpaceEntry(pmem_allocator_->addr2offset(pmem_data_entry),
                        cached_meta->header.b_size, cached_meta->timestamp));
    return Status::Ok;
  }

  uint64_t new_hash_offset = pmem_allocator_->addr2offset(pmem_data_entry);
  bool free_space = entry_base->header.status == HashEntryStatus::Updating;
  hash_table_->Insert(hint, entry_base, cached_meta->type, new_hash_offset,
                      HashOffsetType::DataEntry);
  if (free_space) {
    pmem_allocator_->Free(SizedSpaceEntry(hash_entry.offset,
                                          existing_data_entry.header.b_size,
                                          existing_data_entry.timestamp));
  }

  // If a delete record is the only existing data entry of a key, then we
  // can reuse the hash entry and free the data entry
  entry_base->header.status =
      (!found && (cached_meta->type & DeleteDataEntryType)
           ? HashEntryStatus::Clean
           : HashEntryStatus::Normal);

  return Status::Ok;
}

Status KVEngine::RestoreSortedRecord(DLDataEntry *pmem_data_entry,
                                     DataEntry *cached_meta) {
  assert(pmem_data_entry->type & (SortedDataRecord | SortedDeleteRecord));
  pmem::obj::string_view pmem_key = pmem_data_entry->Key();
  std::string key(string_view_2_string(pmem_key));
  thread_local DLDataEntry existing_data_entry;
  thread_local HashEntry hash_entry;
  HashEntry *entry_base = nullptr;

  auto hint = hash_table_->GetHint(key);
  std::lock_guard<SpinMutex> lg(*hint.spin);
  Status s = hash_table_->Search(
      hint, key, (SortedDataRecord | SortedDeleteRecord), &hash_entry,
      &existing_data_entry, &entry_base, HashTable::SearchPurpose::Recover);

  if (s == Status::MemoryOverflow) {
    return s;
  }

  bool found = s == Status::Ok;
  if (found && existing_data_entry.timestamp >= cached_meta->timestamp) {
    pmem_allocator_->Free(
        SizedSpaceEntry(pmem_allocator_->addr2offset(pmem_data_entry),
                        cached_meta->header.b_size, cached_meta->timestamp));
    return Status::Ok;
  }

  uint64_t new_hash_offset;
  uint64_t old_data_offset;
  SkiplistNode *dram_node;
  if (!found) {
    auto height = Skiplist::RandomHeight();
    if (height > 0) {
      dram_node = SkiplistNode::NewNode(Skiplist::UserKey(key),
                                        (DLDataEntry *)pmem_data_entry, height);
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
    hash_table_->Insert(hint, entry_base, cached_meta->type, new_hash_offset,
                        height > 0 ? HashOffsetType::SkiplistNode
                                   : HashOffsetType::DLDataEntry);
  } else {
    if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
      dram_node = (SkiplistNode *)hash_entry.offset;
      old_data_offset = pmem_allocator_->addr2offset(dram_node->data_entry);
      dram_node->data_entry = (DLDataEntry *)pmem_data_entry;
      entry_base->header.data_type = cached_meta->type;
    } else {
      old_data_offset = hash_entry.offset;
      hash_table_->Insert(hint, entry_base, cached_meta->type,
                          pmem_allocator_->addr2offset(pmem_data_entry),
                          HashOffsetType::DLDataEntry);
    }
    pmem_allocator_->Free(SizedSpaceEntry(old_data_offset,
                                          existing_data_entry.header.b_size,
                                          existing_data_entry.timestamp));
  }
  // If a delete record is the only existing data entry of a key, then we
  // can reuse the hash entry and free the data entry
  entry_base->header.status =
      (!found && (cached_meta->type & DeleteDataEntryType)
           ? HashEntryStatus::Clean
           : HashEntryStatus::Normal);

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

  GlobalLogger.Info("In restoring: iterated %lu records\n", restored_.load());

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

  GlobalLogger.Info("In recovering skiplists\n");

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
  std::unique_ptr<DataEntry> data_entry_meta(
      type_mask & DLDataEntryType ? new DLDataEntry : new DataEntry);
  while (1) {
    HashEntry hash_entry;
    HashEntry *entry_base = nullptr;
    bool is_found =
        hash_table_->Search(hash_table_->GetHint(key), key, type_mask,
                            &hash_entry, data_entry_meta.get(), &entry_base,
                            HashTable::SearchPurpose::Read) == Status::Ok;
    if (!is_found || (hash_entry.header.data_type & DeleteDataEntryType)) {
      return Status::NotFound;
    }

    char *pmem_data_entry = nullptr;
    if (hash_entry.header.data_type & (StringDataRecord | HashListDataRecord)) {
      pmem_data_entry = pmem_allocator_->offset2addr(hash_entry.offset);
    } else if (hash_entry.header.data_type == SortedDataRecord) {
      if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
        SkiplistNode *dram_node = (SkiplistNode *)hash_entry.offset;
        pmem_data_entry = (char *)dram_node->data_entry;
      } else {
        assert(hash_entry.header.offset_type == HashOffsetType::DLDataEntry);
        pmem_data_entry = pmem_allocator_->offset2addr(hash_entry.offset);
      }
    } else {
      return Status::NotSupported;
    }

    // Copy PMem data entry to dram buffer
    auto pmem_data_entry_size =
        data_entry_meta->header.b_size * configs_.pmem_block_size;
    char data_buffer[pmem_data_entry_size];
    memcpy(data_buffer, pmem_data_entry, pmem_data_entry_size);
    // If checksum mismatch, the pmem data entry is corrupted or been reused by
    // another key, redo search
    auto checksum = CalculateChecksum((DataEntry *)data_buffer);
    if (checksum == data_entry_meta->header.checksum) {
      value->assign(data_buffer + key.size() +
                        data_entry_size(hash_entry.header.data_type),
                    data_entry_meta->v_size);
      break;
    }
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
  bool with_buffer = entry_size + key.size() + value.size() <= kDataBufferSize;
  if (with_buffer) {
    if (thread_data_buffer.empty()) {
      thread_data_buffer.resize(kDataBufferSize);
    }
    data_cpy_target = &thread_data_buffer[0];
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

    uint64_t prev_offset = pmem_allocator_->addr2offset_nocheck(splice.prev_data_entry);
    uint64_t next_offset = pmem_allocator_->addr2offset_nocheck(splice.next_data_entry);

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
                            ? pmem_allocator_->addr2offset_nocheck(block_base)
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
                            pmem_allocator_->addr2offset_nocheck(block_base),
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
                       sz_segment);
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

std::shared_ptr<UnorderedCollection> KVEngine::CreateUnorderedCollection(pmem::obj::string_view const collection_name)
{
  std::uint64_t ts = get_timestamp();
  uint64_t id = list_id_.fetch_add(1);
  std::string name(collection_name.data(), collection_name.size());
  std::shared_ptr<UnorderedCollection> sp_uncoll = std::make_shared<UnorderedCollection>(pmem_allocator_, hash_table_, name, id, ts);
  return sp_uncoll;
}

UnorderedCollection* KVEngine::FindUnorderedCollection(pmem::obj::string_view collection_name)
{
  HashTable::KeyHashHint hint = hash_table_->GetHint(collection_name);
  HashEntry hash_entry;
  HashEntry *entry_base = nullptr;
  Status s = hash_table_->Search(hint, collection_name, DataEntryType::DlistRecord, &hash_entry, nullptr,
                                 &entry_base, HashTable::SearchPurpose::Read);
  switch (s)
  {
  case Status::NotFound:
  {
    return nullptr;
  }
  case Status::Ok:
  {
    return hash_entry.p_unordered_collection;
  }
  default:
  {
    throw std::runtime_error{"Invalid state in FindUnorderedCollection()!"};
  }
  }
}

Status KVEngine::HGet(pmem::obj::string_view const collection_name,
                      pmem::obj::string_view const key,
                      std::string* value)
{
  UnorderedCollection* p_uncoll = FindUnorderedCollection(collection_name);
  if (!p_uncoll)
  {
    return Status::NotFound;
  }

  std::string internal_key = p_uncoll->GetInternalKey(key);

  while (true)
  {
    HashTable::KeyHashHint hint = hash_table_->GetHint(internal_key);

    HashEntry hash_entry_found;
    HashEntry hash_entry_found2;
    HashEntry *p_hash_entry_found_in_table = nullptr;
    DataEntry data_entry_found;
    Status search_result = hash_table_->Search(hint, internal_key, DataEntryType::DlistDataRecord, &hash_entry_found, nullptr,
                                  &p_hash_entry_found_in_table, HashTable::SearchPurpose::Read);
    switch (search_result)
    {
      case Status::NotFound:
      {
        return Status::NotFound;
      }
      case Status::Ok:
      {
        void* pmp = pmem_allocator_->offset2addr(hash_entry_found.offset);
        auto val = static_cast<DLDataEntry*>(pmp)->Value();
        value->assign(val.data(), val.size());
        memcpy(&hash_entry_found2, p_hash_entry_found_in_table, sizeof(HashEntry));
        bool modified = false;
        modified = modified || (hash_entry_found.offset != hash_entry_found2.offset);
        modified = modified || (hash_entry_found.header.data_type != hash_entry_found2.header.data_type);
        modified = modified || (hash_entry_found.header.key_prefix != hash_entry_found2.header.key_prefix);
        if (!modified)
        {
          return Status::Ok;
        }
        else
        {
          // data modified, search again.
          continue;
        }
      }
      default:
      {
        throw std::runtime_error{"Invalid state in SearchUnorderedCollection()!"};
      }
    }
  }
}
                    
Status KVEngine::HSetOrHDelete(pmem::obj::string_view const collection_name,
                      pmem::obj::string_view const key,
                      pmem::obj::string_view const value, DataEntryType type)
{
  Status s = MaybeInitWriteThread();
  if (s != Status::Ok) {
    return s;
  }
  assert(type == DataEntryType::DlistDataRecord || type == DataEntryType::DlistDeleteRecord && "Invalid use of HSetOrHDelete!");  
  
  HashTable::KeyHashHint hint = hash_table_->GetHint(collection_name);
  UnorderedCollection* p_uncoll;

  // Creating UnorederedCollection if not found;
  {
    std::unique_lock<SpinMutex> lock_coll{*hint.spin};  
    p_uncoll = FindUnorderedCollection(collection_name);
    if (!p_uncoll)
    {
      if (type == DataEntryType::DlistDeleteRecord)
      {
        throw std::runtime_error{"Trying to HDelete from a non-existing UnorderedCollection"};
      }
      // Only when HSet, we need to create new UnorderedCollection
      // UnorderedCollection not found, create one and insert Entry
      {
        std::lock_guard<std::mutex> lg{list_mu_};
        {
          std::shared_ptr<UnorderedCollection> sp_uncoll = CreateUnorderedCollection(collection_name);
          p_uncoll = sp_uncoll.get();
          _vec_sp_uncolls_.push_back(sp_uncoll);
        }

        HashTable::KeyHashHint hint = hash_table_->GetHint(collection_name);
        HashEntry hash_entry;
        HashEntry *entry_base = nullptr;
        Status s = hash_table_->Search(hint, collection_name, DataEntryType::DlistRecord, &hash_entry, nullptr,
                                      &entry_base, HashTable::SearchPurpose::Write);
        if (s != Status::NotFound)
        {
          throw std::runtime_error{"Fail to found a UnorderedCollection but error when creating a new one!"};
        } 
        hash_table_->Insert(hint, entry_base, DataEntryType::DlistRecord, reinterpret_cast<uint64_t>(p_uncoll), HashOffsetType::UnorderedCollection);
      }
    }
  }

  // Emplace the new DlistDataRecord
  {
    std::uint64_t ts = get_timestamp();
    auto internal_key = p_uncoll->GetInternalKey(key);
    HashTable::KeyHashHint hint = hash_table_->GetHint(internal_key);

    int n_try = 0;
    while (true)
    {
      // for (size_t i = 0; i < hint.key_hash_value % 256; i++)
      // {
      //   _mm_pause();
      // }
      
      ++n_try;

      EmplaceReturn emplace_result{};
      std::unique_lock<SpinMutex> lock{*hint.spin};

      HashEntry hash_entry;
      HashEntry *entry_base = nullptr;
      Status search_status = hash_table_->Search(hint, internal_key, DataEntryType::DlistDataRecord | DataEntryType::DlistDeleteRecord, &hash_entry, nullptr,
                                    &entry_base, HashTable::SearchPurpose::Write);

      // pmp_last_emplacement maybe invalidified by SwapEmplace!
      thread_local std::uint64_t offset_last_emplacement = 0;
      thread_local DLDataEntry* pmp_last_emplacement = nullptr;
      thread_local std::uint64_t id_last = 0;

      switch (search_status)
      {
        case Status::NotFound:
        {
          // Cached position for emplacement not available.
          if (!pmp_last_emplacement || id_last != p_uncoll->ID())
          {
            // Emplace Front or Back according to hash to reduce lock contention
            if (hint.key_hash_value % 2 == 0)
            {
              emplace_result = p_uncoll->EmplaceFront(ts, key, value, type, lock);
            }
            else
            {
              emplace_result = p_uncoll->EmplaceBack(ts, key, value, type, lock);
            }
          }
          else
          {
            emplace_result = p_uncoll->EmplaceBefore(pmp_last_emplacement, ts, key, value, type, lock);
            // emplace_result = p_uncoll->EmplaceFront(ts, key, value, type, lock);
          }
          
          if (!emplace_result.success)
          {
            // Fail to acquire other locks, retry
            continue;
          }
          // Update emplace position cache
          offset_last_emplacement = emplace_result.offset_new;
          pmp_last_emplacement = reinterpret_cast<DLDataEntry*>(pmem_allocator_->offset2addr(offset_last_emplacement));
          id_last = p_uncoll->ID();
          
          hash_table_->Insert(hint, entry_base, type, emplace_result.offset_new, HashOffsetType::UnorderedCollectionElement);

          // if (n_try > 1)
          //   GlobalLogger.Info("HSetOrDelete takes %d tries.\n", n_try);          

          return Status::Ok;
        }
        case Status::Ok:
        {
          DLDataEntry* pmp_old_record = reinterpret_cast<DLDataEntry*>(pmem_allocator_->offset2addr(hash_entry.offset));

          emplace_result = p_uncoll->SwapEmplace(pmp_old_record ,ts, key, value, type, lock);
          if (!emplace_result.success)
          {
            // Fail to acquire other locks, retry
            continue;
          }
          
          // Update emplace position cache
          // SwapEmplace may invalidify pmp_last_emplacement thus this updating is necessary.
          offset_last_emplacement = emplace_result.offset_new;
          pmp_last_emplacement = reinterpret_cast<DLDataEntry*>(pmem_allocator_->offset2addr(offset_last_emplacement));
          id_last = p_uncoll->ID();

          hash_table_->Insert(hint, entry_base, type, emplace_result.offset_new, HashOffsetType::UnorderedCollectionElement);

          // if (n_try > 1)
          //   GlobalLogger.Info("HSetOrDelete takes %d tries.\n", n_try);          

          return Status::Ok;
        }
        default:
        {
          throw std::runtime_error{"Invalid search result when trying to insert a new DlistDataRecord!"};
        }
      }
    }
  }
}

Status KVEngine::HSet(pmem::obj::string_view const collection_name,
                         pmem::obj::string_view const key,
                         pmem::obj::string_view const value)
{
  return HSetOrHDelete(collection_name, key, value, DataEntryType::DlistDataRecord);
}

Status KVEngine::HDelete(pmem::obj::string_view const collection_name,
                         pmem::obj::string_view const key)
{
  return HSetOrHDelete(collection_name, key, "", DataEntryType::DlistDeleteRecord);
}

std::shared_ptr<Iterator>
KVEngine::NewUnorderedIterator(pmem::obj::string_view const collection_name)
{
  UnorderedCollection* p_uncoll = FindUnorderedCollection(collection_name);
  assert(p_uncoll && "Trying to initialize an Iterator for a UnorderedCollection not created yet");
  return p_uncoll ? std::make_shared<UnorderedIterator>(p_uncoll->shared_from_this())
                 : nullptr;
}

Status KVEngine::RestoreDlistRecords(void* pmp_record, DataEntry data_entry_cached)
{
  switch (data_entry_cached.type)
  {
    case DataEntryType::DlistRecord:
    {
      UnorderedCollection* p_uncoll = nullptr;
      std::lock_guard<std::mutex> lg{list_mu_};
      {
        std::shared_ptr<UnorderedCollection> sp_uncoll = 
          std::make_shared<UnorderedCollection>(pmem_allocator_,hash_table_, static_cast<DLDataEntry*>(pmp_record));
        p_uncoll = sp_uncoll.get();
        _vec_sp_uncolls_.emplace_back(sp_uncoll);       
      }

      std::string collection_name = p_uncoll->Name();
      HashTable::KeyHashHint hint = hash_table_->GetHint(collection_name);
      std::unique_lock<SpinMutex>{*hint.spin};
      HashEntry hash_entry;
      HashEntry *entry_base = nullptr;
      Status s = hash_table_->Search(hint, collection_name, DataEntryType::DlistRecord, &hash_entry, nullptr,
                                    &entry_base, HashTable::SearchPurpose::Write);
      if (s != Status::NotFound)
      {
        throw std::runtime_error{"Fail to found a UnorderedCollection but error when creating a new one!"};
      } 
      hash_table_->Insert(hint, entry_base, DataEntryType::DlistRecord, 
                          reinterpret_cast<uint64_t>(p_uncoll), HashOffsetType::UnorderedCollection); 
      return Status::Ok;
    }
    case DataEntryType::DlistHeadRecord:
    {
      DLDataEntry* pmp_data_entry = static_cast<DLDataEntry*>(pmp_record);
      assert(pmp_data_entry->prev == kNullPmemOffset);
      return Status::Ok;
    }
    case DataEntryType::DlistTailRecord:
    {
      DLDataEntry* pmp_data_entry = static_cast<DLDataEntry*>(pmp_record);
      assert(pmp_data_entry->next == kNullPmemOffset);
      return Status::Ok;
    }
    case DataEntryType::DlistDataRecord:
    case DataEntryType::DlistDeleteRecord:
    {
      std::uint64_t offset_record = pmem_allocator_->addr2offset(pmp_record);
      DLDataEntry* pmp_data_entry = static_cast<DLDataEntry*>(pmp_record);
      auto internal_key = pmp_data_entry->Key();
      HashTable::KeyHashHint hint = hash_table_->GetHint(internal_key);

      int n_try = 0;
      while (true)
      {     
        ++n_try;

        std::unique_lock<SpinMutex> lg{*hint.spin};

        HashEntry hash_entry;
        HashEntry *entry_base = nullptr;

        Status search_status = hash_table_->Search(hint, internal_key, 
                                      DataEntryType::DlistDataRecord | DataEntryType::DlistDeleteRecord, 
                                      &hash_entry, nullptr,
                                      &entry_base, HashTable::SearchPurpose::Recover);
        
        
        switch (search_status)
        {
          case Status::NotFound:
          {
            hash_table_->Insert(hint, entry_base, data_entry_cached.type, offset_record, 
                                HashOffsetType::UnorderedCollectionElement);
            return Status::Ok;
          }
          case Status::Ok:
          {
            DLDataEntry* pmp_old_record = reinterpret_cast<DLDataEntry*>(pmem_allocator_->offset2addr(hash_entry.offset));
            if (pmp_old_record->timestamp < data_entry_cached.timestamp)
            {
              hash_table_->Insert(hint, entry_base, data_entry_cached.type, offset_record, 
                                  HashOffsetType::UnorderedCollectionElement);
            }
            return Status::Ok;
          }
          default:
          {
            throw std::runtime_error{"Invalid search result when trying to insert a new DlistDataRecord!"};
          }
        }
      }
    }
    default:
    {
      assert(false && "Wrong type in RestoreDlistRecords!");
      throw std::runtime_error{"Wrong type in RestoreDlistRecords!"};
    }
  }
}

} // namespace KVDK_NAMESPACE
