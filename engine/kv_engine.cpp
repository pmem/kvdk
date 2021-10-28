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
#include <math.h>
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
constexpr uint64_t kMaxWriteBatchSize = (1 << 20);
// fsdax mode align to 2MB by default.
constexpr uint64_t kPMEMMapSizeUnit = (1 << 21);
// Select a record every 10000 into restored skiplist map for multi-thread
// restoring large skiplist.
constexpr uint64_t kRestoreSkiplistStride = 10000;

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
  } else {
    GlobalLogger.Error("Init kvdk instance failed: %d\n", s);
    delete engine;
  }
  return s;
}

void KVEngine::FreeSkiplistDramNodes() {
  for (auto skiplist : skiplists_) {
    skiplist->PurgeObsoleteNodes();
  }
}

void KVEngine::BackgroundWork() {
  // To avoid free a referencing skiplist node, we do freeing in at least every
  // 10 seconds
  // TODO: Maybe free skiplist node in another bg thread?
  double interval_free_skiplist_node =
      std::max(10.0, configs_.background_work_interval);
  while (!closing_) {
    usleep(configs_.background_work_interval * 1000000);
    interval_free_skiplist_node -= configs_.background_work_interval;
    pmem_allocator_->BackgroundWork();
    if ((interval_free_skiplist_node -= configs_.background_work_interval) <=
        0) {
      FreeSkiplistDramNodes();
      interval_free_skiplist_node =
          std::max(10.0, configs_.background_work_interval);
    }
  }
}

Status KVEngine::Init(const std::string &name, const Configs &configs) {
  write_thread.id = 0;
  Status s;
  if (!configs.use_devdax_mode) {
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

  } else {
    configs_ = configs;
    db_file_ = name;

    // The devdax mode need to execute the shell scripts/init_devdax.sh,
    // then a fsdax model namespace will be created and the
    // configs_.devdax_meta_dir will be created on a xfs file system with a
    // fsdax namespace
    dir_ = format_dir_path(configs_.devdax_meta_dir);
    pending_batch_dir_ = dir_ + "pending_batch_files/";

    int res = create_dir_if_missing(pending_batch_dir_);
    if (res != 0) {
      GlobalLogger.Error("Create pending batch files dir %s error\n",
                         pending_batch_dir_.c_str());
      return Status::IOError;
    }
  }

  s = PersistOrRecoverImmutableConfigs();
  if (s != Status::Ok) {
    return s;
  }

  thread_res_.resize(configs_.max_write_threads);
  pmem_allocator_.reset(PMEMAllocator::NewPMEMAllocator(
      db_file_, configs_.pmem_file_size, configs_.pmem_segment_blocks,
      configs_.pmem_block_size, configs_.max_write_threads,
      configs_.use_devdax_mode));
  thread_manager_.reset(new (std::nothrow)
                            ThreadManager(configs_.max_write_threads));
  hash_table_.reset(HashTable::NewHashTable(
      configs_.hash_bucket_num, configs_.hash_bucket_size,
      configs_.num_buckets_per_slot, pmem_allocator_,
      configs_.max_write_threads));
  if (pmem_allocator_ == nullptr || hash_table_ == nullptr ||
      thread_manager_ == nullptr) {
    GlobalLogger.Error("Init kvdk basic components error\n");
    return Status::Abort;
  }

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

Status KVEngine::RestoreData(uint64_t thread_id) {
  write_thread.id = thread_id;

  SizedSpaceEntry segment_recovering;
  DataEntry data_entry_cached;
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

    void *recovering_pmem_record = pmem_allocator_->offset2addr_checked(
        segment_recovering.space_entry.offset);
    memcpy(&data_entry_cached, recovering_pmem_record, sizeof(DataEntry));

    // reach the of of this segment or the segment is empty
    if (data_entry_cached.header.record_size == 0) {
      fetch = true;
      continue;
    }

    segment_recovering.size -= data_entry_cached.header.record_size;
    segment_recovering.space_entry.offset +=
        data_entry_cached.header.record_size;

    switch (data_entry_cached.meta.type) {
    case RecordType::SortedDataRecord:
    case RecordType::SortedHeaderRecord:
    case RecordType::StringDataRecord:
    case RecordType::StringDeleteRecord:
    case RecordType::DlistRecord:
    case RecordType::DlistHeadRecord:
    case RecordType::DlistTailRecord:
    case RecordType::DlistDataRecord: {
      if (!ValidateRecord(recovering_pmem_record)) {
        // Checksum dismatch, mark as padding to be Freed
        // Otherwise the Restore will continue normally
        data_entry_cached.meta.type = RecordType::Padding;
      }
      break;
    }
    case RecordType::Padding:
    case RecordType::Empty: {
      data_entry_cached.meta.type = RecordType::Padding;
      break;
    }
    default: {
      // Report Corrupted Record, but still release it and continues
      GlobalLogger.Error("Corrupted Record met when recovering. It has invalid "
                         "type. Record type: %u\n",
                         data_entry_cached.meta.type);
      data_entry_cached.meta.type = RecordType::Padding;
      break;
    }
    }

    // When met records with invalid checksum
    // or the space is padding, empty or with corrupted record
    // Free the space and fetch another
    if (data_entry_cached.meta.type == RecordType::Padding) {
      RecordType type_padding = RecordType::Padding;
      // TODO: there is a redundant persist for a padding record
      // no need to manually persist here
      static_cast<DataEntry *>(recovering_pmem_record)->Destroy();
      pmem_allocator_->Free(SizedSpaceEntry(
          pmem_allocator_->addr2offset_checked(recovering_pmem_record),
          data_entry_cached.header.record_size,
          data_entry_cached.meta.timestamp));
      continue;
    }

    // Record has valid type and Checksum is correct
    // Continue to restore the Record
    cnt++;

    auto ts_recovering = data_entry_cached.meta.timestamp;
    if (ts_recovering > thread_res_[thread_id].newest_restored_ts) {
      thread_res_[thread_id].newest_restored_ts = ts_recovering;
    }

    Status s(Status::Ok);
    switch (data_entry_cached.meta.type) {
    case RecordType::SortedDataRecord: {
      s = RestoreSkiplistRecord(static_cast<DLRecord *>(recovering_pmem_record),
                                data_entry_cached);
      break;
    }
    case RecordType::SortedHeaderRecord: {
      s = RestoreSkiplistHead(static_cast<DLRecord *>(recovering_pmem_record),
                              data_entry_cached);
      break;
    }
    case RecordType::StringDataRecord:
    case RecordType::StringDeleteRecord: {
      s = RestoreStringRecord(
          static_cast<StringRecord *>(recovering_pmem_record),
          data_entry_cached);
      break;
    }
    case RecordType::DlistRecord:
    case RecordType::DlistHeadRecord:
    case RecordType::DlistTailRecord:
    case RecordType::DlistDataRecord: {
      s = RestoreDlistRecords(static_cast<DLRecord *>(recovering_pmem_record));
      break;
    }
    default: {
      GlobalLogger.Error("Invalid Record type when recovering. Trying "
                         "restoring record. Record type: %u\n",
                         data_entry_cached.meta.type);
      s = Status::Abort;
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
}

bool KVEngine::ValidateRecordAndGetValue(void *data_record,
                                         uint32_t expected_checksum,
                                         std::string *value) {
  assert(data_record);
  assert(value);
  DataEntry *entry = static_cast<DataEntry *>(data_record);
  switch (entry->meta.type) {
  case RecordType::StringDataRecord:
  case RecordType::StringDeleteRecord: {
    StringRecord *string_record = static_cast<StringRecord *>(data_record);
    if (string_record->Validate(expected_checksum)) {
      auto v = string_record->Value();
      value->assign(v.data(), v.size());
      return true;
    }
    return false;
  }
  case RecordType::SortedDataRecord:
  case RecordType::SortedHeaderRecord:
  case RecordType::DlistDataRecord:
  case RecordType::DlistRecord:
  case RecordType::DlistHeadRecord:
  case RecordType::DlistTailRecord: {
    DLRecord *dl_record = static_cast<DLRecord *>(data_record);
    if (dl_record->Validate(expected_checksum)) {
      auto v = dl_record->Value();
      value->assign(v.data(), v.size());
      return true;
    }
    return false;
  }
  default:
    kvdk_assert(false, "Unsupported type in ValidateRecordAndGetValue()!");
    std::abort();
  }
}

bool KVEngine::ValidateRecord(void *data_record) {
  assert(data_record);
  DataEntry *entry = static_cast<DataEntry *>(data_record);
  switch (entry->meta.type) {
  case RecordType::StringDataRecord:
  case RecordType::StringDeleteRecord: {
    return static_cast<StringRecord *>(data_record)->Validate();
  }
  case RecordType::SortedDataRecord:
  case RecordType::SortedHeaderRecord:
  case RecordType::DlistDataRecord:
  case RecordType::DlistRecord:
  case RecordType::DlistHeadRecord:
  case RecordType::DlistTailRecord: {
    return static_cast<DLRecord *>(data_record)->Validate();
  }
  default:
    kvdk_assert(false, "Unsupported type in ValidateRecord()!");
    return false;
  }
}

Status KVEngine::RestoreSkiplistHead(DLRecord *pmem_record, const DataEntry &) {
  assert(pmem_record->entry.meta.type == SortedHeaderRecord);
  pmem::obj::string_view pmem_key = pmem_record->Key();
  std::string key(string_view_2_string(pmem_key));
  HashEntry hash_entry;
  HashEntry *entry_ptr = nullptr;

  uint64_t id;
  memcpy_8(&id, pmem_record->Value().data());
  Skiplist *skiplist;
  {
    std::lock_guard<std::mutex> lg(list_mu_);
    skiplists_.push_back(std::make_shared<Skiplist>(
        pmem_record, key, id, pmem_allocator_, hash_table_));
    skiplist = skiplists_.back().get();
    if (configs_.opt_large_sorted_collection_restore) {
      sorted_rebuilder_.SetEntriesOffsets(
          pmem_allocator_->addr2offset(pmem_record), false, nullptr);
    }
  }
  compare_excange_if_larger(list_id_, id + 1);

  // Here key is the collection name
  auto hint = hash_table_->GetHint(key);
  std::lock_guard<SpinMutex> lg(*hint.spin);
  Status s =
      hash_table_->SearchForWrite(hint, key, SortedHeaderRecord, &entry_ptr,
                                  &hash_entry, nullptr, true /* in recovery */);
  if (s == Status::MemoryOverflow) {
    return s;
  }
  assert(s == Status::NotFound);
  hash_table_->Insert(hint, entry_ptr, SortedHeaderRecord, (uint64_t)skiplist,
                      HashOffsetType::Skiplist);
  return Status::Ok;
}

Status KVEngine::RestoreStringRecord(StringRecord *pmem_record,
                                     const DataEntry &cached_entry) {
  assert(pmem_record->entry.meta.type & StringRecordType);
  pmem::obj::string_view pmem_key = pmem_record->Key();
  std::string key(string_view_2_string(pmem_key));
  DataEntry existing_data_entry;
  HashEntry hash_entry;
  HashEntry *entry_ptr = nullptr;

  auto hint = hash_table_->GetHint(key);
  std::lock_guard<SpinMutex> lg(*hint.spin);
  Status s = hash_table_->SearchForWrite(
      hint, key, StringRecordType, &entry_ptr, &hash_entry,
      &existing_data_entry, true /* in recovery */);

  if (s == Status::MemoryOverflow) {
    return s;
  }

  bool found = s == Status::Ok;
  if (found &&
      existing_data_entry.meta.timestamp >= cached_entry.meta.timestamp) {
    pmem_allocator_->Free(SizedSpaceEntry(
        pmem_allocator_->addr2offset(pmem_record),
        cached_entry.header.record_size, cached_entry.meta.timestamp));
    return Status::Ok;
  }

  uint64_t new_hash_offset = pmem_allocator_->addr2offset(pmem_record);
  bool free_space = entry_ptr->header.status == HashEntryStatus::Updating;
  hash_table_->Insert(hint, entry_ptr, cached_entry.meta.type, new_hash_offset,
                      HashOffsetType::StringRecord);
  if (free_space) {
    pmem_allocator_->Free(SizedSpaceEntry(
        hash_entry.offset, existing_data_entry.header.record_size,
        existing_data_entry.meta.timestamp));
  }

  // If a delete record is the only existing record of a key, then we
  // can reuse the hash entry and free the record
  entry_ptr->header.status =
      (!found && (cached_entry.meta.type & DeleteRecordType)
           ? HashEntryStatus::CleanReusable
           : HashEntryStatus::Normal);

  return Status::Ok;
}

bool KVEngine::CheckAndRepairDLRecord(DLRecord *record) {
  uint64_t offset = pmem_allocator_->addr2offset(record);
  DLRecord *prev = pmem_allocator_->offset2addr<DLRecord>(record->prev);
  DLRecord *next = pmem_allocator_->offset2addr<DLRecord>(record->next);
  if (prev->next != offset) {
    return false;
  }
  // Repair un-finished write
  if (next && next->prev != offset) {
    next->prev = offset;
    pmem_persist(&next->prev, 8);
  }
  return true;
}

Status KVEngine::RestoreSkiplistRecord(DLRecord *pmem_record,
                                       const DataEntry &cached_data_entry) {
  if (!CheckAndRepairDLRecord(pmem_record)) {
    pmem_allocator_->Free(
        SizedSpaceEntry(pmem_allocator_->addr2offset(pmem_record),
                        cached_data_entry.header.record_size,
                        cached_data_entry.meta.timestamp));
    return Status::Ok;
  }

  assert(pmem_record->entry.meta.type & SortedDataRecord);
  pmem::obj::string_view pmem_key = pmem_record->Key();
  std::string key(string_view_2_string(pmem_key));
  DataEntry existing_data_entry;
  HashEntry hash_entry;
  HashEntry *entry_ptr = nullptr;

  auto hint = hash_table_->GetHint(key);
  std::lock_guard<SpinMutex> lg(*hint.spin);
  Status s = hash_table_->SearchForWrite(
      hint, key, SortedDataRecord, &entry_ptr, &hash_entry,
      &existing_data_entry, true /* in recovery */);

  if (s == Status::MemoryOverflow) {
    return s;
  }

  bool found = s == Status::Ok;
  if (found &&
      existing_data_entry.meta.timestamp >= cached_data_entry.meta.timestamp) {
    pmem_allocator_->Free(
        SizedSpaceEntry(pmem_allocator_->addr2offset(pmem_record),
                        cached_data_entry.header.record_size,
                        cached_data_entry.meta.timestamp));
    return Status::Ok;
  }

  uint64_t new_hash_offset;
  uint64_t old_data_offset;
  SkiplistNode *dram_node;
  if (!found) {
    auto height = Skiplist::RandomHeight();
    if (height > 0) {
      dram_node =
          SkiplistNode::NewNode(Skiplist::UserKey(key), pmem_record, height);
      if (dram_node == nullptr) {
        GlobalLogger.Error("Memory overflow in recovery\n");
        return Status::MemoryOverflow;
      }
      new_hash_offset = (uint64_t)dram_node;
      if (configs_.opt_large_sorted_collection_restore &&
          thread_res_[write_thread.id]
                      .visited_skiplist_ids[dram_node->GetSkipListId()]++ %
                  kRestoreSkiplistStride ==
              0) {
        std::lock_guard<std::mutex> lg(list_mu_);
        sorted_rebuilder_.SetEntriesOffsets(
            pmem_allocator_->addr2offset(pmem_record), false, nullptr);
      }
    } else {
      new_hash_offset = pmem_allocator_->addr2offset(pmem_record);
    }

    // Hash entry won't be reused during data recovering so we don't
    // neet to check status here
    hash_table_->Insert(
        hint, entry_ptr, cached_data_entry.meta.type, new_hash_offset,
        height > 0 ? HashOffsetType::SkiplistNode : HashOffsetType::DLRecord);
  } else {
    if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
      dram_node = (SkiplistNode *)hash_entry.offset;
      old_data_offset = pmem_allocator_->addr2offset(dram_node->record);
      dram_node->record = pmem_record;
      entry_ptr->header.data_type = cached_data_entry.meta.type;
    } else {
      assert(hash_entry.header.offset_type == HashOffsetType::DLRecord);
      old_data_offset = hash_entry.offset;
      hash_table_->Insert(hint, entry_ptr, cached_data_entry.meta.type,
                          pmem_allocator_->addr2offset(pmem_record),
                          HashOffsetType::DLRecord);
    }
    pmem_allocator_->Free(
        SizedSpaceEntry(old_data_offset, existing_data_entry.header.record_size,
                        existing_data_entry.meta.timestamp));
  }
  // If a delete record is the only existing record of a key, then we
  // can reuse the hash entry and free the record
  entry_ptr->header.status =
      (!found && (cached_data_entry.meta.type & DeleteRecordType)
           ? HashEntryStatus::CleanReusable
           : HashEntryStatus::Normal);

  return Status::Ok;
}

Status
KVEngine::SearchOrInitPersistentList(const pmem::obj::string_view &collection,
                                     PersistentList **list, bool init,
                                     uint16_t header_type) {
  auto hint = hash_table_->GetHint(collection);
  HashEntry hash_entry;
  HashEntry *entry_ptr = nullptr;
  Status s = hash_table_->SearchForRead(hint, collection, header_type,
                                        &entry_ptr, &hash_entry, nullptr);
  if (s == Status::NotFound) {
    if (init) {
      DataEntry existing_data_entry;
      std::lock_guard<SpinMutex> lg(*hint.spin);
      // Since we do the first search without lock, we need to check again
      entry_ptr = nullptr;
      s = hash_table_->SearchForWrite(hint, collection, header_type, &entry_ptr,
                                      &hash_entry, &existing_data_entry);
      if (s == Status::MemoryOverflow) {
        return s;
      }
      if (s == Status::NotFound) {
        uint32_t request_size =
            sizeof(DLRecord) + collection.size() + 8 /* id */;
        SizedSpaceEntry sized_space_entry =
            pmem_allocator_->Allocate(request_size);
        if (sized_space_entry.size == 0) {
          return Status::PmemOverflow;
        }
        uint64_t id = list_id_.fetch_add(1);
        // PMem level of skiplist is circular, so the next and prev pointers of
        // header point to itself
        DLRecord *pmem_record = DLRecord::PersistDLRecord(
            pmem_allocator_->offset2addr(sized_space_entry.space_entry.offset),
            sized_space_entry.size, get_timestamp(), (RecordType)header_type,
            sized_space_entry.space_entry.offset,
            sized_space_entry.space_entry.offset, collection,
            pmem::obj::string_view((char *)&id, 8));

        {
          std::lock_guard<std::mutex> lg(list_mu_);
          switch (header_type) {
          case SortedHeaderRecord:
            skiplists_.push_back(std::make_shared<Skiplist>(
                pmem_record, std::string(collection.data(), collection.size()),
                id, pmem_allocator_, hash_table_));
            *list = skiplists_.back().get();
            break;
          default:
            return Status::NotSupported;
          }
        }
        auto entry_base_status = entry_ptr->header.status;
        hash_table_->Insert(hint, entry_ptr, header_type, (uint64_t)(*list),
                            HashOffsetType::Skiplist);
        if (entry_base_status == HashEntryStatus::Updating) {
          pmem_allocator_->Free(SizedSpaceEntry(
              hash_entry.offset, existing_data_entry.header.record_size,
              existing_data_entry.meta.timestamp));
        } else if (entry_base_status == HashEntryStatus::DirtyReusable) {
          pmem_allocator_->DelayFree(SizedSpaceEntry(
              hash_entry.offset, existing_data_entry.header.record_size,
              existing_data_entry.meta.timestamp));
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
  uint64_t len =
      kPMEMMapSizeUnit *
      (size_t)ceil(1.0 * sizeof(ImmutableConfigs) / kPMEMMapSizeUnit);
  ImmutableConfigs *configs = (ImmutableConfigs *)pmem_map_file(
      config_file_name().c_str(), len, PMEM_FILE_CREATE, 0666, &mapped_len,
      &is_pmem);
  if (configs == nullptr || !is_pmem || mapped_len != len) {
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
  persisted_pending_file_size =
      kPMEMMapSizeUnit *
      (size_t)ceil(1.0 * persisted_pending_file_size / kPMEMMapSizeUnit);
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
                  pmem_allocator_->offset2addr<DataEntry>(invalid_offsets[i]);
              if (data_entry->meta.timestamp == pending_batch->timestamp) {
                data_entry->meta.type = Padding;
                pmem_persist(&data_entry->meta.type, 8);
              }
            }
            pending_batch->PersistStage(PendingBatch::Stage::Finish);
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
  GlobalLogger.Info("RestorePendingBatch done: iterated %lu records\n",
                    restored_.load());

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

  GlobalLogger.Info("RestoreData done: iterated %lu records\n",
                    restored_.load());

  // restore skiplist by two optimization strategy
  s = sorted_rebuilder_.Rebuild(this);
  if (s != Status::Ok) {
    return s;
  }

  GlobalLogger.Info("Rebuild skiplist done\n");

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
  DataEntry data_entry;
  while (1) {
    HashEntry hash_entry;
    HashEntry *entry_ptr = nullptr;
    bool is_found = hash_table_->SearchForRead(
                        hash_table_->GetHint(key), key, type_mask, &entry_ptr,
                        &hash_entry, &data_entry) == Status::Ok;
    if (!is_found || (hash_entry.header.data_type & DeleteRecordType)) {
      return Status::NotFound;
    }

    void *pmem_record = nullptr;
    if (hash_entry.header.data_type & (StringDataRecord | DlistDataRecord)) {
      pmem_record = pmem_allocator_->offset2addr_checked(hash_entry.offset);
    } else if (hash_entry.header.data_type == SortedDataRecord) {
      if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
        SkiplistNode *dram_node = (SkiplistNode *)hash_entry.offset;
        pmem_record = (char *)dram_node->record;
      } else {
        assert(hash_entry.header.offset_type == HashOffsetType::DLRecord);
        pmem_record = pmem_allocator_->offset2addr_checked(hash_entry.offset);
      }
    } else {
      return Status::NotSupported;
    }

    // Copy PMem data record to dram buffer
    auto record_size = data_entry.header.record_size;
    // Region of data_entry.header.record_size may be corrupted by a write
    // operation if the original reading space entry is merged with the adjacent
    // one by pmem allocator
    if (record_size > configs_.pmem_segment_blocks * configs_.pmem_block_size) {
      continue;
    }
    char data_buffer[record_size];
    memcpy(data_buffer, pmem_record, record_size);
    // If the pmem data record is corrupted or been reused by
    // another key, redo search
    if (ValidateRecordAndGetValue(data_buffer, data_entry.header.checksum,
                                  value)) {
      break;
    }
  }

  return Status::Ok;
}

Status KVEngine::Get(const pmem::obj::string_view key, std::string *value) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  return HashGetImpl(key, value, StringRecordType);
}

Status KVEngine::Delete(const pmem::obj::string_view key) {
  Status s = MaybeInitWriteThread();

  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  return StringDeleteImpl(key);
}

Status KVEngine::SDeleteImpl(Skiplist *skiplist,
                             const pmem::obj::string_view &user_key) {
  uint64_t id = skiplist->id();
  std::string collection_key(skiplist->InternalKey(user_key));
  if (!CheckKeySize(collection_key)) {
    return Status::InvalidDataSize;
  }

  HashEntry hash_entry;
  DataEntry data_entry;
  SkiplistNode *dram_node = nullptr;
  uint64_t existing_record_offset;
  DLRecord *existing_record = nullptr;

  while (1) {
    HashEntry *entry_ptr = nullptr;
    auto hint = hash_table_->GetHint(collection_key);
    std::lock_guard<SpinMutex> lg(*hint.spin);
    Status s =
        hash_table_->SearchForRead(hint, collection_key, SortedDataRecord,
                                   &entry_ptr, &hash_entry, &data_entry);
    switch (s) {
    case Status::Ok:
      break;
    case Status::NotFound:
      return Status::Ok;
    default:
      return s;
    }

    if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
      dram_node = (SkiplistNode *)(hash_entry.offset);
      existing_record = dram_node->record;
      existing_record_offset = pmem_allocator_->addr2offset(existing_record);
    } else {
      assert(hash_entry.header.offset_type == HashOffsetType::DLRecord);
      existing_record =
          pmem_allocator_->offset2addr<DLRecord>(hash_entry.offset);
      existing_record_offset = hash_entry.offset;
    }

    std::unique_lock<SpinMutex> prev_record_lock;
    thread_local Splice splice(nullptr);
    splice.seeking_list = skiplist;
    if (!skiplist->FindUpdatePos(&splice, user_key, hint.spin, existing_record,
                                 &prev_record_lock)) {
      continue;
    }

    skiplist->DeleteRecord(existing_record, &splice, dram_node);

    entry_ptr->Clear();
    pmem_allocator_->Free(SizedSpaceEntry(existing_record_offset,
                                          data_entry.header.record_size,
                                          data_entry.meta.timestamp));
    break;
  }
  return Status::Ok;
}

Status KVEngine::SSetImpl(Skiplist *skiplist,
                          const pmem::obj::string_view &user_key,
                          const pmem::obj::string_view &value) {
  uint64_t id = skiplist->id();
  std::string collection_key(PersistentList::ListKey(user_key, id));
  if (!CheckKeySize(collection_key) || !CheckValueSize(value)) {
    return Status::InvalidDataSize;
  }

  auto request_size = value.size() + collection_key.size() + sizeof(DLRecord);
  SizedSpaceEntry sized_space_entry = pmem_allocator_->Allocate(request_size);
  if (sized_space_entry.size == 0) {
    return Status::PmemOverflow;
  }

  while (1) {
    SkiplistNode *dram_node = nullptr;
    HashEntry *entry_ptr = nullptr;
    DLRecord *existing_record = nullptr;
    uint64_t existing_record_offset;
    HashEntry hash_entry;
    DataEntry data_entry;
    auto hint = hash_table_->GetHint(collection_key);
    std::lock_guard<SpinMutex> lg(*hint.spin);
    Status s =
        hash_table_->SearchForWrite(hint, collection_key, SortedDataRecord,
                                    &entry_ptr, &hash_entry, &data_entry);
    if (s == Status::MemoryOverflow) {
      return s;
    }
    bool found = s == Status::Ok;

    if (found) {
      if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
        dram_node = (SkiplistNode *)(hash_entry.offset);
        existing_record = dram_node->record;
        existing_record_offset = pmem_allocator_->addr2offset(existing_record);
      } else {
        assert(hash_entry.header.offset_type == HashOffsetType::DLRecord);
        existing_record_offset = hash_entry.offset;
        existing_record =
            pmem_allocator_->offset2addr<DLRecord>(existing_record_offset);
      }
    }

    uint64_t new_ts = get_timestamp();
    assert(!found || new_ts > data_entry.meta.timestamp);

    std::unique_lock<SpinMutex> prev_record_lock;
    thread_local Splice splice(nullptr);
    splice.seeking_list = skiplist;
    if (found ? (!skiplist->FindUpdatePos(&splice, user_key, hint.spin,
                                          existing_record, &prev_record_lock))
              : (!skiplist->FindInsertPos(&splice, user_key, hint.spin,
                                          &prev_record_lock))) {
      continue;
    }

    uint64_t prev_offset =
        pmem_allocator_->addr2offset(splice.prev_pmem_record);
    uint64_t next_offset =
        pmem_allocator_->addr2offset(splice.next_pmem_record);

    DLRecord *pmem_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr(sized_space_entry.space_entry.offset),
        sized_space_entry.size, new_ts, SortedDataRecord, prev_offset,
        next_offset, collection_key, value);

    dram_node = skiplist->InsertRecord(&splice, pmem_record, user_key,
                                       dram_node, found);

    if (!found) {
      uint64_t offset = (dram_node == nullptr)
                            ? sized_space_entry.space_entry.offset
                            : (uint64_t)dram_node;
      auto entry_base_status = entry_ptr->header.status;
      hash_table_->Insert(hint, entry_ptr, SortedDataRecord, offset,
                          dram_node ? HashOffsetType::SkiplistNode
                                    : HashOffsetType::DLRecord);
      if (entry_base_status == HashEntryStatus::Updating) {
        pmem_allocator_->Free(SizedSpaceEntry(hash_entry.offset,
                                              data_entry.header.record_size,
                                              data_entry.meta.timestamp));
      } else if (entry_base_status == HashEntryStatus::DirtyReusable) {
        pmem_allocator_->DelayFree(
            SizedSpaceEntry(hash_entry.offset, data_entry.header.record_size,
                            data_entry.meta.timestamp));
      }
    } else {
      // update a height 0 node
      if (dram_node == nullptr) {
        hash_table_->Insert(hint, entry_ptr, SortedDataRecord,
                            sized_space_entry.space_entry.offset,
                            HashOffsetType::DLRecord);
      }
      pmem_allocator_->Free(SizedSpaceEntry(existing_record_offset,
                                            data_entry.header.record_size,
                                            data_entry.meta.timestamp));
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

  return SSetImpl(skiplist, user_key, value);
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

  auto segment_size = configs.pmem_block_size * configs.pmem_segment_blocks;
  if (configs.pmem_file_size % segment_size != 0) {
    GlobalLogger.Error("pmem file size should align to segment "
                       "size(pmem_segment_blocks*pmem_block_size) (%d bytes)\n",
                       segment_size);
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

  return SDeleteImpl(skiplist, user_key);
}

Status KVEngine::MaybeInitPendingBatchFile() {
  if (thread_res_[write_thread.id].persisted_pending_batch == nullptr) {
    int is_pmem;
    size_t mapped_len;
    uint64_t persisted_pending_file_size =
        kMaxWriteBatchSize * 8 + sizeof(PendingBatch);
    persisted_pending_file_size =
        kPMEMMapSizeUnit *
        (size_t)ceil(1.0 * persisted_pending_file_size / kPMEMMapSizeUnit);

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

  // Allocate space for batch
  std::vector<BatchWriteHint> batch_hints(write_batch.Size());
  std::vector<uint64_t> space_entry_offsets(write_batch.Size());
  for (size_t i = 0; i < write_batch.Size(); i++) {
    auto &kv = write_batch.kvs[i];
    uint32_t requested_size =
        kv.key.size() + kv.value.size() + sizeof(StringRecord);
    batch_hints[i].allocated_space = pmem_allocator_->Allocate(requested_size);
    // No enough space for batch write
    if (batch_hints[i].allocated_space.size == 0) {
      for (size_t j = 0; j < i; j++) {
        pmem_allocator_->Free(batch_hints[j].allocated_space);
      }
      return s;
    }

    batch_hints[i].timestamp = ts;
    space_entry_offsets[i] = batch_hints[i].allocated_space.space_entry.offset;
  }

  // Persist batch write status as processing
  PendingBatch pending_batch(PendingBatch::Stage::Processing,
                             write_batch.Size(), ts);
  pending_batch.PersistProcessing(
      thread_res_[write_thread.id].persisted_pending_batch,
      space_entry_offsets);

  // Do batch writes
  for (size_t i = 0; i < write_batch.Size(); i++) {
    if (write_batch.kvs[i].type == StringDataRecord ||
        write_batch.kvs[i].type == StringDeleteRecord) {
      s = StringBatchWriteImpl(write_batch.kvs[i], batch_hints[i]);
    } else {
      return Status::NotSupported;
    }

    // Something wrong
    // TODO: roll back finished writes (hard to roll back hash table)
    if (s != Status::Ok) {
      assert(s == Status::MemoryOverflow);
      std::abort();
    }
  }

  pending_batch.PersistStage(PendingBatch::Stage::Finish);

  // Free updated kvs / unused space
  for (size_t i = 0; i < write_batch.Size(); i++) {
    if (batch_hints[i].free_after_finish.size > 0) {
      if (batch_hints[i].delay_free) {
        pmem_allocator_->DelayFree(batch_hints[i].free_after_finish);
      } else {
        pmem_allocator_->Free(batch_hints[i].free_after_finish);
      }
    }
  }
  return s;
}

Status KVEngine::StringBatchWriteImpl(const WriteBatch::KV &kv,
                                      BatchWriteHint &batch_hint) {
  DataEntry data_entry;
  HashEntry hash_entry;
  HashEntry *entry_ptr = nullptr;

  {
    auto hash_hint = hash_table_->GetHint(kv.key);
    std::lock_guard<SpinMutex> lg(*hash_hint.spin);
    Status s =
        hash_table_->SearchForWrite(hash_hint, kv.key, StringRecordType,
                                    &entry_ptr, &hash_entry, &data_entry);
    if (s == Status::MemoryOverflow) {
      return s;
    }
    bool found = s == Status::Ok;

    // Deleting kv is not existing
    if (kv.type == StringDeleteRecord) {
      if (!found || data_entry.meta.type == StringDeleteRecord) {
        batch_hint.free_after_finish = batch_hint.allocated_space;
        batch_hint.delay_free = false;
        return Status::Ok;
      }
    }

    // A newer version has been set
    if (found && batch_hint.timestamp < data_entry.meta.timestamp) {
      batch_hint.free_after_finish = batch_hint.allocated_space;
      batch_hint.delay_free = false;
      return Status::Ok;
    }

    void *block_base = pmem_allocator_->offset2addr(
        batch_hint.allocated_space.space_entry.offset);

    // We use if here to avoid compilation warning
    if (kv.type == StringDataRecord || kv.type == StringDeleteRecord) {
      StringRecord::PersistStringRecord(
          block_base, batch_hint.allocated_space.size, batch_hint.timestamp,
          static_cast<RecordType>(kv.type), kv.key, kv.value);
    } else {
      // Never reach
      assert(false);
      return Status::NotSupported;
    }

    auto entry_base_status = entry_ptr->header.status;
    hash_table_->Insert(hash_hint, entry_ptr, kv.type,
                        batch_hint.allocated_space.space_entry.offset,
                        HashOffsetType::StringRecord);

    if (entry_base_status == HashEntryStatus::Updating) {
      batch_hint.free_after_finish =
          SizedSpaceEntry(hash_entry.offset, data_entry.header.record_size,
                          data_entry.meta.timestamp);
      batch_hint.delay_free = false;
    } else if (entry_base_status == HashEntryStatus::DirtyReusable) {
      batch_hint.free_after_finish =
          SizedSpaceEntry(hash_entry.offset, data_entry.header.record_size,
                          data_entry.meta.timestamp);
      batch_hint.delay_free = true;
    }
  }

  return Status::Ok;
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
  return HashGetImpl(skiplist_key, value, SortedDataRecord);
}

Status KVEngine::StringDeleteImpl(const pmem::obj::string_view &key) {
  DataEntry data_entry;
  HashEntry hash_entry;
  HashEntry *entry_ptr = nullptr;

  uint32_t requested_size = key.size() + sizeof(StringRecord);
  SizedSpaceEntry sized_space_entry;

  {
    auto hint = hash_table_->GetHint(key);
    std::lock_guard<SpinMutex> lg(*hint.spin);
    Status s = hash_table_->SearchForWrite(
        hint, key, StringDeleteRecord | StringDataRecord, &entry_ptr,
        &hash_entry, &data_entry);

    switch (s) {
    case Status::Ok:
      break;
    case Status::NotFound:
      return Status::Ok;
    default:
      return s;
    }

    if (data_entry.meta.type == StringDeleteRecord) {
      return Status::Ok;
    }

    // Deleted key may not existed, so we allocate space for delete record
    // until we found the key
    sized_space_entry = pmem_allocator_->Allocate(requested_size);
    if (sized_space_entry.size == 0) {
      return Status::PmemOverflow;
    }

    uint64_t new_ts = get_timestamp();
    assert(new_ts > data_entry.meta.timestamp);

    void *block_base =
        pmem_allocator_->offset2addr(sized_space_entry.space_entry.offset);

    StringRecord::PersistStringRecord(block_base, sized_space_entry.size,
                                      new_ts, StringDeleteRecord, key, "");

    assert(entry_ptr->header.status == HashEntryStatus::Updating);
    hash_table_->Insert(hint, entry_ptr, StringDeleteRecord,
                        sized_space_entry.space_entry.offset,
                        HashOffsetType::StringRecord);
    pmem_allocator_->Free(SizedSpaceEntry(hash_entry.offset,
                                          data_entry.header.record_size,
                                          data_entry.meta.timestamp));
  }
  return Status::Ok;
}

Status KVEngine::StringSetImpl(const pmem::obj::string_view &key,
                               const pmem::obj::string_view &value) {
  DataEntry data_entry;
  HashEntry hash_entry;
  HashEntry *entry_ptr = nullptr;
  uint32_t v_size = value.size();

  uint32_t requested_size = v_size + key.size() + sizeof(StringRecord);

  // Space is already allocated for batch writes
  SizedSpaceEntry sized_space_entry = pmem_allocator_->Allocate(requested_size);
  if (sized_space_entry.size == 0) {
    return Status::PmemOverflow;
  }

  {
    auto hint = hash_table_->GetHint(key);
    std::lock_guard<SpinMutex> lg(*hint.spin);
    Status s = hash_table_->SearchForWrite(
        hint, key, StringDeleteRecord | StringDataRecord, &entry_ptr,
        &hash_entry, &data_entry);
    if (s == Status::MemoryOverflow) {
      return s;
    }
    bool found = s == Status::Ok;

    void *block_base =
        pmem_allocator_->offset2addr(sized_space_entry.space_entry.offset);

    uint64_t new_ts = get_timestamp();
    assert(!found || new_ts > data_entry.meta.timestamp);

    StringRecord::PersistStringRecord(block_base, sized_space_entry.size,
                                      new_ts, StringDataRecord, key, value);

    auto entry_base_status = entry_ptr->header.status;
    hash_table_->Insert(hint, entry_ptr, StringDataRecord,
                        sized_space_entry.space_entry.offset,
                        HashOffsetType::StringRecord);
    if (entry_base_status == HashEntryStatus::Updating) {
      pmem_allocator_->Free(SizedSpaceEntry(hash_entry.offset,
                                            data_entry.header.record_size,
                                            data_entry.meta.timestamp));
    } else if (entry_base_status == HashEntryStatus::DirtyReusable) {
      pmem_allocator_->DelayFree(SizedSpaceEntry(hash_entry.offset,
                                                 data_entry.header.record_size,
                                                 data_entry.meta.timestamp));
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
  return StringSetImpl(key, value);
}

} // namespace KVDK_NAMESPACE

namespace KVDK_NAMESPACE {
std::shared_ptr<UnorderedCollection> KVEngine::createUnorderedCollection(
    pmem::obj::string_view const collection_name) {
  std::uint64_t ts = get_timestamp();
  uint64_t id = list_id_.fetch_add(1);
  std::string name(collection_name.data(), collection_name.size());
  std::shared_ptr<UnorderedCollection> sp_uncoll =
      std::make_shared<UnorderedCollection>(pmem_allocator_, hash_table_, name,
                                            id, ts);
  return sp_uncoll;
}

UnorderedCollection *
KVEngine::findUnorderedCollection(pmem::obj::string_view collection_name) {
  HashTable::KeyHashHint hint = hash_table_->GetHint(collection_name);
  HashEntry hash_entry;
  HashEntry *entry_ptr = nullptr;
  Status s =
      hash_table_->SearchForRead(hint, collection_name, RecordType::DlistRecord,
                                 &entry_ptr, &hash_entry, nullptr);
  switch (s) {
  case Status::NotFound: {
    return nullptr;
  }
  case Status::Ok: {
    return hash_entry.p_unordered_collection;
  }
  default: {
    kvdk_assert(false, "Invalid state in findUnorderedCollection()!");
    return nullptr;
  }
  }
}

Status KVEngine::HGet(pmem::obj::string_view const collection_name,
                      pmem::obj::string_view const key, std::string *value) {
  UnorderedCollection *p_uncoll = findUnorderedCollection(collection_name);
  if (!p_uncoll) {
    return Status::NotFound;
  }

  std::string internal_key = p_uncoll->GetInternalKey(key);
  return HashGetImpl(internal_key, value, RecordType::DlistDataRecord);
}

Status KVEngine::HSet(pmem::obj::string_view const collection_name,
                      pmem::obj::string_view const key,
                      pmem::obj::string_view const value) {
  Status s = MaybeInitWriteThread();
  if (s != Status::Ok) {
    return s;
  }

  UnorderedCollection *p_collection;

  // Find UnorederedCollection, create if none exists
  {
    p_collection = findUnorderedCollection(collection_name);
    if (!p_collection) {
      HashTable::KeyHashHint hint_collection =
          hash_table_->GetHint(collection_name);
      std::unique_lock<SpinMutex> lock_collection{*hint_collection.spin};
      {
        // Lock and find again in case other threads have created the
        // UnorderedCollection
        p_collection = findUnorderedCollection(collection_name);
        if (p_collection) {
          // Some thread already created the collection
          // Do nothing
        } else {
          auto sp_collection = createUnorderedCollection(collection_name);
          p_collection = sp_collection.get();
          {
            std::lock_guard<std::mutex> lg{list_mu_};
            vec_sp_unordered_collections_.push_back(sp_collection);
          }

          HashEntry hash_entry_collection;
          HashEntry *p_hash_entry_collection = nullptr;
          Status s = hash_table_->SearchForWrite(
              hint_collection, collection_name, RecordType::DlistRecord,
              &p_hash_entry_collection, &hash_entry_collection, nullptr);
          kvdk_assert(s == Status::NotFound, "Logically impossible!");
          hash_table_->Insert(hint_collection, p_hash_entry_collection,
                              RecordType::DlistRecord,
                              reinterpret_cast<uint64_t>(p_collection),
                              HashOffsetType::UnorderedCollection);
        }
      }
    }
  }

  // Emplace the new DlistDataRecord
  {
    auto internal_key = p_collection->GetInternalKey(key);
    HashTable::KeyHashHint hint_record = hash_table_->GetHint(internal_key);

    int n_try = 0;
    while (true) {
      ++n_try;

      std::unique_lock<SpinMutex> lock_record{*hint_record.spin};

      std::uint64_t ts = get_timestamp();

      HashEntry hash_entry_record;
      HashEntry *p_hash_entry_record = nullptr;
      Status search_result = hash_table_->SearchForWrite(
          hint_record, internal_key, RecordType::DlistDataRecord,
          &p_hash_entry_record, &hash_entry_record, nullptr);

      // pmp_last_emplacement maybe invalidified by Replace!
      thread_local DLRecord *pmp_last_emplacement = nullptr;
      thread_local std::uint64_t id_last = 0;

      EmplaceReturn emplace_result{};
      switch (search_result) {
      case Status::NotFound: {
        // Cached position for emplacement not available.
        if (!pmp_last_emplacement || id_last != p_collection->ID()) {
          // Emplace Front or Back according to hash to reduce lock contention
          if (hint_record.key_hash_value % 2 == 0)
            emplace_result = p_collection->EmplaceFront(
                ts, key, value, RecordType::DlistDataRecord, lock_record);
          else
            emplace_result = p_collection->EmplaceBack(
                ts, key, value, RecordType::DlistDataRecord, lock_record);
        } else {
          // Emplace at cached position
          emplace_result = p_collection->EmplaceBefore(
              pmp_last_emplacement, ts, key, value, RecordType::DlistDataRecord,
              lock_record);
        }
        break;
      }
      case Status::Ok: {
        DLRecord *pmp_old_record =
            pmem_allocator_->offset2addr_checked<DLRecord>(
                hash_entry_record.offset);

        emplace_result =
            p_collection->Replace(pmp_old_record, ts, key, value,
                                  RecordType::DlistDataRecord, lock_record);
        if (emplace_result.success) {
          DLRecord *pmp_new_record =
              pmem_allocator_->offset2addr_checked<DLRecord>(
                  emplace_result.offset_new);
          DLRecord *pmp_old_record2 =
              pmem_allocator_->offset2addr_checked<DLRecord>(
                  emplace_result.offset_old);
          kvdk_assert(pmp_old_record2 == pmp_old_record,
                      "Updated a record, but HashEntry in HashTable is "
                      "inconsistent with data on PMem!")
              kvdk_assert(
                  pmp_old_record->entry.meta.timestamp <
                      pmp_new_record->entry.meta.timestamp,
                  "Old record has newer timestamp than newly inserted record!");
          p_collection->Deallocate(pmp_old_record);
        }
        break;
      }
      default: {
        kvdk_assert(false, "Invalid search result when trying to insert "
                           "a new DlistDataRecord!");
      }
      }

      if (!emplace_result.success) {
        // Fail to acquire other locks, or the linkage is broken, retry
        if (n_try > 2) {
          // Too many fails at emplacing in cached position,
          // the position may have been invalidated.
          // Remove cache so that thread will EmplaceFront or EmplaceBack
          pmp_last_emplacement = nullptr;
          id_last = 0;
        }
        // Retry
        continue;
      } else {
        // Successfully emplaced the new record
        // Update emplace position cache
        pmp_last_emplacement = pmem_allocator_->offset2addr_checked<DLRecord>(
            emplace_result.offset_new);
        id_last = p_collection->ID();

        hash_table_->Insert(hint_record, p_hash_entry_record,
                            RecordType::DlistDataRecord,
                            emplace_result.offset_new,
                            HashOffsetType::UnorderedCollectionElement);
        return Status::Ok;
      }
    }
  }
}

Status KVEngine::HDelete(pmem::obj::string_view const collection_name,
                         pmem::obj::string_view const key) {
  Status s = MaybeInitWriteThread();
  if (s != Status::Ok) {
    return s;
  }
  UnorderedCollection *p_collection = findUnorderedCollection(collection_name);
  if (!p_collection)
    return Status::Ok;

  // Erase DlistDataRecord if found one.
  {
    auto internal_key = p_collection->GetInternalKey(key);
    HashTable::KeyHashHint hint_record = hash_table_->GetHint(internal_key);

    int n_try = 0;
    while (true) {
      ++n_try;
      std::unique_lock<SpinMutex> lock_record{*hint_record.spin};

      HashEntry hash_entry;
      HashEntry *p_hash_entry = nullptr;
      Status search_result = hash_table_->SearchForWrite(
          hint_record, internal_key, RecordType::DlistDataRecord, &p_hash_entry,
          &hash_entry, nullptr);

      EmplaceReturn erase_result{};
      switch (search_result) {
      case Status::NotFound: {
        return Status::Ok;
      }
      case Status::Ok: {
        DLRecord *pmp_old_record =
            pmem_allocator_->offset2addr_checked<DLRecord>(hash_entry.offset);

        erase_result = p_collection->Erase(pmp_old_record, lock_record);
        if (erase_result.success) {
          DLRecord *pmp_old_record2 =
              pmem_allocator_->offset2addr_checked<DLRecord>(
                  erase_result.offset_old);
          p_hash_entry->Clear();
          p_collection->Deallocate(pmp_old_record);
          return Status::Ok;
        } else {
          // !erase_result.success
          continue;
        }
        break;
      }
      default: {
        kvdk_assert(false, "Invalid search result when trying to insert "
                           "a new DlistDataRecord!");
      }
      }
    }
  }
}

std::shared_ptr<Iterator>
KVEngine::NewUnorderedIterator(pmem::obj::string_view const collection_name) {
  UnorderedCollection *p_collection = findUnorderedCollection(collection_name);
  return p_collection ? std::make_shared<UnorderedIterator>(
                            p_collection->shared_from_this())
                      : nullptr;
}

Status KVEngine::RestoreDlistRecords(DLRecord *pmp_record) {
  switch (pmp_record->entry.meta.type) {
  case RecordType::DlistRecord: {
    UnorderedCollection *p_collection = nullptr;
    std::lock_guard<std::mutex> lg{list_mu_};
    {
      std::shared_ptr<UnorderedCollection> sp_collection =
          std::make_shared<UnorderedCollection>(pmem_allocator_, hash_table_,
                                                pmp_record);
      p_collection = sp_collection.get();
      vec_sp_unordered_collections_.emplace_back(sp_collection);
    }

    std::string collection_name = p_collection->Name();
    HashTable::KeyHashHint hint_collection =
        hash_table_->GetHint(collection_name);
    std::unique_lock<SpinMutex>{*hint_collection.spin};

    HashEntry hash_entry_collection;
    HashEntry *p_hash_entry_collection = nullptr;
    Status s = hash_table_->SearchForWrite(
        hint_collection, collection_name, RecordType::DlistRecord,
        &p_hash_entry_collection, &hash_entry_collection, nullptr,
        true /* in recovery */);
    hash_table_->Insert(hint_collection, p_hash_entry_collection,
                        RecordType::DlistRecord,
                        reinterpret_cast<uint64_t>(p_collection),
                        HashOffsetType::UnorderedCollection);
    return Status::Ok;
  }
  case RecordType::DlistHeadRecord: {
    kvdk_assert(pmp_record->prev == kNullPmemOffset &&
                    checkDLRecordLinkageRight(pmp_record),
                "Bad linkage found when RestoreDlistRecords. Broken head.");
    return Status::Ok;
  }
  case RecordType::DlistTailRecord: {
    kvdk_assert(pmp_record->next == kNullPmemOffset &&
                    checkDLRecordLinkageLeft(pmp_record),
                "Bad linkage found when RestoreDlistRecords. Broken tail.");
    return Status::Ok;
  }
  case RecordType::DlistDataRecord: {
    std::uint64_t offset_record =
        pmem_allocator_->addr2offset_checked(pmp_record);
    bool linked = isLinkedDLDataEntry(static_cast<DLRecord *>(pmp_record));
    if (!linked) {
      pmp_record->Destroy();
      pmem_allocator_->Free(
          SizedSpaceEntry(pmem_allocator_->addr2offset(pmp_record),
                          pmp_record->entry.header.record_size,
                          pmp_record->entry.meta.timestamp));
      return Status::Ok;
    }

    auto internal_key = pmp_record->Key();
    HashTable::KeyHashHint hint_record = hash_table_->GetHint(internal_key);
    std::unique_lock<SpinMutex> lock_record{*hint_record.spin};

    HashEntry hash_entry_record;
    HashEntry *p_hash_entry_record = nullptr;
    Status search_status = hash_table_->SearchForWrite(
        hint_record, internal_key, RecordType::DlistDataRecord,
        &p_hash_entry_record, &hash_entry_record, nullptr,
        true /* in recovery */);

    switch (search_status) {
    case Status::NotFound: {
      hash_table_->Insert(hint_record, p_hash_entry_record,
                          pmp_record->entry.meta.type, offset_record,
                          HashOffsetType::UnorderedCollectionElement);
      return Status::Ok;
    }
    case Status::Ok: {
      DLRecord *pmp_old_record = pmem_allocator_->offset2addr_checked<DLRecord>(
          hash_entry_record.offset);
      if (pmp_old_record->entry.meta.timestamp <
          pmp_record->entry.meta.timestamp) {
        if (checkDLRecordLinkageRight((DLRecord *)pmp_old_record) ||
            checkDLRecordLinkageLeft((DLRecord *)pmp_old_record)) {
          assert(false && "Old record is linked in Dlinkedlist!");
          throw std::runtime_error{"Old record is linked in Dlinkedlist!"};
        }
        hash_table_->Insert(hint_record, p_hash_entry_record,
                            pmp_record->entry.meta.type, offset_record,
                            HashOffsetType::UnorderedCollectionElement);
        UnorderedCollection::Deallocate(pmp_old_record, pmem_allocator_.get());
      } else if (pmp_old_record->entry.meta.timestamp ==
                 pmp_record->entry.meta.timestamp) {
        GlobalLogger.Info("Met two DlistRecord with same timestamp");
        UnorderedCollection::Deallocate(pmp_record, pmem_allocator_.get());
      } else {
        if (checkDLRecordLinkageRight((DLRecord *)pmp_record) ||
            checkDLRecordLinkageLeft((DLRecord *)pmp_record)) {
          assert(false && "Old record is linked in Dlinkedlist!");
          throw std::runtime_error{"Old record is linked in Dlinkedlist!"};
        }
        UnorderedCollection::Deallocate(pmp_record, pmem_allocator_.get());
      }
      return Status::Ok;
    }
    default: {
      kvdk_assert(false, "Invalid search result when trying to insert a new "
                         "DlistDataRecord!\n");
      return search_status;
    }
    }
  }
  default: {
    kvdk_assert(false, "Wrong type in RestoreDlistRecords!\n");
    return Status::Abort;
  }
  }
}

} // namespace KVDK_NAMESPACE
