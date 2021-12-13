/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kv_engine.hpp"

#include <cmath>
#include <cstdint>

#include <algorithm>
#include <atomic>
#include <future>
#include <limits>
#include <mutex>
#include <thread>

#include <dirent.h>
#include <libpmem.h>
#include <sys/mman.h>

#include "kvdk/engine.hpp"

#include "configs.hpp"
#include "dram_allocator.hpp"
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
  KVEngine *engine = new KVEngine(configs);
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
    skiplist->PurgeObsoletedNodes();
  }
}

void KVEngine::backgroundWork() {
  // To avoid free a referencing skiplist node, we do freeing in at least every
  // 10 seconds
  // TODO: Maybe free skiplist node in another bg thread?
  double interval_free_skiplist_node =
      std::max(10.0, configs_.background_work_interval);
  while (!closing_) {
    usleep(configs_.background_work_interval * 1000000);
    interval_free_skiplist_node -= configs_.background_work_interval;
    backgroundWorkImpl();
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
  bg_threads_.emplace_back(&KVEngine::backgroundWork, this);
  bg_threads_.emplace_back(&KVEngine::backgroundPendingFreeSpaceHandler, this);

  return s;
}

std::shared_ptr<Iterator>
KVEngine::NewSortedIterator(const StringView collection) {
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
    case RecordType::DlistDataRecord:
    case RecordType::QueueRecord:
    case RecordType::QueueDataRecord:
    case RecordType::QueueHeadRecord:
    case RecordType::QueueTailRecord: {
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
    if (ts_recovering > thread_cache_[thread_id].newest_restored_ts) {
      thread_cache_[thread_id].newest_restored_ts = ts_recovering;
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
    case RecordType::QueueRecord:
    case RecordType::QueueDataRecord:
    case RecordType::QueueHeadRecord:
    case RecordType::QueueTailRecord: {
      s = RestoreQueueRecords(static_cast<DLRecord *>(recovering_pmem_record));
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
  case RecordType::DlistRecord: {
    DLRecord *dl_record = static_cast<DLRecord *>(data_record);
    if (dl_record->Validate(expected_checksum)) {
      auto v = dl_record->Value();
      value->assign(v.data(), v.size());
      return true;
    }
    return false;
  }
  case RecordType::Padding: {
    return false;
  }
  default:
    GlobalLogger.Error("Unsupported type in ValidateRecordAndGetValue()!");
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
  case RecordType::DlistTailRecord:
  case RecordType::QueueRecord:
  case RecordType::QueueDataRecord:
  case RecordType::QueueHeadRecord:
  case RecordType::QueueTailRecord: {
    return static_cast<DLRecord *>(data_record)->Validate();
  }
  default:
    kvdk_assert(false, "Unsupported type in ValidateRecord()!");
    return false;
  }
}

Status KVEngine::RestoreSkiplistHead(DLRecord *pmem_record, const DataEntry &) {
  assert(pmem_record->entry.meta.type == SortedHeaderRecord);
  std::string name = string_view_2_string(pmem_record->Key());
  HashEntry hash_entry;
  HashEntry *entry_ptr = nullptr;

  CollectionIDType id = Skiplist::SkiplistID(pmem_record);
  Skiplist *skiplist;
  {
    std::lock_guard<std::mutex> lg(list_mu_);
    skiplists_.push_back(std::make_shared<Skiplist>(
        pmem_record, name, id, pmem_allocator_, hash_table_));
    skiplist = skiplists_.back().get();
    if (configs_.opt_large_sorted_collection_restore) {
      sorted_rebuilder_.SetEntriesOffsets(
          pmem_allocator_->addr2offset(pmem_record), false, nullptr);
    }
  }
  compare_excange_if_larger(list_id_, id + 1);

  // Here key is the collection name
  auto hint = hash_table_->GetHint(name);
  std::lock_guard<SpinMutex> lg(*hint.spin);
  Status s =
      hash_table_->SearchForWrite(hint, name, SortedHeaderRecord, &entry_ptr,
                                  &hash_entry, nullptr, true /* in recovery */);
  if (s == Status::MemoryOverflow) {
    return s;
  }
  assert(s == Status::NotFound);
  hash_table_->Insert(hint, entry_ptr, SortedHeaderRecord, skiplist,
                      HashOffsetType::Skiplist);
  return Status::Ok;
}

Status KVEngine::RestoreStringRecord(StringRecord *pmem_record,
                                     const DataEntry &cached_entry) {
  assert(pmem_record->entry.meta.type & StringRecordType);
  std::string key(pmem_record->Key());
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
    entry_ptr->header.status = HashEntryStatus::Normal;
    purgeAndFree(pmem_record);
    return Status::Ok;
  }

  bool free_space = entry_ptr->header.status == HashEntryStatus::Updating;
  hash_table_->Insert(hint, entry_ptr, cached_entry.meta.type, pmem_record,
                      HashOffsetType::StringRecord);
  if (free_space) {
    purgeAndFree(hash_entry.index.ptr);
  }

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
    purgeAndFree(pmem_record);
    return Status::Ok;
  }

  assert(pmem_record->entry.meta.type & SortedDataRecord);
  std::string internal_key(pmem_record->Key());
  StringView user_key = Skiplist::ExtractUserKey(internal_key);
  DataEntry existing_data_entry;
  HashEntry hash_entry;
  HashEntry *entry_ptr = nullptr;

  auto hint = hash_table_->GetHint(internal_key);
  std::lock_guard<SpinMutex> lg(*hint.spin);
  Status s = hash_table_->SearchForWrite(
      hint, internal_key, SortedDataRecord, &entry_ptr, &hash_entry,
      &existing_data_entry, true /* in recovery */);

  if (s == Status::MemoryOverflow) {
    return s;
  }

  bool found = s == Status::Ok;
  if (found &&
      existing_data_entry.meta.timestamp >= cached_data_entry.meta.timestamp) {
    purgeAndFree(pmem_record);
    return Status::Ok;
  }

  void *new_hash_index;
  DLRecord *old_pmem_record;
  SkiplistNode *dram_node;
  if (!found) {
    auto height = Skiplist::RandomHeight();
    if (height > 0) {
      dram_node = SkiplistNode::NewNode(user_key, pmem_record, height);
      if (dram_node == nullptr) {
        GlobalLogger.Error("Memory overflow in recovery\n");
        return Status::MemoryOverflow;
      }
      new_hash_index = dram_node;
      if (configs_.opt_large_sorted_collection_restore &&
          thread_cache_[write_thread.id]
                      .visited_skiplist_ids[dram_node->SkiplistID()]++ %
                  kRestoreSkiplistStride ==
              0) {
        std::lock_guard<std::mutex> lg(list_mu_);
        sorted_rebuilder_.SetEntriesOffsets(
            pmem_allocator_->addr2offset(pmem_record), false, nullptr);
      }
    } else {
      new_hash_index = pmem_record;
    }

    // Hash entry won't be reused during data recovering so we don't
    // neet to check status here
    hash_table_->Insert(
        hint, entry_ptr, cached_data_entry.meta.type, new_hash_index,
        height > 0 ? HashOffsetType::SkiplistNode : HashOffsetType::DLRecord);
  } else {
    if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
      dram_node = hash_entry.index.skiplist_node;
      old_pmem_record = dram_node->record;
      dram_node->record = pmem_record;
      entry_ptr->header.data_type = cached_data_entry.meta.type;
    } else {
      assert(hash_entry.header.offset_type == HashOffsetType::DLRecord);
      old_pmem_record = hash_entry.index.dl_record;
      hash_table_->Insert(hint, entry_ptr, cached_data_entry.meta.type,
                          pmem_record, HashOffsetType::DLRecord);
    }
    kvdk_assert(old_pmem_record->entry.meta.timestamp <
                    pmem_record->entry.meta.timestamp,
                "old pmem record has larger ts than new restored one in "
                "restore skiplist record");
    purgeAndFree(old_pmem_record);
  }

  return Status::Ok;
}

Status KVEngine::SearchOrInitCollection(const StringView &collection,
                                        Collection **list, bool init,
                                        uint16_t collection_type) {
  auto hint = hash_table_->GetHint(collection);
  HashEntry hash_entry;
  HashEntry *entry_ptr = nullptr;
  Status s = hash_table_->SearchForRead(hint, collection, collection_type,
                                        &entry_ptr, &hash_entry, nullptr);
  if (s == Status::NotFound) {
    if (init) {
      DataEntry existing_data_entry;
      std::lock_guard<SpinMutex> lg(*hint.spin);
      // Since we do the first search without lock, we need to check again
      entry_ptr = nullptr;
      s = hash_table_->SearchForWrite(hint, collection, collection_type,
                                      &entry_ptr, &hash_entry,
                                      &existing_data_entry);
      if (s == Status::MemoryOverflow) {
        return s;
      }
      if (s == Status::NotFound) {
        uint32_t request_size = sizeof(DLRecord) + collection.size() +
                                sizeof(CollectionIDType) /* id */;
        SizedSpaceEntry sized_space_entry =
            pmem_allocator_->Allocate(request_size);
        if (sized_space_entry.size == 0) {
          return Status::PmemOverflow;
        }
        CollectionIDType id = list_id_.fetch_add(1);
        // PMem level of skiplist is circular, so the next and prev pointers of
        // header point to itself
        DLRecord *pmem_record = DLRecord::PersistDLRecord(
            pmem_allocator_->offset2addr(sized_space_entry.space_entry.offset),
            sized_space_entry.size, get_timestamp(),
            (RecordType)collection_type, sized_space_entry.space_entry.offset,
            sized_space_entry.space_entry.offset, collection,
            StringView((char *)&id, 8));

        {
          std::lock_guard<std::mutex> lg(list_mu_);
          switch (collection_type) {
          case SortedHeaderRecord:
            skiplists_.push_back(std::make_shared<Skiplist>(
                pmem_record, string_view_2_string(collection), id,
                pmem_allocator_, hash_table_));
            *list = skiplists_.back().get();
            break;
          default:
            return Status::NotSupported;
          }
        }
        assert(entry_ptr->header.status == HashEntryStatus::Initializing ||
               entry_ptr->header.status == HashEntryStatus::Empty);
        hash_table_->Insert(hint, entry_ptr, collection_type, *list,
                            HashOffsetType::Skiplist);
        return Status::Ok;
      }
    }
  }

  if (s == Status::Ok) {
    *list = static_cast<Collection *>(hash_entry.index.ptr);
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
            thread_cache_[id].persisted_pending_batch = pending_batch;
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
    for (size_t i = 0; i < thread_cache_.size(); i++) {
      auto &thread_cache = thread_cache_[i];
      if (thread_cache.newest_restored_ts > newest_version_on_startup_) {
        newest_version_on_startup_ = thread_cache.newest_restored_ts;
      }
    }
  }

  smallest_snapshot_ = SnapshotImpl(newest_version_on_startup_);

  return Status::Ok;
}

Status KVEngine::HashGetImpl(const StringView &key, std::string *value,
                             uint16_t type_mask) {
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
      pmem_record = hash_entry.index.ptr;
    } else if (hash_entry.header.data_type == SortedDataRecord) {
      if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
        SkiplistNode *dram_node = hash_entry.index.skiplist_node;
        pmem_record = dram_node->record;
      } else {
        assert(hash_entry.header.offset_type == HashOffsetType::DLRecord);
        pmem_record = hash_entry.index.dl_record;
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

Status KVEngine::Get(const StringView key, std::string *value) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  return HashGetImpl(key, value, StringRecordType);
}

Status KVEngine::Delete(const StringView key) {
  Status s = MaybeInitWriteThread();

  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  return StringDeleteImpl(key);
}

Status KVEngine::SDeleteImpl(Skiplist *skiplist, const StringView &user_key) {
  std::string collection_key(skiplist->InternalKey(user_key));
  if (!CheckKeySize(collection_key)) {
    return Status::InvalidDataSize;
  }

  while (1) {
    HashEntry hash_entry;
    DataEntry data_entry;
    SkiplistNode *dram_node = nullptr;
    DLRecord *existing_record = nullptr;
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
      dram_node = hash_entry.index.skiplist_node;
      existing_record = dram_node->record;
    } else {
      assert(hash_entry.header.offset_type == HashOffsetType::DLRecord);
      existing_record = hash_entry.index.dl_record;
    }

    if (!skiplist->Delete(user_key, existing_record, dram_node, hint.spin)) {
      continue;
    }

    entry_ptr->Clear();
    purgeAndFree(existing_record);
    break;
  }
  return Status::Ok;
}

Status KVEngine::SSetImpl(Skiplist *skiplist, const StringView &user_key,
                          const StringView &value) {
  std::string collection_key(skiplist->InternalKey(user_key));
  if (!CheckKeySize(collection_key) || !CheckValueSize(value)) {
    return Status::InvalidDataSize;
  }

  auto request_size = value.size() + collection_key.size() + sizeof(DLRecord);
  SizedSpaceEntry sized_space_entry = pmem_allocator_->Allocate(request_size);
  if (sized_space_entry.size == 0) {
    return Status::PmemOverflow;
  }
  void *new_record_pmem_ptr =
      pmem_allocator_->offset2addr(sized_space_entry.space_entry.offset);

  while (1) {
    SkiplistNode *dram_node = nullptr;
    HashEntry *entry_ptr = nullptr;
    DLRecord *existing_record = nullptr;
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

    uint64_t new_ts = get_timestamp();
    assert(!found || new_ts > data_entry.meta.timestamp);

    if (found) {
      if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
        dram_node = hash_entry.index.skiplist_node;
        existing_record = dram_node->record;
      } else {
        dram_node = nullptr;
        assert(hash_entry.header.offset_type == HashOffsetType::DLRecord);
        existing_record = hash_entry.index.dl_record;
      }

      if (!skiplist->Update(user_key, value, existing_record, sized_space_entry,
                            new_ts, dram_node, hint.spin)) {
        continue;
      }

      // update a height 0 node
      if (dram_node == nullptr) {
        hash_table_->Insert(hint, entry_ptr, SortedDataRecord,
                            new_record_pmem_ptr, HashOffsetType::DLRecord);
      }
      purgeAndFree(existing_record);
    } else {
      if (!skiplist->Insert(user_key, value, sized_space_entry, new_ts,
                            &dram_node, hint.spin)) {
        continue;
      }

      void *new_hash_index =
          (dram_node == nullptr) ? new_record_pmem_ptr : dram_node;
      auto entry_base_status = entry_ptr->header.status;
      hash_table_->Insert(hint, entry_ptr, SortedDataRecord, new_hash_index,
                          dram_node ? HashOffsetType::SkiplistNode
                                    : HashOffsetType::DLRecord);
      if (entry_base_status == HashEntryStatus::Updating) {
        purgeAndFree(hash_entry.index.dl_record);
      }
    }

    break;
  }
  return Status::Ok;
}

Status KVEngine::SSet(const StringView collection, const StringView user_key,
                      const StringView value) {
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

Status KVEngine::SDelete(const StringView collection,
                         const StringView user_key) {
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
  if (thread_cache_[write_thread.id].persisted_pending_batch == nullptr) {
    int is_pmem;
    size_t mapped_len;
    uint64_t persisted_pending_file_size =
        kMaxWriteBatchSize * 8 + sizeof(PendingBatch);
    persisted_pending_file_size =
        kPMEMMapSizeUnit *
        (size_t)ceil(1.0 * persisted_pending_file_size / kPMEMMapSizeUnit);

    if ((thread_cache_[write_thread.id].persisted_pending_batch =
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

  std::set<SpinMutex *> spins_to_lock;
  std::vector<BatchWriteHint> batch_hints(write_batch.Size());
  std::vector<uint64_t> space_entry_offsets;
  for (size_t i = 0; i < write_batch.Size(); i++) {
    auto &kv = write_batch.kvs[i];
    if (kv.type == StringDataRecord) {
      // Allocate space for data records
      uint32_t requested_size =
          kv.key.size() + kv.value.size() + sizeof(StringRecord);
      batch_hints[i].allocated_space =
          pmem_allocator_->Allocate(requested_size);
      // No enough space for batch write
      if (batch_hints[i].allocated_space.size == 0) {
        for (size_t j = 0; j < i; j++) {
          pmem_allocator_->Free(batch_hints[j].allocated_space);
        }
        return s;
      }
      space_entry_offsets.emplace_back(
          batch_hints[i].allocated_space.space_entry.offset);
    } else {
      kvdk_assert(kv.type == StringDeleteRecord,
                  "only support string type batch write");
    }

    batch_hints[i].hash_hint = hash_table_->GetHint(kv.key);
    spins_to_lock.emplace(batch_hints[i].hash_hint.spin);
  }

  // lock spin mutex with order to avoid deadlock
  std::vector<std::unique_lock<SpinMutex>> ul_locks;
  for (const SpinMutex *l : spins_to_lock) {
    ul_locks.emplace_back(const_cast<SpinMutex &>(*l));
  }

  TimeStampType ts = get_timestamp();
  for (size_t i = 0; i < write_batch.Size(); i++) {
    batch_hints[i].timestamp = ts;
  }

  // Persist batch write status as processing
  PendingBatch pending_batch(PendingBatch::Stage::Processing,
                             space_entry_offsets.size(), ts);
  pending_batch.PersistProcessing(
      thread_cache_[write_thread.id].persisted_pending_batch,
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
    // TODO: roll back finished writes (hard to roll back hash table now)
    if (s != Status::Ok) {
      assert(s == Status::MemoryOverflow);
      std::abort();
    }
  }

  pending_batch.PersistStage(PendingBatch::Stage::Finish);

  // Free updated kvs, we should purge all updated kvs before release locks and
  // after persist write stage
  for (size_t i = 0; i < write_batch.Size(); i++) {
    if (batch_hints[i].pmem_record_to_free != nullptr) {
      purgeAndFree(batch_hints[i].pmem_record_to_free);
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
    auto &hash_hint = batch_hint.hash_hint;
    // hash table for the hint should be alread locked, so we do not lock it
    // here
    Status s =
        hash_table_->SearchForWrite(hash_hint, kv.key, StringRecordType,
                                    &entry_ptr, &hash_entry, &data_entry);
    if (s == Status::MemoryOverflow) {
      return s;
    }
    bool found = s == Status::Ok;

    // Deleting kv is not existing
    if (kv.type == StringDeleteRecord) {
      if (found) {
        batch_hint.pmem_record_to_free = hash_entry.index.string_record;
      }
      return Status::Ok;
    }

    kvdk_assert(!found || batch_hint.timestamp > data_entry.meta.timestamp,
                "ts of new data smaller than existing data in batch write");

    void *block_base = pmem_allocator_->offset2addr(
        batch_hint.allocated_space.space_entry.offset);

    // We use if here to avoid compilation warning
    if (kv.type == StringDataRecord) {
      StringRecord::PersistStringRecord(
          block_base, batch_hint.allocated_space.size, batch_hint.timestamp,
          static_cast<RecordType>(kv.type), kv.key, kv.value);
    } else {
      // Never reach
      kvdk_assert(false, "wrong data type in batch write");
      std::abort();
    }

    auto entry_base_status = entry_ptr->header.status;
    hash_table_->Insert(hash_hint, entry_ptr, kv.type, block_base,
                        HashOffsetType::StringRecord);

    if (entry_base_status == HashEntryStatus::Updating) {
      batch_hint.pmem_record_to_free = hash_entry.index.string_record;
    }
  }

  return Status::Ok;
}

Status KVEngine::SGet(const StringView collection, const StringView user_key,
                      std::string *value) {
  Skiplist *skiplist = nullptr;
  Status s = SearchOrInitSkiplist(collection, &skiplist, false);
  if (s != Status::Ok) {
    return s;
  }
  assert(skiplist);
  std::string skiplist_key(skiplist->InternalKey(user_key));
  return HashGetImpl(skiplist_key, value, SortedDataRecord);
}

Status KVEngine::StringDeleteImpl(const StringView &key) {
  DataEntry data_entry;
  HashEntry hash_entry;
  HashEntry *entry_ptr = nullptr;

  uint32_t requested_size = key.size() + sizeof(StringRecord);
  SizedSpaceEntry sized_space_entry;

  {
    auto hint = hash_table_->GetHint(key);
    std::lock_guard<SpinMutex> lg(*hint.spin);
    // Set current snapshot to this thread
    // TODO: merge these two
    TimeStampType new_ts = get_timestamp();
    SnapshotSetter setter(thread_cache_[write_thread.id].holding_snapshot,
                          new_ts);
    Status s = hash_table_->SearchForWrite(
        hint, key, StringDeleteRecord | StringDataRecord, &entry_ptr,
        &hash_entry, &data_entry);

    switch (s) {
    case Status::Ok: {
      assert(entry_ptr->header.status == HashEntryStatus::Updating);
      if (entry_ptr->header.data_type == StringDeleteRecord) {
        entry_ptr->header.status = HashEntryStatus::Normal;
        return s;
      }
      auto request_size = key.size() + sizeof(StringRecord);
      SizedSpaceEntry sized_space_entry =
          pmem_allocator_->Allocate(request_size);
      if (sized_space_entry.size == 0) {
        return Status::PmemOverflow;
      }

      void *pmem_ptr = pmem_allocator_->offset2addr_checked(
          sized_space_entry.space_entry.offset);

      StringRecord::PersistStringRecord(pmem_ptr, sized_space_entry.size,
                                        new_ts, StringDeleteRecord, key, "");

      hash_table_->Insert(hint, entry_ptr, StringDeleteRecord, pmem_ptr,
                          HashOffsetType::StringRecord);
      delayFree(PendingFreeDataRecord{hash_entry.index.string_record, new_ts});
      // We also delay free this delete record to recycle PMem and DRAM space
      delayFree(
          PendingFreeDeleteRecord{pmem_ptr, new_ts, entry_ptr, hint.spin});
      return s;
    }
    case Status::NotFound:
      return Status::Ok;
    default:
      return s;
    }
  }
}

Status KVEngine::StringSetImpl(const StringView &key, const StringView &value) {
  DataEntry data_entry;
  HashEntry hash_entry;
  HashEntry *hash_entry_ptr = nullptr;
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
    // Set current snapshot to this thread
    TimeStampType new_ts = get_timestamp();
    SnapshotSetter setter(thread_cache_[write_thread.id].holding_snapshot,
                          new_ts);

    // Search position to write index in hash table.
    Status s = hash_table_->SearchForWrite(
        hint, key, StringDeleteRecord | StringDataRecord, &hash_entry_ptr,
        &hash_entry, &data_entry);
    if (s == Status::MemoryOverflow) {
      return s;
    }
    bool found = s == Status::Ok;

    void *block_base =
        pmem_allocator_->offset2addr(sized_space_entry.space_entry.offset);

    assert(!found || new_ts > data_entry.meta.timestamp);

    // Persist key-value pair to PMem
    StringRecord::PersistStringRecord(block_base, sized_space_entry.size,
                                      new_ts, StringDataRecord, key, value);

    auto entry_base_status = hash_entry_ptr->header.status;
    auto updated_type = hash_entry_ptr->header.data_type;
    // Write hash index
    hash_table_->Insert(hint, hash_entry_ptr, StringDataRecord, block_base,
                        HashOffsetType::StringRecord);
    if (entry_base_status == HashEntryStatus::Updating &&
        updated_type == StringDataRecord) {
      /* delete record is self-freed, so we don't need to free it here */
      delayFree(PendingFreeDataRecord{hash_entry.index.string_record, new_ts});
    }
  }

  return Status::Ok;
}

Status KVEngine::Set(const StringView key, const StringView value) {
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
std::shared_ptr<UnorderedCollection>
KVEngine::createUnorderedCollection(StringView const collection_name) {
  TimeStampType ts = get_timestamp();
  CollectionIDType id = list_id_.fetch_add(1);
  std::string name(collection_name.data(), collection_name.size());
  std::shared_ptr<UnorderedCollection> sp_uncoll =
      std::make_shared<UnorderedCollection>(
          hash_table_.get(), pmem_allocator_.get(), name, id, ts);
  return sp_uncoll;
}

UnorderedCollection *
KVEngine::findUnorderedCollection(StringView const collection_name) {
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
    return hash_entry.index.p_unordered_collection;
  }
  default: {
    kvdk_assert(false, "Invalid state in findUnorderedCollection()!");
    return nullptr;
  }
  }
}

Status KVEngine::HGet(StringView const collection_name, StringView const key,
                      std::string *value) {
  UnorderedCollection *p_uncoll = findUnorderedCollection(collection_name);
  if (!p_uncoll) {
    return Status::NotFound;
  }

  std::string internal_key = p_uncoll->InternalKey(key);
  return HashGetImpl(internal_key, value, RecordType::DlistDataRecord);
}

Status KVEngine::HSet(StringView const collection_name, StringView const key,
                      StringView const value) {
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
                              RecordType::DlistRecord, p_collection,
                              HashOffsetType::UnorderedCollection);
        }
      }
    }
  }

  // Emplace the new DlistDataRecord
  {
    auto internal_key = p_collection->InternalKey(key);
    HashTable::KeyHashHint hint_record = hash_table_->GetHint(internal_key);

    int n_try = 0;
    while (true) {
      ++n_try;

      std::unique_lock<SpinMutex> lock_record{*hint_record.spin};

      TimeStampType ts = get_timestamp();

      HashEntry hash_entry_record;
      HashEntry *p_hash_entry_record = nullptr;
      Status search_result = hash_table_->SearchForWrite(
          hint_record, internal_key, RecordType::DlistDataRecord,
          &p_hash_entry_record, &hash_entry_record, nullptr);

      ModifyReturn emplace_result{};
      switch (search_result) {
      case Status::NotFound: {
        emplace_result = p_collection->Emplace(ts, key, value, lock_record);
        break;
      }
      case Status::Ok: {
        DLRecord *pmp_old_record = hash_entry_record.index.dl_record;
        emplace_result =
            p_collection->Replace(pmp_old_record, ts, key, value, lock_record);
        // Additional check
        if (emplace_result.success) {
          kvdk_assert(pmem_allocator_->addr2offset_checked(pmp_old_record) ==
                          emplace_result.offset_old,
                      "Updated a record, but HashEntry in HashTable is "
                      "inconsistent with data on PMem!");

          DLRecord *pmp_new_record =
              pmem_allocator_->offset2addr_checked<DLRecord>(
                  emplace_result.offset_new);
          kvdk_assert(
              pmp_old_record->entry.meta.timestamp <
                  pmp_new_record->entry.meta.timestamp,
              "Old record has newer timestamp than newly inserted record!");
          purgeAndFree(pmp_old_record);
        }
        break;
      }
      default: {
        kvdk_assert(false, "Invalid search result when trying to insert "
                           "a new DlistDataRecord!");
      }
      }

      if (!emplace_result.success) {
        // Retry
        continue;
      } else {
        // Successfully emplaced the new record
        DLRecord *pmp_new_record =
            pmem_allocator_->offset2addr_checked<DLRecord>(
                emplace_result.offset_new);
        hash_table_->Insert(hint_record, p_hash_entry_record,
                            RecordType::DlistDataRecord, pmp_new_record,
                            HashOffsetType::UnorderedCollectionElement);
        return Status::Ok;
      }
    }
  }
}

Status KVEngine::HDelete(StringView const collection_name,
                         StringView const key) {
  Status s = MaybeInitWriteThread();
  if (s != Status::Ok) {
    return s;
  }
  UnorderedCollection *p_collection = findUnorderedCollection(collection_name);
  if (!p_collection)
    return Status::Ok;

  // Erase DlistDataRecord if found one.
  {
    auto internal_key = p_collection->InternalKey(key);
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

      ModifyReturn erase_result{};
      switch (search_result) {
      case Status::NotFound: {
        return Status::Ok;
      }
      case Status::Ok: {
        DLRecord *pmp_old_record = hash_entry.index.dl_record;

        erase_result = p_collection->Erase(pmp_old_record, lock_record);
        if (erase_result.success) {
          p_hash_entry->Clear();
          purgeAndFree(pmp_old_record);
          return Status::Ok;
        } else {
          // !erase_result.success
          continue;
        }
        break;
      }
      default: {
        kvdk_assert(false, "Invalid search result when trying to erase "
                           "a DlistDataRecord!");
      }
      }
    }
  }
}

std::shared_ptr<Iterator>
KVEngine::NewUnorderedIterator(StringView const collection_name) {
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
          std::make_shared<UnorderedCollection>(
              hash_table_.get(), pmem_allocator_.get(), pmp_record);
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
                        RecordType::DlistRecord, p_collection,
                        HashOffsetType::UnorderedCollection);
    kvdk_assert((s == Status::NotFound), "Impossible situation occurs!");
    return Status::Ok;
  }
  case RecordType::DlistHeadRecord: {
    kvdk_assert(pmp_record->prev == kPmemNullOffset &&
                    checkDLRecordLinkageRight(pmp_record),
                "Bad linkage found when RestoreDlistRecords. Broken head.");
    return Status::Ok;
  }
  case RecordType::DlistTailRecord: {
    kvdk_assert(pmp_record->next == kPmemNullOffset &&
                    checkDLRecordLinkageLeft(pmp_record),
                "Bad linkage found when RestoreDlistRecords. Broken tail.");
    return Status::Ok;
  }
  case RecordType::DlistDataRecord: {
    bool linked = checkLinkage(static_cast<DLRecord *>(pmp_record));
    if (!linked) {
      purgeAndFree(pmp_record);

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
                          pmp_record->entry.meta.type, pmp_record,
                          HashOffsetType::UnorderedCollectionElement);
      return Status::Ok;
    }
    case Status::Ok: {
      DLRecord *pmp_old_record = hash_entry_record.index.dl_record;
      if (pmp_old_record->entry.meta.timestamp <
          pmp_record->entry.meta.timestamp) {
        if (checkDLRecordLinkageRight((DLRecord *)pmp_old_record) ||
            checkDLRecordLinkageLeft((DLRecord *)pmp_old_record)) {
          assert(false && "Old record is linked in Dlinkedlist!");
          throw std::runtime_error{"Old record is linked in Dlinkedlist!"};
        }
        hash_table_->Insert(hint_record, p_hash_entry_record,
                            pmp_record->entry.meta.type, pmp_record,
                            HashOffsetType::UnorderedCollectionElement);
        purgeAndFree(pmp_old_record);

      } else if (pmp_old_record->entry.meta.timestamp ==
                 pmp_record->entry.meta.timestamp) {
        GlobalLogger.Info("Met two DlistRecord with same timestamp");
        purgeAndFree(pmp_record);

      } else {
        if (checkDLRecordLinkageRight((DLRecord *)pmp_record) ||
            checkDLRecordLinkageLeft((DLRecord *)pmp_record)) {
          assert(false && "Old record is linked in Dlinkedlist!");
          throw std::runtime_error{"Old record is linked in Dlinkedlist!"};
        }
        purgeAndFree(pmp_record);
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

namespace KVDK_NAMESPACE {
std::unique_ptr<Queue> KVEngine::createQueue(StringView const collection_name) {
  std::uint64_t ts = get_timestamp();
  CollectionIDType id = list_id_.fetch_add(1);
  std::string name(collection_name.data(), collection_name.size());
  return std::unique_ptr<Queue>(new Queue{pmem_allocator_.get(), name, id, ts});
}

Queue *KVEngine::findQueue(StringView const collection_name) {
  HashTable::KeyHashHint hint = hash_table_->GetHint(collection_name);
  HashEntry hash_entry;
  HashEntry *entry_ptr = nullptr;
  Status s =
      hash_table_->SearchForRead(hint, collection_name, RecordType::QueueRecord,
                                 &entry_ptr, &hash_entry, nullptr);
  switch (s) {
  case Status::NotFound: {
    return nullptr;
  }
  case Status::Ok: {
    return hash_entry.index.queue_ptr;
  }
  default: {
    kvdk_assert(false, "Invalid state in findQueue()!");
    return nullptr;
  }
  }
}

Status KVEngine::xPop(StringView const collection_name, std::string *value,
                      KVEngine::QueueOpPosition pop_pos) {
  Status s = MaybeInitWriteThread();
  if (s != Status::Ok) {
    return s;
  }
  Queue *queue_ptr = findQueue(collection_name);
  if (!queue_ptr) {
    return Status::NotFound;
  }
  bool pop_success = false;
  switch (pop_pos) {
  case QueueOpPosition::Left: {
    pop_success = queue_ptr->PopFront(value);
    break;
  }
  case QueueOpPosition::Right: {
    pop_success = queue_ptr->PopBack(value);
    break;
  }
  default: {
    GlobalLogger.Error("Impossible!");
    return Status::Abort;
  }
  }
  if (pop_success) {
    return Status::Ok;
  } else {
    return Status::NotFound;
  }
}

Status KVEngine::xPush(StringView const collection_name, StringView const value,
                       KVEngine::QueueOpPosition push_pos) try {
  Status s = MaybeInitWriteThread();
  if (s != Status::Ok) {
    return s;
  }

  Queue *queue_ptr = nullptr;

  // Find UnorederedCollection, create if none exists
  {
    queue_ptr = findQueue(collection_name);
    if (!queue_ptr) {
      HashTable::KeyHashHint hint_collection =
          hash_table_->GetHint(collection_name);
      std::unique_lock<SpinMutex> lock_collection{*hint_collection.spin};
      {
        // Lock and find again in case other threads have created the
        // UnorderedCollection
        queue_ptr = findQueue(collection_name);
        if (queue_ptr) {
          // Some thread already created the collection
          // Do nothing
        } else {
          {
            std::lock_guard<std::mutex> lg{list_mu_};
            queue_uptr_vec_.emplace_back(createQueue(collection_name));
            queue_ptr = queue_uptr_vec_.back().get();
          }

          HashEntry hash_entry_collection;
          HashEntry *p_hash_entry_collection = nullptr;
          Status s = hash_table_->SearchForWrite(
              hint_collection, collection_name, RecordType::QueueRecord,
              &p_hash_entry_collection, &hash_entry_collection, nullptr);
          kvdk_assert(s == Status::NotFound, "Logically impossible!");
          hash_table_->Insert(hint_collection, p_hash_entry_collection,
                              RecordType::QueueRecord, queue_ptr,
                              HashOffsetType::Queue);
        }
      }
    }
  }

  // Push
  {
    TimeStampType ts = get_timestamp();
    switch (push_pos) {
    case QueueOpPosition::Left: {
      queue_ptr->PushFront(ts, value);
      return Status::Ok;
    }
    case QueueOpPosition::Right: {
      queue_ptr->PushBack(ts, value);
      return Status::Ok;
    }
    default: {
      GlobalLogger.Error("Impossible!");
      return Status::Abort;
    }
    }
  }
} catch (std::bad_alloc const &ex) {
  return Status::PmemOverflow;
}

Status KVEngine::RestoreQueueRecords(DLRecord *pmp_record) {
  switch (pmp_record->entry.meta.type) {
  case RecordType::QueueRecord: {
    Queue *queue_ptr = nullptr;
    std::lock_guard<std::mutex> lg{list_mu_};
    {
      queue_uptr_vec_.emplace_back(
          new Queue{pmem_allocator_.get(), pmp_record});
      queue_ptr = queue_uptr_vec_.back().get();
    }

    std::string collection_name = queue_ptr->Name();
    HashTable::KeyHashHint hint_collection =
        hash_table_->GetHint(collection_name);
    std::unique_lock<SpinMutex>{*hint_collection.spin};

    HashEntry hash_entry_collection;
    HashEntry *p_hash_entry_collection = nullptr;
    Status s = hash_table_->SearchForWrite(
        hint_collection, collection_name, RecordType::QueueRecord,
        &p_hash_entry_collection, &hash_entry_collection, nullptr,
        true /* in recovery */);
    kvdk_assert((s == Status::NotFound), "Impossible situation occurs!");
    hash_table_->Insert(hint_collection, p_hash_entry_collection,
                        RecordType::QueueRecord, queue_ptr,
                        HashOffsetType::Queue);
    return Status::Ok;
  }
  case RecordType::QueueHeadRecord: {
    kvdk_assert(pmp_record->prev == kPmemNullOffset &&
                    checkDLRecordLinkageRight(pmp_record),
                "Bad linkage found when RestoreDlistRecords. Broken head.");
    return Status::Ok;
  }
  case RecordType::QueueTailRecord: {
    kvdk_assert(pmp_record->next == kPmemNullOffset &&
                    checkDLRecordLinkageLeft(pmp_record),
                "Bad linkage found when RestoreDlistRecords. Broken tail.");
    return Status::Ok;
  }
  case RecordType::QueueDataRecord: {
    bool linked = checkLinkage(static_cast<DLRecord *>(pmp_record));
    if (!linked) {
      GlobalLogger.Error("Bad linkage!\n");
      // Bad linkage handled by DlinkedList.
      return Status::Ok;
    } else {
      return Status::Ok;
    }
  }
  default: {
    kvdk_assert(false, "Wrong type in RestoreDlistRecords!\n");
    return Status::Abort;
  }
  }
}

void KVEngine::maybeHandleCachedPendingFreeSpace() {
  kvdk_assert(write_thread.id >= 0,
              "call KVEngine::maybeHandleCachedPendingFreeSpace in a "
              "un-initialized write thread");
  auto &tc = thread_cache_[write_thread.id];
  size_t limit_free = 1;
  while (tc.pending_free_data_records.size() > 0 &&
         tc.pending_free_data_records.front().newer_version_timestamp <
             smallest_snapshot_.GetTimestamp() &&
         limit_free-- > 0) {
    pmem_allocator_->Free(
        handlePendingFreeRecord(tc.pending_free_data_records.front()));
    tc.pending_free_data_records.pop_front();
  }

  if (tc.pending_free_delete_records.size() > 10000 && bg_free_processing_) {
    bg_free_cv_.notify_all();
  }
}

void KVEngine::delayFree(PendingFreeDeleteRecord &&pending_free_delete_record) {
  // To avoid too many records pending free, we upadte global smallest snapshot
  // regularly
  maybeUpdateSmallestSnapshot();

  kvdk_assert(write_thread.id >= 0, "uninitialized write thread in delayFree");
  auto &tc = thread_cache_[write_thread.id];
  std::lock_guard<SpinMutex> lg(tc.pending_free_delete_records_lock);
  tc.pending_free_delete_records.emplace_back(
      std::forward<PendingFreeDeleteRecord>(pending_free_delete_record));
  maybeHandleCachedPendingFreeSpace();
}

void KVEngine::delayFree(PendingFreeDataRecord &&pending_free_data_record) {
  // To avoid too many records pending free, we upadte global smallest snapshot
  // regularly
  maybeUpdateSmallestSnapshot();

  kvdk_assert(write_thread.id >= 0, "uninitialized write thread in delayFree");
  auto &tc = thread_cache_[write_thread.id];
  std::lock_guard<SpinMutex> lg(tc.pending_free_delete_records_lock);
  tc.pending_free_data_records.emplace_back(
      std::forward<PendingFreeDataRecord>(pending_free_data_record));
  maybeHandleCachedPendingFreeSpace();
}

SizedSpaceEntry KVEngine::handlePendingFreeRecord(
    const PendingFreeDeleteRecord &pending_free_delete_record) {
  DataEntry *data_entry =
      static_cast<DataEntry *>(pending_free_delete_record.pmem_data_record);
  switch (data_entry->meta.type) {
  case StringDeleteRecord: {
    if (pending_free_delete_record.hash_entry_ref->index.string_record ==
        pending_free_delete_record.pmem_data_record) {
      std::lock_guard<SpinMutex> lg(
          *pending_free_delete_record.hash_entry_lock);
      if (pending_free_delete_record.hash_entry_ref->index.string_record ==
          pending_free_delete_record.pmem_data_record) {
        pending_free_delete_record.hash_entry_ref->Clear();
      }
    }
    // we don't need to purge a delete record
    return SizedSpaceEntry(pmem_allocator_->addr2offset(data_entry),
                           data_entry->header.record_size,
                           data_entry->meta.timestamp);
  }
  default: {
    std::abort();
  }
  }
}

SizedSpaceEntry KVEngine::handlePendingFreeRecord(
    const PendingFreeDataRecord &pending_free_data_record) {
  DataEntry *data_entry =
      static_cast<DataEntry *>(pending_free_data_record.pmem_data_record);
  switch (data_entry->meta.type) {
  case StringDataRecord: {
    data_entry->Destroy();
    return SizedSpaceEntry(pmem_allocator_->addr2offset(data_entry),
                           data_entry->header.record_size,
                           data_entry->meta.timestamp);
  }
  default:
    std::abort();
  }
}

void KVEngine::updateSmallestSnapshot() {
  TimeStampType ts = get_timestamp();
  for (size_t i = 0; i < thread_cache_.size(); i++) {
    auto &tc = thread_cache_[i];
    ts = std::min(tc.holding_snapshot.GetTimestamp(), ts);
  }
  smallest_snapshot_.timestamp = ts;
}

void KVEngine::maybeUpdateSmallestSnapshot() {
  // To avoid too many records pending free, we upadte global smallest snapshot
  // regularly. We update it every kUpdateSnapshotRound to mitigate the overhead
  static size_t kUpdateSnapshotRound = 10000;
  thread_local size_t round = 0;
  if ((++round) % kUpdateSnapshotRound == 0) {
    updateSmallestSnapshot();
  }
}

void KVEngine::backgroundPendingFreeSpaceHandler() {
  while (1) {
    bg_free_processing_ = false;
    if (closing_) {
      return;
    }
    std::deque<PendingFreeDataRecord> unfreed_data_record;
    std::deque<PendingFreeDeleteRecord> unfreed_delete_record;
    std::vector<SizedSpaceEntry> space_to_free;
    std::unique_lock<SpinMutex> ul(bg_free_lock_);
    bg_free_cv_.wait(ul);
    bg_free_processing_ = true;
    updateSmallestSnapshot();
    TimeStampType smallest_snapshot_ts = smallest_snapshot_.GetTimestamp();
    for (size_t i = 0; i < thread_cache_.size(); i++) {
      auto &thread_cache = thread_cache_[i];
      if (thread_cache.pending_free_data_records.size() > 0 ||
          thread_cache.pending_free_delete_records.size() > 0) {
        std::lock_guard<SpinMutex> lg(
            thread_cache.pending_free_delete_records_lock);

        if (thread_cache.pending_free_data_records.size() > 0) {
          pending_free_data_records_pool_.emplace_back();
          pending_free_data_records_pool_.back().swap(
              thread_cache.pending_free_data_records);
        }

        if (thread_cache.pending_free_delete_records.size() > 0) {
          pending_free_delete_records_pool_.emplace_back();
          pending_free_delete_records_pool_.back().swap(
              thread_cache.pending_free_delete_records);
        }
      }
    }

    for (auto &pending_free_data_records : pending_free_data_records_pool_) {
      for (auto &record : pending_free_data_records) {
        if (record.newer_version_timestamp <= smallest_snapshot_ts) {
          space_to_free.emplace_back(handlePendingFreeRecord(record));
        } else {
          unfreed_data_record.emplace_back(std::move(record));
        }
      }
    }

    for (auto &pending_free_delete_records :
         pending_free_delete_records_pool_) {
      for (auto &record : pending_free_delete_records) {
        if (record.newer_version_timestamp <= smallest_snapshot_ts) {
          space_to_free.emplace_back(handlePendingFreeRecord(record));
        } else {
          unfreed_delete_record.emplace_back(std::move(record));
        }
      }
    }

    pmem_allocator_->BatchFree(space_to_free);

    pending_free_data_records_pool_.clear();
    pending_free_data_records_pool_.emplace_back(
        std::move(unfreed_data_record));
    pending_free_delete_records_pool_.clear();
    pending_free_delete_records_pool_.emplace_back(
        std::move(unfreed_delete_record));
  }
}
} // namespace KVDK_NAMESPACE
