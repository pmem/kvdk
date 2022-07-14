/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kv_engine.hpp"

#include <dirent.h>
#include <libpmem.h>
#include <sys/mman.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <future>
#include <limits>
#include <mutex>
#include <thread>

#include "backup_log.hpp"
#include "configs.hpp"
#include "dram_allocator.hpp"
#include "kvdk/engine.hpp"
#include "sorted_collection/iterator.hpp"
#include "structures.hpp"
#include "utils/sync_point.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {
void PendingBatch::PersistFinish() {
  num_kv = 0;
  stage = Stage::Finish;
  pmem_persist(this, sizeof(PendingBatch));
}

void PendingBatch::PersistProcessing(const std::vector<PMemOffsetType>& records,
                                     TimeStampType ts) {
  pmem_memcpy_persist(record_offsets, records.data(), records.size() * 8);
  timestamp = ts;
  num_kv = records.size();
  stage = Stage::Processing;
  pmem_persist(this, sizeof(PendingBatch));
}

KVEngine::~KVEngine() {
  GlobalLogger.Info("Closing instance ... \n");
  GlobalLogger.Info("Waiting bg threads exit ... \n");
  closing_ = true;
  terminateBackgroundWorks();
  // deleteCollections();
  ReportPMemUsage();
  GlobalLogger.Info("Instance closed\n");
}

Status KVEngine::Open(const std::string& name, Engine** engine_ptr,
                      const Configs& configs) {
  GlobalLogger.Info("Opening kvdk instance from %s ...\n", name.c_str());
  KVEngine* engine = new KVEngine(configs);
  Status s = engine->Init(name, configs);
  if (s == Status::Ok) {
    s = engine->restoreExistingData();
  }
  if (s == Status::Ok) {
    *engine_ptr = engine;
    engine->startBackgroundWorks();
    engine->ReportPMemUsage();
  } else {
    GlobalLogger.Error("Init kvdk instance failed: %d\n", s);
    delete engine;
  }
  return s;
}

Status KVEngine::Restore(const std::string& engine_path,
                         const std::string& backup_log, Engine** engine_ptr,
                         const Configs& configs) {
  GlobalLogger.Info(
      "Restoring kvdk instance from backup log %s to engine path %s\n",
      backup_log.c_str(), engine_path.c_str());
  KVEngine* engine = new KVEngine(configs);
  Status s = engine->Init(engine_path, configs);
  if (s == Status::Ok) {
    s = engine->restoreDataFromBackup(backup_log);
  }

  if (s == Status::Ok) {
    *engine_ptr = engine;
    engine->startBackgroundWorks();
    engine->ReportPMemUsage();
  } else {
    GlobalLogger.Error("Restore kvdk instance from backup log %s failed: %d\n",
                       backup_log.c_str(), s);
    delete engine;
  }
  return s;
}

void KVEngine::FreeSkiplistDramNodes() {
  for (auto skiplist : skiplists_) {
    skiplist.second->CleanObsoletedNodes();
  }
}

void KVEngine::ReportPMemUsage() {
  // Check pmem allocator is initialized before use it.
  // It may not be successfully initialized due to file operation errors.
  if (pmem_allocator_ == nullptr) {
    return;
  }

  auto total = pmem_allocator_->PMemUsageInBytes();
  GlobalLogger.Info("PMem Usage: %ld B, %ld KB, %ld MB, %ld GB\n", total,
                    (total / (1LL << 10)), (total / (1LL << 20)),
                    (total / (1LL << 30)));
}

void KVEngine::startBackgroundWorks() {
  std::unique_lock<SpinMutex> ul(bg_work_signals_.terminating_lock);
  bg_work_signals_.terminating = false;
  bg_threads_.emplace_back(&KVEngine::backgroundPMemAllocatorOrgnizer, this);
  bg_threads_.emplace_back(&KVEngine::backgroundDramCleaner, this);
  bg_threads_.emplace_back(&KVEngine::backgroundPMemUsageReporter, this);

  bool close_reclaimer = false;
  TEST_SYNC_POINT_CALLBACK("KVEngine::backgroundCleaner::NothingToDo",
                           &close_reclaimer);
  if (!close_reclaimer) {
    cleaner_.StartClean();
  }
}

void KVEngine::terminateBackgroundWorks() {
  cleaner_.CloseAllWorkers();
  {
    std::unique_lock<SpinMutex> ul(bg_work_signals_.terminating_lock);
    bg_work_signals_.terminating = true;
    bg_work_signals_.dram_cleaner_cv.notify_all();
    bg_work_signals_.pmem_allocator_organizer_cv.notify_all();
    bg_work_signals_.pmem_usage_reporter_cv.notify_all();
  }
  for (auto& t : bg_threads_) {
    t.join();
  }
}

Status KVEngine::Init(const std::string& name, const Configs& configs) {
  Status s;
  if (!configs.use_devdax_mode) {
    dir_ = format_dir_path(name);
    int res = create_dir_if_missing(dir_);
    if (res != 0) {
      GlobalLogger.Error("Create engine dir %s error\n", dir_.c_str());
      return Status::IOError;
    }

    batch_log_dir_ = dir_ + "batch_logs/";
    if (create_dir_if_missing(batch_log_dir_) != 0) {
      GlobalLogger.Error("Create batch log dir %s error\n",
                         batch_log_dir_.c_str());
      return Status::IOError;
    }

    db_file_ = data_file();
    configs_ = configs;

  } else {
    configs_ = configs;
    db_file_ = name;

    // The devdax mode need to execute the shell scripts/init_devdax.sh,
    // then a fsdax model namespace will be created and the
    // configs_.devdax_meta_dir will be created on a xfs file system with a
    // fsdax namespace
    dir_ = format_dir_path(configs_.devdax_meta_dir);

    batch_log_dir_ = dir_ + "batch_logs/";
    if (create_dir_if_missing(batch_log_dir_) != 0) {
      GlobalLogger.Error("Create batch log dir %s error\n",
                         batch_log_dir_.c_str());
      return Status::IOError;
    }
  }

  s = PersistOrRecoverImmutableConfigs();
  if (s != Status::Ok) {
    return s;
  }

  pmem_allocator_.reset(PMEMAllocator::NewPMEMAllocator(
      db_file_, configs_.pmem_file_size, configs_.pmem_segment_blocks,
      configs_.pmem_block_size, configs_.max_access_threads,
      configs_.populate_pmem_space, configs_.use_devdax_mode,
      &version_controller_));
  thread_manager_.reset(new (std::nothrow)
                            ThreadManager(configs_.max_access_threads));
  hash_table_.reset(HashTable::NewHashTable(
      configs_.hash_bucket_num, configs_.num_buckets_per_slot,
      pmem_allocator_.get(), configs_.max_access_threads));
  dllist_locks_.reset(new LockTable{1UL << 20});
  hash_list_locks_.reset(new LockTable{1UL << 20});
  if (pmem_allocator_ == nullptr || hash_table_ == nullptr ||
      thread_manager_ == nullptr || dllist_locks_ == nullptr ||
      hash_list_locks_ == nullptr) {
    GlobalLogger.Error("Init kvdk basic components error\n");
    return Status::Abort;
  }

  s = initOrRestoreCheckpoint();

  RegisterComparator("default", compare_string_view);
  return s;
}

Status KVEngine::RestoreData() {
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }
  EngineThreadCache& engine_thread_cache =
      engine_thread_cache_[access_thread.id];

  SpaceEntry segment_recovering;
  DataEntry data_entry_cached;
  uint64_t cnt = 0;
  while (true) {
    if (segment_recovering.size == 0) {
      if (!pmem_allocator_->FetchSegment(&segment_recovering)) {
        break;
      }
      assert(segment_recovering.size % configs_.pmem_block_size == 0);
    }

    void* recovering_pmem_record =
        pmem_allocator_->offset2addr_checked(segment_recovering.offset);
    memcpy(&data_entry_cached, recovering_pmem_record, sizeof(DataEntry));

    if (data_entry_cached.header.record_size == 0) {
      // Reach end of the segment, mark it as padding
      DataEntry* recovering_pmem_data_entry =
          static_cast<DataEntry*>(recovering_pmem_record);
      uint64_t padding_size = segment_recovering.size;
      recovering_pmem_data_entry->meta.type = RecordType::Empty;
      pmem_persist(&recovering_pmem_data_entry->meta.type, sizeof(RecordType));
      recovering_pmem_data_entry->header.record_size = padding_size;
      pmem_persist(&recovering_pmem_data_entry->header.record_size,
                   sizeof(uint32_t));
      data_entry_cached = *recovering_pmem_data_entry;
    }

    segment_recovering.size -= data_entry_cached.header.record_size;
    segment_recovering.offset += data_entry_cached.header.record_size;

    switch (data_entry_cached.meta.type) {
      case RecordType::SortedElem:
      case RecordType::SortedHeader:
      case RecordType::String:
      case RecordType::HashHeader:
      case RecordType::HashElem:
      case RecordType::ListRecord:
      case RecordType::ListElem: {
        if (data_entry_cached.meta.status == RecordStatus::Dirty) {
          data_entry_cached.meta.type = RecordType::Empty;
        } else {
          if (!ValidateRecord(recovering_pmem_record)) {
            // Checksum dismatch, mark as padding to be Freed
            // Otherwise the Restore will continue normally
            data_entry_cached.meta.type = RecordType::Empty;
          }
        }
        break;
      }
      case RecordType::Empty: {
        break;
      }
      default: {
        // Report Corrupted Record, but still release it and continues
        GlobalLogger.Error(
            "Corrupted Record met when recovering. It has invalid "
            "type. Record data type: %u, Checksum: %u\n",
            data_entry_cached.meta.type, data_entry_cached.header.checksum);
        kvdk_assert(data_entry_cached.header.checksum == 0, "");
        data_entry_cached.meta.type = RecordType::Empty;
        break;
      }
    }

    // When met records with invalid checksum
    // or the space is padding, empty or with corrupted record
    // Free the space and fetch another
    if (data_entry_cached.meta.type == RecordType::Empty) {
      pmem_allocator_->Free(SpaceEntry(
          pmem_allocator_->addr2offset_checked(recovering_pmem_record),
          data_entry_cached.header.record_size));
      continue;
    }

    // Record has valid type and Checksum is correct
    // Continue to restore the Record
    cnt++;

    engine_thread_cache.newest_restored_ts =
        std::max(data_entry_cached.meta.timestamp,
                 engine_thread_cache.newest_restored_ts);

    switch (data_entry_cached.meta.type) {
      case RecordType::SortedElem: {
        s = restoreSortedElem(static_cast<DLRecord*>(recovering_pmem_record));
        break;
      }
      case RecordType::SortedHeader: {
        s = restoreSortedHeader(static_cast<DLRecord*>(recovering_pmem_record));
        break;
      }
      case RecordType::String: {
        s = restoreStringRecord(
            static_cast<StringRecord*>(recovering_pmem_record),
            data_entry_cached);
        break;
      }
      case RecordType::ListRecord: {
        s = listRestoreList(static_cast<DLRecord*>(recovering_pmem_record));
        break;
      }
      case RecordType::ListElem: {
        s = listRestoreElem(static_cast<DLRecord*>(recovering_pmem_record));
        break;
      }
      case RecordType::HashHeader: {
        s = restoreHashHeader(static_cast<DLRecord*>(recovering_pmem_record));
        break;
      }
      case RecordType::HashElem: {
        s = restoreHashElem(static_cast<DLRecord*>(recovering_pmem_record));
        break;
      }
      default: {
        GlobalLogger.Error(
            "Invalid Record type when recovering. Trying "
            "restoring record. Record data type: %u\n",
            data_entry_cached.meta.type);
        s = Status::Abort;
      }
    }
    if (s != Status::Ok) {
      break;
    }
  }
  restored_.fetch_add(cnt);
  ReleaseAccessThread();
  return s;
}

bool KVEngine::ValidateRecord(void* data_record) {
  assert(data_record);
  DataEntry* entry = static_cast<DataEntry*>(data_record);
  switch (entry->meta.type) {
    case RecordType::String: {
      return static_cast<StringRecord*>(data_record)->Validate();
    }
    case RecordType::SortedHeader:
    case RecordType::SortedElem:
    case RecordType::HashHeader:
    case RecordType::HashElem:
    case RecordType::ListRecord:
    case RecordType::ListElem: {
      return static_cast<DLRecord*>(data_record)->Validate();
    }
    default:
      kvdk_assert(false, "Unsupported type in ValidateRecord()!");
      return false;
  }
}

Status KVEngine::PersistOrRecoverImmutableConfigs() {
  size_t mapped_len;
  int is_pmem;
  uint64_t len =
      kPMEMMapSizeUnit *
      (size_t)ceil(1.0 * sizeof(ImmutableConfigs) / kPMEMMapSizeUnit);
  ImmutableConfigs* configs = (ImmutableConfigs*)pmem_map_file(
      config_file().c_str(), len, PMEM_FILE_CREATE, 0666, &mapped_len,
      &is_pmem);
  if (configs == nullptr || !is_pmem || mapped_len != len) {
    GlobalLogger.Error(
        "Open immutable configs file error %s\n",
        !is_pmem ? (dir_ + "is not a valid pmem path").c_str() : "");
    return Status::IOError;
  }
  if (configs->Valid()) {
    configs->AssignImmutableConfigs(configs_);
  }

  Status s = CheckConfigs(configs_);
  if (s == Status::Ok) {
    configs->PersistImmutableConfigs(configs_);
  }
  pmem_unmap(configs, len);
  return s;
}

Status KVEngine::Backup(const pmem::obj::string_view backup_log,
                        const Snapshot* snapshot) {
  std::string backup_log_file = string_view_2_string(backup_log);
  BackupLog backup;
  Status s = backup.Init(backup_log_file);
  GlobalLogger.Info("Backup instance to %s ...\n", backup_log_file.c_str());
  if (s != Status::Ok) {
    return s;
  }
  TimeStampType backup_ts =
      static_cast<const SnapshotImpl*>(snapshot)->GetTimestamp();
  auto hashtable_iterator =
      hash_table_->GetIterator(0, hash_table_->GetSlotsNum());
  while (hashtable_iterator.Valid()) {
    auto ul = hashtable_iterator.AcquireSlotLock();
    auto slot_iter = hashtable_iterator.Slot();
    while (slot_iter.Valid()) {
      switch (slot_iter->GetRecordType()) {
        case RecordType::String: {
          StringRecord* record = slot_iter->GetIndex().string_record;
          while (record != nullptr && record->GetTimestamp() > backup_ts) {
            record =
                pmem_allocator_->offset2addr<StringRecord>(record->old_version);
          }
          if (record && record->GetRecordStatus() == RecordStatus::Normal &&
              !record->HasExpired()) {
            s = backup.Append(RecordType::String, record->Key(),
                              record->Value(), record->GetExpireTime());
          }
          break;
        }
        case RecordType::SortedHeader: {
          DLRecord* header = slot_iter->GetIndex().skiplist->HeaderRecord();
          while (header != nullptr && header->GetTimestamp() > backup_ts) {
            header =
                pmem_allocator_->offset2addr<DLRecord>(header->old_version);
          }
          if (header && header->GetRecordStatus() == RecordStatus::Normal &&
              !header->HasExpired()) {
            s = backup.Append(RecordType::SortedHeader, header->Key(),
                              header->Value(), header->GetExpireTime());
            if (s == Status::Ok) {
              // Append skiplist elems following the header
              auto skiplist = getSkiplist(Skiplist::SkiplistID(header));
              kvdk_assert(skiplist != nullptr,
                          "Backup skiplist should exist in map");
              auto skiplist_iter = SortedIterator(
                  skiplist.get(), pmem_allocator_.get(),
                  static_cast<const SnapshotImpl*>(snapshot), false);
              for (skiplist_iter.SeekToFirst(); skiplist_iter.Valid();
                   skiplist_iter.Next()) {
                s = backup.Append(RecordType::SortedElem, skiplist_iter.Key(),
                                  skiplist_iter.Value(), kPersistTime);
                if (s != Status::Ok) {
                  break;
                }
              }
            }
          }
          break;
        }
        case RecordType::HashHeader: {
          DLRecord* header = slot_iter->GetIndex().hlist->HeaderRecord();
          while (header != nullptr && header->GetTimestamp() > backup_ts) {
            header =
                pmem_allocator_->offset2addr<DLRecord>(header->old_version);
          }
          if (header && header->GetRecordStatus() == RecordStatus::Normal &&
              !header->HasExpired()) {
            s = backup.Append(RecordType::HashHeader, header->Key(),
                              header->Value(), header->GetExpireTime());
            if (s == Status::Ok) {
              // Append hlist elems following the header
              auto hlist = getHashlist(HashList::HashListID(header));
              kvdk_assert(hlist != nullptr, "Backup hlist should exist in map");
              auto hlist_iter = HashIteratorImpl(
                  this, hlist.get(), static_cast<const SnapshotImpl*>(snapshot),
                  false);
              for (hlist_iter.SeekToFirst(); hlist_iter.Valid();
                   hlist_iter.Next()) {
                s = backup.Append(RecordType::HashElem, hlist_iter.Key(),
                                  hlist_iter.Value(), kPersistTime);
                if (s != Status::Ok) {
                  break;
                }
              }
            }
          }
        }
        default:
          // Hash and list backup is not supported yet
          break;
      }
      if (s != Status::Ok) {
        backup.Destroy();
        return s;
      }
      slot_iter++;
    }
    hashtable_iterator.Next();
  }
  backup.Finish();
  GlobalLogger.Info("Backup instance to %s Finished\n",
                    backup_log_file.c_str());
  return Status::Ok;
}

Status KVEngine::initOrRestoreCheckpoint() {
  size_t mapped_len;
  int is_pmem;
  persist_checkpoint_ = static_cast<CheckPoint*>(
      pmem_map_file(checkpoint_file().c_str(), sizeof(CheckPoint),
                    PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem));
  if (persist_checkpoint_ == nullptr || !is_pmem ||
      mapped_len != sizeof(CheckPoint)) {
    GlobalLogger.Error("Map persistent checkpoint file %s failed\n",
                       checkpoint_file().c_str());
    return Status::IOError;
  }
  return Status::Ok;
}

Status KVEngine::restoreDataFromBackup(const std::string& backup_log) {
  // Todo: make this multi-thread
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }
  defer(ReleaseAccessThread());
  BackupLog backup;
  s = backup.Open(backup_log);
  if (s != Status::Ok) {
    return s;
  }
  auto wo = WriteOptions();
  auto iter = backup.GetIterator();
  if (iter == nullptr) {
    GlobalLogger.Error("Restore from a not finished backup log %s\n",
                       backup_log.c_str());
    return Status::Abort;
  }
  uint64_t cnt = 0;
  GlobalLogger.Info("Start iterating backup log\n");
  while (iter->Valid()) {
    auto record = iter->Record();
    bool expired = TimeUtils::CheckIsExpired(record.expire_time);
    wo.ttl_time = record.expire_time;
    switch (record.type) {
      case RecordType::String: {
        if (!expired) {
          cnt++;
          s = Put(record.key, record.val, wo);
        }
        iter->Next();
        break;
      }
      case RecordType::SortedHeader: {
        // Maybe reuse id?
        std::shared_ptr<Skiplist> skiplist = nullptr;
        if (!expired) {
          CollectionIDType id;
          SortedCollectionConfigs s_configs;
          s = Skiplist::DecodeSortedCollectionValue(record.val, id, s_configs);
          if (s != Status::Ok) {
            break;
          }
          s = buildSkiplist(record.key, s_configs, skiplist);
          if (s == Status::Ok && wo.ttl_time != kPersistTime) {
            skiplist->SetExpireTime(wo.ttl_time,
                                    version_controller_.GetCurrentTimestamp());
          }
          if (s != Status::Ok) {
            break;
          }
          cnt++;
        }
        iter->Next();
        // the header is followed by all its elems in backup log
        while (iter->Valid()) {
          record = iter->Record();
          if (record.type != RecordType::SortedElem) {
            break;
          }
          if (!expired) {
            auto ret = skiplist->Put(record.key, record.val,
                                     version_controller_.GetCurrentTimestamp());
            s = ret.s;
            if (s != Status::Ok) {
              break;
            }
            cnt++;
          }
          iter->Next();
        }
        break;
      }
      case RecordType::HashHeader: {
        std::shared_ptr<HashList> hlist = nullptr;
        if (!expired) {
          s = buildHashlist(record.key, hlist);
          if (s == Status::Ok && wo.ttl_time != kPersistTime) {
            hlist->SetExpireTime(wo.ttl_time,
                                 version_controller_.GetCurrentTimestamp());
          }
          if (s != Status::Ok) {
            break;
          }
          cnt++;
        }
        iter->Next();
        // the header is followed by all its elems in backup log
        while (iter->Valid()) {
          record = iter->Record();
          if (record.type != RecordType::HashElem) {
            break;
          }
          if (!expired) {
            auto ret = hlist->Put(record.key, record.val,
                                  version_controller_.GetCurrentTimestamp());
            s = ret.s;
            if (s != Status::Ok) {
              break;
            }
            cnt++;
          }
          iter->Next();
        }
        break;
      }
      case RecordType::SortedElem: {
        GlobalLogger.Error("sorted elems not lead by header in backup log %s\n",
                           backup_log.c_str());
        return Status::Abort;
      }
      case RecordType::HashElem: {
        GlobalLogger.Error("hash elems not lead by header in backup log %s\n",
                           backup_log.c_str());
        return Status::Abort;
      }
      default:
        GlobalLogger.Error("unsupported record type %u in backup log %s\n",
                           record.type, backup_log.c_str());
        return Status::Abort;
    }
    if (s != Status::Ok) {
      return Status::Abort;
    }
  }
  GlobalLogger.Info("Restore %lu records from backup log\n", cnt);
  return Status::Ok;
}

Status KVEngine::restoreExistingData() {
  access_thread.id = 0;
  defer(access_thread.id = -1);

  sorted_rebuilder_.reset(new SortedCollectionRebuilder(
      this, configs_.opt_large_sorted_collection_recovery,
      configs_.max_access_threads, *persist_checkpoint_));
  list_builder_.reset(new ListBuilder{pmem_allocator_.get(), &lists_,
                                      configs_.max_access_threads, nullptr});
  hash_rebuilder_.reset(
      new HashListRebuilder(pmem_allocator_.get(), hash_table_.get(),
                            dllist_locks_.get(), thread_manager_.get(),
                            configs_.max_access_threads, *persist_checkpoint_));

  Status s = batchWriteRollbackLogs();
  if (s != Status::Ok) {
    return s;
  }

  std::vector<std::future<Status>> fs;
  GlobalLogger.Info("Start restore data\n");
  for (uint32_t i = 0; i < configs_.max_access_threads; i++) {
    fs.push_back(std::async(&KVEngine::RestoreData, this));
  }

  for (auto& f : fs) {
    s = f.get();
    if (s != Status::Ok) {
      return s;
    }
  }
  fs.clear();

  GlobalLogger.Info("RestoreData done: iterated %lu records\n",
                    restored_.load());

  // restore skiplist by two optimization strategy
  auto s_ret = sorted_rebuilder_->Rebuild();
  if (s_ret.s != Status::Ok) {
    return s_ret.s;
  }
  if (list_id_.load() <= s_ret.max_id) {
    list_id_.store(s_ret.max_id + 1);
  }
  skiplists_.swap(s_ret.rebuild_skiplits);

  GlobalLogger.Info("Rebuild skiplist done\n");
  sorted_rebuilder_.reset(nullptr);

  list_builder_->RebuildLists();
  list_builder_->CleanBrokens([&](DLRecord* elem) { directFree(elem); });
  s = listRegisterRecovered();
  if (s != Status::Ok) {
    return s;
  }
  list_builder_.reset(nullptr);
  GlobalLogger.Info("Rebuild Lists done\n");

  auto h_ret = hash_rebuilder_->Rebuild();
  if (h_ret.s != Status::Ok) {
    return s_ret.s;
  }
  if (list_id_.load() <= h_ret.max_id) {
    list_id_.store(h_ret.max_id + 1);
  }
  hlists_.swap(h_ret.rebuilt_hlists);
  GlobalLogger.Info("Rebuild HashLists done\n");
  hash_rebuilder_.reset(nullptr);

#if KVDK_DEBUG_LEVEL > 0
  for (auto skiplist : skiplists_) {
    Status s = skiplist.second->CheckIndex();
    if (s != Status::Ok) {
      GlobalLogger.Error("Check skiplist index error\n");
      return s;
    }
  }

  for (auto hlist : hlists_) {
    Status s = hlist.second->CheckIndex();
    if (s != Status::Ok) {
      GlobalLogger.Error("Check hash index error\n");
      return s;
    }
  }
#endif

  uint64_t latest_version_ts = 0;
  if (restored_.load() > 0) {
    for (size_t i = 0; i < engine_thread_cache_.size(); i++) {
      auto& engine_thread_cache = engine_thread_cache_[i];
      latest_version_ts =
          std::max(engine_thread_cache.newest_restored_ts, latest_version_ts);
    }
  }

  persist_checkpoint_->Release();
  pmem_persist(persist_checkpoint_, sizeof(CheckPoint));

  version_controller_.Init(latest_version_ts);
  old_records_cleaner_.TryGlobalClean();
  kvdk_assert(pmem_allocator_->PMemUsageInBytes() >= 0, "Invalid PMem Usage");
  return Status::Ok;
}

Status KVEngine::CheckConfigs(const Configs& configs) {
  auto is_2pown = [](uint64_t n) { return (n > 0) && (n & (n - 1)) == 0; };

  if (configs.pmem_block_size < kMinPMemBlockSize) {
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
    GlobalLogger.Error(
        "pmem file size should align to segment "
        "size(pmem_segment_blocks*pmem_block_size) (%d bytes)\n",
        segment_size);
    return Status::InvalidConfiguration;
  }

  if (configs.pmem_segment_blocks * configs.pmem_block_size *
          configs.max_access_threads >
      configs.pmem_file_size) {
    GlobalLogger.Error(
        "pmem file too small, should larger than pmem_segment_blocks * "
        "pmem_block_size * max_access_threads\n");
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

Status KVEngine::maybeInitBatchLogFile() {
  auto& tc = engine_thread_cache_[access_thread.id];
  if (tc.batch_log == nullptr) {
    int is_pmem;
    size_t mapped_len;
    std::string log_file_name =
        batch_log_dir_ + std::to_string(access_thread.id);
    void* addr = pmem_map_file(log_file_name.c_str(), BatchWriteLog::MaxBytes(),
                               PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
    if (addr == NULL) {
      GlobalLogger.Error("Fail to Init BatchLog file. %s\n", strerror(errno));
      return Status::PMemMapFileError;
    }
    kvdk_assert(is_pmem != 0 && mapped_len >= BatchWriteLog::MaxBytes(), "");
    tc.batch_log = static_cast<char*>(addr);
  }
  return Status::Ok;
}

Status KVEngine::BatchWrite(std::unique_ptr<WriteBatch> const& batch) {
  WriteBatchImpl const* batch_impl =
      dynamic_cast<WriteBatchImpl const*>(batch.get());
  if (batch_impl == nullptr) {
    return Status::InvalidArgument;
  }

  return batchWriteImpl(*batch_impl);
}

Status KVEngine::batchWriteImpl(WriteBatchImpl const& batch) {
  if (batch.Size() > BatchWriteLog::Capacity()) {
    return Status::InvalidBatchSize;
  }

  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  s = maybeInitBatchLogFile();
  if (s != Status::Ok) {
    return s;
  }

  // Prevent collection and nodes in double linked lists from being deleted
  auto access_token = version_controller_.GetLocalSnapshotHolder();

  std::vector<StringWriteArgs> string_args;
  string_args.reserve(batch.StringOps().size());
  std::vector<SortedWriteArgs> sorted_args;
  sorted_args.reserve(batch.SortedOps().size());
  std::vector<HashWriteArgs> hash_args;
  hash_args.reserve(batch.HashOps().size());

  for (auto const& string_op : batch.StringOps()) {
    string_args.emplace_back();
    string_args.back().Assign(string_op);
  }

  // Lookup Skiplists and Hashes for further operations
  for (auto const& sorted_op : batch.SortedOps()) {
    auto res = lookupKey<false>(sorted_op.collection, RecordType::SortedHeader);
    /// TODO: this is a temporary work-around
    /// We cannot lock both key and field, which may trigger deadlock.
    /// However, if a collection is created and a field is inserted,
    /// Delete operation in batch may skip this field,
    /// which causes inconsistency.
    if (res.s == Status::Outdated) {
      return Status::NotFound;
    }
    if (res.s != Status::Ok) {
      return res.s;
    }
    sorted_args.emplace_back();
    sorted_args.back().Assign(sorted_op);
    sorted_args.back().skiplist = res.entry.GetIndex().skiplist;
  }

  for (auto const& hash_op : batch.HashOps()) {
    HashList* hlist;
    Status s = hashListFind(hash_op.collection, &hlist);
    if (s != Status::Ok) {
      return s;
    }
    hash_args.emplace_back();
    hash_args.back().Assign(hash_op);
    hash_args.back().hlist = hlist;
  }

  // Keys/internal keys to be locked on HashTable
  std::vector<std::string> keys_to_lock;
  for (auto const& string_op : batch.StringOps()) {
    keys_to_lock.push_back(string_op.key);
  }
  for (auto const& arg : sorted_args) {
    keys_to_lock.push_back(arg.skiplist->InternalKey(arg.key));
  }
  for (auto const& arg : hash_args) {
    keys_to_lock.push_back(arg.hlist->InternalKey(arg.key));
  }

  auto guard = hash_table_->RangeLock(keys_to_lock);
  keys_to_lock.clear();

  // Lookup keys, allocate space according to result.
  auto ReleaseResources = [&]() {
  // Don't Free() if we simulate a crash.
#ifndef KVDK_ENABLE_CRASHPOINT
    for (auto iter = hash_args.rbegin(); iter != hash_args.rend(); ++iter) {
      pmem_allocator_->Free(iter->space);
      if (iter->res.entry_ptr->Allocated()) {
        kvdk_assert(iter->res.s == Status::NotFound, "");
        iter->res.entry_ptr->clear();
      }
    }
    for (auto iter = sorted_args.rbegin(); iter != sorted_args.rend(); ++iter) {
      pmem_allocator_->Free(iter->space);
      if (iter->lookup_result.entry_ptr->Allocated()) {
        kvdk_assert(iter->lookup_result.s == Status::NotFound, "");
        iter->lookup_result.entry_ptr->clear();
      }
    }
    for (auto iter = string_args.rbegin(); iter != string_args.rend(); ++iter) {
      pmem_allocator_->Free(iter->space);
      if (iter->res.entry_ptr->Allocated()) {
        kvdk_assert(iter->res.s == Status::NotFound, "");
        iter->res.entry_ptr->clear();
      }
    }
#endif
  };

  defer(ReleaseResources());

  // Prevent generating snapshot newer than this WriteBatch
  auto bw_token = version_controller_.GetBatchWriteToken();

  // Prepare for Strings
  for (auto& args : string_args) {
    args.ts = bw_token.Timestamp();
    Status s = stringWritePrepare(args);
    if (s != Status::Ok) {
      return s;
    }
  }

  // Prepare for Sorted Elements
  for (auto& args : sorted_args) {
    args.ts = bw_token.Timestamp();
    Status s = sortedWritePrepare(args);
    if (s != Status::Ok) {
      return s;
    }
  }

  // Prepare for Hash Elements
  for (auto& args : hash_args) {
    Status s = hashWritePrepare(args, bw_token.Timestamp());
    if (s != Status::Ok) {
      return s;
    }
  }

  // Preparation done. Persist BatchLog for rollback.
  BatchWriteLog log;
  log.SetTimestamp(bw_token.Timestamp());
  auto& tc = engine_thread_cache_[access_thread.id];
  for (auto& args : string_args) {
    if (args.space.size == 0) {
      continue;
    }
    if (args.op == WriteBatchImpl::Op::Put) {
      log.StringPut(args.space.offset);
    } else {
      log.StringDelete(args.space.offset);
    }
  }
  for (auto& args : sorted_args) {
    if (args.space.size == 0) {
      continue;
    }
    if (args.op == WriteBatchImpl::Op::Put) {
      log.SortedPut(args.space.offset);
    } else {
      log.SortedDelete(args.space.offset);
    }
  }

  for (auto& args : hash_args) {
    if (args.space.size == 0) {
      continue;
    }
    if (args.op == WriteBatchImpl::Op::Put) {
      log.HashPut(args.space.offset);
    } else {
      log.HashDelete(args.space.offset);
    }
  }

  log.EncodeTo(tc.batch_log);

  BatchWriteLog::MarkProcessing(tc.batch_log);

  // After preparation stage, no runtime error is allowed for now,
  // otherwise we have to perform runtime rollback.

  // Write Strings
  for (auto& args : string_args) {
    if (args.space.size == 0) {
      continue;
    }
    Status s = stringWrite(args);
    kvdk_assert(s == Status::Ok, "");
  }

  // Write Sorted Elems
  for (auto& args : sorted_args) {
    if (args.space.size == 0) {
      continue;
    }
    Status s = sortedWrite(args);
    kvdk_assert(s == Status::Ok, "");
  }

  // Write Hash Elems
  for (auto& args : hash_args) {
    if (args.space.size == 0) {
      continue;
    }
    Status s = hashListWrite(args);
    kvdk_assert(s == Status::Ok, "");
  }

  TEST_CRASH_POINT("KVEngine::batchWriteImpl::BeforeCommit", "");

  BatchWriteLog::MarkCommitted(tc.batch_log);

  // Publish stages is where Strings and Collections make BatchWrite
  // visible to other threads.
  // This stage allows no failure during runtime,
  // otherwise dirty read may occur.
  // Crash is tolerated as BatchWrite will be recovered.

  // Publish Strings to HashTable
  for (auto const& args : string_args) {
    if (args.space.size == 0) {
      continue;
    }
    Status s = stringWritePublish(args);
    kvdk_assert(s == Status::Ok, "");
  }

  // Publish Sorted Elements to HashTable
  for (auto const& args : sorted_args) {
    if (args.space.size == 0) {
      continue;
    }
    Status s = sortedWritePublish(args);
    kvdk_assert(s == Status::Ok, "");
  }

  // Publish Hash Elements to HashTable
  for (auto& args : hash_args) {
    if (args.space.size == 0) {
      continue;
    }
    Status s = hashListPublish(args);
    kvdk_assert(s == Status::Ok, "");
  }

  hash_args.clear();
  sorted_args.clear();
  string_args.clear();

  return Status::Ok;
}

Status KVEngine::batchWriteRollbackLogs() {
  DIR* dir = opendir(batch_log_dir_.c_str());
  if (dir == NULL) {
    GlobalLogger.Error("Fail to opendir in batchWriteRollbackLogs. %s\n",
                       strerror(errno));
    return Status::IOError;
  }
  dirent* entry;
  while ((entry = readdir(dir)) != NULL) {
    std::string fname = std::string{entry->d_name};
    if (fname == "." || fname == "..") {
      continue;
    }
    std::string log_file_path = batch_log_dir_ + fname;
    size_t mapped_len;
    int is_pmem;
    void* addr = pmem_map_file(log_file_path.c_str(), BatchWriteLog::MaxBytes(),
                               PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
    if (addr == NULL) {
      GlobalLogger.Error("Fail to Rollback BatchLog file. %s\n",
                         strerror(errno));
      return Status::PMemMapFileError;
    }
    kvdk_assert(is_pmem != 0 && mapped_len >= BatchWriteLog::MaxBytes(), "");

    BatchWriteLog log;
    log.DecodeFrom(static_cast<char*>(addr));

    Status s;
    for (auto iter = log.ListLogs().rbegin(); iter != log.ListLogs().rend();
         ++iter) {
      if (iter->op != BatchWriteLog::Op::Delete) {
        DLRecord* rec = static_cast<DLRecord*>(
            pmem_allocator_->offset2addr_checked(iter->offset));
        if (!rec->Validate() || rec->entry.meta.timestamp != log.Timestamp()) {
          continue;
        }
      }
      s = listRollback(*iter);
      if (s != Status::Ok) {
        return s;
      }
    }
    for (auto iter = log.HashLogs().rbegin(); iter != log.HashLogs().rend();
         ++iter) {
      if (iter->op != BatchWriteLog::Op::Delete) {
        DLRecord* rec = static_cast<DLRecord*>(
            pmem_allocator_->offset2addr_checked(iter->offset));
        if (!rec->Validate() || rec->entry.meta.timestamp != log.Timestamp()) {
          continue;
        }
      }
      s = hashListRollback(*iter);
      if (s != Status::Ok) {
        return s;
      }
    }
    for (auto iter = log.SortedLogs().rbegin(); iter != log.SortedLogs().rend();
         ++iter) {
      s = sortedRollback(log.Timestamp(), *iter);
      if (s != Status::Ok) {
        return s;
      }
    }
    for (auto iter = log.StringLogs().rbegin(); iter != log.StringLogs().rend();
         ++iter) {
      s = stringRollback(log.Timestamp(), *iter);
      if (s != Status::Ok) {
        return s;
      }
    }
    log.MarkInitializing(static_cast<char*>(addr));
    if (pmem_unmap(addr, mapped_len) != 0) {
      GlobalLogger.Error("Fail to Rollback BatchLog file. %s\n",
                         strerror(errno));
      return Status::PMemMapFileError;
    }
  }
  closedir(dir);
  std::string cmd{"rm -rf " + batch_log_dir_ + "*"};
  [[gnu::unused]] int ret = system(cmd.c_str());

  return Status::Ok;
}

Status KVEngine::GetTTL(const StringView str, TTLType* ttl_time) {
  *ttl_time = kInvalidTTL;
  auto ul = hash_table_->AcquireLock(str);
  auto res = lookupKey<false>(str, ExpirableRecordType);

  if (res.s == Status::Ok) {
    ExpireTimeType expire_time;
    switch (res.entry_ptr->GetIndexType()) {
      case PointerType::Skiplist: {
        expire_time = res.entry_ptr->GetIndex().skiplist->GetExpireTime();
        break;
      }
      case PointerType::List: {
        expire_time = res.entry_ptr->GetIndex().list->GetExpireTime();
        break;
      }
      case PointerType::HashList: {
        expire_time = res.entry_ptr->GetIndex().hlist->GetExpireTime();
        break;
      }
      case PointerType::StringRecord: {
        expire_time = res.entry_ptr->GetIndex().string_record->GetExpireTime();
        break;
      }
      default: {
        return Status::NotSupported;
      }
    }
    // return ttl time
    *ttl_time = TimeUtils::ExpireTimeToTTL(expire_time);
  }
  return res.s == Status::Outdated ? Status::NotFound : res.s;
}

Status KVEngine::TypeOf(StringView key, ValueType* type) {
  auto res = lookupKey<false>(key, ExpirableRecordType);

  if (res.s == Status::Ok) {
    switch (res.entry_ptr->GetIndexType()) {
      case PointerType::Skiplist: {
        *type = ValueType::SortedSet;
        break;
      }
      case PointerType::List: {
        *type = ValueType::List;
        break;
      }
      case PointerType::HashList: {
        *type = ValueType::HashSet;
        break;
      }
      case PointerType::StringRecord: {
        *type = ValueType::String;
        break;
      }
      default: {
        return Status::Abort;
      }
    }
  }
  return res.s == Status::Outdated ? Status::NotFound : res.s;
}

Status KVEngine::Expire(const StringView str, TTLType ttl_time) {
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  int64_t base_time = TimeUtils::millisecond_time();
  if (!TimeUtils::CheckTTL(ttl_time, base_time)) {
    return Status::InvalidArgument;
  }

  ExpireTimeType expired_time = TimeUtils::TTLToExpireTime(ttl_time, base_time);
  auto ul = hash_table_->AcquireLock(str);
  auto snapshot_holder = version_controller_.GetLocalSnapshotHolder();
  // TODO: maybe have a wrapper function(lookupKeyAndMayClean).
  auto res = lookupKey<false>(str, ExpirableRecordType);
  if (res.s == Status::Outdated) {
    return Status::NotFound;
  }

  if (res.s == Status::Ok) {
    WriteOptions write_option{ttl_time};
    switch (res.entry_ptr->GetIndexType()) {
      case PointerType::StringRecord: {
        ul.unlock();
        version_controller_.ReleaseLocalSnapshot();
        res.s = Modify(
            str,
            [](const std::string* old_val, std::string* new_val, void*) {
              new_val->assign(*old_val);
              return ModifyOperation::Write;
            },
            nullptr, write_option);
        break;
      }
      case PointerType::Skiplist: {
        auto new_ts = snapshot_holder.Timestamp();
        auto ret = res.entry_ptr->GetIndex().skiplist->SetExpireTime(
            expired_time, new_ts);
        res.s = ret.s;
        break;
      }
      case PointerType::HashList: {
        auto new_ts = snapshot_holder.Timestamp();
        HashList* hlist = res.entry_ptr->GetIndex().hlist;
        res.s = hlist->SetExpireTime(expired_time, new_ts).s;
        break;
      }
      case PointerType::List: {
        List* list = res.entry_ptr->GetIndex().list;
        res.s = listExpire(list, expired_time);
        break;
      }
      default: {
        return Status::NotSupported;
      }
    }
  }
  return res.s;
}
}  // namespace KVDK_NAMESPACE

// lookupKey
namespace KVDK_NAMESPACE {
template <bool may_insert>
HashTable::LookupResult KVEngine::lookupElem(StringView key,
                                             uint8_t type_mask) {
  kvdk_assert(type_mask & (RecordType::HashElem | RecordType::SortedElem), "");
  return hash_table_->Lookup<may_insert>(key, type_mask);
}

template HashTable::LookupResult KVEngine::lookupElem<true>(StringView,
                                                            uint8_t);
template HashTable::LookupResult KVEngine::lookupElem<false>(StringView,
                                                             uint8_t);

template <bool may_insert>
HashTable::LookupResult KVEngine::lookupKey(StringView key, uint8_t type_mask) {
  auto result = hash_table_->Lookup<may_insert>(key, PrimaryRecordType);

  if (result.s != Status::Ok) {
    kvdk_assert(
        result.s == Status::NotFound || result.s == Status::MemoryOverflow, "");
    return result;
  }

  RecordType type = result.entry.GetRecordType();
  RecordStatus record_status = result.entry.GetRecordStatus();
  bool type_match = type_mask & type;

  // TODO: fix mvcc of different type keys
  if (!type_match) {
    result.s = Status::WrongType;
  } else if (record_status == RecordStatus::Outdated) {
    result.s = Status::Outdated;
  } else {
    switch (type) {
      case RecordType::String: {
        result.s = result.entry.GetIndex().string_record->HasExpired()
                       ? Status::Outdated
                       : Status::Ok;
        break;
      }
      case RecordType::SortedHeader:
      case RecordType::ListRecord:
      case RecordType::HashHeader: {
        result.s =
            static_cast<Collection*>(result.entry.GetIndex().ptr)->HasExpired()
                ? Status::Outdated
                : Status::Ok;
        break;
      }
      default: {
        kvdk_assert(false, "Unreachable branch!");
        std::abort();
      }
    }
  }

  return result;
}

template HashTable::LookupResult KVEngine::lookupKey<true>(StringView, uint8_t);
template HashTable::LookupResult KVEngine::lookupKey<false>(StringView,
                                                            uint8_t);

template <typename T>
T* KVEngine::removeOutDatedVersion(T* record, TimeStampType min_snapshot_ts) {
  static_assert(
      std::is_same<T, StringRecord>::value || std::is_same<T, DLRecord>::value,
      "Invalid record type, should be StringRecord or DLRecord.");
  T* ret = nullptr;
  auto old_record = record;
  while (old_record && old_record->entry.meta.timestamp > min_snapshot_ts) {
    old_record =
        static_cast<T*>(pmem_allocator_->offset2addr(old_record->old_version));
  }

  // the snapshot should access the old record, so we need to purge and free the
  // older version of the old record
  if (old_record && old_record->old_version != kNullPMemOffset) {
    T* remove_record =
        pmem_allocator_->offset2addr_checked<T>(old_record->old_version);
    ret = remove_record;
    old_record->PersistOldVersion(kNullPMemOffset);
    while (remove_record != nullptr) {
      if (remove_record->GetRecordStatus() == RecordStatus::Normal) {
        remove_record->PersistStatus(RecordStatus::Dirty);
      }
      remove_record =
          pmem_allocator_->offset2addr<T>(remove_record->old_version);
    }
  }
  return ret;
}

template StringRecord* KVEngine::removeOutDatedVersion<StringRecord>(
    StringRecord*, TimeStampType);
template DLRecord* KVEngine::removeOutDatedVersion<DLRecord>(DLRecord*,
                                                             TimeStampType);

}  // namespace KVDK_NAMESPACE

// Snapshot, delayFree and background work
namespace KVDK_NAMESPACE {

/// TODO: move this into VersionController.
Snapshot* KVEngine::GetSnapshot(bool make_checkpoint) {
  Snapshot* ret = version_controller_.NewGlobalSnapshot();

  if (make_checkpoint) {
    std::lock_guard<std::mutex> lg(checkpoint_lock_);
    persist_checkpoint_->MakeCheckpoint(ret);
    pmem_persist(persist_checkpoint_, sizeof(CheckPoint));
  }

  return ret;
}

void KVEngine::delayFree(DLRecord* addr) {
  if (addr == nullptr) {
    return;
  }
  /// TODO: avoid deadlock in cleaner to help Free() deleted records
  old_records_cleaner_.PushToPendingFree(
      addr, version_controller_.GetCurrentTimestamp());
}

void KVEngine::directFree(DLRecord* addr) {
  if (addr == nullptr) {
    return;
  }
  pmem_allocator_->Free(SpaceEntry{pmem_allocator_->addr2offset_checked(addr),
                                   addr->entry.header.record_size});
}

void KVEngine::backgroundPMemUsageReporter() {
  auto interval = std::chrono::milliseconds{
      static_cast<std::uint64_t>(configs_.report_pmem_usage_interval * 1000)};
  while (!bg_work_signals_.terminating) {
    {
      std::unique_lock<SpinMutex> ul(bg_work_signals_.terminating_lock);
      if (!bg_work_signals_.terminating) {
        bg_work_signals_.pmem_usage_reporter_cv.wait_for(ul, interval);
      }
    }
    ReportPMemUsage();
  }
}

void KVEngine::backgroundPMemAllocatorOrgnizer() {
  auto interval = std::chrono::milliseconds{
      static_cast<std::uint64_t>(configs_.background_work_interval * 1000)};
  while (!bg_work_signals_.terminating) {
    {
      std::unique_lock<SpinMutex> ul(bg_work_signals_.terminating_lock);
      if (!bg_work_signals_.terminating) {
        bg_work_signals_.pmem_allocator_organizer_cv.wait_for(ul, interval);
      }
    }
    pmem_allocator_->BackgroundWork();
  }
}

void KVEngine::backgroundDramCleaner() {
  auto interval = std::chrono::milliseconds{1000};
  while (!bg_work_signals_.terminating) {
    {
      std::unique_lock<SpinMutex> ul(bg_work_signals_.terminating_lock);
      if (!bg_work_signals_.terminating) {
        bg_work_signals_.dram_cleaner_cv.wait_for(ul, interval);
      }
    }
    FreeSkiplistDramNodes();
  }
}

void KVEngine::deleteCollections() {
  std::set<List*>::iterator list_it = lists_.begin();
  while (list_it != lists_.end()) {
    delete *list_it;
    list_it = lists_.erase(list_it);
  }
};

}  // namespace KVDK_NAMESPACE
