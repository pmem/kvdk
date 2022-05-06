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

#include "configs.hpp"
#include "dram_allocator.hpp"
#include "kvdk/engine.hpp"
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
  deleteCollections();
  ReportPMemUsage();
  GlobalLogger.Info("Instance closed\n");
}

Status KVEngine::Open(const std::string& name, Engine** engine_ptr,
                      const Configs& configs) {
  KVEngine* engine = new KVEngine(configs);
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
  for (auto& skiplist : skiplists_) {
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
  bg_threads_.emplace_back(&KVEngine::backgroundOldRecordCleaner, this);
  bg_threads_.emplace_back(&KVEngine::backgroundDramCleaner, this);
  bg_threads_.emplace_back(&KVEngine::backgroundPMemUsageReporter, this);
  bg_threads_.emplace_back(&KVEngine::backgroundDestroyCollections, this);
}

void KVEngine::terminateBackgroundWorks() {
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
  access_thread.id = 0;
  defer(access_thread.id = -1);
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
      configs_.pmem_block_size, configs_.max_access_threads,
      configs_.populate_pmem_space, configs_.use_devdax_mode,
      &version_controller_));
  thread_manager_.reset(new (std::nothrow)
                            ThreadManager(configs_.max_access_threads));
  hash_table_.reset(HashTable::NewHashTable(
      configs_.hash_bucket_num, configs_.num_buckets_per_slot,
      pmem_allocator_.get(), configs_.max_access_threads));
  if (pmem_allocator_ == nullptr || hash_table_ == nullptr ||
      thread_manager_ == nullptr) {
    GlobalLogger.Error("Init kvdk basic components error\n");
    return Status::Abort;
  }

  RegisterComparator("default", compare_string_view);
  s = Recovery();
  startBackgroundWorks();

  ReportPMemUsage();
  kvdk_assert(pmem_allocator_->PMemUsageInBytes() >= 0, "Invalid PMem Usage");
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
      recovering_pmem_data_entry->meta.type = RecordType::Padding;
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
      case RecordType::SortedElemDelete:
      case RecordType::SortedHeader:
      case RecordType::SortedHeaderDelete:
      case RecordType::StringDataRecord:
      case RecordType::StringDeleteRecord:
      case RecordType::HashRecord:
      case RecordType::HashElem:
      case RecordType::ListRecord:
      case RecordType::ListElem: {
        if (!ValidateRecord(recovering_pmem_record)) {
          // Checksum dismatch, mark as padding to be Freed
          // Otherwise the Restore will continue normally
          data_entry_cached.meta.type = RecordType::Padding;
        }
        break;
      }
      case RecordType::ListDirtyElem:
      case RecordType::HashDirtyElem:
      case RecordType::ListDirtyRecord:
      case RecordType::HashDirtyRecord:
      case RecordType::Padding:
      case RecordType::Empty: {
        data_entry_cached.meta.type = RecordType::Padding;
        break;
      }
      default: {
        // Report Corrupted Record, but still release it and continues
        GlobalLogger.Error(
            "Corrupted Record met when recovering. It has invalid "
            "type. Record type: %u, Checksum: %u\n",
            data_entry_cached.meta.type, data_entry_cached.header.checksum);
        kvdk_assert(data_entry_cached.header.checksum == 0, "");
        data_entry_cached.meta.type = RecordType::Padding;
        break;
      }
    }

    // When met records with invalid checksum
    // or the space is padding, empty or with corrupted record
    // Free the space and fetch another
    if (data_entry_cached.meta.type == RecordType::Padding) {
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
      case RecordType::SortedElem:
      case RecordType::SortedElemDelete: {
        s = restoreSortedElem(static_cast<DLRecord*>(recovering_pmem_record));
        break;
      }
      case RecordType::SortedHeaderDelete:
      case RecordType::SortedHeader: {
        s = restoreSortedHeader(static_cast<DLRecord*>(recovering_pmem_record));
        break;
      }
      case RecordType::StringDataRecord:
      case RecordType::StringDeleteRecord: {
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
      case RecordType::HashRecord: {
        s = hashListRestoreList(static_cast<DLRecord*>(recovering_pmem_record));
        break;
      }
      case RecordType::HashElem: {
        s = hashListRestoreElem(static_cast<DLRecord*>(recovering_pmem_record));
        break;
      }
      default: {
        GlobalLogger.Error(
            "Invalid Record type when recovering. Trying "
            "restoring record. Record type: %u\n",
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

bool KVEngine::ValidateRecordAndGetValue(void* data_record,
                                         uint32_t expected_checksum,
                                         std::string* value) {
  assert(data_record);
  assert(value);
  DataEntry* entry = static_cast<DataEntry*>(data_record);
  switch (entry->meta.type) {
    case RecordType::StringDataRecord:
    case RecordType::StringDeleteRecord: {
      StringRecord* string_record = static_cast<StringRecord*>(data_record);
      if (string_record->Validate(expected_checksum)) {
        auto v = string_record->Value();
        value->assign(v.data(), v.size());
        return true;
      }
      return false;
    }
    case RecordType::SortedElem:
    case RecordType::SortedElemDelete:
    case RecordType::SortedHeader: {
      DLRecord* dl_record = static_cast<DLRecord*>(data_record);
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

bool KVEngine::ValidateRecord(void* data_record) {
  assert(data_record);
  DataEntry* entry = static_cast<DataEntry*>(data_record);
  switch (entry->meta.type) {
    case RecordType::StringDataRecord:
    case RecordType::StringDeleteRecord: {
      return static_cast<StringRecord*>(data_record)->Validate();
    }
    case RecordType::SortedElem:
    case RecordType::SortedHeader:
    case RecordType::SortedHeaderDelete:
    case RecordType::SortedElemDelete:
    case RecordType::HashRecord:
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

Status KVEngine::Backup(const pmem::obj::string_view backup_path,
                        const Snapshot* snapshot) {
  std::string path = string_view_2_string(backup_path);
  GlobalLogger.Info("Backup to %s\n", path.c_str());
  create_dir_if_missing(path);
  // TODO: make sure backup_path is empty

  size_t mapped_len;
  BackupMark* backup_mark = static_cast<BackupMark*>(
      pmem_map_file(backup_mark_file(path).c_str(), sizeof(BackupMark),
                    PMEM_FILE_CREATE, 0666, &mapped_len,
                    nullptr /* we do not care if backup path is on PMem*/));

  if (backup_mark == nullptr || mapped_len != sizeof(BackupMark)) {
    GlobalLogger.Error("Map backup mark file %s error in Backup\n",
                       backup_mark_file(path).c_str());
    return Status::IOError;
  }

  ImmutableConfigs* imm_configs = static_cast<ImmutableConfigs*>(
      pmem_map_file(config_file(path).c_str(), sizeof(ImmutableConfigs),
                    PMEM_FILE_CREATE, 0666, &mapped_len,
                    nullptr /* we do not care if backup path is on PMem*/));
  if (imm_configs == nullptr || mapped_len != sizeof(ImmutableConfigs)) {
    GlobalLogger.Error("Map persistent config file %s error in Backup\n",
                       config_file(path).c_str());
    return Status::IOError;
  }

  CheckPoint* persistent_checkpoint = static_cast<CheckPoint*>(pmem_map_file(
      checkpoint_file(path).c_str(), sizeof(CheckPoint), PMEM_FILE_CREATE, 0666,
      &mapped_len, nullptr /* we do not care if checkpoint path is on PMem*/));

  backup_mark->stage = BackupMark::Stage::Processing;
  msync(backup_mark, sizeof(BackupMark), MS_SYNC);

  imm_configs->PersistImmutableConfigs(configs_);
  persistent_checkpoint->MakeCheckpoint(snapshot);
  msync(persistent_checkpoint, sizeof(CheckPoint), MS_SYNC);

  Status s = pmem_allocator_->Backup(data_file(path));
  if (s != Status::Ok) {
    return s;
  }

  backup_mark->stage = BackupMark::Stage::Finish;
  msync(backup_mark, sizeof(BackupMark), MS_SYNC);
  GlobalLogger.Info("Backup to %s finished\n", path.c_str());
  return Status::Ok;
}

Status KVEngine::RestoreCheckpoint() {
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

Status KVEngine::MaybeRestoreBackup() {
  size_t mapped_len;
  int is_pmem;
  BackupMark* backup_mark = static_cast<BackupMark*>(
      pmem_map_file(backup_mark_file().c_str(), sizeof(BackupMark),
                    PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem));
  if (backup_mark != nullptr) {
    if (!is_pmem || mapped_len != sizeof(BackupMark)) {
      GlobalLogger.Error("Map persisted backup mark file %s failed\n",
                         backup_mark_file().c_str());
      return Status::IOError;
    }

    switch (backup_mark->stage) {
      case BackupMark::Stage::Init: {
        break;
      }
      case BackupMark::Stage::Processing: {
        GlobalLogger.Error("Recover from a unfinished backup instance\n");
        return Status::Abort;
      }
      case BackupMark::Stage::Finish: {
        GlobalLogger.Info("Recover from a backup KVDK instance\n");
        kvdk_assert(persist_checkpoint_->Valid(),
                    "No valid checkpoint for a backup instance");
        configs_.recover_to_checkpoint = true;
        break;
      }
      default:
        GlobalLogger.Error(
            "Recover from a backup instance with wrong backup stage\n");
        return Status ::Abort;
    }
    pmem_unmap(backup_mark, sizeof(BackupMark));
  }
  return Status::Ok;
}

Status KVEngine::RestorePendingBatch() {
  DIR* dir;
  dirent* ent;
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

        PendingBatch* pending_batch = (PendingBatch*)pmem_map_file(
            pending_batch_file.c_str(), persisted_pending_file_size,
            PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);

        if (pending_batch != nullptr) {
          if (!is_pmem || mapped_len != persisted_pending_file_size) {
            GlobalLogger.Error("Map persisted pending batch file %s failed\n",
                               pending_batch_file.c_str());
            return Status::IOError;
          }
          if (pending_batch->Unfinished()) {
            uint64_t* invalid_offsets = pending_batch->record_offsets;
            for (uint32_t i = 0; i < pending_batch->num_kv; i++) {
              DataEntry* data_entry =
                  pmem_allocator_->offset2addr<DataEntry>(invalid_offsets[i]);
              if (data_entry->meta.timestamp == pending_batch->timestamp) {
                data_entry->meta.type = Padding;
                pmem_persist(&data_entry->meta.type, 8);
              }
            }
            pending_batch->PersistFinish();
          }

          if (id < configs_.max_access_threads) {
            engine_thread_cache_[id].persisted_pending_batch = pending_batch;
          } else {
            remove(pending_batch_file.c_str());
          }
        }
      }
    }
    closedir(dir);
  } else {
    GlobalLogger.Error("Open persisted pending batch dir %s failed\n",
                       pending_batch_dir_.c_str());
    return Status::IOError;
  }

  return Status::Ok;
}

Status KVEngine::Recovery() {
  auto s = RestorePendingBatch();

  if (s == Status::Ok) {
    s = RestoreCheckpoint();
  }

  if (s == Status::Ok) {
    s = MaybeRestoreBackup();
  }

  if (s != Status::Ok) {
    return s;
  }

  skiplist_locks_.reset(new LockTable{1UL << 20});
  sorted_rebuilder_.reset(new SortedCollectionRebuilder(
      this, configs_.opt_large_sorted_collection_recovery,
      configs_.max_access_threads, *persist_checkpoint_));

  list_builder_.reset(new ListBuilder{pmem_allocator_.get(), &lists_,
                                      configs_.max_access_threads, nullptr});

  hash_list_locks_.reset(new LockTable{1UL << 20});
  hash_list_builder_.reset(
      new HashListBuilder{pmem_allocator_.get(), &hash_lists_,
                          configs_.max_access_threads, hash_list_locks_.get()});

  std::vector<std::future<Status>> fs;
  GlobalLogger.Info("Start restore data\n");
  for (uint32_t i = 0; i < configs_.max_access_threads; i++) {
    fs.push_back(std::async(&KVEngine::RestoreData, this));
  }

  for (auto& f : fs) {
    Status s = f.get();
    if (s != Status::Ok) {
      return s;
    }
  }
  fs.clear();

  GlobalLogger.Info("RestoreData done: iterated %lu records\n",
                    restored_.load());

  // restore skiplist by two optimization strategy
  auto ret = sorted_rebuilder_->Rebuild();
  if (ret.s != Status::Ok) {
    return ret.s;
  }
  if (list_id_.load() <= ret.max_id) {
    list_id_.store(ret.max_id + 1);
  }
  skiplists_.swap(ret.rebuild_skiplits);

#if KVDK_DEBUG_LEVEL > 0
  for (auto skiplist : skiplists_) {
    Status s = skiplist.second->CheckIndex();
    if (s != Status::Ok) {
      GlobalLogger.Error("Check skiplist index error\n");
      return s;
    }
  }
#endif
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

  hash_list_builder_->RebuildLists();
  hash_list_builder_->CleanBrokens([&](DLRecord* elem) { directFree(elem); });
  s = hashListRegisterRecovered();
  if (s != Status::Ok) {
    return s;
  }
  hash_list_builder_.reset(nullptr);
  GlobalLogger.Info("Rebuild HashLists done\n");

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
  remove(backup_mark_file().c_str());

  version_controller_.Init(latest_version_ts);
  old_records_cleaner_.TryGlobalClean();

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

Status KVEngine::MaybeInitPendingBatchFile() {
  if (engine_thread_cache_[access_thread.id].persisted_pending_batch ==
      nullptr) {
    int is_pmem;
    size_t mapped_len;
    uint64_t persisted_pending_file_size =
        kMaxWriteBatchSize * 8 + sizeof(PendingBatch);
    persisted_pending_file_size =
        kPMEMMapSizeUnit *
        (size_t)ceil(1.0 * persisted_pending_file_size / kPMEMMapSizeUnit);

    if ((engine_thread_cache_[access_thread.id].persisted_pending_batch =
             (PendingBatch*)pmem_map_file(
                 persisted_pending_block_file(access_thread.id).c_str(),
                 persisted_pending_file_size, PMEM_FILE_CREATE, 0666,
                 &mapped_len, &is_pmem)) == nullptr ||
        !is_pmem || mapped_len != persisted_pending_file_size) {
      return Status::PMemMapFileError;
    }
  }
  return Status::Ok;
}

Status KVEngine::BatchWrite(const WriteBatch& write_batch) {
  if (write_batch.Size() > kMaxWriteBatchSize) {
    return Status::BatchOverflow;
  }

  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  s = MaybeInitPendingBatchFile();
  if (s != Status::Ok) {
    return s;
  }

  engine_thread_cache_[access_thread.id].batch_writing = true;
  defer(engine_thread_cache_[access_thread.id].batch_writing = false;);

  std::set<SpinMutex*> spins_to_lock;
  std::vector<BatchWriteHint> batch_hints(write_batch.Size());
  std::vector<uint64_t> space_entry_offsets;

  for (size_t i = 0; i < write_batch.Size(); i++) {
    auto& kv = write_batch.kvs[i];
    uint32_t requested_size;
    if (kv.type == StringDataRecord) {
      requested_size = kv.key.size() + kv.value.size() + sizeof(StringRecord);
    } else if (kv.type == StringDeleteRecord) {
      requested_size = kv.key.size() + sizeof(StringRecord);
    } else {
      GlobalLogger.Error("only support string type batch write\n");
      return Status::NotSupported;
    }
    batch_hints[i].allocated_space = pmem_allocator_->Allocate(requested_size);
    // No enough space for batch write
    if (batch_hints[i].allocated_space.size == 0) {
      return Status::PmemOverflow;
    }
    space_entry_offsets.emplace_back(batch_hints[i].allocated_space.offset);

    batch_hints[i].hash_hint = hash_table_->GetHint(kv.key);
    spins_to_lock.emplace(batch_hints[i].hash_hint.spin);
  }

  [[gnu::unused]] size_t batch_size = write_batch.Size();
  TEST_SYNC_POINT_CALLBACK("KVEngine::BatchWrite::AllocateRecord::After",
                           &batch_size);
  // lock spin mutex with order to avoid deadlock
  std::vector<std::unique_lock<SpinMutex>> ul_locks;
  for (const SpinMutex* l : spins_to_lock) {
    ul_locks.emplace_back(const_cast<SpinMutex&>(*l));
  }

  auto holder = version_controller_.GetLocalSnapshotHolder();
  TimeStampType ts = holder.Timestamp();
  for (size_t i = 0; i < write_batch.Size(); i++) {
    batch_hints[i].timestamp = ts;
  }

  // Persist batch write status as processing
  engine_thread_cache_[access_thread.id]
      .persisted_pending_batch->PersistProcessing(space_entry_offsets, ts);

  // Do batch writes
  for (size_t i = 0; i < write_batch.Size(); i++) {
    if (write_batch.kvs[i].type == StringDataRecord ||
        write_batch.kvs[i].type == StringDeleteRecord) {
      s = StringBatchWriteImpl(write_batch.kvs[i], batch_hints[i]);
      TEST_SYNC_POINT_CALLBACK("KVEnigne::BatchWrite::BatchWriteRecord", &i);
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

  // Must release all spinlocks before delayFree(),
  // otherwise may deadlock as delayFree() also try to acquire these spinlocks.
  ul_locks.clear();

  engine_thread_cache_[access_thread.id]
      .persisted_pending_batch->PersistFinish();

  // Free outdated kvs
  for (size_t i = 0; i < write_batch.Size(); i++) {
    TEST_SYNC_POINT_CALLBACK("KVEngine::BatchWrite::purgeAndFree::Before", &i);
    if (batch_hints[i].data_record_to_free != nullptr) {
      delayFree(OldDataRecord{batch_hints[i].data_record_to_free, ts});
    }
    if (batch_hints[i].delete_record_to_free != nullptr) {
      delayFree(OldDeleteRecord(
          batch_hints[i].delete_record_to_free, batch_hints[i].hash_entry_ptr,
          PointerType::HashEntry, ts, batch_hints[i].hash_hint.spin));
    }
    if (batch_hints[i].space_not_used) {
      pmem_allocator_->Free(batch_hints[i].allocated_space);
    }
  }
  return Status::Ok;
}

Status KVEngine::GetTTL(const StringView str, TTLType* ttl_time) {
  *ttl_time = kInvalidTTL;
  HashTable::KeyHashHint hint = hash_table_->GetHint(str);
  std::unique_lock<SpinMutex> ul(*hint.spin);
  LookupResult res = lookupKey<false>(str, ExpirableRecordType);

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
  HashTable::KeyHashHint hint = hash_table_->GetHint(str);
  std::unique_lock<SpinMutex> ul(*hint.spin);
  auto snapshot_holder = version_controller_.GetLocalSnapshotHolder();
  // TODO: maybe have a wrapper function(lookupKeyAndMayClean).
  LookupResult res = lookupKey<false>(str, ExpirableRecordType);
  if (res.s == Status::Outdated) {
    if (res.entry_ptr->IsTTLStatus()) {
      // Push the expired record into cleaner and update hash entry status
      // with KeyStatus::Expired.
      // TODO(zhichen): This `if` will be removed when completing collection
      // deletion.
      if (res.entry_ptr->GetIndexType() == PointerType::StringRecord) {
        hash_table_->UpdateEntryStatus(res.entry_ptr, KeyStatus::Expired);
        ul.unlock();
        delayFree(OldDeleteRecord{res.entry_ptr->GetIndex().ptr, res.entry_ptr,
                                  PointerType::HashEntry,
                                  version_controller_.GetCurrentTimestamp(),
                                  hint.spin});
      }
    }
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

        if (ret.s == Status::Ok) {
          delayFree(OldDataRecord{ret.existing_record, new_ts});
        }
        res.s = ret.s;
        break;
      }
      case PointerType::HashList: {
        HashList* hlist = res.entry_ptr->GetIndex().hlist;
        std::unique_lock<std::mutex> guard(hlists_mu_);
        hash_lists_.erase(hlist);
        res.s = hlist->SetExpireTime(expired_time);
        hash_lists_.insert(hlist);
        break;
      }
      case PointerType::List: {
        List* list = res.entry_ptr->GetIndex().list;
        std::unique_lock<std::mutex> guard(lists_mu_);
        lists_.erase(list);
        res.s = list->SetExpireTime(expired_time);
        lists_.emplace(list);
        break;
      }
      default: {
        return Status::NotSupported;
      }
    }
    // Update hash entry status to TTL
    if (res.s == Status::Ok) {
      hash_table_->UpdateEntryStatus(res.entry_ptr, expired_time == kPersistTime
                                                        ? KeyStatus::Persist
                                                        : KeyStatus::Volatile);
    }
  }
  return res.s;
}
}  // namespace KVDK_NAMESPACE

// lookupKey
namespace KVDK_NAMESPACE {
template <bool may_insert>
KVEngine::LookupResult KVEngine::lookupImpl(StringView key,
                                            uint16_t type_mask) {
  LookupResult result;
  auto hint = hash_table_->GetHint(key);
  if (!may_insert) {
    result.s = hash_table_->SearchForRead(
        hint, key, type_mask, &result.entry_ptr, &result.entry, nullptr);
  } else {
    result.s = hash_table_->SearchForWrite(
        hint, key, type_mask, &result.entry_ptr, &result.entry, nullptr);
  }
  return result;
}

template <bool may_insert>
KVEngine::LookupResult KVEngine::lookupElem(StringView key,
                                            uint16_t type_mask) {
  kvdk_assert(type_mask & (HashElem | SortedElemType), "");
  return lookupImpl<may_insert>(key, type_mask);
}
template KVEngine::LookupResult KVEngine::lookupElem<true>(StringView,
                                                           uint16_t);
template KVEngine::LookupResult KVEngine::lookupElem<false>(StringView,
                                                            uint16_t);

template <bool may_insert>
KVEngine::LookupResult KVEngine::lookupKey(StringView key, uint16_t type_mask) {
  LookupResult result = lookupImpl<may_insert>(key, PrimaryRecordType);

  if (result.s != Status::Ok) {
    kvdk_assert(
        result.s == Status::NotFound || result.s == Status::MemoryOverflow, "");
    return result;
  }

  RecordType record_type = result.entry.GetRecordType();
  bool type_match = type_mask & record_type;
  bool expired;

  switch (record_type) {
    case RecordType::SortedHeaderDelete:
    case RecordType::StringDeleteRecord: {
      result.s = type_match ? Status::Outdated : Status::WrongType;
      return result;
    }
    case RecordType::StringDataRecord: {
      expired = result.entry.GetIndex().string_record->HasExpired();
      break;
    }
    case RecordType::HashRecord:
    case RecordType::ListRecord:
    case RecordType::SortedHeader: {
      expired =
          static_cast<Collection*>(result.entry.GetIndex().ptr)->HasExpired();
      break;
    }
    default: {
      kvdk_assert(false, "Unreachable branch!");
      std::abort();
    }
  }

  if (expired) {
    result.s = type_match ? Status::Outdated : Status::NotFound;
  } else {
    result.s = type_match ? Status::Ok : Status::WrongType;
  }
  return result;
}
template KVEngine::LookupResult KVEngine::lookupKey<true>(StringView, uint16_t);
template KVEngine::LookupResult KVEngine::lookupKey<false>(StringView,
                                                           uint16_t);

}  // namespace KVDK_NAMESPACE

// Snapshot, delayFree and background work
namespace KVDK_NAMESPACE {

/// TODO: move this into VersionController.
Snapshot* KVEngine::GetSnapshot(bool make_checkpoint) {
  Snapshot* ret = version_controller_.NewGlobalSnapshot();
  TimeStampType snapshot_ts = static_cast<SnapshotImpl*>(ret)->GetTimestamp();

  // A snapshot should not contain any ongoing batch write
  for (size_t i = 0; i < configs_.max_access_threads; i++) {
    while (engine_thread_cache_[i].batch_writing &&
           snapshot_ts >=
               version_controller_.GetLocalSnapshot(i).GetTimestamp()) {
      asm volatile("pause");
    }
  }

  if (make_checkpoint) {
    std::lock_guard<std::mutex> lg(checkpoint_lock_);
    persist_checkpoint_->MakeCheckpoint(ret);
    pmem_persist(persist_checkpoint_, sizeof(CheckPoint));
  }

  return ret;
}

void KVEngine::delayFree(const OldDataRecord& old_data_record) {
  old_records_cleaner_.PushToCache(old_data_record);
  // To avoid too many cached old records pending clean, we try to clean cached
  // records while pushing new one
  if (!need_clean_records_ &&
      old_records_cleaner_.NumCachedOldRecords() > kMaxCachedOldRecords) {
    need_clean_records_ = true;
  } else {
    old_records_cleaner_.TryCleanCachedOldRecords(
        kLimitForegroundCleanOldRecords);
  }
}

void KVEngine::delayFree(const OldDeleteRecord& old_delete_record) {
  old_records_cleaner_.PushToCache(old_delete_record);
  // To avoid too many cached old records pending clean, we try to clean cached
  // records while pushing new one
  if (!need_clean_records_ &&
      old_records_cleaner_.NumCachedOldRecords() > kMaxCachedOldRecords) {
    need_clean_records_ = true;
  } else {
    old_records_cleaner_.TryCleanCachedOldRecords(
        kLimitForegroundCleanOldRecords);
  }
}

void KVEngine::delayFree(const OutdatedCollection& outdated_collection) {
  old_records_cleaner_.PushToCache(outdated_collection);
  // To avoid too many cached old records pending clean, we try to clean cached
  // records while pushing new one
  if (!need_clean_records_ &&
      old_records_cleaner_.NumCachedOldRecords() > kMaxCachedOldRecords) {
    need_clean_records_ = true;
  } else {
    old_records_cleaner_.TryCleanCachedOldRecords(
        kLimitForegroundCleanOldRecords);
  }
}

void KVEngine::delayFree(DLRecord* addr) {
  /// TODO: avoid deadlock in cleaner to help Free() deleted records
  old_records_cleaner_.PushToPendingFree(
      addr, version_controller_.GetCurrentTimestamp());
}

void KVEngine::directFree(DLRecord* addr) {
  pmem_allocator_->Free(SpaceEntry{pmem_allocator_->addr2offset_checked(addr),
                                   addr->entry.header.record_size});
}

void KVEngine::backgroundOldRecordCleaner() {
  TEST_SYNC_POINT_CALLBACK("KVEngine::backgroundOldRecordCleaner::NothingToDo",
                           nullptr);
  while (!bg_work_signals_.terminating) {
    CleanOutDated();
  }
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

  std::set<HashList*>::iterator hash_it = hash_lists_.begin();
  while (hash_it != hash_lists_.end()) {
    delete *hash_it;
    hash_it = hash_lists_.erase(hash_it);
  }
};

void KVEngine::backgroundDestroyCollections() {
  std::deque<std::pair<TimeStampType, List*>> outdated_lists_;
  std::deque<std::pair<TimeStampType, HashList*>> outdated_hash_lists_;
  std::vector<std::future<Status>> workers;

  while (!bg_work_signals_.terminating) {
    // Scan for one expired List
    {
      List* expired_list = nullptr;
      {
        std::lock_guard<std::mutex> guard{lists_mu_};
        if (!lists_.empty() && (*lists_.begin())->HasExpired()) {
          expired_list = *lists_.begin();
        }
      }
      if (expired_list != nullptr) {
        auto key = expired_list->Name();
        auto guard = hash_table_->AcquireLock(key);
        LookupResult ret = lookupKey<false>(key, RecordType::ListRecord);
        // Make sure the List has indeed expired, it may have been updated.
        if (ret.s == Status::Outdated) {
          expired_list = ret.entry.GetIndex().list;
          removeKeyOrElem(ret);
          std::lock_guard<std::mutex> guard{lists_mu_};
          lists_.erase(expired_list);
          outdated_lists_.emplace_back(
              version_controller_.GetCurrentTimestamp(), expired_list);
        } else if (ret.s == Status::NotFound) {
          std::lock_guard<std::mutex> guard{lists_mu_};
          lists_.erase(expired_list);
          outdated_lists_.emplace_back(
              version_controller_.GetCurrentTimestamp(), expired_list);
        }
      }
    }

    // Scan for one expired HashList
    {
      HashList* expired_hlist = nullptr;
      {
        std::lock_guard<std::mutex> guard{hlists_mu_};
        if (!hash_lists_.empty() && (*hash_lists_.begin())->HasExpired()) {
          expired_hlist = *hash_lists_.begin();
        }
      }
      if (expired_hlist != nullptr) {
        auto key = expired_hlist->Name();
        auto guard = hash_table_->AcquireLock(key);
        LookupResult ret = lookupKey<false>(key, RecordType::HashRecord);
        // Make sure the HashList has indeed expired
        if (ret.s == Status::Outdated) {
          expired_hlist = ret.entry.GetIndex().hlist;
          removeKeyOrElem(ret);
          std::lock_guard<std::mutex> guard{hlists_mu_};
          hash_lists_.erase(expired_hlist);
          outdated_hash_lists_.emplace_back(
              version_controller_.GetCurrentTimestamp(), expired_hlist);
        } else if (ret.s == Status::NotFound) {
          std::lock_guard<std::mutex> guard{hlists_mu_};
          hash_lists_.erase(expired_hlist);
          outdated_hash_lists_.emplace_back(
              version_controller_.GetCurrentTimestamp(), expired_hlist);
        }
      }
    }

    // Clean expired Lists and HashLists that are alreay removed from HashTable
    // and std::set.
    {
      if (!outdated_hash_lists_.empty() || !outdated_lists_.empty()) {
        version_controller_.UpdatedOldestSnapshot();
      }
      TimeStampType earliest_access = version_controller_.OldestSnapshotTS();

      if (!outdated_lists_.empty()) {
        auto& ts_list = outdated_lists_.front();
        if (ts_list.first < earliest_access) {
          workers.emplace_back(std::async(std::launch::deferred,
                                          &KVEngine::listDestroy, this,
                                          ts_list.second));
          outdated_lists_.pop_front();
        }
      }

      if (!outdated_hash_lists_.empty()) {
        auto& ts_hlist = outdated_hash_lists_.front();
        if (ts_hlist.first < earliest_access) {
          workers.emplace_back(std::async(std::launch::deferred,
                                          &KVEngine::hashListDestroy, this,
                                          ts_hlist.second));
          outdated_hash_lists_.pop_front();
        }
      }

      // TODO: add skiplist
      for (auto& worker : workers) {
        // TODO: use this Status.
        worker.get();
      }
      workers.clear();
    }
  }
}

void KVEngine::CleanOutDated() {
  int64_t interval = static_cast<int64_t>(configs_.background_work_interval);
  std::deque<OldDeleteRecord> expired_record_queue;
  std::deque<OutdatedCollection> expired_collection_queue;
  // Iterate hash table
  auto start_ts = std::chrono::system_clock::now();
  auto hashtable_iter = hash_table_->GetIterator();
  while (hashtable_iter.Valid()) {
    {  // Slot lock section
      auto slot_lock(hashtable_iter.AcquireSlotLock());
      auto slot_iter = hashtable_iter.Slot();
      auto new_ts = version_controller_.GetCurrentTimestamp();
      while (slot_iter.Valid()) {
        switch (slot_iter->GetIndexType()) {
          case PointerType::StringRecord: {
            if (slot_iter->IsTTLStatus() &&
                slot_iter->GetIndex().string_record->HasExpired()) {
              hash_table_->UpdateEntryStatus(&(*slot_iter), KeyStatus::Expired);
              // push expired cleaner
              expired_record_queue.push_back(
                  OldDeleteRecord{slot_iter->GetIndex().ptr, &(*slot_iter),
                                  PointerType::HashEntry, new_ts,
                                  hashtable_iter.GetSlotLock()});
            }
            break;
          }
          case PointerType::Skiplist: {
            if (slot_iter->IsTTLStatus() &&
                slot_iter->GetIndex().skiplist->HasExpired()) {
              // push expired to cleaner and clear hash entry
              expired_collection_queue.emplace_back(
                  slot_iter->GetIndex().skiplist,
                  version_controller_.GetCurrentTimestamp());
              hash_table_->Erase(&(*slot_iter));
            }
          }
          default:
            break;
        }
        slot_iter++;
      }
      hashtable_iter.Next();
    }

    if (!expired_record_queue.empty() &&
        (expired_record_queue.size() >= kMaxCachedOldRecords)) {
      old_records_cleaner_.PushToGlobal(expired_record_queue);
      expired_record_queue.clear();
    }

    if (!expired_collection_queue.empty()) {
      old_records_cleaner_.PushToGlobal(std::move(expired_collection_queue));
      expired_collection_queue.clear();
    }

    if (std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now() - start_ts)
                .count() > interval ||
        need_clean_records_) {
      need_clean_records_ = true;
      old_records_cleaner_.TryGlobalClean();
      need_clean_records_ = false;
      start_ts = std::chrono::system_clock::now();
    }
  }
  if (!expired_record_queue.empty()) {
    old_records_cleaner_.PushToGlobal(expired_record_queue);
    expired_record_queue.clear();
  }
}
}  // namespace KVDK_NAMESPACE

// List
namespace KVDK_NAMESPACE {
Status KVEngine::ListLength(StringView key, size_t* sz) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  std::unique_lock<std::recursive_mutex> guard;
  List* list;
  Status s = listFind(key, &list, false, guard);
  if (s != Status::Ok) {
    return s;
  }
  *sz = list->Size();
  return Status::Ok;
}

Status KVEngine::ListPushFront(StringView key, StringView elem) {
  if (!CheckKeySize(key) || !CheckValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  /// TODO: (Ziyan) use gargage collection mechanism from version controller
  /// to perform these operations lockless.
  std::unique_lock<std::recursive_mutex> guard;
  List* list;
  Status s = listFind(key, &list, true, guard);
  if (s != Status::Ok) {
    return s;
  }

  SpaceEntry space = pmem_allocator_->Allocate(
      sizeof(DLRecord) + sizeof(CollectionIDType) + elem.size());
  if (space.size == 0) {
    return Status::PmemOverflow;
  }

  list->PushFront(space, version_controller_.GetCurrentTimestamp(), "", elem);
  return Status::Ok;
}

Status KVEngine::ListPushBack(StringView key, StringView elem) {
  if (!CheckKeySize(key) || !CheckValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  std::unique_lock<std::recursive_mutex> guard;
  List* list;
  Status s = listFind(key, &list, true, guard);
  if (s != Status::Ok) {
    return s;
  }

  SpaceEntry space = pmem_allocator_->Allocate(
      sizeof(DLRecord) + sizeof(CollectionIDType) + elem.size());
  if (space.size == 0) {
    return Status::PmemOverflow;
  }

  list->PushBack(space, version_controller_.GetCurrentTimestamp(), "", elem);
  return Status::Ok;
}

Status KVEngine::ListPopFront(StringView key, std::string* elem) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  std::unique_lock<std::recursive_mutex> guard;
  List* list;
  Status s = listFind(key, &list, false, guard);
  if (s != Status::Ok) {
    return s;
  }
  if (list->Size() == 0) {
    return Status::NotFound;
  }

  StringView sw = list->Front()->Value();
  elem->assign(sw.data(), sw.size());
  list->PopFront([&](DLRecord* rec) { delayFree(rec); });
  return Status::Ok;
}

Status KVEngine::ListPopBack(StringView key, std::string* elem) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  std::unique_lock<std::recursive_mutex> guard;
  List* list;
  Status s = listFind(key, &list, false, guard);
  if (s != Status::Ok) {
    return s;
  }
  if (list->Size() == 0) {
    return Status::NotFound;
  }

  StringView sw = list->Back()->Value();
  elem->assign(sw.data(), sw.size());
  list->PopBack([&](DLRecord* rec) { delayFree(rec); });
  return Status::Ok;
}

Status KVEngine::ListInsertBefore(std::unique_ptr<ListIterator> const& pos,
                                  StringView elem) {
  if (!CheckValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  ListIteratorImpl* iter = dynamic_cast<ListIteratorImpl*>(pos.get());
  kvdk_assert(iter != nullptr, "Invalid iterator!");

  std::unique_lock<std::recursive_mutex> guard;
  List* list;
  Status s = listFind(iter->Owner()->Name(), &list, false, guard);
  if (s != Status::Ok) {
    return s;
  }
  kvdk_assert(list == iter->Owner(), "Iterator outdated!");

  SpaceEntry space = pmem_allocator_->Allocate(
      sizeof(DLRecord) + sizeof(CollectionIDType) + elem.size());
  if (space.size == 0) {
    return Status::PmemOverflow;
  }

  iter->Rep() = list->EmplaceBefore(
      space, iter->Rep(), version_controller_.GetCurrentTimestamp(), "", elem);
  return Status::Ok;
}

Status KVEngine::ListInsertAfter(std::unique_ptr<ListIterator> const& pos,
                                 StringView elem) {
  if (!CheckValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  ListIteratorImpl* iter = dynamic_cast<ListIteratorImpl*>(pos.get());
  kvdk_assert(iter != nullptr, "Invalid iterator!");

  std::unique_lock<std::recursive_mutex> guard;
  List* list;
  Status s = listFind(iter->Owner()->Name(), &list, false, guard);
  if (s != Status::Ok) {
    return s;
  }
  kvdk_assert(list == iter->Owner(), "Iterator outdated!");

  SpaceEntry space = pmem_allocator_->Allocate(
      sizeof(DLRecord) + sizeof(CollectionIDType) + elem.size());
  if (space.size == 0) {
    return Status::PmemOverflow;
  }

  iter->Rep() = list->EmplaceAfter(
      space, iter->Rep(), version_controller_.GetCurrentTimestamp(), "", elem);
  return Status::Ok;
}

Status KVEngine::ListErase(std::unique_ptr<ListIterator> const& pos) {
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  ListIteratorImpl* iter = dynamic_cast<ListIteratorImpl*>(pos.get());
  kvdk_assert(iter != nullptr, "Invalid iterator!");

  std::unique_lock<std::recursive_mutex> guard;
  List* list;
  Status s = listFind(iter->Owner()->Name(), &list, false, guard);
  if (s != Status::Ok) {
    return s;
  }
  kvdk_assert(list == iter->Owner(), "Iterator outdated!");
  kvdk_assert(iter->Valid(), "Trying to erase invalid iterator!");

  iter->Rep() =
      list->Erase(iter->Rep(), [&](DLRecord* rec) { delayFree(rec); });
  return Status::Ok;
}

// Replace the element at pos
Status KVEngine::ListSet(std::unique_ptr<ListIterator> const& pos,
                         StringView elem) {
  if (!CheckValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  ListIteratorImpl* iter = dynamic_cast<ListIteratorImpl*>(pos.get());
  kvdk_assert(iter != nullptr, "Invalid iterator!");

  std::unique_lock<std::recursive_mutex> guard;
  List* list;
  Status s = listFind(iter->Owner()->Name(), &list, false, guard);
  if (s != Status::Ok) {
    return s;
  }
  kvdk_assert(list == iter->Owner(), "Iterator outdated!");

  SpaceEntry space = pmem_allocator_->Allocate(
      sizeof(DLRecord) + sizeof(CollectionIDType) + elem.size());
  if (space.size == 0) {
    return Status::PmemOverflow;
  }
  iter->Rep() = list->Replace(space, iter->Rep(),
                              version_controller_.GetCurrentTimestamp(), "",
                              elem, [&](DLRecord* rec) { delayFree(rec); });
  return Status::Ok;
}

std::unique_ptr<ListIterator> KVEngine::ListCreateIterator(StringView key) {
  if (!CheckKeySize(key)) {
    return nullptr;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return nullptr;
  }

  std::unique_lock<std::recursive_mutex> guard;
  List* list;
  Status s = listFind(key, &list, false, guard);
  if (s != Status::Ok) {
    return nullptr;
  }
  return std::unique_ptr<ListIteratorImpl>{new ListIteratorImpl{list}};
}

Status KVEngine::listRestoreElem(DLRecord* pmp_record) {
  list_builder_->AddListElem(pmp_record);
  return Status::Ok;
}

Status KVEngine::listRestoreList(DLRecord* pmp_record) {
  list_builder_->AddListRecord(pmp_record);
  return Status::Ok;
}

Status KVEngine::listRegisterRecovered() {
  CollectionIDType max_id = 0;
  for (List* list : lists_) {
    auto guard = hash_table_->AcquireLock(list->Name());
    Status s = registerCollection(list);
    if (s != Status::Ok) {
      return s;
    }
    max_id = std::max(max_id, list->ID());
  }
  CollectionIDType old = list_id_.load();
  while (max_id >= old && !list_id_.compare_exchange_strong(old, max_id + 1)) {
  }
  return Status::Ok;
}

Status KVEngine::listDestroy(List* list) {
  std::vector<SpaceEntry> entries;
  auto PushPending = [&](DLRecord* rec) {
    SpaceEntry space{pmem_allocator_->addr2offset_checked(rec),
                     rec->entry.header.record_size};
    entries.push_back(space);
  };
  while (list->Size() > 0) {
    list->PopFront(PushPending);
  }
  list->Destroy(PushPending);
  pmem_allocator_->BatchFree(entries);
  delete list;
  return Status::Ok;
}

Status KVEngine::listFind(StringView key, List** list, bool init_nx,
                          std::unique_lock<std::recursive_mutex>& guard) {
  {
    LookupResult result = lookupKey<false>(key, RecordType::ListRecord);
    if (result.s != Status::Ok && result.s != Status::NotFound) {
      return result.s;
    }
    if (result.s == Status::Ok) {
      (*list) = result.entry.GetIndex().list;
      guard = (*list)->AcquireLock();
      return Status::Ok;
    }
    if (!init_nx) {
      // Uninitialized or Deleted
      return Status::NotFound;
    }
  }

  // Uninitialized or Deleted, initialize new one
  {
    auto guard2 = hash_table_->AcquireLock(key);
    LookupResult result = lookupKey<true>(key, RecordType::ListRecord);
    if (result.s != Status::Ok && result.s != Status::NotFound) {
      return result.s;
    }
    if (result.s == Status::Ok) {
      (*list) = result.entry.GetIndex().list;
      guard = (*list)->AcquireLock();
      return Status::Ok;
    }
    // No other thread have created one, create one here.
    std::uint64_t ts = version_controller_.GetCurrentTimestamp();
    CollectionIDType id = list_id_.fetch_add(1);
    SpaceEntry space = pmem_allocator_->Allocate(sizeof(DLRecord) + key.size() +
                                                 sizeof(CollectionIDType));
    if (space.size == 0) {
      return Status::PmemOverflow;
    }
    *list = new List{};
    (*list)->Init(pmem_allocator_.get(), space, ts, key, id, nullptr);
    {
      std::lock_guard<std::mutex> guard2{lists_mu_};
      lists_.emplace(*list);
    }
    guard = (*list)->AcquireLock();
    insertKeyOrElem(result, key, RecordType::ListRecord, *list);
  }
  return Status::Ok;
}

}  // namespace KVDK_NAMESPACE
