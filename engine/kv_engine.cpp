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
        return Status::Abort;
    }
    pmem_unmap(backup_mark, sizeof(BackupMark));
  }
  return Status::Ok;
}

Status KVEngine::Recovery() {
  Status s = RestoreCheckpoint();
  if (s != Status::Ok) {
    return s;
  }

  s = batchWriteRollbackLogs();
  if (s != Status::Ok) {
    return s;
  }

  s = MaybeRestoreBackup();
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

  // Prevent generating snapshot newer than this WriteBatch
  auto bw_token = version_controller_.GetBatchWriteToken();
  // Prevent collection and nodes in double linked lists from being deleted
  auto access_token = version_controller_.GetLocalSnapshotHolder();

  // Lock all keys
  std::vector<StringView> keys;
  for (auto const& string_op : batch.StringOps()) {
    keys.push_back(string_op.key);
  }
  for (auto const& sorted_op : batch.SortedOps()) {
    keys.push_back(sorted_op.field);
  }
  for (auto const& hash_op : batch.HashOps()) {
    keys.push_back(hash_op.field);
  }
  auto guard = hash_table_->RangeLock(keys);

  // Lookup keys, allocate space according to result.
  std::vector<StringWriteArgs> string_args;
  std::vector<SortedWriteArgs> sorted_args;
  std::vector<HashWriteArgs> hash_args;

  auto ReleaseResources = [&]() {
    for (auto iter = hash_args.rbegin(); iter != hash_args.rend(); ++iter) {
      pmem_allocator_->Free(iter->space);
    }
    for (auto iter = sorted_args.rbegin(); iter != sorted_args.rend(); ++iter) {
      pmem_allocator_->Free(iter->space);
    }
    for (auto iter = string_args.rbegin(); iter != string_args.rend(); ++iter) {
      pmem_allocator_->Free(iter->space);
    }
  };
  defer(ReleaseResources());

  // Prepare for Strings
  for (auto const& string_op : batch.StringOps()) {
    StringWriteArgs args;
    args.Assign(string_op);
    args.ts = bw_token.Timestamp();
    args.res = lookupKey<true>(args.key, StringRecordType);
    if (args.res.s != Status::Ok && args.res.s != Status::NotFound &&
        args.res.s != Status::Outdated) {
      return args.res.s;
    }
    if (args.op == WriteBatchImpl::Op::Delete && args.res.s != Status::Ok) {
      // No need to do anything for delete a non-existing String
      continue;
    }
    args.space = pmem_allocator_->Allocate(
        StringRecord::RecordSize(args.key, args.value));
    if (args.space.size == 0) {
      return Status::PmemOverflow;
    }

    string_args.push_back(args);
  }

  // Prepare for Sorted Elements
  for (auto const& sorted_op : batch.SortedOps()) {
    auto res = lookupKey<false>(sorted_op.key, SortedHeaderType);
    if (res.s != Status::Ok && res.s != Status::NotFound &&
        res.s != Status::Outdated) {
      return res.s;
    }
    if (res.s != Status::Ok) {
      if (sorted_op.op == WriteBatchImpl::Op::Delete) {
        // No need to do anything for delete a field from non-existing Skiplist.
        continue;
      } else {
        // Batch Write to collection not found, return Status::NotFound.
        return Status::NotFound;
      }
    }
    Skiplist* slist = res.entry.GetIndex().skiplist;
    SortedWriteArgs args;
    args.Assign(sorted_op);
    args.ts = bw_token.Timestamp();
    std::string internal_key = slist->InternalKey(args.field);
    args.res = lookupElem<true>(internal_key, SortedElemType);
    if (args.res.s != Status::Ok && args.res.s != Status::NotFound) {
      return args.res.s;
    }
    if (args.op == WriteBatchImpl::Op::Delete &&
        args.res.s == Status::NotFound) {
      // No need to do anything for delete a non-existing Sorted element
      continue;
    }
    args.space = pmem_allocator_->Allocate(
        DLRecord::RecordSize(internal_key, args.value));
    if (args.space.size == 0) {
      return Status::PmemOverflow;
    }
    sorted_args.push_back(args);
  }

  // Prepare for Hash Elements
  for (auto const& hash_op : batch.HashOps()) {
    HashList* hlist;
    Status s = hashListFind(hash_op.key, &hlist);
    if (s != Status::Ok && s != Status::NotFound) {
      return s;
    }
    if (s == Status::NotFound) {
      if (hash_op.op == WriteBatchImpl::Op::Delete) {
        // No need to do anything for delete a field from non-existing Hash Set.
        continue;
      } else {
        // Batch Write to collection not found, return Status::NotFound.
        return Status::NotFound;
      }
    }
    HashWriteArgs args;
    args.Assign(hash_op);
    args.ts = bw_token.Timestamp();
    std::string internal_key = hlist->InternalKey(args.field);
    args.res = lookupElem<true>(internal_key, HashElem);
    if (args.res.s != Status::Ok && args.res.s != Status::NotFound) {
      return args.res.s;
    }
    if (args.op == WriteBatchImpl::Op::Delete &&
        args.res.s == Status::NotFound) {
      // No need to do anything for delete a non-existing Sorted element
      continue;
    }
    args.space = pmem_allocator_->Allocate(
        DLRecord::RecordSize(internal_key, args.value));
    if (args.space.size == 0) {
      return Status::PmemOverflow;
    }
    hash_args.push_back(args);
  }

  // Preparation done. Persist BatchLog for rollback.
  BatchWriteLog log;
  auto& tc = engine_thread_cache_[access_thread.id];
  for (auto& arg : string_args) {
    if (arg.op == WriteBatchImpl::Op::Put) {
      log.StringPut(arg.space.offset);
    } else {
      log.StringDelete(arg.space.offset);
    }
  }
  for (auto& arg : sorted_args) {
    if (arg.op == WriteBatchImpl::Op::Put) {
      log.SortedPut(arg.space.offset);
    } else {
      log.SortedDelete(arg.space.offset);
    }
  }
  for (auto& arg : hash_args) {
    if (arg.op == WriteBatchImpl::Op::Put) {
      log.HashPut(arg.space.offset);
    } else {
      log.HashDelete(arg.space.offset);
    }
  }

  log.EncodeTo(tc.batch_log);

  BatchWriteLog::MarkProcessing(tc.batch_log);

  // Write Strings
  for (auto& string_arg : string_args) {
    Status s = stringWrite(string_arg);
    if (s != Status::Ok) {
      return s;
    }
  }

  // Write Sorted Elems
  for (auto& sorted_arg : sorted_args) {
    Status s = sortedWrite(sorted_arg);
    if (s != Status::Ok) {
      return s;
    }
  }

  // Write Hash Elems
  for (auto& hash_arg : hash_args) {
    Status s = hashListWrite(hash_arg);
    if (s != Status::Ok) {
      return s;
    }
  }

  TEST_CRASH_POINT("KVEngine::batchWriteImpl::BeforeCommit", "");

  BatchWriteLog::MarkCommitted(tc.batch_log);

  // Publish stages is where Strings and Collections make BatchWrite
  // visible to other threads.
  // This stage allows no failure during runtime,
  // otherwise dirty read may occur.
  // Crash is tolerated as BatchWrite will be recovered.

  // Publish Strings to HashTable
  for (auto const& string_arg : string_args) {
    Status s = stringPublish(string_arg);
    kvdk_assert(s == Status::Ok, "");
  }

  // Publish Sorted Elements to HashTable
  for (auto& sorted_arg : sorted_args) {
    Status s = sortedPublish(sorted_arg);
    kvdk_assert(s == Status::Ok, "");
  }

  // Publish Hash Elements to HashTable
  for (auto& hash_arg : hash_args) {
    Status s = hashListPublish(hash_arg);
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
    for (auto iter = log.HashLogs().rbegin(); iter != log.HashLogs().rend();
         ++iter) {
      s = hashListRollback(log.Timestamp(), *iter);
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
    if (pmem_unmap(addr, mapped_len) != 0) {
      GlobalLogger.Error("Fail to Rollback BatchLog file. %s\n",
                         strerror(errno));
      return Status::PMemMapFileError;
    }
  }
  closedir(dir);

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
    if (res.entry_ptr->IsTTLStatus()) {
      // Push the expired record into cleaner and update hash entry status
      // with KeyStatus::Expired.
      // TODO(zhichen): This `if` will be removed when completing collection
      // deletion.
      if (res.entry_ptr->GetIndexType() == PointerType::StringRecord) {
        hash_table_->UpdateEntryStatus(res.entry_ptr, KeyStatus::Expired);
        ul.unlock();
        SpinMutex* hash_lock = ul.release();
        delayFree(OldDeleteRecord{res.entry_ptr->GetIndex().ptr, res.entry_ptr,
                                  PointerType::HashEntry,
                                  version_controller_.GetCurrentTimestamp(),
                                  hash_lock});
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
        res.s = hashListExpire(hlist, expired_time);
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
HashTable::LookupResult KVEngine::lookupElem(StringView key,
                                             uint16_t type_mask) {
  kvdk_assert(type_mask & (HashElem | SortedElemType), "");
  return hash_table_->Lookup<may_insert>(key, type_mask);
}

template HashTable::LookupResult KVEngine::lookupElem<true>(StringView,
                                                            uint16_t);
template HashTable::LookupResult KVEngine::lookupElem<false>(StringView,
                                                             uint16_t);

template <bool may_insert>
HashTable::LookupResult KVEngine::lookupKey(StringView key,
                                            uint16_t type_mask) {
  auto result = hash_table_->Lookup<may_insert>(key, PrimaryRecordType);

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
template HashTable::LookupResult KVEngine::lookupKey<true>(StringView,
                                                           uint16_t);
template HashTable::LookupResult KVEngine::lookupKey<false>(StringView,
                                                            uint16_t);

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
        auto lookup_result = lookupKey<false>(key, RecordType::ListRecord);
        // Make sure the List has indeed expired, it may have been updated.
        if (lookup_result.s == Status::Outdated) {
          expired_list = lookup_result.entry.GetIndex().list;
          removeKeyOrElem(lookup_result);
          std::lock_guard<std::mutex> guard{lists_mu_};
          lists_.erase(expired_list);
          outdated_lists_.emplace_back(
              version_controller_.GetCurrentTimestamp(), expired_list);
        } else if (lookup_result.s == Status::NotFound) {
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
        auto lookup_result = lookupKey<false>(key, RecordType::HashRecord);
        // Make sure the HashList has indeed expired
        if (lookup_result.s == Status::Outdated) {
          expired_hlist = lookup_result.entry.GetIndex().hlist;
          removeKeyOrElem(lookup_result);
          std::lock_guard<std::mutex> guard{hlists_mu_};
          hash_lists_.erase(expired_hlist);
          outdated_hash_lists_.emplace_back(
              version_controller_.GetCurrentTimestamp(), expired_hlist);
        } else if (lookup_result.s == Status::NotFound) {
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
Status KVEngine::ListCreate(StringView key) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto guard = hash_table_->AcquireLock(key);
  auto result = lookupKey<true>(key, RecordType::ListRecord);
  if (result.s == Status::Ok) {
    return Status::Existed;
  }
  if (result.s != Status::NotFound && result.s != Status::Outdated) {
    return result.s;
  }
  SpaceEntry space = pmem_allocator_->Allocate(sizeof(DLRecord) + key.size() +
                                               sizeof(CollectionIDType));
  if (space.size == 0) {
    return Status::PmemOverflow;
  }
  List* list = new List{};
  list->Init(pmem_allocator_.get(), space,
             version_controller_.GetCurrentTimestamp(), key,
             list_id_.fetch_add(1), nullptr);
  {
    std::lock_guard<std::mutex> guard2{lists_mu_};
    lists_.emplace(list);
  }
  insertKeyOrElem(result, RecordType::ListRecord, list);
  return Status::Ok;
}

Status KVEngine::ListDestroy(StringView key) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();
  auto guard = hash_table_->AcquireLock(key);
  std::unique_lock<std::recursive_mutex> guard2;
  List* list;
  Status s = listFind(key, &list);
  if (s != Status::Ok) {
    return s;
  }
  return listExpire(list, 0);
}

Status KVEngine::ListLength(StringView key, size_t* sz) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();

  List* list;
  Status s = listFind(key, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
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
  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(key, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

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

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(key, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

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

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(key, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
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

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(key, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
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

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(iter->Owner()->Name(), &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
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

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(iter->Owner()->Name(), &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
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

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(iter->Owner()->Name(), &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
  kvdk_assert(list == iter->Owner(), "Iterator outdated!");
  kvdk_assert(iter->Valid(), "Trying to erase invalid iterator!");

  iter->Rep() =
      list->Erase(iter->Rep(), [&](DLRecord* rec) { delayFree(rec); });
  return Status::Ok;
}

// Replace the element at pos
Status KVEngine::ListPut(std::unique_ptr<ListIterator> const& pos,
                         StringView elem) {
  if (!CheckValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  ListIteratorImpl* iter = dynamic_cast<ListIteratorImpl*>(pos.get());
  kvdk_assert(iter != nullptr, "Invalid iterator!");

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(iter->Owner()->Name(), &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
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

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(key, &list);
  if (s != Status::Ok) {
    return nullptr;
  }
  auto guard = list->AcquireLock();
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

Status KVEngine::listExpire(List* list, ExpireTimeType t) {
  std::lock_guard<std::mutex> guard(lists_mu_);
  lists_.erase(list);
  Status s = list->SetExpireTime(t);
  lists_.insert(list);
  return s;
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

Status KVEngine::listFind(StringView key, List** list) {
  auto result = lookupKey<false>(key, RecordType::ListRecord);
  if (result.s == Status::Outdated) {
    return Status::NotFound;
  }
  if (result.s != Status::Ok) {
    return result.s;
  }
  (*list) = result.entry.GetIndex().list;
  return Status::Ok;
}

}  // namespace KVDK_NAMESPACE
