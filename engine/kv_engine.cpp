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
#include "sorted_collection/iterator.hpp"
#include "sorted_collection/skiplist.hpp"
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
      configs_.hash_bucket_num, configs_.hash_bucket_size,
      configs_.num_buckets_per_slot, pmem_allocator_,
      configs_.max_access_threads));
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

Status KVEngine::CreateSortedCollection(
    const StringView collection_name,
    const SortedCollectionConfigs& s_configs) {
  Status s = MaybeInitAccessThread();
  defer(ReleaseAccessThread());
  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(collection_name)) {
    return Status::InvalidDataSize;
  }

  auto hint = hash_table_->GetHint(collection_name);
  HashEntry hash_entry;
  HashEntry* entry_ptr = nullptr;
  DataEntry existing_data_entry;
  std::lock_guard<SpinMutex> lg(*hint.spin);
  entry_ptr = nullptr;
  s = hash_table_->SearchForWrite(hint, collection_name, SortedHeaderRecord,
                                  &entry_ptr, &hash_entry,
                                  &existing_data_entry);
  if (s == Status::NotFound) {
    auto comparator = comparators_.GetComparator(s_configs.comparator_name);
    if (comparator == nullptr) {
      GlobalLogger.Error("Compare function %s is not registered\n",
                         s_configs.comparator_name);
      return Status::Abort;
    }
    CollectionIDType id = list_id_.fetch_add(1);
    std::string value_str =
        Skiplist::EncodeSortedCollectionValue(id, s_configs);
    uint32_t request_size =
        sizeof(DLRecord) + collection_name.size() + value_str.size();
    SpaceEntry space_entry = pmem_allocator_->Allocate(request_size);
    if (space_entry.size == 0) {
      return Status::PmemOverflow;
    }
    // PMem level of skiplist is circular, so the next and prev pointers of
    // header point to itself
    DLRecord* pmem_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr(space_entry.offset), space_entry.size,
        version_controller_.GetCurrentTimestamp(), SortedHeaderRecord,
        kNullPMemOffset, space_entry.offset, space_entry.offset,
        collection_name, value_str);

    auto skiplist = std::make_shared<Skiplist>(
        pmem_record, string_view_2_string(collection_name), id, comparator,
        pmem_allocator_, hash_table_, s_configs.index_with_hashtable);
    {
      std::lock_guard<std::mutex> lg(list_mu_);
      skiplists_.insert({id, skiplist});
    }
    hash_table_->Insert(hint, entry_ptr, SortedHeaderRecord, skiplist.get(),
                        PointerType::Skiplist);
  } else {
    return s;
  }
  return Status::Ok;
}

Iterator* KVEngine::NewSortedIterator(const StringView collection,
                                      Snapshot* snapshot) {
  Skiplist* skiplist;
  Status s =
      FindCollection(collection, &skiplist, RecordType::SortedHeaderRecord);
  bool create_snapshot = snapshot == nullptr;
  if (create_snapshot) {
    snapshot = GetSnapshot(false);
  }

  return s == Status::Ok
             ? new SortedIterator(skiplist, pmem_allocator_,
                                  static_cast<SnapshotImpl*>(snapshot),
                                  create_snapshot)
             : nullptr;
}

void KVEngine::ReleaseSortedIterator(Iterator* sorted_iterator) {
  if (sorted_iterator == nullptr) {
    GlobalLogger.Info("pass a nullptr in KVEngine::ReleaseSortedIterator!\n");
    return;
  }
  SortedIterator* iter = static_cast<SortedIterator*>(sorted_iterator);
  if (iter->own_snapshot_) {
    ReleaseSnapshot(iter->snapshot_);
  }
  delete iter;
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
      case RecordType::SortedDataRecord:
      case RecordType::SortedDeleteRecord:
      case RecordType::SortedHeaderRecord:
      case RecordType::StringDataRecord:
      case RecordType::StringDeleteRecord:
      case RecordType::DlistRecord:
      case RecordType::DlistHeadRecord:
      case RecordType::DlistTailRecord:
      case RecordType::DlistDataRecord:
      case RecordType::ListRecord:
      case RecordType::ListElem: {
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
      case RecordType::SortedDataRecord:
      case RecordType::SortedDeleteRecord: {
        s = RestoreSkiplistRecord(
            static_cast<DLRecord*>(recovering_pmem_record));
        break;
      }
      case RecordType::SortedHeaderRecord: {
        s = RestoreSkiplistHeader(
            static_cast<DLRecord*>(recovering_pmem_record));
        break;
      }
      case RecordType::StringDataRecord:
      case RecordType::StringDeleteRecord: {
        s = RestoreStringRecord(
            static_cast<StringRecord*>(recovering_pmem_record),
            data_entry_cached);
        break;
      }
      case RecordType::DlistRecord:
      case RecordType::DlistHeadRecord:
      case RecordType::DlistTailRecord:
      case RecordType::DlistDataRecord: {
        s = RestoreDlistRecords(static_cast<DLRecord*>(recovering_pmem_record));
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
    case RecordType::SortedDataRecord:
    case RecordType::SortedDeleteRecord:
    case RecordType::SortedHeaderRecord:
    case RecordType::DlistDataRecord:
    case RecordType::DlistRecord: {
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
    case RecordType::SortedDataRecord:
    case RecordType::SortedHeaderRecord:
    case RecordType::SortedDeleteRecord:
    case RecordType::DlistDataRecord:
    case RecordType::DlistRecord:
    case RecordType::DlistHeadRecord:
    case RecordType::DlistTailRecord:
    case RecordType::ListRecord:
    case RecordType::ListElem: {
      return static_cast<DLRecord*>(data_record)->Validate();
    }
    default:
      kvdk_assert(false, "Unsupported type in ValidateRecord()!");
      return false;
  }
}

Status KVEngine::RestoreSkiplistHeader(DLRecord* header_record) {
  return sorted_rebuilder_->AddHeader(header_record);
}

Status KVEngine::RestoreStringRecord(StringRecord* pmem_record,
                                     const DataEntry& cached_entry) {
  assert(pmem_record->entry.meta.type & StringRecordType);
  if (RecoverToCheckpoint() &&
      cached_entry.meta.timestamp > persist_checkpoint_->CheckpointTS()) {
    purgeAndFree(pmem_record);
    return Status::Ok;
  }
  auto view = pmem_record->Key();
  std::string key{view.data(), view.size()};
  DataEntry existing_data_entry;
  HashEntry hash_entry;
  HashEntry* entry_ptr = nullptr;

  auto hint = hash_table_->GetHint(key);
  std::lock_guard<SpinMutex> lg(*hint.spin);
  Status s =
      hash_table_->SearchForWrite(hint, key, StringRecordType, &entry_ptr,
                                  &hash_entry, &existing_data_entry);

  if (s == Status::MemoryOverflow) {
    return s;
  }

  bool found = s == Status::Ok;
  if (found &&
      existing_data_entry.meta.timestamp >= cached_entry.meta.timestamp) {
    purgeAndFree(pmem_record);
    return Status::Ok;
  }

  hash_table_->Insert(hint, entry_ptr, cached_entry.meta.type, pmem_record,
                      PointerType::StringRecord);
  if (found) {
    purgeAndFree(hash_entry.GetIndex().ptr);
  }

  return Status::Ok;
}

bool KVEngine::CheckAndRepairDLRecord(DLRecord* record) {
  uint64_t offset = pmem_allocator_->addr2offset(record);
  DLRecord* prev = pmem_allocator_->offset2addr<DLRecord>(record->prev);
  DLRecord* next = pmem_allocator_->offset2addr<DLRecord>(record->next);
  if (prev->next != offset && next->prev != offset) {
    return false;
  }
  // Repair un-finished write
  if (next && next->prev != offset) {
    next->prev = offset;
    pmem_persist(&next->prev, 8);
  }
  return true;
}

Status KVEngine::RestoreSkiplistRecord(DLRecord* pmem_record) {
  return sorted_rebuilder_->AddElement(pmem_record);
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

  sorted_rebuilder_.reset(new SortedCollectionRebuilder(
      this, configs_.opt_large_sorted_collection_recovery,
      configs_.max_access_threads, *persist_checkpoint_));

  list_builder_.reset(new ListBuilder{pmem_allocator_.get(), &lists_, 1U});

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
  list_id_ = ret.max_id + 1;
  skiplists_.swap(ret.rebuild_skiplits);

#ifdef KVDK_DEBUG_CHECK
  for (auto skiplist : skiplists_) {
    Status s = skiplist.second->CheckIndex();
    if (s != Status::Ok) {
      GlobalLogger.Info("Check skiplist index error\n");
      return s;
    }
  }
#endif
  GlobalLogger.Info("Rebuild skiplist done\n");
  sorted_rebuilder_.reset(nullptr);

  list_builder_->RebuildLists();
  list_builder_->CleanBrokens([&](DLRecord* elem) { purgeAndFree(elem); });
  s = listRegisterRecovered();
  if (s != Status::Ok) {
    return s;
  }
  list_builder_.reset(nullptr);
  GlobalLogger.Info("Rebuild Lists done\n");

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

Status KVEngine::HashGetImpl(const StringView& key, std::string* value,
                             uint16_t type_mask) {
  DataEntry data_entry;
  while (1) {
    HashEntry hash_entry;
    HashEntry* entry_ptr = nullptr;
    bool is_found = hash_table_->SearchForRead(
                        hash_table_->GetHint(key), key, type_mask, &entry_ptr,
                        &hash_entry, &data_entry) == Status::Ok;
    if (!is_found || (hash_entry.GetRecordType() & DeleteRecordType) ||
        hash_entry.IsExpiredStatus()) {
      return Status::NotFound;
    }
    void* pmem_record = nullptr;
    if (hash_entry.GetRecordType() == StringDataRecord) {
      if (hash_entry.GetIndex().string_record->HasExpired()) {
        // push the expired string record into cleaner during scaning hash
        // table, because need to update hash entry status
        return Status::NotFound;
      }
      pmem_record = hash_entry.GetIndex().string_record;
    } else if (hash_entry.GetRecordType() == DlistDataRecord) {
      pmem_record = hash_entry.GetIndex().ptr;
    } else if (hash_entry.GetRecordType() == SortedDataRecord) {
      if (hash_entry.GetIndexType() == PointerType::SkiplistNode) {
        SkiplistNode* dram_node = hash_entry.GetIndex().skiplist_node;
        pmem_record = dram_node->record;
      } else {
        assert(hash_entry.GetIndexType() == PointerType::DLRecord);
        pmem_record = hash_entry.GetIndex().dl_record;
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

    // TODO. remove checksum validation when unordered_collection supports mvcc
    void* data_buffer = malloc(record_size);
    memcpy(data_buffer, pmem_record, record_size);
    // If the pmem data record is corrupted or modified during get, redo search
    bool valid = ValidateRecordAndGetValue(data_buffer,
                                           data_entry.header.checksum, value);
    free(data_buffer);
    if (valid) {
      break;
    }
  }

  return Status::Ok;
}

Status KVEngine::Get(const StringView key, std::string* value) {
  Status s = MaybeInitAccessThread();

  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  return HashGetImpl(key, value, StringRecordType);
}

Status KVEngine::Delete(const StringView key) {
  Status s = MaybeInitAccessThread();

  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }

  return StringDeleteImpl(key);
}

Status KVEngine::SDeleteImpl(Skiplist* skiplist, const StringView& user_key) {
  std::string collection_key(skiplist->InternalKey(user_key));
  if (!CheckKeySize(collection_key)) {
    return Status::InvalidDataSize;
  }

  auto hint = hash_table_->GetHint(collection_key);

  while (1) {
    std::unique_lock<SpinMutex> ul(*hint.spin);
    version_controller_.HoldLocalSnapshot();
    defer(version_controller_.ReleaseLocalSnapshot());
    TimeStampType new_ts =
        version_controller_.GetLocalSnapshot().GetTimestamp();

    auto ret = skiplist->Delete(user_key, hint, new_ts);
    switch (ret.s) {
      case Status::Fail:
        continue;
      case Status::PmemOverflow:
        return ret.s;
      case Status::Ok:
        ul.unlock();
        if (ret.write_record != nullptr) {
          kvdk_assert(
              ret.existing_record != nullptr &&
                  ret.existing_record->entry.meta.type == SortedDataRecord,
              "Wrong existing record type while insert a delete reocrd for "
              "sorted collection");
          delayFree(OldDataRecord{ret.existing_record, new_ts});
          if (ret.hash_entry_ptr != nullptr) {
            // delete record indexed by hash table
            delayFree(OldDeleteRecord(ret.write_record, ret.hash_entry_ptr,
                                      PointerType::HashEntry, new_ts,
                                      hint.spin));
          } else if (ret.dram_node != nullptr) {
            // no hash index, by a skiplist node points to delete record
            delayFree(OldDeleteRecord(ret.write_record, ret.dram_node,
                                      PointerType::SkiplistNode, new_ts,
                                      hint.spin));
          } else {
            // delete record nor pointed by hash entry nor skiplist node
            delayFree(OldDeleteRecord(ret.write_record, nullptr,
                                      PointerType::Empty, new_ts, hint.spin));
          }
        }
        return ret.s;
      default:
        std::abort();
    }
    break;
  }
  return Status::Ok;
}

Status KVEngine::SSetImpl(Skiplist* skiplist, const StringView& user_key,
                          const StringView& value) {
  std::string collection_key(skiplist->InternalKey(user_key));
  if (!CheckKeySize(collection_key) || !CheckValueSize(value)) {
    return Status::InvalidDataSize;
  }

  auto hint = hash_table_->GetHint(collection_key);
  while (1) {
    std::unique_lock<SpinMutex> ul(*hint.spin);
    version_controller_.HoldLocalSnapshot();
    defer(version_controller_.ReleaseLocalSnapshot());
    TimeStampType new_ts =
        version_controller_.GetLocalSnapshot().GetTimestamp();
    auto ret = skiplist->Set(user_key, value, hint, new_ts);
    switch (ret.s) {
      case Status::Fail:
        continue;
      case Status::PmemOverflow:
        break;
      case Status::Ok:
        if (ret.existing_record &&
            ret.existing_record->entry.meta.type == SortedDataRecord) {
          ul.unlock();
          delayFree(OldDataRecord{ret.existing_record, new_ts});
        }
        break;
      default:
        std::abort();  // never shoud reach
    }
    return ret.s;
  }
  return Status::Ok;
}

Status KVEngine::SSet(const StringView collection, const StringView user_key,
                      const StringView value) {
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  Skiplist* skiplist = nullptr;
  s = FindCollection(collection, &skiplist, RecordType::SortedHeaderRecord);
  if (s != Status::Ok) {
    return s;
  }
  return SSetImpl(skiplist, user_key, value);
}

Status KVEngine::CheckConfigs(const Configs& configs) {
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
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  Skiplist* skiplist = nullptr;
  s = FindCollection(collection, &skiplist, RecordType::SortedHeaderRecord);
  if (s != Status::Ok) {
    return s == Status::NotFound ? Status::Ok : s;
  }

  return SDeleteImpl(skiplist, user_key);
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

  version_controller_.HoldLocalSnapshot();
  defer(version_controller_.ReleaseLocalSnapshot());
  TimeStampType ts = version_controller_.GetLocalSnapshot().GetTimestamp();
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

Status KVEngine::Modify(const StringView key, std::string* new_value,
                        ModifyFunction modify_func,
                        const WriteOptions& write_options) {
  int64_t base_time = TimeUtils::millisecond_time();
  if (write_options.ttl_time <= 0 ||
      !TimeUtils::CheckTTL(write_options.ttl_time, base_time)) {
    return Status::InvalidArgument;
  }
  ExpireTimeType expired_time = write_options.ttl_time == kPersistTime
                                    ? kPersistTime
                                    : write_options.ttl_time + base_time;

  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }
  auto hint = hash_table_->GetHint(key);
  std::unique_lock<SpinMutex> ul(*hint.spin);
  version_controller_.HoldLocalSnapshot();
  defer(version_controller_.ReleaseLocalSnapshot());
  TimeStampType new_ts = version_controller_.GetLocalSnapshot().GetTimestamp();
  auto ret = lookupKey(key, static_cast<RecordType>(StringRecordType));
  // push it into cleaner
  if (ret.s == Status::Expired) {
    hash_table_->UpdateEntryStatus(ret.entry_ptr, HashEntryStatus::Expired);
    ul.unlock();
    delayFree(OldDeleteRecord{ret.entry.GetIndex().ptr, ret.entry_ptr,
                              PointerType::HashEntry, new_ts, hint.spin});
    return Status::NotFound;
  }
  if (ret.s == Status::Ok) {
    StringRecord* old_record = ret.entry.GetIndex().string_record;

    std::string modified_value = modify_func(old_record->Value());
    if (!CheckValueSize(modified_value)) {
      return Status::InvalidDataSize;
    }

    auto requested_size =
        key.size() + modified_value.size() + sizeof(StringRecord);
    SpaceEntry space_entry = pmem_allocator_->Allocate(requested_size);
    if (space_entry.size == 0) {
      return Status::PmemOverflow;
    }

    kvdk_assert(new_ts > old_record->GetTimestamp(),
                "old record has newer timestamp");
    StringRecord* new_record_ptr =
        pmem_allocator_->offset2addr<StringRecord>(space_entry.offset);
    StringRecord::PersistStringRecord(
        new_record_ptr, space_entry.size, new_ts, StringDataRecord,
        pmem_allocator_->addr2offset_checked(old_record), key, modified_value,
        expired_time);

    hash_table_->Insert(hint, ret.entry_ptr, StringDataRecord, new_record_ptr,
                        PointerType::StringRecord);
    ul.unlock();
    delayFree(OldDataRecord({old_record, new_ts}));
    new_value->assign(modified_value);
  }
  return ret.s;
}

Status KVEngine::StringBatchWriteImpl(const WriteBatch::KV& kv,
                                      BatchWriteHint& batch_hint) {
  DataEntry data_entry;
  HashEntry hash_entry;
  HashEntry* entry_ptr = nullptr;

  {
    auto& hash_hint = batch_hint.hash_hint;
    // hash table for the hint should be alread locked, so we do not lock it
    // here
    Status s =
        hash_table_->SearchForWrite(hash_hint, kv.key, StringRecordType,
                                    &entry_ptr, &hash_entry, &data_entry);
    if (s == Status::MemoryOverflow) {
      return s;
    }
    batch_hint.hash_entry_ptr = entry_ptr;
    bool found = s == Status::Ok;

    // Deleting kv is not existing
    if (kv.type == StringDeleteRecord && !found) {
      batch_hint.space_not_used = true;
      return Status::Ok;
    }

    kvdk_assert(!found || batch_hint.timestamp >= data_entry.meta.timestamp,
                "ts of new data smaller than existing data in batch write");

    void* block_base =
        pmem_allocator_->offset2addr(batch_hint.allocated_space.offset);

    TEST_SYNC_POINT(
        "KVEngine::BatchWrite::StringBatchWriteImpl::Pesistent::Before");

    StringRecord::PersistStringRecord(
        block_base, batch_hint.allocated_space.size, batch_hint.timestamp,
        static_cast<RecordType>(kv.type),
        found ? pmem_allocator_->addr2offset_checked(
                    hash_entry.GetIndex().string_record)
              : kNullPMemOffset,
        kv.key, kv.type == StringDataRecord ? kv.value : "");

    hash_table_->Insert(hash_hint, entry_ptr, (RecordType)kv.type, block_base,
                        PointerType::StringRecord);

    if (found) {
      if (kv.type == StringDeleteRecord) {
        batch_hint.delete_record_to_free = block_base;
      }
      if (hash_entry.GetRecordType() == StringDataRecord) {
        batch_hint.data_record_to_free = hash_entry.GetIndex().string_record;
      }
    }
  }

  return Status::Ok;
}

Status KVEngine::SGet(const StringView collection, const StringView user_key,
                      std::string* value) {
  Status s = MaybeInitAccessThread();

  if (s != Status::Ok) {
    return s;
  }
  Skiplist* skiplist = nullptr;
  s = FindCollection(collection, &skiplist, RecordType::SortedHeaderRecord);

  if (s != Status::Ok) {
    return s;
  }

  assert(skiplist);
  // Set current snapshot to this thread
  version_controller_.HoldLocalSnapshot();
  defer(version_controller_.ReleaseLocalSnapshot());
  return skiplist->Get(user_key, value);
}

Status KVEngine::GetTTL(const StringView str, TTLType* ttl_time) {
  HashTable::KeyHashHint hint = hash_table_->GetHint(str);
  std::unique_lock<SpinMutex> ul(*hint.spin);
  LookupResult res = lookupKey(str, ExpirableRecordType);
  *ttl_time = kInvalidTTL;
  if (res.s == Status::Expired) {
    return Status::NotFound;
  }

  if (res.s == Status::Ok) {
    ExpireTimeType expire_time;
    switch (res.entry_ptr->GetIndexType()) {
      case PointerType::Skiplist: {
        expire_time = res.entry_ptr->GetIndex().skiplist->GetExpireTime();
        break;
      }
      case PointerType::UnorderedCollection: {
        expire_time =
            res.entry_ptr->GetIndex().p_unordered_collection->GetExpireTime();
        break;
      }

      case PointerType::List: {
        expire_time = res.entry_ptr->GetIndex().list->GetExpireTime();
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
  return res.s;
}

Status KVEngine::Expire(const StringView str, TTLType ttl_time) {
  int64_t base_time = TimeUtils::millisecond_time();
  if (!TimeUtils::CheckTTL(ttl_time, base_time)) {
    return Status::InvalidArgument;
  }

  ExpireTimeType expired_time = TimeUtils::TTLToExpireTime(ttl_time, base_time);

  HashTable::KeyHashHint hint = hash_table_->GetHint(str);
  std::unique_lock<SpinMutex> ul(*hint.spin);
  // TODO: maybe have a wrapper function(lookupKeyAndMayClean).
  LookupResult res = lookupKey(str, ExpirableRecordType);
  if (ttl_time <= 0 /*immediately expired*/ || (res.s == Status::Expired)) {
    // Push the expired record into cleaner and update hash entry status with
    // HashEntryStatus::Expired.
    // TODO(zhichen): This `if` will be removed when completing collection
    // deletion.
    if (res.entry_ptr->GetIndexType() == PointerType::StringRecord) {
      hash_table_->UpdateEntryStatus(res.entry_ptr, HashEntryStatus::Expired);
      ul.unlock();
      delayFree(OldDeleteRecord{
          res.entry_ptr->GetIndex().ptr, res.entry_ptr, PointerType::HashEntry,
          version_controller_.GetCurrentTimestamp(), hint.spin});
    }
    return Status::NotFound;
  }

  if (res.s == Status::Ok) {
    std::string val;
    WriteOptions write_option{ttl_time, false};
    switch (res.entry_ptr->GetIndexType()) {
      case PointerType::StringRecord: {
        ul.unlock();
        res.s = Modify(
            str, &val,
            [](const StringView& val) { return string_view_2_string(val); },
            write_option);
        break;
      }
      case PointerType::Skiplist: {
        res.s = res.entry_ptr->GetIndex().skiplist->SetExpireTime(expired_time);
        break;
      }
      case PointerType::UnorderedCollection: {
        UnorderedCollection* pcoll =
            res.entry.GetIndex().p_unordered_collection;
        /// TODO: put these unregister and delete work in findKey()
        res.s = pcoll->SetExpireTime(expired_time);
        break;
      }
      case PointerType::List: {
        res.s = res.entry_ptr->GetIndex().list->SetExpireTime(expired_time);
        break;
      }
      default: {
        return Status::NotSupported;
      }
    }
  }

  // Update hash entry status to TTL
  if (res.s == Status::Ok) {
    hash_table_->UpdateEntryStatus(res.entry_ptr, expired_time == kPersistTime
                                                      ? HashEntryStatus::Persist
                                                      : HashEntryStatus::TTL);
  }
  return res.s;
}

Status KVEngine::StringDeleteImpl(const StringView& key) {
  DataEntry data_entry;
  HashEntry hash_entry;
  HashEntry* entry_ptr = nullptr;

  SpaceEntry sized_space_entry;

  {
    auto hint = hash_table_->GetHint(key);
    std::unique_lock<SpinMutex> ul(*hint.spin);
    // Set current snapshot to this thread
    version_controller_.HoldLocalSnapshot();
    defer(version_controller_.ReleaseLocalSnapshot());
    TimeStampType new_ts =
        version_controller_.GetLocalSnapshot().GetTimestamp();
    Status s = hash_table_->SearchForWrite(
        hint, key, StringDeleteRecord | StringDataRecord, &entry_ptr,
        &hash_entry, &data_entry);

    switch (s) {
      case Status::Ok: {
        if (entry_ptr->GetRecordType() == StringDeleteRecord) {
          return s;
        }
        auto request_size = key.size() + sizeof(StringRecord);
        SpaceEntry sized_space_entry = pmem_allocator_->Allocate(request_size);
        if (sized_space_entry.size == 0) {
          return Status::PmemOverflow;
        }

        void* pmem_ptr =
            pmem_allocator_->offset2addr_checked(sized_space_entry.offset);

        StringRecord::PersistStringRecord(
            pmem_ptr, sized_space_entry.size, new_ts, StringDeleteRecord,
            pmem_allocator_->addr2offset_checked(
                hash_entry.GetIndex().string_record),
            key, "");

        hash_table_->Insert(hint, entry_ptr, StringDeleteRecord, pmem_ptr,
                            PointerType::StringRecord);
        ul.unlock();
        delayFree(OldDataRecord{hash_entry.GetIndex().string_record, new_ts});
        // We also delay free this delete record to recycle PMem and DRAM
        // space
        delayFree(OldDeleteRecord(pmem_ptr, entry_ptr, PointerType::HashEntry,
                                  new_ts, hint.spin));

        return s;
      }
      case Status::NotFound:
        return Status::Ok;
      default:
        return s;
    }
  }
}

Status KVEngine::StringSetImpl(const StringView& key, const StringView& value,
                               const WriteOptions& write_options) {
  int64_t base_time = TimeUtils::millisecond_time();
  if (write_options.ttl_time <= 0 ||
      !TimeUtils::CheckTTL(write_options.ttl_time, base_time)) {
    return Status::InvalidArgument;
  }

  ExpireTimeType expired_time =
      TimeUtils::TTLToExpireTime(write_options.ttl_time, base_time);

  DataEntry data_entry;
  HashEntry hash_entry;
  HashEntry* hash_entry_ptr = nullptr;
  uint32_t v_size = value.size();

  uint32_t requested_size = v_size + key.size() + sizeof(StringRecord);

  // Space is already allocated for batch writes
  SpaceEntry sized_space_entry = pmem_allocator_->Allocate(requested_size);
  if (sized_space_entry.size == 0) {
    return Status::PmemOverflow;
  }

  {
    auto hint = hash_table_->GetHint(key);
    TEST_SYNC_POINT("KVEngine::StringSetImpl::BeforeLock");
    std::unique_lock<SpinMutex> ul(*hint.spin);
    // Set current snapshot to this thread
    version_controller_.HoldLocalSnapshot();
    defer(version_controller_.ReleaseLocalSnapshot());
    TimeStampType new_ts =
        version_controller_.GetLocalSnapshot().GetTimestamp();

    // Search position to write index in hash table.
    Status s = hash_table_->SearchForWrite(
        hint, key, StringDeleteRecord | StringDataRecord, &hash_entry_ptr,
        &hash_entry, &data_entry);
    if (s == Status::MemoryOverflow) {
      return s;
    }
    bool found = s == Status::Ok;

    void* block_base = pmem_allocator_->offset2addr(sized_space_entry.offset);

    kvdk_assert(!found || new_ts > data_entry.meta.timestamp,
                "old record has newer timestamp!");
    // Persist key-value pair to PMem
    StringRecord::PersistStringRecord(
        block_base, sized_space_entry.size, new_ts, StringDataRecord,
        found ? pmem_allocator_->addr2offset_checked(
                    hash_entry.GetIndex().string_record)
              : kNullPMemOffset,
        key, value, expired_time);

    auto updated_type = hash_entry_ptr->GetRecordType();

    hash_table_->Insert(hint, hash_entry_ptr, StringDataRecord, block_base,
                        PointerType::StringRecord);
    if (found &&
        updated_type == StringDataRecord
        /* delete record is self-freed, so we don't need to free it here */
        && !hash_entry_ptr->IsExpiredStatus()) {
      ul.unlock();
      delayFree(OldDataRecord{hash_entry.GetIndex().string_record, new_ts});
    }
  }

  return Status::Ok;
}

Status KVEngine::Set(const StringView key, const StringView value,
                     const WriteOptions& options) {
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(key) || !CheckValueSize(value)) {
    return Status::InvalidDataSize;
  }

  return StringSetImpl(key, value, options);
}

}  // namespace KVDK_NAMESPACE

// UnorderedCollection
namespace KVDK_NAMESPACE {
KVEngine::LookupResult KVEngine::lookupKey(StringView key, uint16_t type_mask) {
  LookupResult result;
  auto hint = hash_table_->GetHint(key);
  result.s = hash_table_->SearchForRead(
      hint, key, PrimaryRecordType, &result.entry_ptr, &result.entry, nullptr);
  if (result.s != Status::Ok) {
    kvdk_assert(result.s == Status::NotFound, "");
    return result;
  }

  if (result.entry_ptr->IsExpiredStatus()) {
    result.s = Status::NotFound;
    return result;
  }

  RecordType record_type = result.entry.GetRecordType();

  if (type_mask & record_type) {
    if (record_type == RecordType::StringDeleteRecord) {
      result.s = Status::NotFound;
      return result;
    }
    bool has_expired;
    switch (record_type) {
      case RecordType::StringDataRecord: {
        has_expired = result.entry.GetIndex().string_record->HasExpired();
        break;
      }
      case RecordType::DlistRecord:
      case RecordType::ListRecord:
      case RecordType::SortedHeaderRecord: {
        has_expired = TimeUtils::CheckIsExpired(
            static_cast<Collection*>(result.entry.GetIndex().ptr)
                ->GetExpireTime());
        break;
      }
      default: {
        kvdk_assert(false, "Unreachable branch!");
        std::abort();
      }
    }

    if (has_expired) {
      result.s = Status::Expired;
    }
  } else {
    result.s = Status::WrongType;
  }
  return result;
}

std::shared_ptr<UnorderedCollection> KVEngine::createUnorderedCollection(
    StringView const collection_name) {
  TimeStampType ts = version_controller_.GetCurrentTimestamp();
  CollectionIDType id = list_id_.fetch_add(1);
  std::string name(collection_name.data(), collection_name.size());
  std::shared_ptr<UnorderedCollection> sp_uncoll =
      std::make_shared<UnorderedCollection>(
          hash_table_.get(), pmem_allocator_.get(), name, id, ts);
  return sp_uncoll;
}

Status KVEngine::HGet(StringView const collection_name, StringView const key,
                      std::string* value) {
  Status s = MaybeInitAccessThread();

  if (s != Status::Ok) {
    return s;
  }

  UnorderedCollection* p_uncoll;
  s = FindCollection(collection_name, &p_uncoll, RecordType::DlistRecord);
  if (s != Status::Ok) {
    return s;
  }

  std::string internal_key = p_uncoll->InternalKey(key);
  return HashGetImpl(internal_key, value, RecordType::DlistDataRecord);
}

Status KVEngine::HSet(StringView const collection_name, StringView const key,
                      StringView const value) {
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  UnorderedCollection* p_collection;

  // Find UnorederedCollection, create if none exists
  {
    s = FindCollection(collection_name, &p_collection, RecordType::DlistRecord);
    if (s == Status::NotFound) {
      HashTable::KeyHashHint hint_collection =
          hash_table_->GetHint(collection_name);
      std::unique_lock<SpinMutex> lock_collection{*hint_collection.spin};
      {
        // Lock and find again in case other threads have created the
        // UnorderedCollection
        s = FindCollection(collection_name, &p_collection,
                           RecordType::DlistRecord);
        if (s == Status::Ok) {
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
          HashEntry* p_hash_entry_collection = nullptr;
          Status s = hash_table_->SearchForWrite(
              hint_collection, collection_name, RecordType::DlistRecord,
              &p_hash_entry_collection, &hash_entry_collection, nullptr);
          kvdk_assert(s == Status::NotFound, "Logically impossible!");
          hash_table_->Insert(hint_collection, p_hash_entry_collection,
                              RecordType::DlistRecord, p_collection,
                              PointerType::UnorderedCollection);
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

      TimeStampType ts = version_controller_.GetCurrentTimestamp();

      HashEntry hash_entry_record;
      HashEntry* p_hash_entry_record = nullptr;
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
          DLRecord* pmp_old_record = hash_entry_record.GetIndex().dl_record;
          emplace_result = p_collection->Replace(pmp_old_record, ts, key, value,
                                                 lock_record);
          // Additional check
          if (emplace_result.success) {
            kvdk_assert(pmem_allocator_->addr2offset_checked(pmp_old_record) ==
                            emplace_result.offset_old,
                        "Updated a record, but HashEntry in HashTable is "
                        "inconsistent with data on PMem!");

            DLRecord* pmp_new_record =
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
          kvdk_assert(false,
                      "Invalid search result when trying to insert "
                      "a new DlistDataRecord!");
        }
      }

      if (!emplace_result.success) {
        // Retry
        continue;
      } else {
        // Successfully emplaced the new record
        DLRecord* pmp_new_record =
            pmem_allocator_->offset2addr_checked<DLRecord>(
                emplace_result.offset_new);
        hash_table_->Insert(hint_record, p_hash_entry_record,
                            RecordType::DlistDataRecord, pmp_new_record,
                            PointerType::UnorderedCollectionElement);
        return Status::Ok;
      }
    }
  }
}

Status KVEngine::HDelete(StringView const collection_name,
                         StringView const key) {
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }
  UnorderedCollection* p_collection;
  s = FindCollection(collection_name, &p_collection, RecordType::DlistRecord);
  if (s == Status::NotFound) return Status::Ok;

  // Erase DlistDataRecord if found one.
  {
    auto internal_key = p_collection->InternalKey(key);
    HashTable::KeyHashHint hint_record = hash_table_->GetHint(internal_key);

    int n_try = 0;
    while (true) {
      ++n_try;
      std::unique_lock<SpinMutex> lock_record{*hint_record.spin};

      HashEntry hash_entry;
      HashEntry* p_hash_entry = nullptr;
      Status search_result = hash_table_->SearchForWrite(
          hint_record, internal_key, RecordType::DlistDataRecord, &p_hash_entry,
          &hash_entry, nullptr);

      ModifyReturn erase_result{};
      switch (search_result) {
        case Status::NotFound: {
          return Status::Ok;
        }
        case Status::Ok: {
          DLRecord* pmp_old_record = hash_entry.GetIndex().dl_record;

          erase_result = p_collection->Erase(pmp_old_record, lock_record);
          if (erase_result.success) {
            hash_table_->Erase(p_hash_entry);
            purgeAndFree(pmp_old_record);
            return Status::Ok;
          } else {
            // !erase_result.success
            continue;
          }
          break;
        }
        default: {
          kvdk_assert(false,
                      "Invalid search result when trying to erase "
                      "a DlistDataRecord!");
        }
      }
    }
  }
}

std::unique_ptr<Iterator> KVEngine::NewUnorderedIterator(
    StringView const collection_name) {
  UnorderedCollection* p_collection;
  Status s =
      FindCollection(collection_name, &p_collection, RecordType::DlistRecord);
  return (s == Status::Ok)
             ? std::unique_ptr<UnorderedIterator>(
                   new UnorderedIterator{p_collection->shared_from_this()})
             : nullptr;
}

Status KVEngine::RestoreDlistRecords(DLRecord* pmp_record) {
  switch (pmp_record->entry.meta.type) {
    case RecordType::DlistRecord: {
      UnorderedCollection* p_collection = nullptr;
      std::lock_guard<std::mutex> lg{list_mu_};
      {
        std::shared_ptr<UnorderedCollection> sp_collection =
            std::make_shared<UnorderedCollection>(
                hash_table_.get(), pmem_allocator_.get(), pmp_record);
        p_collection = sp_collection.get();
        vec_sp_unordered_collections_.emplace_back(sp_collection);
        CollectionIDType id = p_collection->ID();
        auto old = list_id_.load();
        while (id >= old && !list_id_.compare_exchange_strong(old, id + 1)) {
        }
      }

      std::string collection_name = p_collection->Name();
      HashTable::KeyHashHint hint_collection =
          hash_table_->GetHint(collection_name);
      std::unique_lock<SpinMutex> guard{*hint_collection.spin};

      HashEntry hash_entry_collection;
      HashEntry* p_hash_entry_collection = nullptr;
      Status s = hash_table_->SearchForWrite(
          hint_collection, collection_name, RecordType::DlistRecord,
          &p_hash_entry_collection, &hash_entry_collection, nullptr);
      hash_table_->Insert(hint_collection, p_hash_entry_collection,
                          RecordType::DlistRecord, p_collection,
                          PointerType::UnorderedCollection);
      kvdk_assert((s == Status::NotFound), "Impossible situation occurs!");
      return Status::Ok;
    }
    case RecordType::DlistHeadRecord: {
      kvdk_assert(pmp_record->prev == kNullPMemOffset &&
                      checkDLRecordLinkageRight(pmp_record),
                  "Bad linkage found when RestoreDlistRecords. Broken head.");
      return Status::Ok;
    }
    case RecordType::DlistTailRecord: {
      kvdk_assert(pmp_record->next == kNullPMemOffset &&
                      checkDLRecordLinkageLeft(pmp_record),
                  "Bad linkage found when RestoreDlistRecords. Broken tail.");
      return Status::Ok;
    }
    case RecordType::DlistDataRecord: {
      bool linked = checkLinkage(static_cast<DLRecord*>(pmp_record));
      if (!linked) {
        GlobalLogger.Error("Bad linkage!\n");
#if KVDK_DEBUG_LEVEL == 0
        GlobalLogger.Error(
            "Bad linkage can only be repaired when KVDK_DEBUG_LEVEL > 0!\n");
        std::abort();
#endif
        return Status::Ok;
      }

      auto internal_key = pmp_record->Key();
      HashTable::KeyHashHint hint_record = hash_table_->GetHint(internal_key);
      std::unique_lock<SpinMutex> lock_record{*hint_record.spin};

      HashEntry hash_entry_record;
      HashEntry* p_hash_entry_record = nullptr;
      Status search_status = hash_table_->SearchForWrite(
          hint_record, internal_key, RecordType::DlistDataRecord,
          &p_hash_entry_record, &hash_entry_record, nullptr);

      switch (search_status) {
        case Status::NotFound: {
          hash_table_->Insert(hint_record, p_hash_entry_record,
                              pmp_record->entry.meta.type, pmp_record,
                              PointerType::UnorderedCollectionElement);
          return Status::Ok;
        }
        case Status::Ok: {
          DLRecord* pmp_old_record = hash_entry_record.GetIndex().dl_record;
          if (pmp_old_record->entry.meta.timestamp <
              pmp_record->entry.meta.timestamp) {
            if (checkDLRecordLinkageRight((DLRecord*)pmp_old_record) ||
                checkDLRecordLinkageLeft((DLRecord*)pmp_old_record)) {
              assert(false && "Old record is linked in Dlinkedlist!");
              throw std::runtime_error{"Old record is linked in Dlinkedlist!"};
            }
            hash_table_->Insert(hint_record, p_hash_entry_record,
                                pmp_record->entry.meta.type, pmp_record,
                                PointerType::UnorderedCollectionElement);
            purgeAndFree(pmp_old_record);

          } else if (pmp_old_record->entry.meta.timestamp ==
                     pmp_record->entry.meta.timestamp) {
            GlobalLogger.Info("Met two DlistRecord with same timestamp");
            purgeAndFree(pmp_record);

          } else {
            if (checkDLRecordLinkageRight((DLRecord*)pmp_record) ||
                checkDLRecordLinkageLeft((DLRecord*)pmp_record)) {
              assert(false && "Old record is linked in Dlinkedlist!");
              throw std::runtime_error{"Old record is linked in Dlinkedlist!"};
            }
            purgeAndFree(pmp_record);
          }
          return Status::Ok;
        }
        default: {
          kvdk_assert(false,
                      "Invalid search result when trying to insert a new "
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

}  // namespace KVDK_NAMESPACE

// Snapshot, delayFree and background work
namespace KVDK_NAMESPACE {

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

void KVEngine::CleanOutDated() {
  int64_t interval = static_cast<int64_t>(configs_.background_work_interval);
  std::deque<OldDeleteRecord> expired_record_queue;
  // Iterate hash table
  auto start_ts = std::chrono::system_clock::now();
  auto slot_iter = hash_table_->GetSlotIterator();
  while (slot_iter.Valid()) {
    auto bucket_iter = slot_iter.Begin();
    auto end_bucket_iter = slot_iter.End();
    auto new_ts = version_controller_.GetCurrentTimestamp();
    while (bucket_iter != end_bucket_iter) {
      switch (bucket_iter->GetIndexType()) {
        case PointerType::StringRecord: {
          if (bucket_iter->IsTTLStatus() &&
              bucket_iter->GetIndex().string_record->HasExpired()) {
            hash_table_->UpdateEntryStatus(&(*bucket_iter),
                                           HashEntryStatus::Expired);
            // push expired cleaner
            expired_record_queue.push_back(OldDeleteRecord{
                bucket_iter->GetIndex().ptr, &(*bucket_iter),
                PointerType::HashEntry, new_ts, slot_iter.GetSlotLock()});
          }
          break;
        }
        case PointerType::UnorderedCollection:
        case PointerType::List:
        case PointerType::Skiplist: {
          // TODO(zhichen): check expired. and push into collection cleaner.
          break;
        }
        default:
          break;
      }
      bucket_iter++;
    }

    if (!expired_record_queue.empty() &&
        (expired_record_queue.size() >= kMaxCachedOldRecords)) {
      old_records_cleaner_.PushToGlobal(expired_record_queue);
      expired_record_queue.clear();
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
    slot_iter.Next();
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
  /// TODO: (Ziyan) use gargage collection mechanism from version controller
  /// to perform these operations lockless.
  std::unique_lock<std::recursive_mutex> guard;
  List* list;
  Status s = listFind(key, &list, true, guard);
  if (s != Status::Ok) {
    return s;
  }

  auto space = pmem_allocator_->Allocate(
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
  std::unique_lock<std::recursive_mutex> guard;
  List* list;
  Status s = listFind(key, &list, true, guard);
  if (s != Status::Ok) {
    return s;
  }

  auto space = pmem_allocator_->Allocate(
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
  std::unique_lock<std::recursive_mutex> guard;
  List* list;
  Status s = listFind(key, &list, false, guard);
  if (s != Status::Ok) {
    return s;
  }

  kvdk_assert(list->Size() != 0, "");
  auto sw = list->Front()->Value();
  elem->assign(sw.data(), sw.size());
  list->PopFront([&](DLRecord* rec) { purgeAndFree(rec); });

  if (list->Size() == 0) {
    auto guard = hash_table_->AcquireLock(key);
    auto result = removeKey(key);
    kvdk_assert(result.s == Status::Ok, "");
    listDestroy(list);
  }
  return Status::Ok;
}

Status KVEngine::ListPopBack(StringView key, std::string* elem) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  std::unique_lock<std::recursive_mutex> guard;
  List* list;
  Status s = listFind(key, &list, false, guard);
  if (s != Status::Ok) {
    return s;
  }

  kvdk_assert(list->Size() != 0, "");
  auto sw = list->Back()->Value();
  elem->assign(sw.data(), sw.size());
  list->PopBack([&](DLRecord* rec) { purgeAndFree(rec); });

  if (list->Size() == 0) {
    auto guard = hash_table_->AcquireLock(key);
    auto result = removeKey(key);
    kvdk_assert(result.s == Status::Ok, "");
    listDestroy(list);
  }
  return Status::Ok;
}

Status KVEngine::ListInsert(std::unique_ptr<ListIterator> const& pos,
                            StringView elem) {
  if (!CheckValueSize(elem)) {
    return Status::InvalidDataSize;
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

  auto space = pmem_allocator_->Allocate(
      sizeof(DLRecord) + sizeof(CollectionIDType) + elem.size());
  if (space.size == 0) {
    return Status::PmemOverflow;
  }

  iter->Rep() = list->EmplaceBefore(
      space, iter->Rep(), version_controller_.GetCurrentTimestamp(), "", elem);
  return Status::Ok;
}

Status KVEngine::ListErase(std::unique_ptr<ListIterator> const& pos) {
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
      list->Erase(iter->Rep(), [&](DLRecord* rec) { purgeAndFree(rec); });

  if (list->Size() == 0) {
    auto key = list->Name();
    auto guard = hash_table_->AcquireLock(key);
    auto result = removeKey(key);
    kvdk_assert(result.s == Status::Ok, "");
    listDestroy(list);
  }
  return Status::Ok;
}

// Replace the element at pos
Status KVEngine::ListSet(std::unique_ptr<ListIterator> const& pos,
                         StringView elem) {
  if (!CheckValueSize(elem)) {
    return Status::InvalidDataSize;
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

  auto space = pmem_allocator_->Allocate(
      sizeof(DLRecord) + sizeof(CollectionIDType) + elem.size());
  if (space.size == 0) {
    return Status::PmemOverflow;
  }
  iter->Rep() = list->Replace(space, iter->Rep(),
                              version_controller_.GetCurrentTimestamp(), "",
                              elem, [&](DLRecord* rec) { purgeAndFree(rec); });
  return Status::Ok;
}

std::unique_ptr<ListIterator> KVEngine::ListMakeIterator(StringView key) {
  if (!CheckKeySize(key)) {
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

List* KVEngine::listCreate(StringView key) {
  std::uint64_t ts = version_controller_.GetCurrentTimestamp();
  CollectionIDType id = list_id_.fetch_add(1);
  List* list = new List{};
  auto space = pmem_allocator_->Allocate(sizeof(DLRecord) + key.size() +
                                         sizeof(CollectionIDType));
  if (space.size == 0) {
    return nullptr;
  }
  list->Init(pmem_allocator_.get(), space, ts, key, id);
  std::lock_guard<std::mutex> guard{list_mu_};
  lists_.emplace_back(list);
  return list;
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
  for (auto const& list : lists_) {
    auto guard = hash_table_->AcquireLock(list->Name());
    Status s = registerCollection(list.get());
    if (s != Status::Ok) {
      return s;
    }
    max_id = std::max(max_id, list->ID());
  }
  auto old = list_id_.load();
  while (max_id >= old && !list_id_.compare_exchange_strong(old, max_id + 1)) {
  }
  return Status::Ok;
}

Status KVEngine::listDestroy(List* list) {
  while (list->Size() > 0) {
    list->PopFront([&](DLRecord* elem) { purgeAndFree(elem); });
  }
  list->Destroy([&](DLRecord* lrec) { purgeAndFree(lrec); });
  return Status::Ok;
}

Status KVEngine::listFind(StringView key, List** list, bool init_nx,
                          std::unique_lock<std::recursive_mutex>& guard) {
  // The life cycle of a List includes following stages
  // Uninitialized. List not created yet.
  // Active. Initialized and registered on HashTable.
  // Inactive. Destroyed and unregistered from HashTable.
  // Always lock List visible to other threads first,
  // then lock the HashTable to avoid deadlock.
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }
  {
    auto result = lookupKey(key, RecordType::ListRecord);
    if (result.s != Status::Ok && result.s != Status::NotFound) {
      return result.s;
    }
    if (result.s == Status::Ok) {
      (*list) = result.entry.GetIndex().list;
      guard = (*list)->AcquireLock();
      if ((*list)->Valid()) {
        // Active and successfully locked
        return Status::Ok;
      }
      // Inactive, already destroyed by other thread.
      // The inactive List will be removed from HashTable
      // by caller that destroys it with HashTable locked.
    }
    if (!init_nx) {
      // Uninitialized or Inactive
      return Status::NotFound;
    }
  }

  // Uninitialized or Inactive, initialize new one
  /// TODO: may deadlock!
  {
    auto guard2 = hash_table_->AcquireLock(key);
    auto result = lookupKey(key, RecordType::ListRecord);
    if (result.s != Status::Ok && result.s != Status::NotFound) {
      return result.s;
    }
    if (result.s == Status::Ok) {
      (*list) = result.entry.GetIndex().list;
      guard = (*list)->AcquireLock();
      kvdk_assert((*list)->Valid(), "Invalid list should have been removed!");
      return Status::Ok;
    }
    // No other thread have created one, create one here.
    (*list) = listCreate(key);
    if ((*list) == nullptr) {
      return Status::PmemOverflow;
    }
    guard = (*list)->AcquireLock();
    return registerCollection(*list);
  }
}

}  // namespace KVDK_NAMESPACE
