/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <deque>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

#include "data_record.hpp"
#include "dram_allocator.hpp"
#include "hash_table.hpp"
#include "kvdk/engine.hpp"
#include "logger.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "queue.hpp"
#include "skiplist.hpp"
#include "structures.hpp"
#include "thread_manager.hpp"
#include "unordered_collection.hpp"
#include "utils/utils.hpp"
#include "version/old_records_cleaner.hpp"
#include "version/version_controller.hpp"

namespace KVDK_NAMESPACE {
class KVEngine : public Engine {
  friend class SortedCollectionRebuilder;

 public:
  ~KVEngine();

  static Status Open(const std::string& name, Engine** engine_ptr,
                     const Configs& configs);

  Snapshot* GetSnapshot(bool make_checkpoint) override;

  Status Backup(const pmem::obj::string_view backup_path,
                const Snapshot* snapshot) override;

  void ReleaseSnapshot(const Snapshot* snapshot) override {
    {
      std::lock_guard<std::mutex> lg(checkpoint_lock_);
      persist_checkpoint_->MaybeRelease(
          static_cast<const SnapshotImpl*>(snapshot));
    }
    version_controller_.ReleaseSnapshot(
        static_cast<const SnapshotImpl*>(snapshot));
  }
  void ReportPMemUsage();

  Status GetTTL(const StringView str, TTLTimeType* ttl_time) override;

  Status Expire(const StringView str, TTLTimeType ttl_time) override;

  // Global Anonymous Collection
  Status Get(const StringView key, std::string* value) override;
  Status Set(const StringView key, const StringView value,
             const WriteOptions& write_options) override;
  Status Delete(const StringView key) override;
  Status BatchWrite(const WriteBatch& write_batch) override;

  // Sorted Collection
  Status SGet(const StringView collection, const StringView user_key,
              std::string* value) override;
  Status SSet(const StringView collection, const StringView user_key,
              const StringView value) override;
  Status SDelete(const StringView collection,
                 const StringView user_key) override;
  Iterator* NewSortedIterator(const StringView collection,
                              Snapshot* snapshot) override;
  void ReleaseSortedIterator(Iterator* sorted_iterator) override;

  // Unordered Collection
  virtual Status HGet(StringView const collection_name, StringView const key,
                      std::string* value) override;
  virtual Status HSet(StringView const collection_name, StringView const key,
                      StringView const value) override;
  virtual Status HDelete(StringView const collection_name,
                         StringView const key) override;
  std::shared_ptr<Iterator> NewUnorderedIterator(
      StringView const collection_name) override;

  // Queue
  virtual Status LPop(StringView const collection_name,
                      std::string* value) override {
    return xPop(collection_name, value, QueueOpPosition::Left);
  }

  virtual Status RPop(StringView const collection_name,
                      std::string* value) override {
    return xPop(collection_name, value, QueueOpPosition::Right);
  }

  virtual Status LPush(StringView const collection_name,
                       StringView const value) override {
    return xPush(collection_name, value, QueueOpPosition::Left);
  }

  virtual Status RPush(StringView const collection_name,
                       StringView const value) override {
    return xPush(collection_name, value, QueueOpPosition::Right);
  }

  void ReleaseAccessThread() override { access_thread.Release(); }

  const std::unordered_map<uint64_t, std::shared_ptr<Skiplist>>&
  GetSkiplists() {
    return skiplists_;
  };

  // Used by test case.
  const std::shared_ptr<HashTable>& GetHashTable() { return hash_table_; }

 private:
  friend OldRecordsCleaner;

  KVEngine(const Configs& configs)
      : engine_thread_cache_(configs.max_access_threads),
        version_controller_(configs.max_access_threads),
        old_records_cleaner_(this, configs.max_access_threads),
        comparators_(configs.comparator){};

  struct BatchWriteHint {
    TimeStampType timestamp{0};
    SpaceEntry allocated_space{};
    HashTable::KeyHashHint hash_hint{};
    HashEntry* hash_entry_ptr = nullptr;
    void* data_record_to_free = nullptr;
    void* delete_record_to_free = nullptr;
    bool space_not_used{false};
  };

  struct EngineThreadCache {
    EngineThreadCache() = default;

    PendingBatch* persisted_pending_batch = nullptr;
    // This thread is doing batch write
    bool batch_writing = false;

    // Info used in recovery
    uint64_t newest_restored_ts = 0;
    std::unordered_map<uint64_t, int> visited_skiplist_ids{};
  };

  bool CheckKeySize(const StringView& key) { return key.size() <= UINT16_MAX; }

  bool CheckValueSize(const StringView& value) {
    return value.size() <= UINT32_MAX;
  }

  bool CheckTTLOverFlow(TTLTimeType ttl_time, int64_t base_time) {
    // check overflow
    if (ttl_time > INT64_MAX - base_time) {
      return false;
    }
    return true;
  }

  Status Init(const std::string& name, const Configs& configs);

  Status HashGetImpl(const StringView& key, std::string* value,
                     uint16_t type_mask);

  inline Status MaybeInitAccessThread();

  bool RegisterComparator(const StringView& collection_name,
                          Comparator comp_func) {
    return comparators_.RegisterComparator(collection_name, comp_func);
  }

  Status CreateSortedCollection(
      const StringView collection_name, Collection** collection_ptr,
      const SortedCollectionConfigs& configs) override;

 private:
  std::shared_ptr<UnorderedCollection> createUnorderedCollection(
      StringView const collection_name);
  std::unique_ptr<Queue> createQueue(StringView const collection_name);

  template <typename CollectionType>
  Status FindCollection(const StringView collection_name,
                        CollectionType** collection_ptr, uint64_t record_type) {
    HashTable::KeyHashHint hint = hash_table_->GetHint(collection_name);
    HashEntry hash_entry;
    HashEntry* entry_ptr = nullptr;
    Status s = hash_table_->SearchForRead(hint, collection_name, record_type,
                                          &entry_ptr, &hash_entry, nullptr);

    *collection_ptr = nullptr;
    if (s != Status::Ok) {
      return s;
    }
    *collection_ptr = (CollectionType*)hash_entry.GetIndex().ptr;

    // check collection is expired.
    if (TimeUtils::CheckIsExpired((*collection_ptr)->GetExpiredTime())) {
      hash_table_->Erase(entry_ptr);
      // TODO(Zhichen): add background cleaner.
      return Status::NotFound;
    }
    return s;
  }

  enum class QueueOpPosition { Left, Right };
  Status xPush(StringView const collection_name, StringView const value,
               QueueOpPosition push_pos);

  Status xPop(StringView const collection_name, std::string* value,
              QueueOpPosition pop_pos);

  Status MaybeInitPendingBatchFile();

  Status StringSetImpl(const StringView& key, const StringView& value,
                       const WriteOptions& write_options);

  Status StringDeleteImpl(const StringView& key);

  Status StringBatchWriteImpl(const WriteBatch::KV& kv,
                              BatchWriteHint& batch_hint);

  Status SSetImpl(Skiplist* skiplist, const StringView& collection_key,
                  const StringView& value);

  Status UpdateHeadWithExpiredTime(Skiplist* skiplist,
                                   ExpiredTimeType expired_time);

  Status InplaceUpdatedExpiredTime(const StringView& str,
                                   ExpiredTimeType expired_time,
                                   RecordType record_type);

  Status SDeleteImpl(Skiplist* skiplist, const StringView& user_key);

  Status Recovery();

  Status RestoreData();

  Status RestoreSkiplistHead(DLRecord* pmem_record,
                             const DataEntry& cached_entry);

  Status RestoreStringRecord(StringRecord* pmem_record,
                             const DataEntry& cached_entry);

  Status RestoreSkiplistRecord(DLRecord* pmem_record,
                               const DataEntry& cached_data_entry);

  // Check if a doubly linked record has been successfully inserted, and try
  // repair un-finished prev pointer
  bool CheckAndRepairDLRecord(DLRecord* record);

  bool ValidateRecord(void* data_record);

  bool ValidateRecordAndGetValue(void* data_record, uint32_t expected_checksum,
                                 std::string* value);

  Status RestorePendingBatch();

  Status MaybeRestoreBackup();

  Status RestoreCheckpoint();

  Status PersistOrRecoverImmutableConfigs();

  Status RestoreDlistRecords(DLRecord* pmp_record);

  Status RestoreQueueRecords(DLRecord* pmp_record);

  Status CheckConfigs(const Configs& configs);

  void FreeSkiplistDramNodes();

  inline void delayFree(const OldDeleteRecord&);

  inline void delayFree(const OldDataRecord&);

  inline std::string data_file() { return data_file(dir_); }

  inline static std::string data_file(const std::string& instance_path) {
    return format_dir_path(instance_path) + "data";
  }

  inline std::string persisted_pending_block_file(int thread_id) {
    return pending_batch_dir_ + std::to_string(thread_id);
  }

  inline std::string backup_mark_file() { return backup_mark_file(dir_); }

  inline std::string checkpoint_file() { return checkpoint_file(dir_); }

  inline static std::string checkpoint_file(const std::string& instance_path) {
    return format_dir_path(instance_path) + "checkpoint";
  }

  inline static std::string backup_mark_file(const std::string& instance_path) {
    return format_dir_path(instance_path) + "backup_mark";
  }

  inline std::string config_file() { return config_file(dir_); }

  inline static std::string config_file(const std::string& instance_path) {
    return format_dir_path(instance_path) + "configs";
  }

  inline bool checkDLRecordLinkageLeft(DLRecord* pmp_record) {
    uint64_t offset = pmem_allocator_->addr2offset_checked(pmp_record);
    DLRecord* pmem_record_prev =
        pmem_allocator_->offset2addr_checked<DLRecord>(pmp_record->prev);
    return pmem_record_prev->next == offset;
  }

  inline bool checkDLRecordLinkageRight(DLRecord* pmp_record) {
    uint64_t offset = pmem_allocator_->addr2offset_checked(pmp_record);
    DLRecord* pmp_next =
        pmem_allocator_->offset2addr_checked<DLRecord>(pmp_record->next);
    return pmp_next->prev == offset;
  }

  // If this instance is a backup of another kvdk instance
  bool RecoverToCheckpoint() {
    return configs_.recover_to_checkpoint && persist_checkpoint_->Valid();
  }

  bool checkLinkage(DLRecord* pmp_record) {
    uint64_t offset = pmem_allocator_->addr2offset_checked(pmp_record);
    DLRecord* pmp_prev =
        pmem_allocator_->offset2addr_checked<DLRecord>(pmp_record->prev);
    DLRecord* pmp_next =
        pmem_allocator_->offset2addr_checked<DLRecord>(pmp_record->next);
    bool is_linked_left = (pmp_prev->next == offset);
    bool is_linked_right = (pmp_next->prev == offset);

    if (is_linked_left && is_linked_right) {
      return true;
    } else if (!is_linked_left && !is_linked_right) {
      return false;
    } else if (is_linked_left && !is_linked_right) {
      /// TODO: Repair this situation
      GlobalLogger.Error(
          "Broken DLDataEntry linkage: prev<=>curr->right, abort...\n");
      std::abort();
    } else {
      GlobalLogger.Error(
          "Broken DLDataEntry linkage: prev<-curr<=>right, "
          "which is logically impossible! Abort...\n");
      std::abort();
    }
  }

  inline void purgeAndFree(void* pmem_record) {
    DataEntry* data_entry = static_cast<DataEntry*>(pmem_record);
    data_entry->Destroy();
    pmem_allocator_->Free(
        SpaceEntry(pmem_allocator_->addr2offset_checked(pmem_record),
                   data_entry->header.record_size));
  }

  // Run in background to clean old records regularly
  void backgroundOldRecordCleaner();

  // Run in background to report PMem usage regularly
  void backgroundPMemUsageReporter();

  // Run in background to merge and balance free space of PMem Allocator
  void backgroundPMemAllocatorOrgnizer();

  // Run in background to free obsolete DRAM space
  void backgroundDramCleaner();

  // void backgroundWorkCoordinator();

  uint64_t ExpiredCleaner();
  void backgroundExpiredCleaner();

  void startBackgroundWorks();

  void terminateBackgroundWorks();

  Array<EngineThreadCache> engine_thread_cache_;

  // restored kvs in reopen
  std::atomic<uint64_t> restored_{0};
  std::atomic<CollectionIDType> list_id_{0};

  std::shared_ptr<HashTable> hash_table_;

  std::unordered_map<uint64_t, std::shared_ptr<Skiplist>> skiplists_;
  std::vector<std::shared_ptr<UnorderedCollection>>
      vec_sp_unordered_collections_;
  std::vector<std::unique_ptr<Queue>> queue_uptr_vec_;
  std::mutex list_mu_;

  std::string dir_;
  std::string pending_batch_dir_;
  std::string db_file_;
  std::shared_ptr<ThreadManager> thread_manager_;
  std::shared_ptr<PMEMAllocator> pmem_allocator_;
  Configs configs_;
  bool closing_{false};
  std::vector<std::thread> bg_threads_;
  std::unique_ptr<SortedCollectionRebuilder> sorted_rebuilder_;
  VersionController version_controller_;
  OldRecordsCleaner old_records_cleaner_;

  bool bg_cleaner_processing_;

  ComparatorTable comparators_;

  struct BackgroundWorkSignals {
    BackgroundWorkSignals() = default;
    BackgroundWorkSignals(const BackgroundWorkSignals&) = delete;

    std::condition_variable_any old_records_cleaner_cv;
    std::condition_variable_any pmem_usage_reporter_cv;
    std::condition_variable_any pmem_allocator_organizer_cv;
    std::condition_variable_any dram_cleaner_cv;

    SpinMutex terminating_lock;
    bool terminating = false;
  };

  CheckPoint* persist_checkpoint_;
  std::mutex checkpoint_lock_;

  BackgroundWorkSignals bg_work_signals_;
};

}  // namespace KVDK_NAMESPACE
