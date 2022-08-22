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

#include "alias.hpp"
#include "data_record.hpp"
#include "dram_allocator.hpp"
#include "hash_collection/hash_list.hpp"
#include "hash_collection/rebuilder.hpp"
#include "hash_table.hpp"
#include "kvdk/engine.hpp"
#include "list_collection/list.hpp"
#include "list_collection/rebuilder.hpp"
#include "lock_table.hpp"
#include "logger.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "sorted_collection/rebuilder.hpp"
#include "sorted_collection/skiplist.hpp"
#include "structures.hpp"
#include "thread_manager.hpp"
#include "utils/utils.hpp"
#include "version/old_records_cleaner.hpp"
#include "version/version_controller.hpp"
#include "write_batch_impl.hpp"

namespace KVDK_NAMESPACE {
class KVEngine : public Engine {
  friend class SortedCollectionRebuilder;

 public:
  ~KVEngine();

  static Status Open(const std::string& name, Engine** engine_ptr,
                     const Configs& configs);

  static Status Restore(const std::string& engine_path,
                        const std::string& backup_log, Engine** engine_ptr,
                        const Configs& configs);

  Snapshot* GetSnapshot(bool make_checkpoint) override;

  Status Backup(const pmem::obj::string_view backup_log,
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

  // Expire str after ttl_time
  //
  // Notice:
  // 1. Expire assumes that str is not duplicated among all types, which is not
  // implemented yet
  // 2. Expire is not compatible with checkpoint for now
  Status Expire(const StringView str, TTLType ttl_time) override;
  // Get time to expire of str
  //
  // Notice:
  // Expire assumes that str is not duplicated among all types, which is not
  // implemented yet
  Status GetTTL(const StringView str, TTLType* ttl_time) override;

  Status TypeOf(StringView key, ValueType* type) final;

  // Global Anonymous Collection
  Status Get(const StringView key, std::string* value) override;
  Status Put(const StringView key, const StringView value,
             const WriteOptions& write_options) override;
  Status Delete(const StringView key) override;
  Status Modify(const StringView key, ModifyFunc modify_func, void* modify_args,
                const WriteOptions& options) override;

  // Sorted Collection
  Status SortedCreate(const StringView collection_name,
                      const SortedCollectionConfigs& configs) override;
  Status SortedDestroy(const StringView collection_name) override;
  Status SortedSize(const StringView collection, size_t* size) override;
  Status SortedGet(const StringView collection, const StringView user_key,
                   std::string* value) override;
  Status SortedPut(const StringView collection, const StringView user_key,
                   const StringView value) override;
  Status SortedDelete(const StringView collection,
                      const StringView user_key) override;
  Iterator* SortedIteratorCreate(const StringView collection,
                                 Snapshot* snapshot, Status* s) override;
  void SortedIteratorRelease(Iterator* sorted_iterator) override;

  void ReleaseAccessThread() override { access_thread.Release(); }

  const std::unordered_map<CollectionIDType, std::shared_ptr<Skiplist>>&
  GetSkiplists() {
    return skiplists_;
  };

  // Used by test case.
  HashTable* GetHashTable() { return hash_table_.get(); }

  void TestCleanOutDated(size_t start_slot_idx, size_t end_slot_idx);

  Cleaner* EngineCleaner() { return &cleaner_; }

 private:
  friend OldRecordsCleaner;
  friend Cleaner;

  KVEngine(const Configs& configs)
      : engine_thread_cache_(configs.max_access_threads),
        cleaner_thread_cache_(configs.max_access_threads),
        version_controller_(configs.max_access_threads),
        old_records_cleaner_(this, configs.max_access_threads),
        cleaner_(this, configs.clean_threads),
        comparators_(configs.comparator){};

  struct EngineThreadCache {
    EngineThreadCache() = default;

    char* batch_log = nullptr;

    // Info used in recovery
    uint64_t newest_restored_ts = 0;
    std::unordered_map<uint64_t, int> visited_skiplist_ids{};
  };

  struct CleanerThreadCache {
    template <typename T>
    struct OutdatedRecord {
      OutdatedRecord(T* _record, TimeStampType _release_time)
          : record(_record), release_time(_release_time) {}

      T* record;
      TimeStampType release_time;
    };

    CleanerThreadCache() = default;
    std::deque<OutdatedRecord<StringRecord>> outdated_string_records;
    std::deque<OutdatedRecord<DLRecord>> outdated_dl_records;
    SpinMutex mtx;
  };

  bool CheckKeySize(const StringView& key) { return key.size() <= UINT16_MAX; }

  bool CheckValueSize(const StringView& value) {
    return value.size() <= UINT32_MAX;
  }

  // Init basic components of the engine
  Status Init(const std::string& name, const Configs& configs);

  Status HashGetImpl(const StringView& key, std::string* value,
                     uint16_t type_mask);

  inline Status MaybeInitAccessThread() {
    return thread_manager_->MaybeInitThread(access_thread);
  }

  bool RegisterComparator(const StringView& collection_name,
                          Comparator comp_func) {
    return comparators_.RegisterComparator(collection_name, comp_func);
  }

  // List
  Status ListCreate(StringView key) final;
  Status ListDestroy(StringView key) final;
  Status ListSize(StringView key, size_t* sz) final;
  Status ListPushFront(StringView key, StringView elem) final;
  Status ListPushBack(StringView key, StringView elem) final;
  Status ListPopFront(StringView key, std::string* elem) final;
  Status ListPopBack(StringView key, std::string* elem) final;
  Status ListBatchPushFront(StringView key,
                            std::vector<std::string> const& elems) final;
  Status ListBatchPushFront(StringView key,
                            std::vector<StringView> const& elems) final;
  Status ListBatchPushBack(StringView key,
                           std::vector<std::string> const& elems) final;
  Status ListBatchPushBack(StringView key,
                           std::vector<StringView> const& elems) final;
  Status ListBatchPopFront(StringView key, size_t n,
                           std::vector<std::string>* elems) final;
  Status ListBatchPopBack(StringView key, size_t n,
                          std::vector<std::string>* elems) final;
  Status ListMove(StringView src, ListPos src_pos, StringView dst, ListPos dst_pos,
                  std::string* elem) final;
  Status ListInsertAt(StringView collection, StringView key, long index) final;
  Status ListInsertBefore(StringView collection, StringView key,
                          StringView pos) final;
  Status ListInsertAfter(StringView collection, StringView key,
                         StringView pos) final;
  Status ListErase(StringView collection, long index, std::string* elem) final;

  Status ListReplace(StringView list_name, long index, StringView elem) final;
  ListIterator* ListIteratorCreate(StringView collection, Snapshot* snapshot,
                                   Status* status) final;
  void ListIteratorRelease(ListIterator* iter) final;

  // Hash
  Status HashCreate(StringView key) final;
  Status HashDestroy(StringView key) final;
  Status HashSize(StringView key, size_t* len) final;
  Status HashGet(StringView key, StringView field, std::string* value) final;
  Status HashPut(StringView key, StringView field, StringView value) final;
  Status HashDelete(StringView key, StringView field) final;
  Status HashModify(StringView key, StringView field, ModifyFunc modify_func,
                    void* cb_args) final;
  HashIterator* HashIteratorCreate(StringView key, Snapshot* snapshot,
                                   Status* s) final;
  void HashIteratorRelease(HashIterator*) final;

 private:
  // Look up a first level key in hash table(e.g. collections or string, not
  // collection elems), the first level key should be unique among all types
  //
  // Store a copy of hash entry in LookupResult::entry, and a pointer to the
  // hash entry in LookupResult::entry_ptr
  // If may_insert is true and key not found, then store
  // pointer of a free-to-write hash entry in LookupResult::entry_ptr.
  //
  // return status:
  // Status::Ok if key exist and alive
  // Status::NotFound is key is not found.
  // Status::WrongType if type_mask does not match.
  // Status::Outdated if key has been expired or deleted
  // Status::MemoryOverflow if may_insert is true but failed to allocate new
  // hash entry
  //
  // Notice: key should be locked if set may_insert to true
  template <bool may_insert>
  HashTable::LookupResult lookupKey(StringView key, uint8_t type_mask);

  // Look up a collection element in hash table
  //
  // Store a copy of hash entry in LookupResult::entry, and a pointer to the
  // hash entry in LookupResult::entry_ptr
  // If may_insert is true and key not found, then store
  // pointer of a free-to-write hash entry in LookupResult::entry_ptr.
  //
  // return status:
  // Status::Ok if key exist
  // Status::NotFound is key is not found.
  // Status::MemoryOverflow if may_insert is true but failed to allocate new
  // hash entry
  //
  // Notice: elem should be locked if set may_insert to true
  template <bool may_insert>
  HashTable::LookupResult lookupElem(StringView key, uint8_t type_mask);

  // Remove a key or elem from hash table, ret should be return of
  // lookupKey/lookupElem
  void removeKeyOrElem(HashTable::LookupResult ret) {
    kvdk_assert(ret.s == Status::Ok || ret.s == Status::Outdated, "");
    hash_table_->Erase(ret.entry_ptr);
  }

  // insert/update key or elem to hashtable, ret must be return value of
  // lookupElem or lookupKey
  void insertKeyOrElem(HashTable::LookupResult ret, RecordType type,
                       RecordStatus status, void* addr) {
    hash_table_->Insert(ret, type, status, addr, pointerType(type));
  }

  template <typename CollectionType>
  static constexpr RecordType collectionType() {
    static_assert(std::is_same<CollectionType, Skiplist>::value ||
                      std::is_same<CollectionType, List>::value ||
                      std::is_same<CollectionType, HashList>::value,
                  "Invalid type!");
    return std::is_same<CollectionType, Skiplist>::value
               ? RecordType::SortedHeader
           : std::is_same<CollectionType, List>::value ? RecordType::ListHeader
           : std::is_same<CollectionType, HashList>::value
               ? RecordType::HashHeader
               : RecordType::Empty;
  }

  static PointerType pointerType(RecordType rtype) {
    switch (rtype) {
      case RecordType::Empty: {
        return PointerType::Empty;
      }
      case RecordType::String: {
        return PointerType::StringRecord;
      }
      case RecordType::SortedElem: {
        kvdk_assert(false, "Not supported!");
        return PointerType::Invalid;
      }
      case RecordType::SortedHeader: {
        return PointerType::Skiplist;
      }
      case RecordType::ListHeader: {
        return PointerType::List;
      }
      case RecordType::HashHeader: {
        return PointerType::HashList;
      }
      case RecordType::HashElem: {
        return PointerType::HashElem;
      }
      case RecordType::ListElem:
      default: {
        kvdk_assert(false, "Invalid type!");
        return PointerType::Invalid;
      }
    }
  }

  // Lockless. It's up to caller to lock the HashTable
  template <typename CollectionType>
  Status registerCollection(CollectionType* coll) {
    auto type = collectionType<CollectionType>();
    auto ret = lookupKey<true>(coll->Name(), type);
    if (ret.s == Status::Ok) {
      kvdk_assert(false, "Collection already registered!");
      return Status::Abort;
    }
    if (ret.s != Status::NotFound && ret.s != Status::Outdated) {
      return ret.s;
    }
    insertKeyOrElem(ret, type, RecordStatus::Normal, coll);
    return Status::Ok;
  }

  Status maybeInitBatchLogFile();

  // BatchWrite takes 3 stages
  // Stage 1: Preparation
  //  BatchWrite() sort the keys and remove duplicants,
  //  lock the keys/fields in HashTable,
  //  and allocate spaces and persist BatchWriteLog
  // Stage 2: Execution
  //  Batches are dispatched to different data types
  //  Each data type update keys/fields
  //  Outdated records are not purged in this stage.
  // Stage 3: Publish
  //  Each data type commits its batch, clean up outdated data.
  Status BatchWrite(std::unique_ptr<WriteBatch> const& batch) final;

  std::unique_ptr<WriteBatch> WriteBatchCreate() final {
    return std::unique_ptr<WriteBatch>{new WriteBatchImpl{}};
  }

  Status StringPutImpl(const StringView& key, const StringView& value,
                       const WriteOptions& write_options);

  Status StringDeleteImpl(const StringView& key);

  Status stringWritePrepare(StringWriteArgs& args, TimeStampType ts);
  Status stringWrite(StringWriteArgs& args);
  Status stringWritePublish(StringWriteArgs const& args);
  Status stringRollback(TimeStampType ts,
                        BatchWriteLog::StringLogEntry const& entry);

  Status SortedPutImpl(Skiplist* skiplist, const StringView& collection_key,
                       const StringView& value);

  Status SortedDeleteImpl(Skiplist* skiplist, const StringView& user_key);

  Status restoreExistingData();

  Status restoreDataFromBackup(const std::string& backup_log);
  Status sortedWritePrepare(SortedWriteArgs& args, TimeStampType ts);
  Status sortedWrite(SortedWriteArgs& args);
  Status sortedWritePublish(SortedWriteArgs const& args);
  Status sortedRollback(BatchWriteLog::SortedLogEntry const& entry);

  Status RestoreData();

  Status restoreSortedHeader(DLRecord* header);

  Status restoreSortedElem(DLRecord* elem);

  Status restoreStringRecord(StringRecord* pmem_record,
                             const DataEntry& cached_entry);

  bool ValidateRecord(void* data_record);

  Status initOrRestoreCheckpoint();

  Status PersistOrRecoverImmutableConfigs();

  Status batchWriteImpl(WriteBatchImpl const& batch);

  Status batchWriteRollbackLogs();

  /// List helper functions
  // Find and lock the list. Initialize non-existing if required.
  // Guarantees always return a valid List and lockes it if returns Status::Ok
  Status listFind(StringView key, List** list);

  Status listRestoreElem(DLRecord* pmp_record);

  Status listRestoreList(DLRecord* pmp_record);

  Status listBatchPushImpl(StringView key, ListPos pos,
                           std::vector<StringView> const& elems);
  Status listBatchPopImpl(StringView list_name, ListPos pos, size_t n,
                          std::vector<std::string>* elems);
  Status listRollback(BatchWriteLog::ListLogEntry const& entry);

  /// Hash helper funtions
  Status hashListFind(StringView key, HashList** hlist);

  Status restoreHashElem(DLRecord* rec);

  Status restoreHashHeader(DLRecord* rec);

  Status hashListWrite(HashWriteArgs& args);
  Status hashWritePrepare(HashWriteArgs& args, TimeStampType ts);
  Status hashListPublish(HashWriteArgs const& args);
  Status hashListRollback(BatchWriteLog::HashLogEntry const& entry);

  /// Other
  Status CheckConfigs(const Configs& configs);

  void FreeSkiplistDramNodes();

  void purgeAndFreeStringRecords(const std::vector<StringRecord*>& old_offset);

  void purgeAndFreeDLRecords(const std::vector<DLRecord*>& old_offset);

  // remove outdated records which without snapshot hold.
  template <typename T>
  T* removeOutDatedVersion(T* record, TimeStampType min_snapshot_ts);

  template <typename T>
  void removeOutdatedCollection(T* collection);

  // find delete and old records in skiplist with no hash index
  void cleanNoHashIndexedSkiplist(Skiplist* skiplist,
                                  std::vector<DLRecord*>& purge_dl_records);

  void cleanList(List* list, std::vector<DLRecord*>& purge_dl_records);

  double cleanOutDated(PendingCleanRecords& pending_clean_records,
                       size_t start_slot_idx, size_t slot_block_size);

  void purgeAndFreeAllType(PendingCleanRecords& pending_clean_records);

  void delayFree(DLRecord* addr);

  void directFree(DLRecord* addr);

  TimeStampType getTimestamp() {
    return version_controller_.GetCurrentTimestamp();
  }

  void removeSkiplist(CollectionIDType id) {
    std::lock_guard<std::mutex> lg(skiplists_mu_);
    skiplists_.erase(id);
  }

  void addSkiplistToMap(std::shared_ptr<Skiplist> skiplist) {
    std::lock_guard<std::mutex> lg(skiplists_mu_);
    skiplists_.emplace(skiplist->ID(), skiplist);
  }

  std::shared_ptr<Skiplist> getSkiplist(CollectionIDType id) {
    std::lock_guard<std::mutex> lg(skiplists_mu_);
    return skiplists_[id];
  }

  void removeHashlist(CollectionIDType id) {
    std::lock_guard<std::mutex> lg(hlists_mu_);
    hlists_.erase(id);
  }

  void addHashlistToMap(std::shared_ptr<HashList> hlist) {
    std::lock_guard<std::mutex> lg(hlists_mu_);
    hlists_.emplace(hlist->ID(), hlist);
  }

  std::shared_ptr<HashList> getHashlist(CollectionIDType id) {
    std::lock_guard<std::mutex> lg(hlists_mu_);
    return hlists_[id];
  }

  void removeList(CollectionIDType id) {
    std::lock_guard<std::mutex> lg(lists_mu_);
    lists_.erase(id);
  }

  void addListToMap(std::shared_ptr<List> list) {
    std::lock_guard<std::mutex> lg(hlists_mu_);
    lists_.emplace(list->ID(), list);
  }

  std::shared_ptr<List> getList(CollectionIDType id) {
    std::lock_guard<std::mutex> lg(lists_mu_);
    return lists_[id];
  }

  Status buildSkiplist(const StringView& name,
                       const SortedCollectionConfigs& s_configs,
                       std::shared_ptr<Skiplist>& skiplist);

  Status buildHashlist(const StringView& name,
                       std::shared_ptr<HashList>& hlist);

  Status buildList(const StringView& name, std::shared_ptr<List>& list);

  inline std::string data_file() { return data_file(dir_); }

  inline static std::string data_file(const std::string& instance_path) {
    return format_dir_path(instance_path) + "data";
  }

  inline std::string checkpoint_file() { return checkpoint_file(dir_); }

  inline static std::string checkpoint_file(const std::string& instance_path) {
    return format_dir_path(instance_path) + "checkpoint";
  }

  inline std::string config_file() { return config_file(dir_); }

  inline static std::string config_file(const std::string& instance_path) {
    return format_dir_path(instance_path) + "configs";
  }

  // If this instance is a backup of another kvdk instance
  bool RecoverToCheckpoint() {
    return configs_.recover_to_checkpoint && persist_checkpoint_->Valid();
  }

  // Run in background to report PMem usage regularly
  void backgroundPMemUsageReporter();

  // Run in background to merge and balance free space of PMem Allocator
  void backgroundPMemAllocatorOrgnizer();

  // Run in background to free obsolete DRAM space
  void backgroundDramCleaner();

  /* functions for cleaner thread cache */
  // Remove old version records from version chain of new_record and cache it
  template <typename T>
  void removeAndCacheOutdatedVersion(T* new_record);
  // Clean a outdated record in cleaner_thread_cache_
  void tryCleanCachedOutdatedRecord();
  template <typename T>
  void cleanOutdatedRecordImpl(T* record);
  // Cleaner thread fetches cached outdated records
  void FetchCachedOutdatedVersion(
      PendingCleanRecords& pending_clean_records,
      std::vector<StringRecord*>& purge_string_records,
      std::vector<DLRecord*>& purge_dl_records);
  /* functions for cleaner thread cache */

  void startBackgroundWorks();

  void terminateBackgroundWorks();

  Array<EngineThreadCache> engine_thread_cache_;
  Array<CleanerThreadCache> cleaner_thread_cache_;

  // restored kvs in reopen
  std::atomic<uint64_t> restored_{0};
  std::atomic<CollectionIDType> collection_id_{0};

  std::unique_ptr<HashTable> hash_table_;

  std::mutex skiplists_mu_;
  std::unordered_map<CollectionIDType, std::shared_ptr<Skiplist>> skiplists_;
  std::set<Skiplist*, Collection::TTLCmp> expirable_skiplists_;

  std::mutex lists_mu_;
  std::unordered_map<CollectionIDType, std::shared_ptr<List>> lists_;
  std::set<List*, Collection::TTLCmp> expirable_lists_;

  std::mutex hlists_mu_;
  std::unordered_map<CollectionIDType, std::shared_ptr<HashList>> hlists_;
  std::set<HashList*, Collection::TTLCmp> expirable_hlists_;

  std::unique_ptr<LockTable> dllist_locks_;

  std::string dir_;
  std::string batch_log_dir_;
  std::string db_file_;
  std::shared_ptr<ThreadManager> thread_manager_;
  std::unique_ptr<PMEMAllocator> pmem_allocator_;
  Configs configs_;
  bool closing_{false};
  std::vector<std::thread> bg_threads_;

  std::unique_ptr<SortedCollectionRebuilder> sorted_rebuilder_;
  std::unique_ptr<HashListRebuilder> hash_rebuilder_;
  std::unique_ptr<ListRebuilder> list_rebuilder_;
  VersionController version_controller_;
  OldRecordsCleaner old_records_cleaner_;
  Cleaner cleaner_;

  ComparatorTable comparators_;

  struct BackgroundWorkSignals {
    BackgroundWorkSignals() = default;
    BackgroundWorkSignals(const BackgroundWorkSignals&) = delete;

    std::condition_variable_any pmem_usage_reporter_cv;
    std::condition_variable_any pmem_allocator_organizer_cv;
    std::condition_variable_any dram_cleaner_cv;

    SpinMutex terminating_lock;
    bool terminating = false;
  };

  CheckPoint* persist_checkpoint_;
  std::mutex checkpoint_lock_;

  BackgroundWorkSignals bg_work_signals_;

  std::atomic<int64_t> round_robin_id_{0};
};

}  // namespace KVDK_NAMESPACE
