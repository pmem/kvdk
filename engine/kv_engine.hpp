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
#include "hash_list.hpp"
#include "hash_table.hpp"
#include "kvdk/engine.hpp"
#include "lock_table.hpp"
#include "logger.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "simple_list.hpp"
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
  Iterator* NewSortedIterator(const StringView collection,
                              Snapshot* snapshot) override;
  void ReleaseSortedIterator(Iterator* sorted_iterator) override;

  void ReleaseAccessThread() override { access_thread.Release(); }

  const std::unordered_map<CollectionIDType, std::shared_ptr<Skiplist>>&
  GetSkiplists() {
    return skiplists_;
  };

  // Used by test case.
  HashTable* GetHashTable() { return hash_table_.get(); }

  void CleanOutDated(size_t start_slot_idx, size_t end_slot_idx);

 private:
  friend OldRecordsCleaner;

  KVEngine(const Configs& configs)
      : engine_thread_cache_(configs.max_access_threads),
        version_controller_(configs.max_access_threads),
        old_records_cleaner_(this, configs.max_access_threads),
        comparators_(configs.comparator){};

  struct EngineThreadCache {
    EngineThreadCache() = default;

    char* batch_log = nullptr;

    // Info used in recovery
    uint64_t newest_restored_ts = 0;
    std::unordered_map<uint64_t, int> visited_skiplist_ids{};
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
  Status ListLength(StringView key, size_t* sz) final;
  Status ListPushFront(StringView key, StringView elem) final;
  Status ListPushBack(StringView key, StringView elem) final;
  Status ListPopFront(StringView key, std::string* elem) final;
  Status ListPopBack(StringView key, std::string* elem) final;
  Status ListInsertBefore(std::unique_ptr<ListIterator> const& pos,
                          StringView elem) final;
  Status ListInsertAfter(std::unique_ptr<ListIterator> const& pos,
                         StringView elem) final;
  Status ListErase(std::unique_ptr<ListIterator> const& pos) final;
  Status ListPut(std::unique_ptr<ListIterator> const& pos,
                 StringView elem) final;
  std::unique_ptr<ListIterator> ListCreateIterator(StringView key) final;

  // Hash
  Status HashCreate(StringView key) final;
  Status HashDestroy(StringView key) final;
  Status HashLength(StringView key, size_t* len) final;
  Status HashGet(StringView key, StringView field, std::string* value) final;
  Status HashPut(StringView key, StringView field, StringView value) final;
  Status HashDelete(StringView key, StringView field) final;
  Status HashModify(StringView key, StringView field, ModifyFunc modify_func,
                    void* cb_args) final;
  std::unique_ptr<HashIterator> HashCreateIterator(StringView key) final;

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
  HashTable::LookupResult lookupKey(StringView key, uint16_t type_mask);

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
  HashTable::LookupResult lookupElem(StringView key, uint16_t type_mask);

  // Remove a key or elem from hash table, ret should be return of
  // lookupKey/lookupElem
  void removeKeyOrElem(HashTable::LookupResult ret) {
    kvdk_assert(ret.s == Status::Ok || ret.s == Status::Outdated, "");
    hash_table_->Erase(ret.entry_ptr);
  }

  // insert/update key or elem to hashtable, ret must be return value of
  // lookupElem or lookupKey
  void insertKeyOrElem(HashTable::LookupResult ret, RecordType type,
                       void* addr) {
    hash_table_->Insert(ret, type, addr, pointerType(type));
  }

  template <typename CollectionType>
  static constexpr RecordType collectionType() {
    static_assert(std::is_same<CollectionType, Skiplist>::value ||
                      std::is_same<CollectionType, List>::value ||
                      std::is_same<CollectionType, HashList>::value ||
                      std::is_same<CollectionType, StringRecord>::value,
                  "Invalid type!");
    return std::is_same<CollectionType, Skiplist>::value
               ? RecordType::SortedHeader
               : std::is_same<CollectionType, List>::value
                     ? RecordType::ListRecord
                     : std::is_same<CollectionType, HashList>::value
                           ? RecordType::HashRecord
                           : RecordType::Empty;
  }

  static PointerType pointerType(RecordType rtype) {
    switch (rtype) {
      case RecordType::Empty: {
        return PointerType::Empty;
      }
      case RecordType::StringDataRecord:
      case RecordType::StringDeleteRecord: {
        return PointerType::StringRecord;
      }
      case RecordType::SortedElem:
      case RecordType::SortedElemDelete: {
        kvdk_assert(false, "Not supported!");
        return PointerType::Invalid;
      }
      case RecordType::SortedHeaderDelete:
      case RecordType::SortedHeader: {
        return PointerType::Skiplist;
      }
      case RecordType::ListRecord: {
        return PointerType::List;
      }
      case RecordType::HashRecord: {
        return PointerType::HashList;
      }
      case RecordType::HashElem: {
        return PointerType::HashElem;
      }
      case RecordType::ListElem:
      default: {
        /// TODO: Remove Expire Flag
        kvdk_assert(false, "Invalid type!");
        return PointerType::Invalid;
      }
    }
  }

  // May lock HashTable internally, caller must call this without lock
  // HashTable!
  //
  // TODO (jiayu): replace this with lookupKey
  template <typename CollectionType>
  Status FindCollection(const StringView collection_name,
                        CollectionType** collection_ptr, uint64_t record_type) {
    auto res = lookupKey<false>(collection_name, record_type);
    if (res.s == Status::Outdated) {
      return Status::NotFound;
    }
    *collection_ptr =
        res.s == Status::Ok
            ? static_cast<CollectionType*>(res.entry_ptr->GetIndex().ptr)
            : nullptr;
    return res.s;
  }

  // Lockless. It's up to caller to lock the HashTable
  template <typename CollectionType>
  Status registerCollection(CollectionType* coll) {
    RecordType type = collectionType<CollectionType>();
    auto ret = lookupKey<true>(coll->Name(), type);
    if (ret.s == Status::Ok) {
      kvdk_assert(false, "Collection already registered!");
      return Status::Abort;
    }
    if (ret.s != Status::NotFound && ret.s != Status::Outdated) {
      return ret.s;
    }
    insertKeyOrElem(ret, type, coll);
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

  Status stringWritePrepare(StringWriteArgs& args);
  Status stringWrite(StringWriteArgs& args);
  Status stringWritePublish(StringWriteArgs const& args);
  Status stringRollback(TimeStampType ts,
                        BatchWriteLog::StringLogEntry const& entry);

  Status SortedPutImpl(Skiplist* skiplist, const StringView& collection_key,
                       const StringView& value);

  Status SortedDeleteImpl(Skiplist* skiplist, const StringView& user_key);

  Status restoreExistingData();

  Status restoreDataFromBackup(const std::string& backup_log);
  Status sortedWritePrepare(SortedWriteArgs& args);
  Status sortedWrite(SortedWriteArgs& args);
  Status sortedWritePublish(SortedWriteArgs const& args);
  Status sortedRollback(TimeStampType ts,
                        BatchWriteLog::SortedLogEntry const& entry);

  Status RestoreData();

  Status restoreSortedHeader(DLRecord* header);

  Status restoreSortedElem(DLRecord* elem);

  Status restoreStringRecord(StringRecord* pmem_record,
                             const DataEntry& cached_entry);

  bool ValidateRecord(void* data_record);

  bool ValidateRecordAndGetValue(void* data_record, uint32_t expected_checksum,
                                 std::string* value);

  Status initOrRestoreCheckpoint();

  Status PersistOrRecoverImmutableConfigs();

  Status batchWriteImpl(WriteBatchImpl const& batch);

  Status batchWriteRollbackLogs();

  /// List helper functions
  // Find and lock the list. Initialize non-existing if required.
  // Guarantees always return a valid List and lockes it if returns Status::Ok
  Status listFind(StringView key, List** list);

  Status listExpire(List* list, ExpireTimeType t);

  Status listRestoreElem(DLRecord* pmp_record);

  Status listRestoreList(DLRecord* pmp_record);

  Status listRegisterRecovered();

  // Should only be called when the List is no longer
  // accessible to any other thread.
  Status listDestroy(List* list);

  /// Hash helper funtions
  Status hashListFind(StringView key, HashList** hlist);

  Status hashListExpire(HashList* hlist, ExpireTimeType t);

  // CallBack should have signature
  // ModifyOperation(StringView const* old, StringView* new, void* args).
  // for ModifyOperation::Delete and Noop, return Status of the field.
  // for ModifyOperation::Write, return the Status of the Write.
  // for ModifyOperation::Abort, return Status::Abort.
  enum class hashElemOpImplCaller { HashGet, HashPut, HashModify, HashDelete };
  template <hashElemOpImplCaller caller, typename CallBack>
  Status hashElemOpImpl(StringView key, StringView field, CallBack cb,
                        void* cb_args);

  Status hashListRestoreElem(DLRecord* rec);

  Status hashListRestoreList(DLRecord* rec);

  Status hashListRegisterRecovered();

  // Destroy a HashList already removed from HashTable
  // Should only be called when the HashList is no longer
  // accessible to any other thread.
  Status hashListDestroy(HashList* hlist);

  Status hashListWrite(HashWriteArgs& args);
  Status hashListPublish(HashWriteArgs const& args);
  Status hashListRollback(BatchWriteLog::HashLogEntry const& entry);

  /// Other
  Status CheckConfigs(const Configs& configs);

  void FreeSkiplistDramNodes();

  void purgeAndFreeStringRecords(const std::vector<StringRecord*>& old_offset);

  void purgeAndFreeDLRecords(const std::vector<DLRecord*>& old_offset);

  // remove outdated records which without snapshot hold.
  template <typename T>
  T* removeOutDatedVersion(T* record);

  // find delete and old records in skiplist with no hash index
  void iterSkiplistWithNoHashIndex(Skiplist* skiplist,
                                   std::vector<DLRecord*>& purge_dl_records);

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

  Status buildSkiplist(const StringView& name,
                       const SortedCollectionConfigs& s_configs,
                       std::shared_ptr<Skiplist>& skiplist);

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

  // Run in background to report PMem usage regularly
  void backgroundPMemUsageReporter();

  // Run in background to merge and balance free space of PMem Allocator
  void backgroundPMemAllocatorOrgnizer();

  // Run in background to free obsolete DRAM space
  void backgroundDramCleaner();

  void backgroundCleanRecords(size_t start_slot_idx, size_t end_slot_idx);

  void deleteCollections();

  void startBackgroundWorks();

  void terminateBackgroundWorks();

  Array<EngineThreadCache> engine_thread_cache_;

  // restored kvs in reopen
  std::atomic<uint64_t> restored_{0};
  std::atomic<CollectionIDType> list_id_{0};

  std::unique_ptr<HashTable> hash_table_;

  std::mutex skiplists_mu_;
  std::unordered_map<CollectionIDType, std::shared_ptr<Skiplist>> skiplists_;

  std::mutex lists_mu_;
  std::set<List*, Collection::TTLCmp> lists_;
  std::unique_ptr<ListBuilder> list_builder_;

  std::mutex hlists_mu_;
  std::set<HashList*, Collection::TTLCmp> hash_lists_;
  std::unique_ptr<HashListBuilder> hash_list_builder_;
  std::unique_ptr<LockTable> hash_list_locks_;
  std::unique_ptr<LockTable> skiplist_locks_;

  std::string dir_;
  std::string batch_log_dir_;
  std::string db_file_;
  std::shared_ptr<ThreadManager> thread_manager_;
  std::unique_ptr<PMEMAllocator> pmem_allocator_;
  Configs configs_;
  bool closing_{false};
  std::vector<std::thread> bg_threads_;

  std::unique_ptr<SortedCollectionRebuilder> sorted_rebuilder_;
  VersionController version_controller_;
  OldRecordsCleaner old_records_cleaner_;

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
};

}  // namespace KVDK_NAMESPACE
