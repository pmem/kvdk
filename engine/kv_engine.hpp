/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cassert>
#include <cstdint>
#include <ctime>

#include <atomic>
#include <deque>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "data_record.hpp"
#include "dram_allocator.hpp"
#include "hash_table.hpp"
#include "hashmap/hashmap.hpp"
#include "kvdk/engine.hpp"
#include "logger.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "queue.hpp"
#include "skiplist.hpp"
#include "structures.hpp"
#include "thread_manager.hpp"
#include "unordered_collection.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {
class KVEngine : public Engine {
  friend class SortedCollectionRebuilder;

public:
  KVEngine();
  ~KVEngine();

  static Status Open(const std::string &name, Engine **engine_ptr,
                     const Configs &configs);

  void ReportPMemUsage();

  // Global Anonymous Collection
  Status Get(const StringView key, std::string *value) override;
  Status Set(const StringView key, const StringView value) override;
  Status Delete(const StringView key) override;
  Status BatchWrite(const WriteBatch &write_batch) override;

  // Sorted Collection
  Status SGet(const StringView collection, const StringView user_key,
              std::string *value) override;
  Status SSet(const StringView collection, const StringView user_key,
              const StringView value) override;
  // TODO: Release delete record and deleted nodes
  Status SDelete(const StringView collection,
                 const StringView user_key) override;
  std::shared_ptr<Iterator>
  NewSortedIterator(const StringView collection) override;

  // Unordered Collection
  virtual Status HGet(StringView const collection_name, StringView const key,
                      std::string *value) override;
  virtual Status HSet(StringView const collection_name, StringView const key,
                      StringView const value) override;
  virtual Status HDelete(StringView const collection_name,
                         StringView const key) override;
  std::shared_ptr<Iterator>
  NewUnorderedIterator(StringView const collection_name) override;

  // Queue
  virtual Status LPop(StringView const collection_name,
                      std::string *value) override {
    return xPop(collection_name, value, QueueOpPosition::Left);
  }

  virtual Status RPop(StringView const collection_name,
                      std::string *value) override {
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

  void ReleaseWriteThread() override { write_thread.Release(); }

  const std::vector<std::shared_ptr<Skiplist>> &GetSkiplists() {
    return skiplists_;
  };

private:
  struct BatchWriteHint {
    TimeStampType timestamp{0};
    SpaceEntry allocated_space{};
    HashTable::KeyHashHint hash_hint{};
    void *pmem_record_to_free = nullptr;
  };

  struct ThreadLocalRes {
    ThreadLocalRes() = default;

    uint64_t newest_restored_ts = 0;
    PendingBatch *persisted_pending_batch = nullptr;
    std::unordered_map<uint64_t, int> visited_skiplist_ids;
  };

  bool CheckKeySize(const StringView &key) { return key.size() <= UINT16_MAX; }

  bool CheckValueSize(const StringView &value) {
    return value.size() <= UINT32_MAX;
  }

  Status Init(const std::string &name, const Configs &configs);

  Status HashGetImpl(const StringView &key, std::string *value,
                     uint16_t type_mask);

  inline Status MaybeInitWriteThread();

  void SetCompareFunc(const pmem::obj::string_view &collection_name,
                      std::function<int(const pmem::obj::string_view &src,
                                        const pmem::obj::string_view &target)>
                          comp_func) {
    comparator_.SetComparaFunc(collection_name, comp_func);
  }

  Status
  CreateSortedCollection(const StringView collection_name,
                         Collection **collection_ptr,
                         const pmem::obj::string_view &comp_name) override;

private:
  Status InitCollection(const StringView &collection, Collection **list,
                        uint16_t collection_type);
  std::shared_ptr<UnorderedCollection>
  createUnorderedCollection(StringView const collection_name);
  std::unique_ptr<Queue> createQueue(StringView const collection_name);

  template <typename CollectionType>
  Status FindCollection(const StringView collection_name,
                        CollectionType **collection_ptr, uint64_t record_type) {
    HashTable::KeyHashHint hint = hash_table_->GetHint(collection_name);
    HashEntry hash_entry;
    HashEntry *entry_ptr = nullptr;
    Status s = hash_table_->SearchForRead(hint, collection_name, record_type,
                                          &entry_ptr, &hash_entry, nullptr);

    *collection_ptr = nullptr;
    if (s != Status::Ok) {
      return s;
    }
    *collection_ptr = (CollectionType *)hash_entry.index.ptr;
    return s;
  }

  enum class QueueOpPosition { Left, Right };
  Status xPush(StringView const collection_name, StringView const value,
               QueueOpPosition push_pos);

  Status xPop(StringView const collection_name, std::string *value,
              QueueOpPosition pop_pos);

  Status MaybeInitPendingBatchFile();

  Status StringSetImpl(const StringView &key, const StringView &value);

  Status StringDeleteImpl(const StringView &key);

  Status StringBatchWriteImpl(const WriteBatch::KV &kv,
                              BatchWriteHint &batch_hint);

  Status StringGetImpl2(const StringView &key, std::string *value);

  Status StringSetImpl2(const StringView &key, const StringView &value);

  Status StringDeleteImpl2(const StringView &key);

  Status StringBatchWriteImpl2(const WriteBatch::KV &kv,
                              BatchWriteHint &batch_hint);

  Status StringRecordRestoreImpl2(StringRecord *pmem_record);


  Status BatchWriteImpl2(const WriteBatch &write_batch);


  Status SSetImpl(Skiplist *skiplist, const StringView &user_key,
                  const StringView &value);

  Status SDeleteImpl(Skiplist *skiplist, const StringView &user_key);

  Status Recovery();

  Status RestoreData();

  Status RestoreSkiplistHead(DLRecord *pmem_record,
                             const DataEntry &cached_entry);

  Status RestoreStringRecord(StringRecord *pmem_record,
                             const DataEntry &cached_entry);

  Status RestoreSkiplistRecord(DLRecord *pmem_record,
                               const DataEntry &cached_data_entry);

  // Check if a doubly linked record has been successfully inserted, and try
  // repair un-finished prev pointer
  bool CheckAndRepairDLRecord(DLRecord *record);

  bool ValidateRecord(void *data_record);

  bool ValidateRecordAndGetValue(void *data_record, uint32_t expected_checksum,
                                 std::string *value);

  Status RestorePendingBatch();

  Status PersistOrRecoverImmutableConfigs();

  Status RestoreDlistRecords(DLRecord *pmp_record);

  Status RestoreQueueRecords(DLRecord *pmp_record);

  // Regularly works excecuted by background thread
  void BackgroundWork();

  Status CheckConfigs(const Configs &configs);

  void FreeSkiplistDramNodes();

  inline uint64_t get_cpu_tsc() {
    uint32_t lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)lo) | (((uint64_t)hi) << 32);
  }

  inline TimeStampType get_timestamp() {
    auto res = get_cpu_tsc() - ts_on_startup_ + newest_version_on_startup_;
    return res;
  }

  inline std::string db_file_name() { return dir_ + "data"; }

  inline std::string persisted_pending_block_file(int thread_id) {
    return pending_batch_dir_ + std::to_string(thread_id);
  }

  inline std::string config_file_name() { return dir_ + "configs"; }

  inline bool checkDLRecordLinkageLeft(DLRecord *pmp_record) {
    uint64_t offset = pmem_allocator_->addr2offset_checked(pmp_record);
    DLRecord *pmem_record_prev =
        pmem_allocator_->offset2addr_checked<DLRecord>(pmp_record->prev);
    return pmem_record_prev->next == offset;
  }

  inline bool checkDLRecordLinkageRight(DLRecord *pmp_record) {
    uint64_t offset = pmem_allocator_->addr2offset_checked(pmp_record);
    DLRecord *pmp_next =
        pmem_allocator_->offset2addr_checked<DLRecord>(pmp_record->next);
    return pmp_next->prev == offset;
  }

  bool checkLinkage(DLRecord *pmp_record) {
    uint64_t offset = pmem_allocator_->addr2offset_checked(pmp_record);
    DLRecord *pmp_prev =
        pmem_allocator_->offset2addr_checked<DLRecord>(pmp_record->prev);
    DLRecord *pmp_next =
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
      GlobalLogger.Error("Broken DLDataEntry linkage: prev<-curr<=>right, "
                         "which is logically impossible! Abort...\n");
      std::abort();
    }
  }

  inline void purgeAndFree(void *pmem_record) {
    DataEntry *data_entry = static_cast<DataEntry *>(pmem_record);
    data_entry->Destroy();
    pmem_allocator_->Free(
        SpaceEntry(pmem_allocator_->addr2offset_checked(pmem_record),
                   data_entry->header.record_size));
  }

  std::vector<ThreadLocalRes> thread_res_;

  // restored kvs in reopen
  std::atomic<uint64_t> restored_{0};
  std::atomic<CollectionIDType> list_id_{0};

  uint64_t ts_on_startup_ = 0;
  uint64_t newest_version_on_startup_ = 0;
  std::shared_ptr<HashTable> hash_table_;
  
  inline static StringView StringRecordExtractKey(StringRecord* p)
  {
    return p->Key();
  }

  using HMap = decltype(construct_hashmap<StringView, StringRecord*>(0, StringRecordExtractKey, std::hash<StringView>{}, std::equal_to<StringView>{}, AlignedAllocator<StringRecord*>{}));
  std::unique_ptr<HMap> hmap_;

  std::vector<std::shared_ptr<Skiplist>> skiplists_;
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
  SortedCollectionRebuilder sorted_rebuilder_;
  Comparator comparator_;
};

} // namespace KVDK_NAMESPACE
