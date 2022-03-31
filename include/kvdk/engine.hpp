/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <memory>
#include <string>

#include "comparator.hpp"
#include "configs.hpp"
#include "iterator.hpp"
#include "status.hpp"
#include "types.hpp"
#include "write_batch.hpp"

namespace kvdk = KVDK_NAMESPACE;

namespace KVDK_NAMESPACE {

using ModifyFunction = std::function<std::string(const StringView& src)>;

// This is the abstraction of a persistent KVDK instance
class Engine {
 public:
  using GetterCallBack = void (*)(StringView, void*);
  // Default GetterCallBack
  static void CopyToString(StringView src, void* dst) {
    static_cast<std::string*>(dst)->assign(src.data(), src.size());
  }

  // Open a new KVDK instance or restore a existing KVDK instance with the
  // specified "name". The "name" indicates the dir path that persist the
  // instance.
  //
  // Stores a pointer to the instance in *engine_ptr on success, write logs
  // during runtime to log_file if it's not null.
  //
  // To close the instance, just delete *engine_ptr.
  static Status Open(const std::string& name, Engine** engine_ptr,
                     const Configs& configs, FILE* log_file = stdout);

  // Insert a STRING-type KV to set "key" to hold "value", return Ok on
  // successful persistence, return non-Ok on any error.
  virtual Status Set(const StringView key, const StringView value,
                     const WriteOptions& options = WriteOptions()) = 0;

  // Modify value of existing key in the engine according to "modify_func"
  //
  // Store modify result in "new_value" and return Ok if key exist and
  // sccessfully update value to modifed one
  virtual Status Modify(const StringView key, std::string* new_value,
                        ModifyFunction modify_func,
                        const WriteOptions& options = WriteOptions()) = 0;

  virtual Status BatchWrite(const WriteBatch& write_batch) = 0;

  // Search the STRING-type KV of "key" and store the corresponding value to
  // *value on success. If the "key" does not exist, return NotFound.
  virtual Status Get(const StringView key, std::string* value) = 0;

  // Search the STRING-type or Collection and store the corresponding expired
  // time to *expired_time on success.
  /*
   * @param ttl_time.
   * If the key is persist, ttl_time is INT64_MAX and Status::Ok;
   * If the key is expired or does not exist, ttl_time is 0 and return
   * Status::NotFound.
   */
  virtual Status GetTTL(const StringView str, int64_t* ttl_time) = 0;

  /* Set the STRING-type or Collection type expired time.
   * @param ttl_time is negetive or positive number.
   * If ttl_time == INT64_MAX, the key is persistent;
   * If ttl_time <=0, the key is expired immediately;
   */
  virtual Status Expire(const StringView str, int64_t ttl_time) = 0;

  // Remove STRING-type KV of "key".
  // Return Ok on success or the "key" did not exist, return non-Ok on any
  // error.
  virtual Status Delete(const StringView key) = 0;

  virtual Status CreateSortedCollection(
      const StringView collection_name,
      const SortedCollectionConfigs& configs = SortedCollectionConfigs()) = 0;

  // Insert a SORTED-type KV to set "key" of sorted collection "collection"
  // to hold "value", if "collection" not exist, it will be created, return Ok
  // on successful persistence, return non-Ok on any error.
  virtual Status SSet(const StringView collection, const StringView key,
                      const StringView value) = 0;

  // Search the SORTED-type KV of "key" in sorted collection "collection"
  // and store the corresponding value to *value on success. If the
  // "collection"/"key" did not exist, return NotFound.
  virtual Status SGet(const StringView collection, const StringView key,
                      std::string* value) = 0;

  // Remove SORTED-type KV of "key" in the sorted collection "collection".
  // Return Ok on success or the "collection"/"key" did not exist, return non-Ok
  // on any error.
  virtual Status SDelete(const StringView collection, const StringView key) = 0;

  virtual Status HSet(const StringView collection, const StringView key,
                      const StringView value) = 0;

  virtual Status HGet(const StringView collection, const StringView key,
                      std::string* value) = 0;

  virtual Status HDelete(const StringView collection, const StringView key) = 0;

  /// List
  // When the type of key is not a list, an error is returned.
  enum class ListPosition { Before, After, Left, Right };
  // List operations are guaranteed to be atomic.
  // User may manually lock the list to atomically perform multiple operations
  virtual Status ListLock(StringView key) = 0;
  virtual Status ListTryLock(StringView key) = 0;
  virtual Status ListUnlock(StringView key) = 0;

  // Total elements in List
  virtual Status ListLength(StringView key, size_t* sz) = 0;

  // Scan the list and find the element(s) specified. Return the index/indices.
  virtual Status ListPos(StringView key, StringView elem,
                         std::vector<size_t>* indices, IndexType rank = 1,
                         size_t count = 1, size_t max_len = 0) = 0;
  virtual Status ListPos(StringView key, StringView elem, size_t* index,
                         IndexType rank = 1, size_t max_len = 0) = 0;

  // Iterate through elements in a certain range in List
  virtual Status ListRange(StringView key, IndexType start, IndexType stop,
                           GetterCallBack cb, void* cb_args) = 0;

  // Indexing a element in List.
  // First element has index 0, last element has index -1
  virtual Status ListIndex(StringView key, IndexType index, GetterCallBack cb,
                           void* cb_args) = 0;
  virtual Status ListIndex(StringView key, IndexType index,
                           std::string* elem) = 0;

  // Push element to List at pos
  // pos must be Position::Left or Position::Right
  virtual Status ListPush(StringView key, ListPosition pos,
                          StringView elem) = 0;

  // pos must be Position::Left or Position::Right
  virtual Status ListPop(StringView key, ListPosition pos, GetterCallBack cb,
                         void* cb_args, size_t cnt = 1) = 0;
  virtual Status ListPop(StringView key, ListPosition pos,
                         std::string* elem) = 0;

  // Insert a element Before or After pivot.
  // Use rank to skip the first (rank-1) pivot.
  virtual Status ListInsert(StringView key, ListPosition pos, IndexType pivot,
                            StringView elem) = 0;
  virtual Status ListInsert(StringView key, ListPosition pos, StringView pivot,
                            StringView elem, IndexType rank = 1) = 0;

  // Remove specified element
  // negative cnt will remove |cnt| elem from end of list
  virtual Status ListRemove(StringView key, IndexType cnt, StringView elem) = 0;

  // Replace the element at index
  virtual Status ListSet(StringView key, IndexType index, StringView elem) = 0;

  // Get a snapshot of the instance at this moment.
  // If set make_checkpoint to true, a persistent checkpoint will be made until
  // this snapshot is released. You can recover KVDK instance to the checkpoint
  // version during recovery, then the checkpoint will be removed.
  //
  // Notice:
  // 1. You can maintain multiple snapshot but only the last checkpoint.
  // 2. Please release the snapshot as soon as it is not needed, as it will
  // forbid newer data being freed
  virtual Snapshot* GetSnapshot(bool make_checkpoint) = 0;

  // Make a backup on "snapshot" to "backup_path"
  virtual Status Backup(const pmem::obj::string_view backup_path,
                        const Snapshot* snapshot) = 0;

  // Release a snapshot of the instance
  virtual void ReleaseSnapshot(const Snapshot*) = 0;

  // Create a KV iterator on sorted collection "collection", which is able to
  // sequentially iterate all KVs in the "collection" at "snapshot" version, if
  // snapshot is nullptr, then a internal snapshot will be created at current
  // version and the iterator will be created on it
  //
  // Notice:
  // 1. Iterator will be invalid after the passed snapshot is released
  // 2. Please release the iterator as soon as it is not needed, as the holding
  // snapshot will forbid newer data being freed
  virtual Iterator* NewSortedIterator(const StringView collection,
                                      Snapshot* snapshot = nullptr) = 0;

  // Release a sorted iterator
  virtual void ReleaseSortedIterator(Iterator*) = 0;

  virtual std::shared_ptr<Iterator> NewUnorderedIterator(
      StringView const collection_name) = 0;

  // Release resources occupied by this access thread so new thread can take
  // part. New write requests of this thread need to re-request write resources.
  virtual void ReleaseAccessThread() = 0;

  // Register a customized comparator to the engine on runtime
  //
  // Return true on success, return false if a comparator of comparator_name
  // already existed
  virtual bool RegisterComparator(const StringView& comparator_name,
                                  Comparator) = 0;

  // Close the instance on exit.
  virtual ~Engine() = 0;
};

}  // namespace KVDK_NAMESPACE
