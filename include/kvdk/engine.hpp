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
// This is the abstraction of a persistent KVDK instance
class Engine {
 public:
  // Open a new KVDK instance or restore a existing KVDK instance
  //
  // Para:
  // * engine_path: indicates the dir path that persist the instance
  // * engine_ptr: store the pointer to restored instance
  // * configs: engine configs4
  // * log_file: file to print out runtime logs
  //
  // Return:
  // Return Status::Ok on sucess, return other status for any error
  //
  // To close the instance, just delete *engine_ptr.
  static Status Open(const std::string& engine_path, Engine** engine_ptr,
                     const Configs& configs, FILE* log_file = stdout);

  // Restore a KVDK instance from a backup log file.
  //
  // Para:
  // * engine_path: indicates the dir path that persist the instance
  // * backup_log: the backup log file restored from
  // * engine_ptr: store the pointer to restored instance
  // * configs: engine configs
  // * log_file: file to print out runtime logs
  //
  // Return:
  // Return Status::Ok on sucess, return other status for any error
  //
  // Notice:
  // "engine_path" should be an empty dir
  static Status Restore(const std::string& engine_path,
                        const std::string& backup_log, Engine** engine_ptr,
                        const Configs& configs, FILE* log_file = stdout);

  // Insert a STRING-type KV to set "key" to hold "value", return Ok on
  // successful persistence, return non-Ok on any error.
  virtual Status Put(const StringView key, const StringView value,
                     const WriteOptions& options = WriteOptions()) = 0;

  // Modify value of existing key in the engine
  //
  // Para:
  // * modify_func: customized function to modify existing value of key. See
  // definition of ModifyFunc (types.hpp) for more details.
  // * modify_args: customized arguments of modify_func.
  //
  // Return:
  // Return Status::Ok if modify success.
  // Return Status::Abort if modify function abort modifying.
  // Return other non-Ok status on any error.
  virtual Status Modify(const StringView key, ModifyFunc modify_func,
                        void* modify_args,
                        const WriteOptions& options = WriteOptions()) = 0;

  virtual Status BatchWrite(std::unique_ptr<WriteBatch> const& batch) = 0;

  virtual std::unique_ptr<WriteBatch> WriteBatchCreate() = 0;

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

  /* Put the STRING-type or Collection type expired time.
   * @param ttl_time is negetive or positive number.
   * If ttl_time == INT64_MAX, the key is persistent;
   * If ttl_time <=0, the key is expired immediately;
   */
  virtual Status Expire(const StringView str, int64_t ttl_time) = 0;

  // Remove STRING-type KV of "key".
  // Return Ok on success or the "key" did not exist, return non-Ok on any
  // error.
  virtual Status Delete(const StringView key) = 0;

  virtual Status SortedCreate(
      const StringView collection_name,
      const SortedCollectionConfigs& configs = SortedCollectionConfigs()) = 0;

  virtual Status SortedDestroy(const StringView collection_name) = 0;

  // Get number of elements in a sorted collection
  //
  // Return Ok on success, return NotFound if collection not exist
  virtual Status SortedSize(const StringView collection, size_t* size) = 0;

  // Insert a SORTED-type KV to set "key" of sorted collection "collection"
  // to hold "value", if "collection" not exist, it will be created, return
  // Ok on successful persistence, return non-Ok on any error.
  virtual Status SortedPut(const StringView collection, const StringView key,
                           const StringView value) = 0;

  // Search the SORTED-type KV of "key" in sorted collection "collection"
  // and store the corresponding value to *value on success. If the
  // "collection"/"key" did not exist, return NotFound.
  virtual Status SortedGet(const StringView collection, const StringView key,
                           std::string* value) = 0;

  // Remove SORTED-type KV of "key" in the sorted collection "collection".
  // Return Ok on success or the "collection"/"key" did not exist, return non-Ok
  // on any error.
  virtual Status SortedDelete(const StringView collection,
                              const StringView key) = 0;

  /// List APIs ///////////////////////////////////////////////////////////////

  // Create an empty List.
  // Return:
  //    Status::WrongType if key is not a List.
  //    Status::Existed if a List named key already exists.
  //    Status::PMemOverflow if PMem exhausted.
  //    Status::Ok if successfully created the List.
  virtual Status ListCreate(StringView key) = 0;

  // Destroy a List associated with key
  // Return:
  //    Status::WrongType if key is not a List.
  //    Status::Ok if successfully destroyed the List.
  //    Status::NotFound if key does not exist.
  virtual Status ListDestroy(StringView key) = 0;

  // Total elements in List.
  // Return:
  //    Status::InvalidDataSize if key is too long
  //    Status::WrongType if key is not a List.
  //    Status::NotFound if key does not exist or has expired.
  //    Status::Ok and length of List if List exists.
  virtual Status ListLength(StringView key, size_t* sz) = 0;

  // Push element as first element of List
  // Return:
  //    Status::InvalidDataSize if key or elem is too long
  //    Status::WrongType if key is not a List.
  //    Status::PMemOverflow if PMem exhausted.
  //    Status::Ok if operation succeeded.
  virtual Status ListPushFront(StringView key, StringView elem) = 0;

  // Push element as last element of List
  // Return:
  //    Status::InvalidDataSize if key or elem is too long
  //    Status::WrongType if key is not a List.
  //    Status::PMemOverflow if PMem exhausted.
  //    Status::Ok if operation succeeded.
  virtual Status ListPushBack(StringView key, StringView elem) = 0;

  // Pop first element of List
  // Erase the key if List is empty after pop.
  // Return:
  //    Status::InvalidDataSize if key is too long
  //    Status::WrongType if key is not a List.
  //    Status::NotFound if key does not exist or has expired.
  //    Status::Ok and element if operation succeeded.
  virtual Status ListPopFront(StringView key, std::string* elem) = 0;

  // Pop last element of List
  // Return:
  //    Status::InvalidDataSize if key is too long
  //    Status::WrongType if key is not a List.
  //    Status::NotFound if key does not exist or has expired.
  //    Status::Ok and element if operation succeeded.
  virtual Status ListPopBack(StringView key, std::string* elem) = 0;

  // Push multiple elements to the front of List
  // Return:
  //    Status::InvalidDataSize if key or elem is too long
  //    Status::WrongType if key is not a List.
  //    Status::PMemOverflow if PMem exhausted.
  //    Status::Ok if operation succeeded.
  virtual Status ListMultiPushFront(StringView key,
                                    std::vector<std::string> const& elems) = 0;
  virtual Status ListMultiPushFront(StringView key,
                                    std::vector<StringView> const& elems) = 0;

  // Push multiple elements to the back of List
  // Return:
  //    Status::InvalidDataSize if key or elem is too long
  //    Status::WrongType if key is not a List.
  //    Status::PMemOverflow if PMem exhausted.
  //    Status::Ok if operation succeeded.
  virtual Status ListMultiPushBack(StringView key,
                                   std::vector<std::string> const& elems) = 0;
  virtual Status ListMultiPushBack(StringView key,
                                   std::vector<StringView> const& elems) = 0;

  // Pop first N element of List
  // Return:
  //    Status::InvalidDataSize if key is too long
  //    Status::WrongType if key is not a List.
  //    Status::NotFound if key does not exist or has expired.
  //    Status::Ok and element if operation succeeded.
  virtual Status ListMultiPopFront(StringView key, size_t n,
                                   std::vector<std::string>* elems) = 0;

  // Pop last N element of List
  // Return:
  //    Status::InvalidDataSize if key is too long
  //    Status::WrongType if key is not a List.
  //    Status::NotFound if key does not exist or has expired.
  //    Status::Ok and element if operation succeeded.
  virtual Status ListMultiPopBack(StringView key, size_t n,
                                  std::vector<std::string>* elems) = 0;

  // Move element in src List at src_pos to dst List at dst_pos
  // src_pos and dst_pos can only be 0, indicating List front,
  // or -1, indicating List back.
  // Return:
  //    Status::InvalidDataSize if key is too long
  //    Status::WrongType if key is not a List.
  //    Status::NotFound if key does not exist or has expired.
  //    Status::Ok and element if operation succeeded.
  virtual Status ListMove(StringView src, int src_pos, StringView dst,
                          int dst_pos, std::string* elem) = 0;

  // Insert an element before element indexed by ListIterator pos
  // Return:
  //    Status::InvalidDataSize if elem is too long. pos is unchanged.
  //    Status::PMemOverflow if PMem exhausted. pos is unchanged.
  //    Status::NotFound if List of the ListIterator has expired or been
  //    deleted. pos is unchanged but is invalid.
  //    Status::Ok if operation succeeded. pos points to inserted element.
  virtual Status ListInsertBefore(std::unique_ptr<ListIterator> const& pos,
                                  StringView elem) = 0;

  // Insert an element after element indexed by ListIterator pos
  // Return:
  //    Status::InvalidDataSize if elem is too long. pos is unchanged.
  //    Status::PMemOverflow if PMem exhausted. pos is unchanged.
  //    Status::NotFound if List of the ListIterator has expired or been
  //    deleted. pos is unchanged but is invalid.
  //    Status::Ok if operation succeeded. pos points to inserted element.
  virtual Status ListInsertAfter(std::unique_ptr<ListIterator> const& pos,
                                 StringView elem) = 0;

  // Remove the element indexed by ListIterator pos
  // Return:
  //    Status::NotFound if List of the ListIterator has expired or been
  //    deleted. pos is unchanged but is invalid.
  //    Status::Ok if operation succeeded. pos points
  //    to successor of erased element.
  virtual Status ListErase(std::unique_ptr<ListIterator> const& pos) = 0;

  // Replace the element at pos
  // Return:
  //    Status::InvalidDataSize if elem is too long
  //    Status::NotFound if List of the ListIterator has expired or been
  //    deleted. pos is unchanged but invalid.
  //    Status::Ok if operation succeeded. pos points to updated element.
  virtual Status ListReplace(std::unique_ptr<ListIterator> const& pos,
                             StringView elem) = 0;

  // Create an ListIterator from List
  // Return:
  //    nullptr if List is not found or has expired, or key is not of type List,
  //    otherwise return ListIterator to First element of List
  // Internally ListIterator holds an recursive of List, which is relased
  // on destruction of ListIterator
  virtual std::unique_ptr<ListIterator> ListCreateIterator(StringView key) = 0;

  /// Hash APIs ///////////////////////////////////////////////////////////////

  virtual Status HashCreate(StringView key) = 0;
  virtual Status HashDestroy(StringView key) = 0;
  virtual Status HashLength(StringView key, size_t* len) = 0;
  virtual Status HashGet(StringView key, StringView field,
                         std::string* value) = 0;
  virtual Status HashPut(StringView key, StringView field,
                         StringView value) = 0;
  virtual Status HashDelete(StringView key, StringView field) = 0;
  virtual Status HashModify(StringView key, StringView field,
                            ModifyFunc modify_func, void* cb_args) = 0;
  // Warning: HashIterator internally holds a snapshot,
  // prevents some resources from being freed.
  // The HashIterator should be destroyed as long as it is no longer used.
  virtual std::unique_ptr<HashIterator> HashCreateIterator(StringView key) = 0;

  /// Other ///////////////////////////////////////////////////////////////////

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

  // Make a backup on "snapshot" to "backup_log"
  virtual Status Backup(const pmem::obj::string_view backup_log,
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
