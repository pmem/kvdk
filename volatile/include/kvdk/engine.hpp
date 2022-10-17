/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <memory>
#include <string>

#include "comparator.hpp"
#include "configs.hpp"
#include "iterator.hpp"
#include "snapshot.hpp"
#include "types.hpp"
#include "write_batch.hpp"

namespace kvdk = KVDK_NAMESPACE;

namespace KVDK_NAMESPACE {
// This is the abstraction of a persistent KVDK instance
class Engine {
 public:
  // Open a new KVDK instance or restore a existing KVDK instance
  //
  // Args:
  // * engine_path: indicates the dir path that persist the instance
  // * engine_ptr: store the pointer to restored instance
  // * configs: engine configs4
  // * log_file: file to print out runtime logs
  //
  // Return:
  // Return Status::Ok on sucess, return other status for any error
  //
  // To close the instance, just delete *engine_ptr.
  static Status Open(const StringView engine_path, Engine** engine_ptr,
                     const Configs& configs, FILE* log_file = stdout);

  // Restore a KVDK instance from a backup log file.
  //
  // Args:
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
  static Status Restore(const StringView engine_path,
                        const StringView backup_log, Engine** engine_ptr,
                        const Configs& configs, FILE* log_file = stdout);

  // Get type of key, it can be String, SortedCollection, HashCollection or List
  //
  // Return:
  // Status::Ok and store type to "*type" on success
  // Status::NotFound if key does not exist
  virtual Status TypeOf(StringView key, ValueType* type) = 0;

  // Insert a STRING-type KV to set "key" to hold "value".
  //
  // Args:
  // *options: customized write options
  //
  // Return:
  // Status Ok on success .
  // Status::WrongType if key exists but is a collection.
  // Status::PMemOverflow/Status::MemoryOverflow if PMem/DRAM exhausted.
  virtual Status Put(const StringView key, const StringView value,
                     const WriteOptions& options = WriteOptions()) = 0;

  // Search the STRING-type KV of "key" in the kvdk instance.
  //
  // Return:
  // Return Status::Ok and store the corresponding value to *value on success.
  // Return Status::NotFound if the "key" does not exist.
  virtual Status Get(const StringView key, std::string* value) = 0;

  // Remove STRING-type KV of "key".
  //
  // Return:
  // Status::Ok on success or the "key" did not exist
  // Status::WrongType if key exists but is a collection type
  // Status::PMemOverflow if PMem exhausted
  virtual Status Delete(const StringView key) = 0;

  // Modify value of existing key in the engine
  //
  // Args:
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

  // Atomically do a batch of operations (Put or Delete) to the instance, these
  // operations either all succeed, or all fail. The data will be rollbacked if
  // the instance crash during a batch write
  //
  // Return:
  // Status::Ok on success
  // Status::NotFound if a collection operated by the batch does not exist
  // Status::PMemOverflow/Status::MemoryOverflow if PMem/DRAM exhausted
  //
  // Notice:
  // BatchWrite has no isolation guaranteed, if you need it, you should use
  // Transaction API
  virtual Status BatchWrite(std::unique_ptr<WriteBatch> const& batch) = 0;

  // Create a write batch for BatchWrite operation
  virtual std::unique_ptr<WriteBatch> WriteBatchCreate() = 0;

  // time to *expired_time on success.
  //
  // Args:
  // * key: STRING-type key or collection name to search.
  // * ttl_time: store TTL result.
  //
  // Return:
  // Status::Ok and store ttl time to *ttl_time on success
  // Status::NotFound if key is expired or does not exist
  virtual Status GetTTL(const StringView key, int64_t* ttl_time) = 0;

  // Set ttl_time for STRING-type or Collection type data
  //
  // Args:
  // * key: STRING-type key or collection name to set ttl_time.
  // * ttl_time: ttl time to set. if ttl_time == kPersistTTL, the name will not
  // be expired. If ttl_time <=0, the name is expired immediately.
  //
  // Return:
  // Status::Ok on success.
  // Status::NotFound if key does not exist.
  virtual Status Expire(const StringView key, int64_t ttl_time) = 0;

  // Create a empty sorted collection with configs. You should always create
  // collection before you do any operations on it
  //
  // Args:
  // * configs: customized config of creating collection
  //
  // Return:
  // Status::Ok on success
  // Status::Existed if sorted collection already existed
  // Status::WrongType if collection existed but not a sorted collection
  // Status::PMemOverflow/Status::MemoryOverflow if PMem/DRAM exhausted
  virtual Status SortedCreate(
      const StringView collection,
      const SortedCollectionConfigs& configs = SortedCollectionConfigs()) = 0;

  // Destroy a sorted collection
  // Return:
  // Status::Ok on success
  // Status::WrongType if collection existed but not a sorted collection
  // Status::MemoryOverflow if runtime/KV memory exhausted
  virtual Status SortedDestroy(const StringView collection) = 0;

  // Get number of elements in a sorted collection
  //
  // Return:
  // Status::Ok on success
  // Status::NotFound if collection not exist
  virtual Status SortedSize(const StringView collection, size_t* size) = 0;

  // Insert a KV to set "key" in sorted collection "collection"
  // to hold "value"
  // Return:
  // Status::Ok on success.
  // Status::NotFound if collection not exist.
  // Status::WrongType if collection exists but is not a sorted collection.
  // Status::PMemOverflow/Status::MemoryOverflow if PMem/DRAM exhausted.
  virtual Status SortedPut(const StringView collection, const StringView key,
                           const StringView value) = 0;

  // Search the KV of "key" in sorted collection "collection"
  //
  // Return:
  // Status::Ok and store the corresponding value to *value on success.
  // Status::NotFound If the "collection" or "key" does not exist.
  virtual Status SortedGet(const StringView collection, const StringView key,
                           std::string* value) = 0;

  // Remove KV of "key" in the sorted collection "collection".
  //
  // Return:
  // Status::Ok on success or key not existed in collection
  // Status::NotFound if collection not exist
  // Status::WrongType if collection exists but is not a sorted collection.
  // Status::PMemOverflow if PMem exhausted.
  virtual Status SortedDelete(const StringView collection,
                              const StringView key) = 0;

  // Create a KV iterator on sorted collection "collection", which is able to
  // sequentially iterate all KVs in the "collection".
  //
  // Args:
  // * snapshot: iterator will iterate all elems a t "snapshot" version, if
  // snapshot is nullptr, then a internal snapshot will be created at current
  // version and the iterator will be created on it
  // * status: store operation status if not null
  //
  // Return:
  // Return A pointer to iterator on success.
  // Return nullptr if collection not exist or any other errors, and store error
  // status to "status"
  //
  // Notice:
  // 1. Iterator will be invalid after the passed snapshot is released
  // 2. Please release the iterator as soon as it is not needed, as the holding
  // snapshot will forbid newer data being freed
  virtual SortedIterator* SortedIteratorCreate(const StringView collection,
                                               Snapshot* snapshot = nullptr,
                                               Status* s = nullptr) = 0;

  // Release a sorted iterator and its holding resouces
  virtual void SortedIteratorRelease(SortedIterator*) = 0;

  /// List APIs ///////////////////////////////////////////////////////////////

  // Create an empty List.
  // Return:
  // Status::WrongType if list name existed but is not a List.
  // Status::Existed if a List named list already exists.
  // Status::PMemOverflow if PMem exhausted.
  // Status::Ok if successfully created the List.
  virtual Status ListCreate(StringView list) = 0;

  // Destroy a List associated with key
  // Return:
  // Status::WrongType if list name is not a List.
  // Status::Ok if successfully destroyed the List or the List not existed
  // Status::PMemOverflow if PMem exhausted
  virtual Status ListDestroy(StringView list) = 0;

  // Total elements in List.
  // Return:
  // Status::InvalidDataSize if list name is too long
  // Status::WrongType if list name is not a List.
  // Status::NotFound if list does not exist or has expired.
  // Status::Ok and length of List if List exists.
  virtual Status ListSize(StringView list, size_t* sz) = 0;

  // Push element as first element of List
  // Return:
  // Status::InvalidDataSize if list name or elem is too long
  // Status::WrongType if list name is not a List.
  // Status::PMemOverflow if PMem exhausted.
  // Status::Ok if operation succeeded.
  virtual Status ListPushFront(StringView list, StringView elem) = 0;

  // Push element as last element of List
  // Return:
  // Status::InvalidDataSize if list name or elem is too long
  // Status::WrongType if list name in the instance is not a List.
  // Status::PMemOverflow if PMem exhausted.
  // Status::Ok if operation succeeded.
  virtual Status ListPushBack(StringView list, StringView elem) = 0;

  // Pop first element of list
  // Return:
  // Status::InvalidDataSize if list name is too long
  // Status::WrongType if list is not a List.
  // Status::NotFound if list does not exist or has expired.
  // Status::Ok and element if operation succeeded.
  virtual Status ListPopFront(StringView list, std::string* elem) = 0;

  // Pop last element of List
  // Return:
  // Status::InvalidDataSize if list name is too long
  // Status::WrongType if list is not a List.
  // Status::NotFound if list does not exist or has expired.
  // Status::Ok and element if operation succeeded.
  virtual Status ListPopBack(StringView list, std::string* elem) = 0;

  // Push multiple elements to the front of List
  // Return:
  // Status::InvalidDataSize if list name or elem is too long
  // Status::WrongType if list is not a List.
  // Status::PMemOverflow if PMem exhausted.
  // Status::Ok if operation succeeded.
  virtual Status ListBatchPushFront(StringView list,
                                    std::vector<std::string> const& elems) = 0;
  virtual Status ListBatchPushFront(StringView list,
                                    std::vector<StringView> const& elems) = 0;

  // Push multiple elements to the back of List
  // Return:
  // Status::InvalidDataSize if list or elem is too long
  // Status::WrongType if list name is not a List.
  // Status::PMemOverflow if PMem exhausted.
  // Status::Ok if operation succeeded.
  virtual Status ListBatchPushBack(StringView list,
                                   std::vector<std::string> const& elems) = 0;
  virtual Status ListBatchPushBack(StringView list,
                                   std::vector<StringView> const& elems) = 0;

  // Pop first N element of List
  // Return:
  // Status::InvalidDataSize if list is too long
  // Status::WrongType if list is not a List.
  // Status::NotFound if list does not exist or has expired.
  // Status::Ok and element if operation succeeded.
  virtual Status ListBatchPopFront(StringView list, size_t n,
                                   std::vector<std::string>* elems) = 0;

  // Pop last N element of List
  // Return:
  // Status::InvalidDataSize if list is too long
  // Status::WrongType if list is not a List.
  // Status::NotFound if list does not exist or has expired.
  // Status::Ok and element if operation succeeded.
  virtual Status ListBatchPopBack(StringView list, size_t n,
                                  std::vector<std::string>* elems) = 0;

  // Move element in src List at src_pos to dst List at dst_pos
  // src_pos and dst_pos can only be 0, indicating List front,
  // or -1, indicating List back.
  // Return:
  // Status::WrongType if src or dst is not a List.
  // Status::NotFound if list or element not exist
  // Status::Ok and moved element if operation succeeded.
  virtual Status ListMove(StringView src_list, ListPos src_pos,
                          StringView dst_list, ListPos dst_pos,
                          std::string* elem) = 0;

  // Insert a element to a list at index, the index can be positive or
  // negative
  // Return:
  // Status::InvalidDataSize if list name is too large
  // Status::WrongType if collection is not a list
  // Status::NotFound if collection not found or index is beyond list size
  // Status::Ok if operation succeeded
  virtual Status ListInsertAt(StringView list, StringView elem, long index) = 0;

  // Insert an element before element "pos" in list "collection"
  // Return:
  // Status::InvalidDataSize if elem is too long.
  // Status::PMemOverflow if PMem exhausted.
  // Status::NotFound if List of the ListIterator has expired or been
  // deleted, or "pos" not exist in the list
  // Status::Ok if operation succeeded.
  virtual Status ListInsertBefore(StringView list, StringView elem,
                                  StringView pos) = 0;

  // Insert an element after element "pos" in list "collection"
  // Return:
  // Status::InvalidDataSize if elem is too long.
  // Status::PMemOverflow if PMem exhausted.
  // Status::NotFound if List of the ListIterator has expired or been
  // deleted, or "pos" not exist in the list
  // Status::Ok if operation succeeded.
  virtual Status ListInsertAfter(StringView list, StringView elem,
                                 StringView pos) = 0;

  // Remove the element at index
  // Return:
  // Status::NotFound if the index beyond list size.
  // Status::Ok if operation succeeded, and store removed elem in "elem"
  virtual Status ListErase(StringView list, long index, std::string* elem) = 0;

  // Replace the element at index
  // Return:
  // Status::InvalidDataSize if elem is too long
  // Status::NotFound if if the index beyond list size.
  // Status::Ok if operation succeeded.
  virtual Status ListReplace(StringView list, long index, StringView elem) = 0;

  // Create a KV iterator on list "list", which is able to iterate all elems in
  // the list
  //
  // Args:
  // * snapshot: iterator will iterate all elems a t "snapshot" version, if
  // snapshot is nullptr, then a internal snapshot will be created at current
  // version and the iterator will be created on it
  // * status: store operation status if not null
  //
  // Return:
  // Return A pointer to iterator on success.
  // Return nullptr if list not exist or any other errors, and store error
  // status to "status"
  //
  // Notice:
  // 1. Iterator will be invalid after the passed snapshot is released
  // 2. Please release the iterator as soon as it is not needed, as the holding
  // snapshot will forbid newer data being freed
  virtual ListIterator* ListIteratorCreate(StringView list,
                                           Snapshot* snapshot = nullptr,
                                           Status* status = nullptr) = 0;
  // Release a ListIterator and its holding resources
  virtual void ListIteratorRelease(ListIterator*) = 0;

  /// Hash APIs ///////////////////////////////////////////////////////////////

  // Create a empty hash collection. You should always create collection before
  // you do any operations on it
  //
  // Return:
  // Status::Ok on success
  // Status::Existed if hash collection already existed
  // Status::WrongType if collection existed but not a hash collection
  // Status::PMemOverflow/Status::MemoryOverflow if PMem/DRAM exhausted
  virtual Status HashCreate(StringView collection) = 0;

  // Destroy a hash collection
  // Return:
  // Status::Ok on success
  // Status::WrongType if collection existed but not a hash collection
  // Status::PMemOverflow if PMem exhausted
  virtual Status HashDestroy(StringView collection) = 0;

  // Get number of elements in a hash collection
  //
  // Return:
  // Status::Ok on success
  // Status::NotFound if collection not exist
  virtual Status HashSize(StringView collection, size_t* len) = 0;

  // Search the KV of "key" in hash collection "collection"
  //
  // Return:
  // Status::Ok and store the corresponding value to *value on success.
  // Status::NotFound If the "collection" or "key" does not exist.
  virtual Status HashGet(StringView collection, StringView key,
                         std::string* value) = 0;

  // Insert a KV to set "key" in hash collection "collection"
  // to hold "value"
  // Return:
  // Status::Ok on success.
  // Status::NotFound if collection not exist.
  // Status::WrongType if collection exists but is not a hash collection.
  // Status::PMemOverflow/Status::MemoryOverflow if PMem/DRAM exhausted.
  virtual Status HashPut(StringView collection, StringView key,
                         StringView value) = 0;

  // Remove KV of "key" in the hash collection "collection".
  //
  // Return:
  // Status::Ok on success or key not existed in collection
  // Status::NotFound if collection not exist
  // Status::WrongType if collection exists but is not a hash collection.
  // Status::PMemOverflow if PMem exhausted.
  virtual Status HashDelete(StringView collection, StringView key) = 0;

  // Modify value of a existing key in a hash collection
  //
  // Args:
  // * modify_func: customized function to modify existing value of key. See
  // definition of ModifyFunc (types.hpp) for more details.
  // * modify_args: customized arguments of modify_func.
  //
  // Return:
  // Return Status::Ok if modify success.
  // Return Status::Abort if modify function abort modifying.
  // Return other non-Ok status on any error.
  virtual Status HashModify(StringView collection, StringView key,
                            ModifyFunc modify_func, void* cb_args) = 0;

  // Create a KV iterator on hash collection "collection", which is able to
  // iterate all elems in the collection at "snapshot" version, if snapshot is
  // nullptr, then a internal snapshot will be created at current version and
  // the iterator will be created on it
  //
  // Notice:
  // 1. Iterator will be invalid after the passed snapshot is released
  // 2. Please release the iterator as soon as it is not needed, as the holding
  // snapshot will forbid newer data being freed
  virtual HashIterator* HashIteratorCreate(StringView collection,
                                           Snapshot* snapshot = nullptr,
                                           Status* s = nullptr) = 0;
  virtual void HashIteratorRelease(HashIterator*) = 0;

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

  // Register a customized comparator to the engine on runtime
  //
  // Return:
  // Return true on success
  // Return false if a comparator of comparator_name already existed
  virtual bool registerComparator(const StringView& comparator_name,
                                  Comparator) = 0;

  // Close the instance on exit.
  virtual ~Engine() = 0;
};

}  // namespace KVDK_NAMESPACE
