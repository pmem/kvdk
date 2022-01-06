/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <string>

#include "collection.hpp"
#include "comparator.hpp"
#include "configs.hpp"
#include "iterator.hpp"
#include "libpmemobj++/string_view.hpp"
#include "namespace.hpp"
#include "status.hpp"
#include "write_batch.hpp"

namespace KVDK_NAMESPACE {

// This is the abstraction of a persistent KVDK instance
class Engine {
public:
  // Open a new KVDK instance or restore a existing KVDK instance with the
  // specified "name". The "name" indicates the dir path that persist the
  // instance.
  //
  // Stores a pointer to the instance in *engine_ptr on success, write logs
  // during runtime to log_file if it's not null.
  //
  // To close the instance, just delete *engine_ptr.
  static Status Open(const std::string &name, Engine **engine_ptr,
                     const Configs &configs, FILE *log_file = stdout);

  // Insert a STRING-type KV to set "key" to hold "value", return Ok on
  // successful persistence, return non-Ok on any error.
  virtual Status Set(const pmem::obj::string_view key,
                     const pmem::obj::string_view value) = 0;

  virtual Status BatchWrite(const WriteBatch &write_batch) = 0;

  // Search the STRING-type KV of "key" and store the corresponding value to
  // *value on success. If the "key" does not exist, return NotFound.
  virtual Status Get(const pmem::obj::string_view key, std::string *value) = 0;

  // Remove STRING-type KV of "key".
  // Return Ok on success or the "key" did not exist, return non-Ok on any
  // error.
  virtual Status Delete(const pmem::obj::string_view key) = 0;

  virtual Status
  CreateSortedCollection(const pmem::obj::string_view collection_name,
                         Collection **sorted_collection,
                         const pmem::obj::string_view &comp_name = "") = 0;

  // Insert a SORTED-type KV to set "key" of sorted collection "collection"
  // to hold "value", if "collection" not exist, it will be created, return Ok
  // on successful persistence, return non-Ok on any error.
  virtual Status SSet(const pmem::obj::string_view collection,
                      const pmem::obj::string_view key,
                      const pmem::obj::string_view value) = 0;

  // Search the SORTED-type KV of "key" in sorted collection "collection"
  // and store the corresponding value to *value on success. If the
  // "collection"/"key" did not exist, return NotFound.
  virtual Status SGet(const pmem::obj::string_view collection,
                      const pmem::obj::string_view key, std::string *value) = 0;

  // Remove SORTED-type KV of "key" in the sorted collection "collection".
  // Return Ok on success or the "collection"/"key" did not exist, return non-Ok
  // on any error.
  virtual Status SDelete(const pmem::obj::string_view collection,
                         const pmem::obj::string_view key) = 0;

  virtual Status HSet(const pmem::obj::string_view collection,
                      const pmem::obj::string_view key,
                      const pmem::obj::string_view value) = 0;

  virtual Status HGet(const pmem::obj::string_view collection,
                      const pmem::obj::string_view key, std::string *value) = 0;

  virtual Status HDelete(const pmem::obj::string_view collection,
                         const pmem::obj::string_view key) = 0;

  // Queue
  virtual Status LPop(pmem::obj::string_view const collection_name,
                      std::string *value) = 0;

  virtual Status RPop(pmem::obj::string_view const collection_name,
                      std::string *value) = 0;

  virtual Status LPush(pmem::obj::string_view const collection_name,
                       pmem::obj::string_view const value) = 0;

  virtual Status RPush(pmem::obj::string_view const collection_name,
                       pmem::obj::string_view const value) = 0;

  // Create a KV iterator on sorted collection "collection", which is able to
  // sequentially iterate all KVs in the "collection".
  virtual std::shared_ptr<Iterator>
  NewSortedIterator(const pmem::obj::string_view collection) = 0;

  virtual std::shared_ptr<Iterator>
  NewUnorderedIterator(pmem::obj::string_view const collection_name) = 0;

  // Release resources occupied by this write thread so new thread can take
  // part. New write requests of this thread need to re-request write resources.
  virtual void ReleaseWriteThread() = 0;

  virtual void
  SetCompareFunc(const pmem::obj::string_view &collection_name,
                 std::function<int(const pmem::obj::string_view &src,
                                   const pmem::obj::string_view &target)>) = 0;

  // Close the instance on exit.
  virtual ~Engine() = 0;
};

} // namespace KVDK_NAMESPACE
