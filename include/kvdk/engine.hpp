/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdio>
#include <cstring>
#include <memory>
#include <string>

#include "configs.hpp"
#include "iterator.hpp"
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
  virtual Status Set(const std::string &key, const std::string &value) = 0;

  virtual Status BatchWrite(const WriteBatch &write_batch) = 0;

  // Search the STRING-type KV of "key" and store the corresponding value to
  // *value on success. If the "key" does not exist, return NotFound.
  virtual Status Get(const std::string &key, std::string *value) = 0;

  // Remove STRING-type KV of "key".
  // Return Ok on success or the "key" did not exist, return non-Ok on any
  // error.
  virtual Status Delete(const std::string &key) = 0;

  // Insert a SORTED-type KV to set "key" of sorted collection "collection"
  // to hold "value", if "collection" not exist, it will be created, return Ok
  // on successful persistence, return non-Ok on any error.
  virtual Status SSet(const std::string &collection, const std::string &key,
                      const std::string &value) = 0;

  // Search the SORTED-type KV of "key" in sorted collection "collection"
  // and store the corresponding value to *value on success. If the
  // "collection"/"key" did not exist, return NotFound.
  virtual Status SGet(const std::string &collection, const std::string &key,
                      std::string *value) = 0;

  // Remove SORTED-type KV of "key" in the sorted collection "collection".
  // Return Ok on success or the "collection"/"key" did not exist, return non-Ok
  // on any error.
  virtual Status SDelete(const std::string &collection,
                         const std::string &key) = 0;

  // Create a KV iterator on sorted collection "collection", which is able to
  // sequentially iterate all KVs in the "collection".
  virtual std::shared_ptr<Iterator>
  NewSortedIterator(const std::string &collection) = 0;

  // Close the instance on exit.
  virtual ~Engine() = 0;
};

} // namespace KVDK_NAMESPACE