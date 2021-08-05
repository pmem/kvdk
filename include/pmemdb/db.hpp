/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "namespace.hpp"
#include "options.hpp"
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>

namespace PMEMDB_NAMESPACE {

enum class Status : unsigned char {
  Ok = 1,
  NotFound,
  MemoryOverflow,
  PmemOverflow,
  NotSupported,
};

class Iterator {
public:
  virtual void Seek(const std::string &key) = 0;

  virtual void SeekToFirst() = 0;

  virtual bool Valid() = 0;

  virtual bool Next() = 0;

  virtual bool Prev() = 0;

  virtual std::string Key() = 0;

  virtual std::string Value() = 0;
};

class DB {
public:
  /*
   *  Open and recover db from pmem-file.
   *  Write logs to log_file.
   */
  static Status Open(const std::string &name, DB **dbptr,
                     const DBOptions &options, FILE *log_file = nullptr);

  /*
   *  Get the value of key.
   *  If the key does not exist the Status::NotFound is returned.
   */
  virtual Status Get(const std::string &key, std::string *value) = 0;

  /*
   *  Set key to hold the string value.
   */
  virtual Status Set(const std::string &key, const std::string &value) = 0;

  /*
   *  Delete corresponding key-value of the key.
   */
  virtual Status Delete(const std::string &key) = 0;

  virtual Status SSet(const std::string &key, const std::string &value) {
    return Status::NotSupported;
  }

  virtual Status SGet(const std::string &key, std::string *value) {
    return Status::NotSupported;
  }

  virtual std::shared_ptr<Iterator> NewSortedIterator() { return nullptr; }

  /*
   * Close the db on exit.
   */
  virtual ~DB() = 0;
};

} // namespace PMEMDB_NAMESPACE