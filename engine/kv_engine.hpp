/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdlib>
#include <memory>
#include <vector>

#include "engine_impl.hpp"
#include "pmemdb/db.hpp"
#include "utils.hpp"

namespace PMEMDB_NAMESPACE {

class KVEngine : DB {
public:
  static Status Open(const std::string &name, DB **dbptr,
                     const DBOptions &options);

  KVEngine();
  ~KVEngine();

  void Init(const std::string &name, const DBOptions &options);

  // may return notfound
  Status Get(const std::string &key, std::string *value) override;

  Status Set(const std::string &key, const std::string &value) override;

  Status Delete(const std::string &key) override { return pmem_.Delete(key); }

  Status SGet(const std::string &key, std::string *value) override {
    return pmem_.SGet(key, value);
  }

  Status SSet(const std::string &key, const std::string &value) override {
    return pmem_.SSet(key, value);
  }

  virtual std::shared_ptr<Iterator> NewSortedIterator() override {
    return pmem_.NewSortedIterator();
  }

private:
  EngineImpl pmem_;
};
} // namespace PMEMDB_NAMESPACE