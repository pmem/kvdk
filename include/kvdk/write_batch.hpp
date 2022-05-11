/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#pragma once

#include <string>
#include <vector>

#include "types.hpp"

namespace KVDK_NAMESPACE {

// TODO: implement write batch deduplication
struct WriteBatch {
  struct KV {
    std::string key;
    std::string value;
    uint16_t type;
  };

  void Put(const std::string& key, const std::string& value);

  void Delete(const std::string& key);

  void Clear() { kvs.clear(); }

  size_t Size() const { return kvs.size(); }

  std::vector<KV> kvs;
};

class WriteBatch2 {
 public:
  virtual void StringPut(std::string const& key, std::string const& value) = 0;
  virtual void StringDelete(std::string const& key) = 0;

  virtual void SortedPut(std::string const& key, std::string const& field,
                         std::string const& value) = 0;
  virtual void SortedDelete(std::string const& key,
                            std::string const& field) = 0;

  virtual void HashPut(std::string const& key, std::string const& field,
                       std::string const& value) = 0;
  virtual void HashDelete(std::string const& key, std::string const& field) = 0;

  virtual void Clear() = 0;

  virtual ~WriteBatch2() = default;
};

}  // namespace KVDK_NAMESPACE