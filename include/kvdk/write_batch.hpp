/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#pragma once

#include <string>
#include <vector>

#include "namespace.hpp"

namespace KVDK_NAMESPACE {

struct WriteBatch {
  struct KV {
    std::string key;
    std::string value;
    uint16_t type;
  };

  void Put(const std::string &key, const std::string &value);

  void Delete(const std::string &key);

  void Clear() { kvs.clear(); }

  size_t Size() const { return kvs.size(); }

  std::vector<KV> kvs;
};
} // namespace KVDK_NAMESPACE