/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <string>

#include "types.hpp"

namespace KVDK_NAMESPACE {
class Transaction {
 public:
  // TODO: use StringView instead of std::string
  virtual Status StringPut(const std::string& key,
                           const std::string& value) = 0;
  virtual Status StringDelete(const std::string& key) = 0;
  virtual Status StringGet(const std::string& key, std::string* value) = 0;

  virtual Status Commit() = 0;
  virtual void Rollback() = 0;
  virtual Status InternalStatus() = 0;
  virtual ~Transaction() = default;
};
}  // namespace KVDK_NAMESPACE