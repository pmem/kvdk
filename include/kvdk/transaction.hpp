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
  virtual Status StringPut(const StringView key, const StringView value) = 0;
  virtual Status StringDelete(const StringView key) = 0;
  virtual Status StringGet(const StringView key, std::string* value) = 0;
  virtual Status SortedPut(const StringView collection, const StringView key,
                           const StringView value) = 0;
  virtual Status SortedDelete(const StringView collection,
                              const StringView key) = 0;
  virtual Status SortedGet(const StringView collection, const StringView key,
                           std::string* value) = 0;
  virtual Status HashPut(const StringView collection, const StringView key,
                         const StringView value) = 0;
  virtual Status HashDelete(const StringView collection,
                            const StringView key) = 0;
  virtual Status HashGet(const StringView collection, const StringView key,
                         std::string* value) = 0;

  virtual Status Commit() = 0;
  virtual void Rollback() = 0;
  virtual Status InternalStatus() = 0;
  virtual ~Transaction() = default;
};
}  // namespace KVDK_NAMESPACE