/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#pragma once

#include <string>
#include <vector>

#include "types.hpp"

namespace KVDK_NAMESPACE {

class WriteBatch {
 public:
  virtual void StringPut(const StringView key, const StringView value) = 0;
  virtual void StringDelete(const StringView key) = 0;

  virtual void SortedPut(const StringView collection, const StringView key,
                         const StringView value) = 0;
  virtual void SortedDelete(const StringView collection,
                            const StringView key) = 0;

  virtual void HashPut(const StringView collection, const StringView key,
                       const StringView value) = 0;
  virtual void HashDelete(const StringView collection,
                          const StringView key) = 0;

  virtual void Clear() = 0;

  virtual size_t Size() const = 0;

  virtual ~WriteBatch() = default;
};

}  // namespace KVDK_NAMESPACE