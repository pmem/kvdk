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

  virtual ~WriteBatch() = default;
};

}  // namespace KVDK_NAMESPACE