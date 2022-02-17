/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <functional>
#include <string>
#include <unordered_map>

#include "kvdk/namespace.hpp"
#include "libpmemobj++/string_view.hpp"

namespace KVDK_NAMESPACE {
using StringView = pmem::obj::string_view;
using CompareFunc =
    std::function<int(const StringView& src, const StringView& target)>;

class ComparatorTable {
 public:
  // Register a string compare function to the table
  //
  // Return true on success, return false if compara_name already existed
  bool RegisterCompareFunc(const StringView& compara_name,
                           CompareFunc comp_func) {
    std::string name(compara_name.data(), compara_name.size());
    if (compara_table_.find(name) == compara_table_.end()) {
      compara_table_.emplace(name, comp_func);
      return true;
    } else {
      return false;
    }
  }

  CompareFunc GetCompareFunc(const StringView& compara_name) {
    std::string name(compara_name.data(), compara_name.size());
    auto iter = compara_table_.find(name);
    if (iter != compara_table_.end()) {
      return iter->second;
    }
    return nullptr;
  };

 private:
  std::unordered_map<std::string, CompareFunc> compara_table_;
};
}  // namespace KVDK_NAMESPACE