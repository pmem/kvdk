/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <functional>
#include <string>
#include <unordered_map>

#include "types.hpp"

namespace KVDK_NAMESPACE {
using StringView = pmem::obj::string_view;
using Comparator =
    std::function<int(const StringView& src, const StringView& target)>;

class ComparatorTable {
 public:
  // Register a string compare function to the table
  //
  // Return true on success, return false if comparator_name already existed
  bool RegisterComparator(const StringView& comparator_name,
                          Comparator comp_func) {
    std::string name(comparator_name.data(), comparator_name.size());
    if (comparator_table_.find(name) == comparator_table_.end()) {
      comparator_table_.emplace(name, comp_func);
      return true;
    } else {
      return false;
    }
  }

  // Return a registered comparator "comparator_name" on success, return nullptr
  // if it's not existing
  Comparator GetComparator(const StringView& comparator_name) {
    std::string name(comparator_name.data(), comparator_name.size());
    auto iter = comparator_table_.find(name);
    if (iter != comparator_table_.end()) {
      return iter->second;
    }
    return nullptr;
  };

 private:
  std::unordered_map<std::string, Comparator> comparator_table_;
};
}  // namespace KVDK_NAMESPACE