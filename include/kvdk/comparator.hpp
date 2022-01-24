/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <string>
#include <unordered_map>

#include "kvdk/namespace.hpp"
#include "libpmemobj++/string_view.hpp"

class Comparator {
public:
  using StringView = pmem::obj::string_view;

  using compare =
      std::function<int(const StringView &src, const StringView &target)>;
  void SetComparaFunc(const StringView &compara_name, compare comp_func) {
    if (compara_table_.find(
            std::string{compara_name.data(), compara_name.size()}) ==
        compara_table_.end()) {
      compara_table_.emplace(
          std::string{compara_name.data(), compara_name.size()}, comp_func);
    }
  }
  compare GetComparaFunc(const StringView &compara_name) {
    if (compara_table_.find(
            std::string{compara_name.data(), compara_name.size()}) !=
        compara_table_.end()) {
      return compara_table_[std::string{compara_name.data(),
                                        compara_name.size()}];
    }
    return nullptr;
  };

private:
  std::unordered_map<std::string, compare> compara_table_;
};