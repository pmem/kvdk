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
  using compare = std::function<int(const pmem::obj::string_view &src,
                                    const pmem::obj::string_view &target)>;
  void SetComparaFunc(const pmem::obj::string_view &compara_name,
                      compare comp_func) {
    if (compara_table_.find(compara_name) == compara_table_.end()) {
      compara_table_.insert({compara_name, comp_func});
    }
  }
  compare GetComparaFunc(const pmem::obj::string_view &compara_name) {
    if (compara_table_.find(compara_name) != compara_table_.end()) {
      return compara_table_[compara_name];
    }
    return nullptr;
  };

private:
  std::unordered_map<pmem::obj::string_view, compare> compara_table_;
};