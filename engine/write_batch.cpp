/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kvdk/write_batch.hpp"
#include "data_entry.hpp"

namespace KVDK_NAMESPACE {

void WriteBatch::Put(const std::string &key, const std::string &value) {
  kvs.push_back({
      key,
      value,
      STRING_DATA_RECORD,
  });
}

void WriteBatch::Delete(const std::string &key) {
  kvs.push_back({key, "", STRING_DELETE_RECORD});
}
} // namespace KVDK_NAMESPACE