/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kvdk/write_batch.hpp"

#include "data_record.hpp"

namespace KVDK_NAMESPACE {

void WriteBatch::Put(const std::string& key, const std::string& value) {
  kvs.push_back({
      key,
      value,
      StringDataRecord,
  });
}

void WriteBatch::Delete(const std::string& key) {
  kvs.push_back({key, "", StringDeleteRecord});
}
}  // namespace KVDK_NAMESPACE