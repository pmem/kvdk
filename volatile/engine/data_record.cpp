/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "data_record.hpp"

namespace KVDK_NAMESPACE {

StringRecord* StringRecord::PersistStringRecord(
    void* addr, uint32_t record_size, TimestampType timestamp, RecordType type,
    RecordStatus status, MemoryOffsetType old_version, const StringView& key,
    const StringView& value, ExpireTimeType expired_time) {
  void* data_cpy_target = addr;
  StringRecord::ConstructStringRecord(data_cpy_target, record_size, timestamp,
                                      type, status, old_version, key, value,
                                      expired_time);
  return static_cast<StringRecord*>(addr);
}

DLRecord* DLRecord::PersistDLRecord(
    void* addr, uint32_t record_size, TimestampType timestamp, RecordType type,
    RecordStatus status, MemoryOffsetType old_version, MemoryOffsetType prev,
    MemoryOffsetType next, const StringView& key, const StringView& value,
    ExpireTimeType expired_time) {
  void* data_cpy_target = addr;
  DLRecord::ConstructDLRecord(data_cpy_target, record_size, timestamp, type,
                              status, old_version, prev, next, key, value,
                              expired_time);
  return static_cast<DLRecord*>(addr);
}

}  // namespace KVDK_NAMESPACE