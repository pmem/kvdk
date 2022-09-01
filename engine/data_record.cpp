/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "data_record.hpp"

namespace KVDK_NAMESPACE {
#ifdef KVDK_WITH_PMEM
// use buffer to acc nt-write
thread_local std::string thread_data_buffer;
static constexpr int kDataBufferSize = 1024 * 1024;
#endif

StringRecord* StringRecord::PersistStringRecord(
    void* addr, uint32_t record_size, TimestampType timestamp, RecordType type,
    RecordStatus status, MemoryOffsetType old_version, const StringView& key,
    const StringView& value, ExpireTimeType expired_time) {
  void* data_cpy_target = addr;

#ifdef KVDK_WITH_PMEM
  bool with_buffer = false;
  auto write_size = key.size() + value.size() + sizeof(StringRecord);
  with_buffer = write_size <= kDataBufferSize;
  if (with_buffer) {
    if (thread_data_buffer.empty()) {
      thread_data_buffer.resize(kDataBufferSize);
    }
    data_cpy_target = &thread_data_buffer[0];
  }
#endif

  StringRecord::ConstructStringRecord(data_cpy_target, record_size, timestamp,
                                      type, status, old_version, key, value,
                                      expired_time);
#ifdef KVDK_WITH_PMEM
  if (with_buffer) {
    pmem_memcpy(addr, data_cpy_target, write_size, PMEM_F_MEM_NONTEMPORAL);
    pmem_drain();
  } else {
    pmem_persist(addr, write_size);
  }
#endif

  return static_cast<StringRecord*>(addr);
}

DLRecord* DLRecord::PersistDLRecord(
    void* addr, uint32_t record_size, TimestampType timestamp, RecordType type,
    RecordStatus status, MemoryOffsetType old_version, MemoryOffsetType prev,
    MemoryOffsetType next, const StringView& key, const StringView& value,
    ExpireTimeType expired_time) {
  void* data_cpy_target = addr;

#ifdef KVDK_WITH_PMEM
  bool with_buffer = false;
  auto write_size = key.size() + value.size() + sizeof(DLRecord);
  with_buffer = write_size <= kDataBufferSize;
  if (with_buffer) {
    if (thread_data_buffer.empty()) {
      thread_data_buffer.resize(kDataBufferSize);
    }
    data_cpy_target = &thread_data_buffer[0];
  }
#endif

  DLRecord::ConstructDLRecord(data_cpy_target, record_size, timestamp, type,
                              status, old_version, prev, next, key, value,
                              expired_time);
#ifdef KVDK_WITH_PMEM
  if (with_buffer) {
    pmem_memcpy(addr, data_cpy_target, write_size, PMEM_F_MEM_NONTEMPORAL);
    pmem_drain();
  } else {
    pmem_persist(addr, write_size);
  }
#endif

  return static_cast<DLRecord*>(addr);
}

}  // namespace KVDK_NAMESPACE