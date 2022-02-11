/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "data_record.hpp"

#include "alias.hpp"
#include "libpmem.h"

namespace KVDK_NAMESPACE {
// use buffer to acc nt-write
thread_local std::string thread_data_buffer;
static constexpr int kDataBufferSize = 1024 * 1024;

StringRecord* StringRecord::PersistStringRecord(
    GuardedSpace& space, TimeStampType timestamp, RecordType type,
    PMemOffsetType older_version_record, const StringView& key,
    const StringView& value) {
  void* data_cpy_target = nullptr;
  void* record_address = space.Address();
  auto write_size = key.size() + value.size() + sizeof(StringRecord);
  bool with_buffer = write_size <= kDataBufferSize;
  if (with_buffer) {
    if (thread_data_buffer.empty()) {
      thread_data_buffer.resize(kDataBufferSize);
    }
    data_cpy_target = &thread_data_buffer[0];
  } else {
    data_cpy_target = record_address;
  }
  StringRecord::ConstructStringRecord(data_cpy_target, space.Size(), timestamp,
                                      type, older_version_record, key, value);
  if (with_buffer) {
    pmem_memcpy(record_address, data_cpy_target, write_size,
                PMEM_F_MEM_NONTEMPORAL);
    pmem_drain();
  } else {
    pmem_persist(record_address, write_size);
  }

  space.Release();

  return static_cast<StringRecord*>(record_address);
}

DLRecord* DLRecord::PersistDLRecord(GuardedSpace& space,
                                    TimeStampType timestamp, RecordType type,
                                    PMemOffsetType older_version_record,
                                    PMemOffsetType prev, PMemOffsetType next,
                                    const StringView& key,
                                    const StringView& value) {
  void* data_cpy_target = nullptr;
  void* record_address = space.Address();
  auto write_size = key.size() + value.size() + sizeof(DLRecord);
  bool with_buffer = write_size <= kDataBufferSize;
  if (with_buffer) {
    if (thread_data_buffer.empty()) {
      thread_data_buffer.resize(kDataBufferSize);
    }
    data_cpy_target = &thread_data_buffer[0];
  } else {
    data_cpy_target = record_address;
  }
  DLRecord::ConstructDLRecord(data_cpy_target, space.Size(), timestamp, type,
                              older_version_record, prev, next, key, value);
  if (with_buffer) {
    pmem_memcpy(record_address, data_cpy_target, write_size,
                PMEM_F_MEM_NONTEMPORAL);
    pmem_drain();
  } else {
    pmem_persist(record_address, write_size);
  }

  space.Release();

  return static_cast<DLRecord*>(record_address);
}

}  // namespace KVDK_NAMESPACE