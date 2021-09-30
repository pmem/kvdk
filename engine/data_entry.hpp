/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "kvdk/namespace.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {

enum DataEntryType : uint16_t {
  Empty = 0,
  StringDataRecord = 1,
  StringDeleteRecord = 1 << 1,

  SortedDataRecord = 1 << 2,
  SortedHeaderRecord = 1 << 4,

  HashListDataRecord = 1 << 5,
  HashListDeleteRecord = 1 << 6,
  HashListHeaderRecord = 1 << 7,

  DlistDataRecord = 1 << 8,
  DlistDeleteRecord = 1 << 9,
  DlistHeadRecord = 1 << 10,
  DlistTailRecord = 1 << 11,
  DlistRecord = 1 << 12,

  Padding = 1 << 15,
};

const uint16_t SortedDataEntryType = (SortedDataRecord | SortedHeaderRecord);

const uint16_t DLDataEntryType =
    (SortedDataRecord | SortedHeaderRecord | HashListDataRecord |
     HashListDeleteRecord | HashListHeaderRecord);

const uint16_t DeleteDataEntryType =
    (HashListDeleteRecord | StringDeleteRecord);

const uint16_t StringDataEntryType = (StringDataRecord | StringDeleteRecord);

struct DataHeader {
  DataHeader() = default;
  DataHeader(uint32_t c, uint32_t s) : checksum(c), b_size(s) {}

  uint32_t checksum;
  // entry size in blocks
  uint32_t b_size;
};

// We do not make this virtual because we need to persist it
struct DataEntry {
  DataEntry(uint32_t c, uint32_t bs, uint64_t v, uint16_t t, uint16_t ks,
            uint32_t vs)
      : header(c, bs), timestamp(v), type(t), k_size(ks), v_size(vs) {}
  DataEntry() = default;
  // header, it can be atomically written to pmem
  alignas(8) DataHeader header;
  // meta
  uint64_t timestamp;
  uint16_t type;
  uint16_t k_size;
  uint32_t v_size;
  char data[0];

  // Calculate checksum, a valid checksum is always > 0
  // make sure there is data followed in data[0]
  // "pmem_block_size" is used for checking validation of k_size and v_size, as
  // data entry may be left corrupted
  // TODO: store actual size in data header so we don't need this parameter
  uint32_t Checksum(uint32_t pmem_block_size) {
    uint32_t checksum_size =
        k_size + v_size + sizeof(DataEntry) - sizeof(DataHeader);
    if (checksum_size + sizeof(DataHeader) >= pmem_block_size * header.b_size) {
      // Fixme: return error instead of 0
      return 0;
    }
    return std::max(
        get_checksum((char *)this + sizeof(DataHeader), checksum_size), 1UL);
  }

  // make sure there is data followed in data[0]
  pmem::obj::string_view Key() { return pmem::obj::string_view(data, k_size); }

  // make sure there is data followed in data[0]
  pmem::obj::string_view Value() {
    return pmem::obj::string_view(data + k_size, v_size);
  }
};

// Doublely linked
struct DLDataEntry : public DataEntry {
  DLDataEntry() = default;
  DLDataEntry(uint32_t c, uint32_t bs, uint64_t v, uint16_t t, uint16_t ks,
              uint32_t vs, uint64_t pr, uint64_t ne)
      : DataEntry(c, bs, v, t, ks, vs), prev(pr), next(ne) {}

  uint64_t prev;
  uint64_t next;
  char data[0];

  // Calculate checksum, a valid checksum is always > 0
  // make sure there is data followed in data[0]
  // "pmem_block_size" is used for checking validation of k_size and v_size, as
  // data entry may be left corrupted
  // TODO: store actual size in data header so we don't need this parameter
  uint32_t Checksum(uint32_t pmem_block_size) {
    uint32_t meta_checksum_size =
        sizeof(DataEntry) - sizeof(DataHeader) /* we don't checksum pointers */;
    uint32_t data_checksum_size = k_size + v_size;
    if (meta_checksum_size + data_checksum_size + 16 /*pointers*/ +
            sizeof(DataHeader) >
        pmem_block_size * header.b_size) {
      return 0;
    }
    return std::max(get_checksum((char *)this + sizeof(DataHeader),
                                 sizeof(DataEntry) - sizeof(DataHeader)) +
                        get_checksum(data, v_size + k_size),
                    1UL);
  }

  // make sure there is data followed in data[0]
  pmem::obj::string_view Key() { return pmem::obj::string_view(data, k_size); }

  // make sure there is data followed in data[0]
  pmem::obj::string_view Value() {
    return pmem::obj::string_view(data + k_size, v_size);
  }
};

static uint64_t data_entry_size(uint16_t type) {
  if (type & DLDataEntryType) {
    return sizeof(DLDataEntry);
  } else {
    return sizeof(DataEntry);
  }
}
} // namespace KVDK_NAMESPACE