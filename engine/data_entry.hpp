/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "kvdk/namespace.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {

enum DATA_ENTRY_TYPE : uint16_t {
  STRING_DATA_RECORD = 1,
  STRING_DELETE_RECORD = 1 << 1,

  SORTED_DATA_RECORD = 1 << 2,
  SORTED_DELETE_RECORD = 1 << 3,
  SORTED_HEADER_RECORD = 1 << 4,

  HASH_LIST_DATA_RECORD = 1 << 5,
  HASH_LIST_DELETE_RECORD = 1 << 6,
  HASH_LIST_HEADER_RECORD = 1 << 7,

  PADDING = 1 << 15,
};

const uint16_t DLDataEntryType =
    (SORTED_DATA_RECORD | SORTED_DELETE_RECORD | SORTED_HEADER_RECORD |
     HASH_LIST_DATA_RECORD | HASH_LIST_DELETE_RECORD | HASH_LIST_HEADER_RECORD);

const uint16_t DeleteDataEntryType =
    (SORTED_DELETE_RECORD | HASH_LIST_DELETE_RECORD | STRING_DELETE_RECORD);

const uint16_t StringDataEntryType =
    (STRING_DATA_RECORD | STRING_DELETE_RECORD);

struct DataHeader {
  DataHeader() = default;
  DataHeader(uint32_t c, uint32_t s) : checksum(c), b_size(s) {}

  uint32_t checksum;
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

  // make sure there is data followed in data[0]
  uint32_t Checksum() {
    return get_checksum((char *)this + sizeof(DataHeader),
                        sizeof(DataEntry) - sizeof(DataHeader) + k_size +
                            v_size);
  }

  // make sure there is data followed in data[0]
  Slice Key() { return Slice(data, k_size); }

  // make sure there is data followed in data[0]
  Slice Value() { return Slice(data + k_size, v_size); }
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

  // make sure there is data followed in data[0]
  uint32_t Checksum() {
    return get_checksum((char *)this + sizeof(DataHeader),
                        sizeof(DataEntry) - sizeof(DataHeader)) +
           get_checksum(data, v_size + k_size);
  }

  // make sure there is data followed in data[0]
  Slice Key() { return Slice(data, k_size); }

  // make sure there is data followed in data[0]
  Slice Value() { return Slice(data + k_size, v_size); }
};

// Singly linked
struct SLDataEntry : public DataEntry {
  uint64_t next;
  char data[0];

  uint32_t Checksum() {
    return get_checksum((char *)this + sizeof(DataHeader),
                        sizeof(DataEntry) - sizeof(DataHeader)) +
           get_checksum(data, v_size + k_size);
  }

  Slice Key() { return Slice(data, k_size); }

  Slice Value() { return Slice(data + k_size, v_size); }
};

static uint64_t data_entry_size(uint16_t type) {
  if (type & DLDataEntryType) {
    return sizeof(DLDataEntry);
  } else {
    return sizeof(DataEntry);
  }
}
} // namespace KVDK_NAMESPACE