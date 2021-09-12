/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "kvdk/namespace.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {

enum DataEntryType : uint16_t {
  StringDataRecord = 1,
  StringDeleteRecord = 1 << 1,

  SortedDataRecord = 1 << 2,
  SortedDeleteRecord = 1 << 3,
  SortedHeaderRecord = 1 << 4,

  HashListDataRecord = 1 << 5,
  HashListDeleteRecord = 1 << 6,
  HashListHeaderRecord = 1 << 7,

<<<<<<< HEAD
  DLIST_DATA_RECORD = 1 << 8,
  DLIST_DELETE_RECORD = (1 << 8) * 2,
  DLIST_HEAD_RECORD = (1 << 8) * 3,
  DLIST_TAIL_RECORD = (1 << 8) * 4,
  DLIST_RECORD = (1 << 8) *5,
=======
  DlistDataRecord = 1 << 8,
  DlistDeleteRecord = 1 << 9,
  DlistHeadRecord = 1 << 10,
  DlistTailRecord = 1 << 11,
  DlistRecord = 1 << 12,
>>>>>>> 6b93780 (refactor RestoreData and rename DATA_ENTRY_TYPE)

  Padding = 1 << 15,
};

const uint16_t SortedDataEntryType =
    (SortedDataRecord | SortedDeleteRecord | SortedHeaderRecord);

const uint16_t DLDataEntryType =
    (SortedDataRecord | SortedDeleteRecord | SortedHeaderRecord |
     HashListDataRecord | HashListDeleteRecord | HashListHeaderRecord);

const uint16_t DeleteDataEntryType =
    (SortedDeleteRecord | HashListDeleteRecord | StringDeleteRecord);

const uint16_t StringDataEntryType =
    (StringDataRecord | StringDeleteRecord);

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

  // make sure there is data followed in data[0]
  uint32_t Checksum() {
    return get_checksum((char *)this + sizeof(DataHeader),
                        sizeof(DataEntry) - sizeof(DataHeader)) +
           get_checksum(data, v_size + k_size);
  }

  // make sure there is data followed in data[0]
  pmem::obj::string_view Key() { return pmem::obj::string_view(data, k_size); }

  // make sure there is data followed in data[0]
  pmem::obj::string_view Value() {
    return pmem::obj::string_view(data + k_size, v_size);
  }
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

  pmem::obj::string_view Key() { return pmem::obj::string_view(data, k_size); }

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