/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "libpmem.h"

#include "kvdk/namespace.hpp"

#include "utils.hpp"

namespace KVDK_NAMESPACE {

enum RecordType : uint16_t {
  Empty = 0,
  StringDataRecord = (1 << 0),
  // Notice: StringDeleteReocrd is unused now, but will be used while
  // implementing mvcc, so we do not remove it now
  StringDeleteRecord = (1 << 1),

  SortedDataRecord = (1 << 2),
  SortedDeleteRecord = (1 << 3),
  SortedHeaderRecord = (1 << 4),

  DlistDataRecord = (1 << 5),
  DlistHeadRecord = (1 << 6),
  DlistTailRecord = (1 << 7),
  DlistRecord = (1 << 8),

  QueueDataRecord = (1 << 9),
  QueueHeadRecord = (1 << 10),
  QueueTailRecord = (1 << 11),
  QueueRecord = (1 << 12),

  Padding = 1 << 15,
};

const uint16_t SortedRecordType =
    (SortedDataRecord | SortedDeleteRecord | SortedHeaderRecord);

const uint16_t DLRecordType =
    (SortedDataRecord | SortedDeleteRecord | SortedHeaderRecord |
     DlistDataRecord | DlistHeadRecord | DlistTailRecord | DlistRecord |
     QueueDataRecord | QueueHeadRecord | QueueTailRecord | QueueRecord);

const uint16_t DeleteRecordType = (StringDeleteRecord | SortedDeleteRecord);

const uint16_t StringRecordType = (StringDataRecord | StringDeleteRecord);

struct DataHeader {
  DataHeader() = default;
  DataHeader(uint32_t c, uint32_t s) : checksum(c), record_size(s) {}

  uint32_t checksum;
  // Record size on Pmem in the unit of block
  // TODO jiayu: use actual size
  uint32_t record_size;
};

struct DataMeta {
  DataMeta() = default;
  DataMeta(TimestampType _timestamp, RecordType _record_type,
           uint16_t _key_size, uint32_t _value_size)
      : timestamp(_timestamp), type(_record_type), k_size(_key_size),
        v_size(_value_size) {}

  TimestampType timestamp;
  RecordType type;
  uint16_t k_size;
  uint32_t v_size;
};

// Header and metadata of a data record
struct DataEntry {
  // TODO jiayu: use typename for timestamp and record type instead of a number
  DataEntry(uint32_t _checksum, uint32_t _record_size /* size in blocks */,
            TimestampType _timestamp, RecordType _record_type,
            uint16_t _key_size, uint32_t _value_size)
      : header(_checksum, _record_size),
        meta(_timestamp, _record_type, _key_size, _value_size) {}

  DataEntry() = default;

  void Destroy() {
    meta.type = RecordType::Padding;
    pmem_persist(&meta.type, sizeof(RecordType));
  }

  // TODO jiayu: use function to access these
  DataHeader header;
  DataMeta meta;
};

struct StringRecord {
public:
  DataEntry entry;
  PMemOffsetType older_version;
  char data[0];

  // Construct a StringRecord instance at target_address. As the record need
  // additional space to store data, we need pre-allocate enough space for it.
  //
  // target_address: pre-allocated space to store constructed record, it
  // should larger than sizeof(StringRecord) + key size + value size
  static StringRecord *
  ConstructStringRecord(void *target_address, uint32_t _record_size,
                        TimestampType _timestamp, RecordType _record_type,
                        PMemOffsetType _older_version, const StringView &_key,
                        const StringView &_value) {
    StringRecord *record = new (target_address) StringRecord(
        _record_size, _timestamp, _record_type, _older_version, _key, _value);
    return record;
  }

  // Construct and persist a string record at pmem address "addr"
  static StringRecord *
  PersistStringRecord(void *addr, uint32_t record_size, TimestampType timestamp,
                      RecordType type, PMemOffsetType older_version,
                      const StringView &key, const StringView &value);

  void Destroy() { entry.Destroy(); }

  // make sure there is data followed in data[0]
  StringView Key() const { return StringView(data, entry.meta.k_size); }

  // make sure there is data followed in data[0]
  StringView Value() const {
    return StringView(data + entry.meta.k_size, entry.meta.v_size);
  }

  // Check whether the record corrupted
  bool Validate() {
    if (ValidateRecordSize()) {
      return Checksum() == entry.header.checksum;
    }
    return false;
  }

  // Check whether the record corrupted with expected checksum
  bool Validate(uint32_t expected_checksum) {
    if (ValidateRecordSize()) {
      return Checksum() == expected_checksum;
    }
    return false;
  }

private:
  StringRecord(uint32_t _record_size, TimestampType _timestamp,
               RecordType _record_type, PMemOffsetType _older_version,
               const StringView &_key, const StringView &_value)
      : entry(0, _record_size, _timestamp, _record_type, _key.size(),
              _value.size()),
        older_version(_older_version) {
    assert(_record_type == StringDataRecord ||
           _record_type == StringDeleteRecord);
    memcpy(data, _key.data(), _key.size());
    memcpy(data + _key.size(), _value.data(), _value.size());
    entry.header.checksum = Checksum();
  }

  // check validation of k_size and v_size, as record may be left corrupted
  bool ValidateRecordSize() {
    return entry.meta.k_size + entry.meta.v_size + sizeof(StringRecord) <=
           entry.header.record_size * 64 /* TODO jiayu: 64 is default block
                                            size. use actual size instead. */
        ;
  }

  uint32_t Checksum() {
    uint32_t checksum_size = entry.meta.k_size + entry.meta.v_size +
                             sizeof(StringRecord) - sizeof(DataHeader);
    return get_checksum((char *)&entry.meta, checksum_size);
  }
};

// doubly linked record
// TODO jiayu: use typename for next and prev pointer instead of a number
struct DLRecord {
public:
  DataEntry entry;
  PMemOffsetType older_version;
  PMemOffsetType prev;
  PMemOffsetType next;
  char data[0];

  // Construct a DLRecord instance at "target_address". As the record need
  // additional space to store data, we need pre-allocate enough space for it.
  //
  // target_address: pre-allocated space to store constructed record, it
  // should no smaller than sizeof(DLRecord) + key size + value size
  static DLRecord *
  ConstructDLRecord(void *target_address, uint32_t record_size,
                    TimestampType timestamp, RecordType record_type,
                    PMemOffsetType older_version, uint64_t prev, uint64_t next,
                    const StringView &key, const StringView &value) {
    DLRecord *record =
        new (target_address) DLRecord(record_size, timestamp, record_type,
                                      older_version, prev, next, key, value);
    return record;
  }

  void Destroy() { entry.Destroy(); }

  bool Validate() {
    if (ValidateRecordSize()) {
      return Checksum() == entry.header.checksum;
    }
    return false;
  }

  bool Validate(uint32_t expected_checksum) {
    if (ValidateRecordSize()) {
      return Checksum() == expected_checksum;
    }
    return false;
  }

  StringView Key() const { return StringView(data, entry.meta.k_size); }

  StringView Value() const {
    return StringView(data + entry.meta.k_size, entry.meta.v_size);
  }

  // Construct and persist a dl record to PMem address "addr"
  static DLRecord *PersistDLRecord(void *addr, uint32_t record_size,
                                   TimestampType timestamp, RecordType type,
                                   PMemOffsetType older_version,
                                   PMemOffsetType prev, PMemOffsetType next,
                                   const StringView &key,
                                   const StringView &value);

private:
  DLRecord(uint32_t _record_size, TimestampType _timestamp,
           RecordType _record_type, PMemOffsetType _older_version,
           PMemOffsetType _prev, PMemOffsetType _next, const StringView &_key,
           const StringView &_value)
      : entry(0, _record_size, _timestamp, _record_type, _key.size(),
              _value.size()),
        older_version(_older_version), prev(_prev), next(_next) {
    assert(_record_type & DLRecordType);
    memcpy(data, _key.data(), _key.size());
    memcpy(data + _key.size(), _value.data(), _value.size());
    entry.header.checksum = Checksum();
  }

  // check validation of k_size and v_size, as record may be left corrupted
  bool ValidateRecordSize() {
    return entry.meta.k_size + entry.meta.v_size + sizeof(DLRecord) <=
           entry.header.record_size * 64;
  }

  uint32_t Checksum() {
    uint32_t meta_checksum_size =
        sizeof(DataMeta); /* we don't checksum pointers */
    uint32_t data_checksum_size = entry.meta.k_size + entry.meta.v_size;

    return get_checksum((char *)&entry.meta, meta_checksum_size) +
           get_checksum(data, data_checksum_size);
  }
};
} // namespace KVDK_NAMESPACE