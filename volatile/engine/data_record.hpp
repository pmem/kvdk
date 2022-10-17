/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <immintrin.h>
#include <x86intrin.h>

#include "alias.hpp"
#include "kvdk/configs.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

enum RecordType : uint8_t {
  Empty = 0,
  String = (1 << 0),
  SortedHeader = (1 << 1),
  SortedElem = (1 << 2),
  HashHeader = (1 << 3),
  HashElem = (1 << 4),
  ListHeader = (1 << 5),
  ListElem = (1 << 6),
};

enum class RecordStatus : uint8_t {
  // Indicate a up-to-date record
  Normal = 0,
  // Indicate key of the record is updated
  Dirty,
  // Indicate deleted or expired record
  Outdated,
};

const uint8_t ExpirableRecordType =
    (RecordType::String | RecordType::SortedHeader | RecordType::HashHeader |
     RecordType::ListHeader);

const uint8_t PrimaryRecordType = ExpirableRecordType;

const uint8_t ElemType =
    (RecordType::SortedElem | RecordType::HashElem | RecordType::ListElem);

const uint8_t CollectionType =
    (RecordType::SortedHeader | RecordType::HashHeader |
     RecordType::ListHeader);

struct DataHeader {
  DataHeader() = default;
  DataHeader(uint32_t c, uint32_t s) : checksum(c), record_size(s) {}

  uint32_t checksum;
  uint32_t record_size;
};

struct DataMeta {
  DataMeta() = default;
  DataMeta(TimestampType _timestamp, RecordType _type, RecordStatus _status,
           uint16_t _key_size, uint32_t _value_size)
      : type(_type),
        status(_status),
        k_size(_key_size),
        v_size(_value_size),
        timestamp(_timestamp) {}

  RecordType type;
  RecordStatus status;
  uint16_t k_size;
  uint32_t v_size;
  TimestampType timestamp;
};

// Header and metadata of a data record
struct DataEntry {
  DataEntry(uint32_t _checksum, uint32_t _record_size /* size in blocks */,
            TimestampType _timestamp, RecordType _type, RecordStatus _status,
            uint16_t _key_size, uint32_t _value_size)
      : header(_checksum, _record_size),
        meta(_timestamp, _type, _status, _key_size, _value_size) {}

  DataEntry() = default;

  void Destroy() { meta.type = RecordType::Empty; }

  // TODO jiayu: use function to access these
  DataHeader header;
  DataMeta meta;
};
static_assert(sizeof(DataEntry) <= kMinMemoryBlockSize);

struct StringRecord {
 public:
  DataEntry entry;
  MemoryOffsetType old_version;
  ExpireTimeType expired_time;
  char data[0];

  // Construct a StringRecord instance at target_address. As the record need
  // additional space to store data, we need pre-allocate enough space for it.
  //
  // target_address: pre-allocated space to store constructed record, it
  // should larger than sizeof(StringRecord) + key size + value size
  static StringRecord* ConstructStringRecord(
      void* target_address, uint32_t _record_size, TimestampType _timestamp,
      RecordType _record_type, RecordStatus _record_status,
      MemoryOffsetType _old_version, const StringView& _key,
      const StringView& _value, ExpireTimeType _expired_time) {
    StringRecord* record = new (target_address)
        StringRecord(_record_size, _timestamp, _record_type, _record_status,
                     _old_version, _key, _value, _expired_time);
    return record;
  }

  // Construct a string record at "addr"
  static StringRecord* PersistStringRecord(
      void* addr, uint32_t record_size, TimestampType timestamp,
      RecordType type, RecordStatus status, MemoryOffsetType old_version,
      const StringView& key, const StringView& value,
      ExpireTimeType expired_time = kPersistTime);

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

  bool ValidOrDirty() {
    bool valid = Validate();
    asm volatile("" ::: "memory");
    bool dirty = (GetRecordStatus() == RecordStatus::Dirty);
    return (valid || dirty);
  }

  // Check whether the record corrupted with expected checksum
  bool Validate(uint32_t expected_checksum) {
    if (ValidateRecordSize()) {
      return Checksum() == expected_checksum;
    }
    return false;
  }

  ExpireTimeType GetExpireTime() const { return expired_time; }
  bool HasExpired() const { return TimeUtils::CheckIsExpired(GetExpireTime()); }

  void PersistExpireTimeNT(ExpireTimeType time) {
    _mm_stream_si64(reinterpret_cast<long long*>(&expired_time),
                    static_cast<long long>(time));
    _mm_mfence();
  }

  void PersistExpireTimeCLWB(ExpireTimeType time) {
    expired_time = time;
    _mm_clwb(&expired_time);
    _mm_mfence();
  }

  void PersistOldVersion(MemoryOffsetType offset) {
    _mm_stream_si64(reinterpret_cast<long long*>(&old_version),
                    static_cast<long long>(offset));
    _mm_mfence();
  }

  void PersistStatus(RecordStatus status) {
    entry.meta.status = status;
    _mm_clwb(&entry.meta.status);
    _mm_mfence();
  }

  TimestampType GetTimestamp() const { return entry.meta.timestamp; }

  RecordType GetRecordType() const { return entry.meta.type; }

  RecordStatus GetRecordStatus() const { return entry.meta.status; }

  uint32_t GetRecordSize() const { return entry.header.record_size; }

  static uint32_t RecordSize(const StringView& key, const StringView& value) {
    return key.size() + value.size() + sizeof(StringRecord);
  }

 private:
  StringRecord(uint32_t _record_size, TimestampType _timestamp,
               RecordType _record_type, RecordStatus _record_status,
               MemoryOffsetType _old_version, const StringView& _key,
               const StringView& _value, ExpireTimeType _expired_time)
      : entry(0, _record_size, _timestamp, _record_type, _record_status,
              _key.size(), _value.size()),
        old_version(_old_version),
        expired_time(_expired_time) {
    kvdk_assert(_record_type == RecordType::String, "");
    memcpy(data, _key.data(), _key.size());
    memcpy(data + _key.size(), _value.data(), _value.size());
    entry.header.checksum = Checksum();
  }

  // check validation of k_size and v_size, as record may be left corrupted
  bool ValidateRecordSize() {
    return entry.meta.k_size + entry.meta.v_size + sizeof(StringRecord) <=
           entry.header.record_size;
  }

  uint32_t Checksum() {
    // we don't checksum expire time and old version
    uint32_t meta_checksum_size = sizeof(DataMeta);
    uint32_t data_checksum_size = entry.meta.k_size + entry.meta.v_size;

    return get_checksum((char*)&entry.meta, meta_checksum_size) +
           get_checksum(data, data_checksum_size);
  }
};

// doubly linked record
struct DLRecord {
 public:
  DataEntry entry;
  MemoryOffsetType old_version;
  MemoryOffsetType prev;
  MemoryOffsetType next;
  ExpireTimeType expired_time;

  char data[0];

  // Construct a DLRecord instance at "target_address". As the record need
  // additional space to store data, we need pre-allocate enough space for it.
  //
  // target_address: pre-allocated space to store constructed record, it
  // should no smaller than sizeof(DLRecord) + key size + value size
  static DLRecord* ConstructDLRecord(
      void* target_address, uint32_t record_size, TimestampType timestamp,
      RecordType record_type, RecordStatus record_status,
      MemoryOffsetType old_version, uint64_t prev, uint64_t next,
      const StringView& key, const StringView& value,
      ExpireTimeType expired_time) {
    DLRecord* record = new (target_address)
        DLRecord(record_size, timestamp, record_type, record_status,
                 old_version, prev, next, key, value, expired_time);
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

  void PersistNextNT(MemoryOffsetType offset) {
    _mm_stream_si64(reinterpret_cast<long long*>(&next),
                    static_cast<long long>(offset));
    _mm_mfence();
  }

  void PersistPrevNT(MemoryOffsetType offset) {
    _mm_stream_si64(reinterpret_cast<long long*>(&prev),
                    static_cast<long long>(offset));
    _mm_mfence();
  }

  void PersistExpireTimeNT(ExpireTimeType time) {
    kvdk_assert(entry.meta.type & ExpirableRecordType, "");
    _mm_stream_si64(reinterpret_cast<long long*>(&expired_time),
                    static_cast<long long>(time));
    _mm_mfence();
  }

  void PersistNextCLWB(MemoryOffsetType offset) {
    next = offset;
    _mm_clwb(&next);
    _mm_mfence();
  }

  void PersistPrevCLWB(MemoryOffsetType offset) {
    prev = offset;
    _mm_clwb(&prev);
    _mm_mfence();
  }

  void PersistExpireTimeCLWB(ExpireTimeType time) {
    kvdk_assert(entry.meta.type & ExpirableRecordType, "");
    expired_time = time;
    _mm_clwb(&expired_time);
    _mm_mfence();
  }

  void PersistOldVersion(MemoryOffsetType offset) {
    _mm_stream_si64(reinterpret_cast<long long*>(&old_version),
                    static_cast<long long>(offset));
    _mm_mfence();
  }

  void PersistStatus(RecordStatus status) {
    entry.meta.status = status;
    _mm_clwb(&entry.meta.status);
    _mm_mfence();
  }

  ExpireTimeType GetExpireTime() const {
    kvdk_assert(entry.meta.type & ExpirableRecordType,
                "Call DLRecord::GetExpireTime with an unexpirable type");
    return expired_time;
  }

  RecordType GetRecordType() const { return entry.meta.type; }

  RecordStatus GetRecordStatus() const { return entry.meta.status; }

  bool HasExpired() const { return TimeUtils::CheckIsExpired(GetExpireTime()); }
  TimestampType GetTimestamp() const { return entry.meta.timestamp; }

  // Construct a dl record at "addr"
  static DLRecord* PersistDLRecord(
      void* addr, uint32_t record_size, TimestampType timestamp,
      RecordType type, RecordStatus status, MemoryOffsetType old_version,
      MemoryOffsetType prev, MemoryOffsetType next, const StringView& key,
      const StringView& value, ExpireTimeType expired_time = kPersistTime);

  uint32_t GetRecordSize() const { return entry.header.record_size; }

  static uint32_t RecordSize(const StringView& key, const StringView& value) {
    return sizeof(DLRecord) + key.size() + value.size();
  }

 private:
  DLRecord(uint32_t _record_size, TimestampType _timestamp, RecordType _type,
           RecordStatus _status, MemoryOffsetType _old_version,
           MemoryOffsetType _prev, MemoryOffsetType _next,
           const StringView& _key, const StringView& _value,
           ExpireTimeType _expired_time)
      : entry(0, _record_size, _timestamp, _type, _status, _key.size(),
              _value.size()),
        old_version(_old_version),
        prev(_prev),
        next(_next),
        expired_time(_expired_time) {
    kvdk_assert(_type & (RecordType::SortedElem | RecordType::SortedHeader |
                         RecordType::HashElem | RecordType::HashHeader |
                         RecordType::ListElem | RecordType::ListHeader),
                "");
    memcpy(data, _key.data(), _key.size());
    memcpy(data + _key.size(), _value.data(), _value.size());
    entry.header.checksum = Checksum();
  }

  // check validation of k_size and v_size, as record may be left corrupted
  bool ValidateRecordSize() {
    return entry.meta.k_size + entry.meta.v_size + sizeof(DLRecord) <=
           entry.header.record_size;
  }

  uint32_t Checksum() {
    // we don't checksum next/prev pointers, expire time and old_version
    uint32_t meta_checksum_size = sizeof(DataMeta);
    uint32_t data_checksum_size = entry.meta.k_size + entry.meta.v_size;

    return get_checksum((char*)&entry.meta, meta_checksum_size) +
           get_checksum(data, data_checksum_size);
  }
};
}  // namespace KVDK_NAMESPACE