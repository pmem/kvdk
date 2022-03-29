/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <malloc.h>

#include <cstdint>

#include "kvdk/namespace.hpp"
#include "logger.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

enum class PointerType : uint8_t {
  // Value uninitialized considered as Invalid
  Invalid = 0,
  // Point to a string record on PMem
  StringRecord = 1,
  // Point to a doubly linked record on PMem
  DLRecord = 2,
  // Point to a dram skiplist node object
  SkiplistNode = 3,
  // Point to a dram Skiplist object
  Skiplist = 4,
  // Point to a UnorderedCollection object on DRAM
  UnorderedCollection = 5,
  // Point to a element of UnorderedCollection
  UnorderedCollectionElement = 6,
  // Point to a dram List object
  List = 7,
  // Point to a hash entry of hash table
  HashEntry = 8,
  // Empty which point to nothing
  Empty = 100,
};

// A pointer with additional information on high 16 bits
template <typename PointerType, typename TagType>
class PointerWithTag {
 public:
  static constexpr uint64_t kPointerMask = (((uint64_t)1 << 48) - 1);

  // TODO: Maybe explicit
  PointerWithTag(PointerType* pointer) : tagged_pointer((uint64_t)pointer) {
    assert(sizeof(TagType) <= 2);
  }

  explicit PointerWithTag(PointerType* pointer, TagType tag)
      : tagged_pointer((uint64_t)pointer | ((uint64_t)tag << 48)) {
    assert(sizeof(TagType) <= 2);
  }

  PointerWithTag() : tagged_pointer(0) {}

  PointerType* RawPointer() {
    return (PointerType*)(tagged_pointer & kPointerMask);
  }

  const PointerType* RawPointer() const {
    return (const PointerType*)(tagged_pointer & kPointerMask);
  }

  bool Null() const { return RawPointer() == nullptr; }

  TagType GetTag() const { return static_cast<TagType>(tagged_pointer >> 48); }

  void ClearTag() { tagged_pointer &= kPointerMask; }

  void SetTag(TagType tag) { tagged_pointer |= ((uint64_t)tag << 48); }

  const PointerType& operator*() const { return *RawPointer(); }

  PointerType& operator*() { return *(RawPointer()); }

  const PointerType* operator->() const { return RawPointer(); }

  PointerType* operator->() { return RawPointer(); }

  bool operator==(const PointerType* raw_pointer) {
    return RawPointer() == raw_pointer;
  }

  bool operator==(const PointerType* raw_pointer) const {
    return RawPointer() == raw_pointer;
  }

 private:
  uint64_t tagged_pointer;
};

struct BackupMark {
  enum class Stage {
    Init = 0,
    Processing = 1,
    Finish = 2,
  };
  TimeStampType backup_ts;
  Stage stage;
};

// Used to record batch write stage and related records address, this should be
// persisted on PMem
//
// The stage of a processing batch write will be Processing, the stage of a
// initialized pending batch file or a finished batch write will be Finish
//
// Layout: batch write stage | num_kv in writing | timestamp of this batch write
// | record address
struct PendingBatch {
  enum class Stage {
    Finish = 0,
    Processing = 1,
  };

  PendingBatch(Stage s, uint32_t nkv, TimeStampType ts)
      : stage(s), num_kv(nkv), timestamp(ts) {}

  // Mark batch write as process and record writing offsets.
  // Make sure the struct is on PMem and there is enough space followed the
  // struct to store record
  void PersistProcessing(const std::vector<PMemOffsetType>& record,
                         TimeStampType ts);

  // Mark batch write as finished.
  void PersistFinish();

  bool Unfinished() { return stage == Stage::Processing; }

  Stage stage;
  uint32_t num_kv;
  TimeStampType timestamp;
  PMemOffsetType record_offsets[0];
};
}  // namespace KVDK_NAMESPACE
