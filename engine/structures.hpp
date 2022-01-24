/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdint>
#include <malloc.h>

#include "kvdk/namespace.hpp"
#include "logger.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

// A pointer with additional information on high 16 bits
template <typename T> class PointerWithTag {
public:
  static constexpr uint64_t kPointerMask = (((uint64_t)1 << 48) - 1);

  // TODO: Maybe explicit
  PointerWithTag(T *pointer) : tagged_pointer((uint64_t)pointer) {}

  explicit PointerWithTag(T *pointer, uint16_t tag)
      : tagged_pointer((uint64_t)pointer | ((uint64_t)tag << 48)) {}

  PointerWithTag() : tagged_pointer(0) {}

  T *RawPointer() { return (T *)(tagged_pointer & kPointerMask); }

  const T *RawPointer() const {
    return (const T *)(tagged_pointer & kPointerMask);
  }

  bool Null() { return RawPointer() == nullptr; }

  uint16_t GetTag() { return tagged_pointer >> 48; }

  void ClearTag() { tagged_pointer &= kPointerMask; }

  void SetTag(uint16_t tag) { tagged_pointer |= ((uint64_t)tag << 48); }

  const T &operator*() const { return *RawPointer(); }

  T &operator*() { return *(RawPointer()); }

  const T *operator->() const { return RawPointer(); }

  T *operator->() { return RawPointer(); }

  bool operator==(const T *raw_pointer) { return RawPointer() == raw_pointer; }

  bool operator==(const T *raw_pointer) const {
    return RawPointer() == raw_pointer;
  }

private:
  uint64_t tagged_pointer;
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
  void PersistProcessing(const std::vector<PMemOffsetType> &record,
                         TimeStampType ts);

  // Mark batch write as finished.
  void PersistFinish();

  bool Unfinished() { return stage == Stage::Processing; }

  Stage stage;
  uint32_t num_kv;
  TimeStampType timestamp;
  PMemOffsetType record_offsets[0];
};
} // namespace KVDK_NAMESPACE
