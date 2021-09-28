/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdint>
#include <malloc.h>

#include "kvdk/namespace.hpp"
#include "logger.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {

// A pointer with additional information on high 16 bits
template <typename T> struct PointerWithTag {
  uint64_t tagged_pointer;
  static constexpr uint64_t kPointerMask = (((uint64_t)1 << 48) - 1);

  PointerWithTag(T *pointer) : tagged_pointer((uint64_t)pointer) {}

  explicit PointerWithTag(T *pointer, uint16_t code)
      : tagged_pointer((uint64_t)pointer | ((uint64_t)code << 48)) {}

  PointerWithTag() : tagged_pointer(0) {}

  T *RawPointer() { return (T *)(tagged_pointer & kPointerMask); }

  const T *RawPointer() const {
    return (const T *)(tagged_pointer & kPointerMask);
  }

  bool Null() { return RawPointer() == nullptr; }

  uint16_t GetTag() { return tagged_pointer >> 48; }

  void ClearTag() { tagged_pointer &= kPointerMask; }

  void SetTag(uint16_t info) { tagged_pointer |= ((uint64_t)info << 48); }

  const T &operator*() const { return *RawPointer(); }

  T &operator*() { return *(RawPointer()); }

  const T *operator->() const { return RawPointer(); }

  T *operator->() { return RawPointer(); }

  bool operator==(const T *raw_pointer) { return RawPointer() == raw_pointer; }

  bool operator==(const T *raw_pointer) const {
    return RawPointer() == raw_pointer;
  }
};

struct PendingBatch {
  enum class Stage {
    Finish = 0,
    Processing = 1,
  };

  PendingBatch(Stage s, uint32_t nkv, uint64_t ts)
      : stage(s), num_kv(nkv), timestamp(ts) {}

  void PersistProcessing(void *target,
                         const std::vector<uint64_t> &entry_offsets);

  void Restore(char *target, std::vector<uint64_t> *entry_offsets);

  void PersistStage(Stage s);

  bool Unfinished() { return stage == Stage::Processing; }

  Stage stage;
  uint32_t num_kv;
  uint64_t timestamp;
};

class PersistentList {
public:
  virtual uint64_t id() = 0;

  inline static std::string ListKey(const pmem::obj::string_view &user_key,
                                    uint64_t list_id) {
    return std::string((char *)&list_id, 8)
        .append(user_key.data(), user_key.size());
  }
};
} // namespace KVDK_NAMESPACE
