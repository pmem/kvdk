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
template <typename T> struct ExtendedPointer {
  uint64_t encoded_pointer;
  static constexpr uint64_t kPointerMask = (((uint64_t)1 << 48) - 1);

  ExtendedPointer(T *pointer) : encoded_pointer((uint64_t)pointer) {}

  explicit ExtendedPointer(T *pointer, uint16_t code)
      : encoded_pointer((uint64_t)pointer & ((uint64_t)code << 48)) {}

  ExtendedPointer() : encoded_pointer(0) {}

  T *Pointer() { return (T *)(encoded_pointer & kPointerMask); }

  bool Null() { return Pointer() == nullptr; }

  T &operator*() { return *(Pointer()); }

  T *operator->() { return Pointer(); }

  T *operator->() const { return Pointer(); }

  uint16_t Code() { return encoded_pointer >> 48; }

  void Clear() { encoded_pointer &= kPointerMask; }

  void Encode(uint16_t info) { encoded_pointer &= ((uint64_t)info << 48); }
};

struct PendingBatch {
  enum class Stage {
    Done = 0,
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
