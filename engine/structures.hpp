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

struct PendingBatch {
  enum class Stage {
    Finish = 0,
    Processing = 1,
  };

  PendingBatch(Stage s, uint32_t nkv, TimeStampType ts)
      : stage(s), num_kv(nkv), timestamp(ts) {}

  void PersistProcessing(void *target,
                         const std::vector<uint64_t> &entry_offsets);

  void Restore(char *target, std::vector<uint64_t> *entry_offsets);

  void PersistStage(Stage s);

  bool Unfinished() { return stage == Stage::Processing; }

  Stage stage;
  uint32_t num_kv;
  TimeStampType timestamp;
};

class PersistentList {
public:
  virtual uint64_t id() = 0;

  inline static std::string ListKey(const StringView &user_key,
                                    uint64_t list_id) {
    return std::string((char *)&list_id, 8)
        .append(user_key.data(), user_key.size());
  }
};

struct CompContext {
public:
  KeyCompareFunc key_cmp = compare_string_view;
  ValueCompareFunc val_cmp = compare_string_view;
  bool priority_key = true;
};

template <typename child> class CompStrategy {
public:
  int Comparekey(const StringView src_key, const StringView target_key) {
    return static_cast<child *>(this)->cmp_ctx.key_cmp(
        src_key.data(), src_key.size(), target_key.data(), target_key.size());
  }

  int CompareValue(const StringView src_val, const StringView &target_val) {
    return static_cast<child *>(this)->cmp_ctx.val_cmp(
        src_val.data(), src_val.size(), target_val.data(), target_val.size());
  }

  CompContext cmp_ctx;
};

} // namespace KVDK_NAMESPACE
