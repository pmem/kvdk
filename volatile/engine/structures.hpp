/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdint>

#include "alias.hpp"
#include "logger.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

enum class PointerType : uint8_t {
  // Value uninitialized considered as Invalid
  Invalid = 0,
  // Point to a string record
  StringRecord = 1,
  // Point to a doubly linked record
  DLRecord = 2,
  // Point to a dram skiplist node object
  SkiplistNode = 3,
  // Point to a dram Skiplist object
  Skiplist = 4,
  // Hash
  HashList = 5,
  // Element in Hash
  HashElem = 6,
  // List
  List = 7,
  // Point to a hash entry of hash table
  HashEntry = 8,
  // Allocated for later insertion
  Allocated,
  // Empty which point to nothing
  Empty = 10,
};

// A pointer with additional information on high 16 bits
template <typename PointerType, typename TagType>
class PointerWithTag {
 public:
  static constexpr uint64_t kPointerMask = (((uint64_t)1 << 48) - 1);

  // TODO: Maybe explicit
  PointerWithTag(PointerType* pointer) : tagged_pointer_((uint64_t)pointer) {
    assert(sizeof(TagType) <= 2);
  }

  explicit PointerWithTag(PointerType* pointer, TagType tag)
      : tagged_pointer_((uint64_t)pointer | ((uint64_t)tag << 48)) {
    assert(sizeof(TagType) <= 2);
  }

  PointerWithTag() : tagged_pointer_(0) {}

  PointerType* RawPointer() {
    return (PointerType*)(tagged_pointer_ & kPointerMask);
  }

  const PointerType* RawPointer() const {
    return (const PointerType*)(tagged_pointer_ & kPointerMask);
  }

  bool Null() const { return RawPointer() == nullptr; }

  TagType GetTag() const { return static_cast<TagType>(tagged_pointer_ >> 48); }

  void ClearTag() { tagged_pointer_ &= kPointerMask; }

  void SetTag(TagType tag) { tagged_pointer_ |= ((uint64_t)tag << 48); }

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
  uint64_t tagged_pointer_;
};
}  // namespace KVDK_NAMESPACE
