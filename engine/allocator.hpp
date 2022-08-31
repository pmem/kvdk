/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cassert>
#include <vector>

#include "alias.hpp"

namespace KVDK_NAMESPACE {

struct DLRecord;
struct StringRecord;

constexpr MemoryOffsetType kNullMemoryOffset = UINT64_MAX;

struct SpaceEntry {
  class SpaceCmp {
   public:
    bool operator()(const SpaceEntry& s1, const SpaceEntry& s2) const {
      if (s1.size > s2.size) return true;
      if (s1.size == s2.size && s1.offset < s2.offset) return true;
      return false;
    }
  };
  SpaceEntry() = default;

  SpaceEntry(uint64_t _offset, uint64_t _size) : offset(_offset), size(_size) {}
  uint64_t offset;
  uint64_t size = 0;
};

class Allocator {
 public:
  virtual SpaceEntry Allocate(uint64_t size) = 0;
  virtual SpaceEntry AllocateAligned(size_t alignment, uint64_t size) = 0;
  virtual void Free(const SpaceEntry& entry) = 0;
  virtual int64_t BytesAllocated() = 0;
  virtual std::string AllocatorName() = 0;

  virtual void BatchFree(const std::vector<SpaceEntry>& entries) {
    for (const SpaceEntry& entry : entries) {
      this->Free(entry);
    }
  }

  Allocator(char* base_addr, uint64_t max_offset)
      : base_addr_(base_addr), max_offset_(max_offset) {}

  // Purge a kvdk data record and free it
  template <typename T>
  void PurgeAndFree(T* data_record) {
    static_assert(std::is_same<T, DLRecord>::value ||
                      std::is_same<T, StringRecord>::value,
                  "");
    data_record->Destroy();
    Free(SpaceEntry(addr2offset_checked(data_record),
                    data_record->GetRecordSize()));
  }

  // translate offset of allocated space to address
  inline void* offset2addr_checked(MemoryOffsetType offset) const {
    assert(validate_offset(offset) && "Trying to access invalid offset");
    return base_addr_ + offset;
  }

  inline void* offset2addr(MemoryOffsetType offset) const {
    if (validate_offset(offset)) {
      return base_addr_ + offset;
    }
    return nullptr;
  }

  template <typename T>
  inline T* offset2addr_checked(MemoryOffsetType offset) const {
    return static_cast<T*>(offset2addr_checked(offset));
  }

  template <typename T>
  inline T* offset2addr(MemoryOffsetType offset) const {
    return static_cast<T*>(offset2addr(offset));
  }

  // translate address of allocated space to offset
  inline MemoryOffsetType addr2offset_checked(const void* addr) const {
    assert((char*)addr >= base_addr_);
    MemoryOffsetType offset = (char*)addr - base_addr_;
    assert(validate_offset(offset) && "Trying to create invalid offset");
    return offset;
  }

  inline MemoryOffsetType addr2offset(const void* addr) const {
    if (addr) {
      MemoryOffsetType offset = (char*)addr - base_addr_;
      if (validate_offset(offset)) {
        return offset;
      }
    }
    return kNullMemoryOffset;
  }

  inline bool validate_offset(uint64_t offset) const {
    return offset < max_offset_;
  }

 private:
  char* base_addr_;
  uint64_t max_offset_;
};

// Global default memory allocator
Allocator* default_memory_allocator();

}  // namespace KVDK_NAMESPACE