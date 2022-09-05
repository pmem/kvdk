/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "alias.hpp"

namespace KVDK_NAMESPACE {
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
  virtual void Free(const SpaceEntry& entry) = 0;
};

class IVolatileAllocator {
    public:
    // No throw, It's up to caller to check for nullptr.
    virtual void* Allocate(size_t bytes) = 0;
    virtual void Deallocate(void* addr, size_t bytes) = 0;
};

class CharAllocator final : public IVolatileAllocator {
    void* Allocate(size_t n) final { return ::malloc(n); }
    void Deallocate(void* addr, size_t) final { ::free(addr); }
};

}  // namespace KVDK_NAMESPACE