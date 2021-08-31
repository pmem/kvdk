/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "kvdk/namespace.hpp"
#include "structures.hpp"

namespace KVDK_NAMESPACE {
// Free pmem blocks
struct SpaceEntry {
  uint64_t offset = 0;
  // Allocator specific information
  // For example, in PMEMAllocator, it indicates timestamp of the data stored in
  // the space
  uint64_t info = 0;

  SpaceEntry() = default;
  explicit SpaceEntry(uint64_t bo, uint64_t i) : offset(bo), info(i) {}
};

struct SizedSpaceEntry {
  SizedSpaceEntry() = default;
  SizedSpaceEntry(uint64_t offset, uint64_t size, uint64_t i)
      : space_entry(offset, i), size(size) {}
  SpaceEntry space_entry;
  uint64_t size = 0;
};

class Allocator {
public:
  virtual SizedSpaceEntry Allocate(uint64_t size) = 0;
  virtual void Free(const SizedSpaceEntry &entry) = 0;
};
} // namespace KVDK_NAMESPACE