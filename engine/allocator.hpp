/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "kvdk/namespace.hpp"
#include "structures.hpp"

namespace KVDK_NAMESPACE {
struct SizedSpaceEntry {
  SizedSpaceEntry() = default;

  SizedSpaceEntry(uint64_t _offset, uint64_t _size)
      : offset(_offset), size(_size) {}
  PMemOffsetType offset;
  uint64_t size = 0;
};

class Allocator {
public:
  virtual SizedSpaceEntry Allocate(uint64_t size) = 0;
  virtual void Free(const SizedSpaceEntry &entry) = 0;
};
} // namespace KVDK_NAMESPACE