/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "alias.hpp"

namespace KVDK_NAMESPACE {
struct SpaceEntry {
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
}  // namespace KVDK_NAMESPACE