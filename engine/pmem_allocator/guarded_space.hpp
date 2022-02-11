/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once
#ifndef GUARDED_SPACE_HPP
#define GUARDED_SPACE_HPP

#include <cstdint>

#include "../alias.hpp"
#include "../allocator.hpp"

namespace KVDK_NAMESPACE {
static constexpr PMemOffsetType NullPMemOffset =
    std::numeric_limits<PMemOffsetType>::max();
class PMEMAllocator;
class GuardedSpace {
 private:
  friend class PMEMAllocator;
  using size_type = std::uint32_t;

  PMEMAllocator* alloc{nullptr};
  PMemOffsetType offset{NullPMemOffset};
  size_type size{};

  GuardedSpace(PMEMAllocator& a, size_type sz);

 public:
  GuardedSpace() = default;
  GuardedSpace(GuardedSpace const&) = delete;
  GuardedSpace& operator=(GuardedSpace const&) = delete;
  // Transfer ownership to another guard
  GuardedSpace(GuardedSpace&& other);
  GuardedSpace& operator=(GuardedSpace&& other);

  // Release ownership of allocated space.
  SpaceEntry Release();

  PMemOffsetType Offset() const { return offset; }
  void* Address() const;
  size_type Size() const { return size; }
  SpaceEntry ToSpaceEntry();
  ~GuardedSpace();
};
}  // namespace KVDK_NAMESPACE

#endif  // GUARDED_SPACE_HPP