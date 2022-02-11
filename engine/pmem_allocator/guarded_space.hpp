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

// PMEMAllocator Allocate() space from segment or freelist.

// KVEngine makes the assumption that once the space is allocated,
// it will be immediately used by caller of Allocate()
// and contents on the space will be overwritten with valid
// StringRecord or DLRecord, which contains the size of the space.

// KVEngine::RestoreData() depends on this presumption.
// If user directly call Free() after Allocate(),
// the size field on this space will not be updated,
// and KVEngine::RestoreData() will fail.

// GuardedSpace solves this problem with RAII idiom.
// PMemAllocator::GuardedAllocate() will allocate GuardedSpace,
// which is then consumed by PersistStringRecord() or PersistDLRecord().
// These consumers will write valid record on the space,
// then explicitly acquire ownership of the space from GuardedSpace.
// Otherwise, since no valid record is written on the space,
// GuardedSpace will write padding information on it by destructor.

// Thus, space allocated by PMemAllocator::GuardedAllocate() will always
// have valid data on it, either a Record or a padding.
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