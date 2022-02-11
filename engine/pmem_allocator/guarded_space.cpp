/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "guarded_space.hpp"

#include <libpmem.h>

#include "../data_record.hpp"
#include "../logger.hpp"
#include "pmem_allocator.hpp"

namespace KVDK_NAMESPACE {
GuardedSpace::GuardedSpace(PMEMAllocator& a, size_type sz) : alloc{&a} {
  SpaceEntry se = alloc->Allocate(sz);
  offset = se.offset;
  size = se.size;
}

GuardedSpace::GuardedSpace(GuardedSpace&& other) { *this = std::move(other); }
GuardedSpace& GuardedSpace::operator=(GuardedSpace&& other) {
  kvdk_assert(alloc == nullptr && size == 0,
              "Cannot asigned to non-empty GuardedSpace");

  using std::swap;
  swap(alloc, other.alloc);
  swap(offset, other.offset);
  swap(size, other.size);
  return *this;
}
// Release ownership of allocated space.
SpaceEntry GuardedSpace::Release() {
  SpaceEntry ret{ToSpaceEntry()};
  alloc = nullptr;
  offset = kNullPMemOffset;
  size = 0;
  return ret;
}
void* GuardedSpace::Address() const { return alloc->offset2addr(offset); }
SpaceEntry GuardedSpace::ToSpaceEntry() { return SpaceEntry{offset, size}; }
GuardedSpace::~GuardedSpace() {
  if (alloc != nullptr && size != 0) {
#if DEBUG_LEVEL > 0
    GlobalLogger.Info("GuardedSpace unused. Padded and freed!\n");
#endif
    DataHeader padding{0, size};
    static_assert(sizeof(DataHeader) == 8, "");
    pmem_memcpy_persist(Address(), &padding, sizeof(DataHeader));
    alloc->Free(ToSpaceEntry());
  }
}

}  // namespace KVDK_NAMESPACE
