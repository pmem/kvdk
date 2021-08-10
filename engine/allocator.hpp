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
  // This is used for de-allocation of delete records, will be removed in future
  HashEntry *hash_entry_reference = nullptr;
  SpinMutex *hash_entry_mutex = nullptr;

  SpaceEntry() = default;
  SpaceEntry(uint64_t bo, HashEntry *r, SpinMutex *mutex)
      : offset(bo), hash_entry_reference(r), hash_entry_mutex(mutex) {}

  void MaybeDeref() {
    if (hash_entry_reference != nullptr) {
      assert(hash_entry_mutex != nullptr);
      std::lock_guard<SpinMutex> lg(*hash_entry_mutex);
      hash_entry_reference->header.reference =
          hash_entry_reference->header.reference - 1;
    }
  }
};

struct SizedSpaceEntry {
  SizedSpaceEntry() = default;
  SizedSpaceEntry(uint64_t offset, uint32_t size, HashEntry *hash_entry_ref,
                  SpinMutex *mutex)
      : space_entry(offset, hash_entry_ref, mutex), size(size) {}
  SpaceEntry space_entry;
  uint32_t size = 0;

  void MaybeDeref() { return space_entry.MaybeDeref(); }
};

class Allocator {
public:
  // TODO: do not use space entry
  virtual SizedSpaceEntry Allocate(uint64_t size) = 0;
  virtual void Free(const SizedSpaceEntry &entry) = 0;
};
} // namespace KVDK_NAMESPACE