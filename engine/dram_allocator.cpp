/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "dram_allocator.hpp"

#include "thread_manager.hpp"

namespace KVDK_NAMESPACE {

void ChunkBasedAllocator::Free(const SpaceEntry&) {
  // Not supported yet
}

SpaceEntry ChunkBasedAllocator::Allocate(uint64_t size) {
  kvdk_assert(ThreadManager::ThreadID() >= 0, "");
  SpaceEntry entry;
  auto& tc = dalloc_thread_cache_[ThreadManager::ThreadID() %
                                  dalloc_thread_cache_.size()];
  if (size > chunk_size_) {
    void* addr = aligned_alloc(64, size);
    if (addr != nullptr) {
      entry.size = chunk_size_;
      entry.offset = addr2offset(addr);
      tc.allocated_chunks.push_back(addr);
    }
    return entry;
  }

  if (tc.usable_bytes < size) {
    void* addr = aligned_alloc(64, chunk_size_);
    if (addr == nullptr) {
      return entry;
    }
    tc.chunk_addr = (char*)addr;
    tc.usable_bytes = chunk_size_;
    tc.allocated_chunks.push_back(addr);
  }

  entry.size = size;
  entry.offset = addr2offset(tc.chunk_addr);
  tc.chunk_addr += size;
  tc.usable_bytes -= size;
  return entry;
}
}  // namespace KVDK_NAMESPACE