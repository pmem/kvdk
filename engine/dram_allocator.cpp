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
  SpaceEntry entry;
  if (size > chunk_size_) {
    void* addr = aligned_alloc(64, size);
    if (addr != nullptr) {
      entry.size = chunk_size_;
      entry.offset = addr2offset(addr);
      dalloc_thread_cache_[access_thread.id].allocated_chunks.push_back(addr);
    }
    return entry;
  }

  if (dalloc_thread_cache_[access_thread.id].usable_bytes < size) {
    void* addr = aligned_alloc(64, chunk_size_);
    if (addr == nullptr) {
      return entry;
    }
    dalloc_thread_cache_[access_thread.id].chunk_addr = (char*)addr;
    dalloc_thread_cache_[access_thread.id].usable_bytes = chunk_size_;
    dalloc_thread_cache_[access_thread.id].allocated_chunks.push_back(addr);
  }

  entry.size = size;
  entry.offset = addr2offset(dalloc_thread_cache_[access_thread.id].chunk_addr);
  dalloc_thread_cache_[access_thread.id].chunk_addr += size;
  dalloc_thread_cache_[access_thread.id].usable_bytes -= size;
  return entry;
}
}  // namespace KVDK_NAMESPACE