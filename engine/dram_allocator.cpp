/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "dram_allocator.hpp"

#include "thread_manager.hpp"

namespace KVDK_NAMESPACE {

constexpr size_t kDefaultChunkAlignment = 64;

void ChunkBasedAllocator::Free(const SpaceEntry&) {
  // Not supported yet
}

SpaceEntry ChunkBasedAllocator::Allocate(uint64_t size) {
  SpaceEntry entry;
  if (size > chunk_size_) {
    entry = alloc_->AllocateAligned(kDefaultChunkAlignment, chunk_size_);
    if (entry.size != 0) {
      dalloc_thread_cache_[access_thread.id].allocated_chunks.push_back(entry);
    }
    return entry;
  }

  if (dalloc_thread_cache_[access_thread.id].usable_bytes < size) {
    entry = alloc_->AllocateAligned(kDefaultChunkAlignment, chunk_size_);
    if (entry.size == 0) {
      return entry;
    }
    void* addr = alloc_->offset2addr(entry.offset);
    dalloc_thread_cache_[access_thread.id].chunk_addr = (char*)addr;
    dalloc_thread_cache_[access_thread.id].usable_bytes = chunk_size_;
    dalloc_thread_cache_[access_thread.id].allocated_chunks.push_back(entry);
  }

  entry.size = size;
  entry.offset =
      alloc_->addr2offset(dalloc_thread_cache_[access_thread.id].chunk_addr);
  dalloc_thread_cache_[access_thread.id].chunk_addr += size;
  dalloc_thread_cache_[access_thread.id].usable_bytes -= size;
  return entry;
}

static struct GlobalState {
  SystemMemoryAllocator system_memory_allocator;
} global_state;

Allocator* default_memory_allocator() {
  return &global_state.system_memory_allocator;
}

}  // namespace KVDK_NAMESPACE