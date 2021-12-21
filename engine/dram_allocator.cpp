/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "dram_allocator.hpp"
#include "kvdk/namespace.hpp"
#include "thread_manager.hpp"

namespace KVDK_NAMESPACE {

void ChunkBasedAllocator::Free(const SpaceEntry &entry) {
  // Not supported yet
}

SpaceEntry ChunkBasedAllocator::Allocate(uint64_t size) {
  SpaceEntry entry;
  if (size > chunk_size_) {
    void *addr = malloc(size);
    if (addr != nullptr) {
      entry.size = chunk_size_;
      entry.offset = addr2offset(addr);
      thread_cache_[write_thread.id].allocated_chunks.push_back(addr);
    }
    return entry;
  }

  if (thread_cache_[write_thread.id].usable_bytes < size) {
    void *addr = malloc(chunk_size_);
    if (addr == nullptr) {
      return entry;
    }
    thread_cache_[write_thread.id].chunk_addr = (char *)addr;
    thread_cache_[write_thread.id].usable_bytes = chunk_size_;
    thread_cache_[write_thread.id].allocated_chunks.push_back(addr);
  }

  entry.size = size;
  entry.offset = addr2offset(thread_cache_[write_thread.id].chunk_addr);
  thread_cache_[write_thread.id].chunk_addr += size;
  thread_cache_[write_thread.id].usable_bytes -= size;
  return entry;
}
} // namespace KVDK_NAMESPACE