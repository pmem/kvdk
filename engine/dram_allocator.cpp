/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "dram_allocator.hpp"
#include "kvdk/namespace.hpp"
#include "thread_manager.hpp"

namespace KVDK_NAMESPACE {

void DRAMAllocator::Free(const SizedSpaceEntry &entry) {
  // Not supported yet
}

SizedSpaceEntry DRAMAllocator::Allocate(uint64_t size) {
  SizedSpaceEntry entry;
  if (size > chunk_size_) {
    void *addr = malloc(size);
    if (addr != nullptr) {
      entry.size = chunk_size_;
      entry.space_entry.offset = addr2offset(addr);
    }
    return entry;
  }

  if (thread_cache_[write_thread.id].usable_bytes < size) {
    thread_cache_[write_thread.id].chunk_addr = (char *)malloc(chunk_size_);
    if (thread_cache_[write_thread.id].chunk_addr == nullptr) {
      return entry;
    }
    thread_cache_[write_thread.id].usable_bytes = chunk_size_;
  }

  entry.size = size;
  entry.space_entry.offset =
      addr2offset(thread_cache_[write_thread.id].chunk_addr);
  thread_cache_[write_thread.id].chunk_addr += size;
  thread_cache_[write_thread.id].usable_bytes -= size;
  return entry;
}
} // namespace KVDK_NAMESPACE