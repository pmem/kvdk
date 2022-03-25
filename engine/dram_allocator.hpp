/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <sys/mman.h>

#include <atomic>
#include <cassert>
#include <string>

#include "alias.hpp"
#include "allocator.hpp"
#include "kvdk/engine.hpp"
#include "logger.hpp"
#include "structures.hpp"

namespace KVDK_NAMESPACE {

// Chunk based simple implementation
// TODO: optimize, implement free
class ChunkBasedAllocator : Allocator {
 public:
  SpaceEntry Allocate(uint64_t size) override;
  void Free(const SpaceEntry& entry) override;
  inline void* offset2addr(uint64_t offset) { return (void*)offset; }
  template <typename T>
  inline T* offset2addr(uint64_t offset) {
    return static_cast<T*>(offset);
  }
  inline uint64_t addr2offset(void* addr) { return (uint64_t)addr; }
  ChunkBasedAllocator(uint32_t max_access_threads)
      : dalloc_thread_cache_(max_access_threads) {}
  ChunkBasedAllocator(ChunkBasedAllocator const&) = delete;
  ChunkBasedAllocator(ChunkBasedAllocator&&) = delete;
  ~ChunkBasedAllocator() {
    for (uint64_t i = 0; i < dalloc_thread_cache_.size(); i++) {
      auto& tc = dalloc_thread_cache_[i];
      for (void* chunk : tc.allocated_chunks) {
        free(chunk);
      }
    }
  }

 private:
  struct alignas(64) DAllocThreadCache {
    char* chunk_addr = nullptr;
    uint64_t usable_bytes = 0;
    std::vector<void*> allocated_chunks;

    DAllocThreadCache() = default;
    DAllocThreadCache(const DAllocThreadCache&) = delete;
    DAllocThreadCache(DAllocThreadCache&&) = delete;
  };

  const uint32_t chunk_size_ = (1 << 20);
  Array<DAllocThreadCache> dalloc_thread_cache_;
};
}  // namespace KVDK_NAMESPACE