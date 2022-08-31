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

// System memory allocator used for default scenarios.
class SystemMemoryAllocator : public Allocator {
 public:
  SystemMemoryAllocator() : Allocator(0, UINT64_MAX) {}

  SpaceEntry Allocate(uint64_t size) override {
    SpaceEntry entry;
    void* addr = malloc(size);
    if (addr != nullptr) {
      bytes_allocated_.fetch_add(size);
      entry.offset = reinterpret_cast<uint64_t>(addr);
      entry.size = size;
    }

    return entry;
  }

  SpaceEntry AllocateAligned(size_t alignment, uint64_t size) override {
    SpaceEntry entry;
    void* addr = aligned_alloc(alignment, size);
    if (addr != nullptr) {
      bytes_allocated_.fetch_add(size);
      entry.offset = reinterpret_cast<uint64_t>(addr);
      entry.size = size;
    }

    return entry;
  }

  void Free(const SpaceEntry& entry) override {
    if (entry.offset) {
      free(reinterpret_cast<void*>(entry.offset));
      bytes_allocated_.fetch_add(-entry.size);
    }
  }
  int64_t BytesAllocated() override { return bytes_allocated_.load(); }

  std::string AllocatorName() override { return "System"; }

 private:
  std::atomic<int64_t> bytes_allocated_{0};
};

// Chunk based simple implementation
// TODO: optimize, implement free
class ChunkBasedAllocator {
 public:
  SpaceEntry Allocate(uint64_t size);
  void Free(const SpaceEntry& entry);

  ChunkBasedAllocator(uint32_t max_access_threads)
      : dalloc_thread_cache_(max_access_threads) {}
  ChunkBasedAllocator(ChunkBasedAllocator const&) = delete;
  ChunkBasedAllocator(ChunkBasedAllocator&&) = delete;
  ~ChunkBasedAllocator() {
    for (uint64_t i = 0; i < dalloc_thread_cache_.size(); i++) {
      auto& tc = dalloc_thread_cache_[i];
      for (auto chunk : tc.allocated_chunks) {
        alloc_->Free(chunk);
      }
    }
  }

  template <typename T>
  inline T* offset2addr(MemoryOffsetType offset) const {
    return alloc_->offset2addr<T>(offset);
  }

 private:
  struct alignas(64) DAllocThreadCache {
    char* chunk_addr = nullptr;
    uint64_t usable_bytes = 0;
    std::vector<SpaceEntry> allocated_chunks;

    DAllocThreadCache() = default;
    DAllocThreadCache(const DAllocThreadCache&) = delete;
    DAllocThreadCache(DAllocThreadCache&&) = delete;
  };

  const uint32_t chunk_size_ = (1 << 20);
  Array<DAllocThreadCache> dalloc_thread_cache_;
  Allocator* alloc_ = default_memory_allocator();
};
}  // namespace KVDK_NAMESPACE