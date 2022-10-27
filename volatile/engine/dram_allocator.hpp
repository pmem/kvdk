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
#include "kvdk/volatile/engine.hpp"
#include "logger.hpp"
#include "macros.hpp"
#include "structures.hpp"

#ifdef KVDK_WITH_JEMALLOC
#include "jemalloc/jemalloc.h"
#include "numa.h"
#include "numaif.h"
#endif

namespace KVDK_NAMESPACE {

// System memory allocator used for default scenarios.
class SystemMemoryAllocator : public Allocator {
 public:
  SystemMemoryAllocator() : Allocator(0, UINT64_MAX) {}

  SpaceEntry Allocate(uint64_t size) override {
    SpaceEntry entry;
    void* addr = malloc(size);
    if (addr != nullptr) {
      LogAllocation(ThreadManager::ThreadID(), size);
      entry.offset = reinterpret_cast<uint64_t>(addr);
      entry.size = size;
    }

    return entry;
  }

  SpaceEntry AllocateAligned(size_t alignment, uint64_t size) override {
    SpaceEntry entry;
    void* addr = aligned_alloc(alignment, size);
    if (addr != nullptr) {
      LogAllocation(ThreadManager::ThreadID(), size);
      entry.offset = reinterpret_cast<uint64_t>(addr);
      entry.size = size;
    }

    return entry;
  }

  void Free(const SpaceEntry& entry) override {
    if (entry.offset) {
      free(reinterpret_cast<void*>(entry.offset));
      LogDeallocation(ThreadManager::ThreadID(), entry.size);
    }
  }

  std::string AllocatorName() override { return "System"; }
};

#ifdef KVDK_WITH_JEMALLOC
// jemalloc memory allocator used for KV.
class JemallocMemoryAllocator : public Allocator {
 public:
  JemallocMemoryAllocator() : Allocator(0, UINT64_MAX) {}

  SpaceEntry Allocate(uint64_t size) override {
    SpaceEntry entry;

    void* addr = nullptr;
    if (KVDK_UNLIKELY(num_arenas_ == 0)) {
      addr = je_kvdk_malloc(size);
    } else {
      initializeArenas();
      unsigned arena = getArenaForCurrentThread();
      addr =
          allocateWithFlags(size, MALLOCX_ARENA(arena) | MALLOCX_TCACHE_NONE);
    }

    if (KVDK_LIKELY(addr != nullptr)) {
      LogAllocation(ThreadManager::ThreadID(), size);
      entry.offset = reinterpret_cast<uint64_t>(addr);
      entry.size = size;
    }

    return entry;
  }

  SpaceEntry AllocateAligned(size_t alignment, uint64_t size) override {
    SpaceEntry entry;

    void* addr = nullptr;
    if (KVDK_UNLIKELY(num_arenas_ == 0)) {
      addr = je_kvdk_aligned_alloc(alignment, size);
    } else {
      if (KVDK_LIKELY(checkAlignment(alignment) == 0)) {
        initializeArenas();
        unsigned arena = getArenaForCurrentThread();
        addr = allocateWithFlags(size, MALLOCX_ALIGN(alignment) |
                                           MALLOCX_ARENA(arena) |
                                           MALLOCX_TCACHE_NONE);
      }
    }

    if (KVDK_LIKELY(addr != nullptr)) {
      LogAllocation(ThreadManager::ThreadID(), size);
      entry.offset = reinterpret_cast<uint64_t>(addr);
      entry.size = size;
    }

    return entry;
  }

  void Free(const SpaceEntry& entry) override {
    if (KVDK_UNLIKELY(entry.offset == 0)) {
      return;
    }

    void* ptr = reinterpret_cast<void*>(entry.offset);
    if (KVDK_UNLIKELY(num_arenas_ == 0)) {
      je_kvdk_free(ptr);
    } else {
      je_kvdk_dallocx(ptr, MALLOCX_TCACHE_NONE);
    }

    LogDeallocation(ThreadManager::ThreadID(), entry.size);
  }

  std::string AllocatorName() override { return "jemalloc"; }

  bool SetMaxAccessThreads(uint32_t max_access_threads) override;

  void SetDestMemoryNodes(std::string dest_memory_nodes) override {
    auto ul = Allocator::AcquireLock();

    dest_memory_nodes_ = dest_memory_nodes;
    setDestMemoryNodesMask();
  }

  int GetMbindMode() { return MPOL_BIND; }

  bitmask* GetMbindNodesMask() { return dest_memory_nodes_mask_; }

  ~JemallocMemoryAllocator() {
    destroyArenas();

    if (dest_memory_nodes_mask_) {
      numa_bitmask_free(dest_memory_nodes_mask_);
      dest_memory_nodes_mask_ = nullptr;
    }
  }

 private:
  uint32_t num_arenas_{0};
  uint32_t arena_mask_{0};

  bool arenas_initialized_{false};
  std::vector<unsigned> arena_index_;

  std::string dest_memory_nodes_;
  bitmask* dest_memory_nodes_mask_ = nullptr;

  unsigned createOneArena();
  unsigned getArenaForCurrentThread();
  void destroyArenas();

  static void* allocateWithFlags(size_t size, int flags);
  static int checkAlignment(size_t alignment);

  inline void initializeArenas() {
    if (KVDK_LIKELY(arenas_initialized_ || num_arenas_ == 0)) {
      return;
    }

    auto ul = Allocator::AcquireLock();
    if (arenas_initialized_ || num_arenas_ == 0) {
      return;
    }

    for (uint32_t i = 0; i < num_arenas_; i++) {
      unsigned arena_index = createOneArena();
      arena_index_.push_back(arena_index);
    }

    arenas_initialized_ = true;
  }

  void setDestMemoryNodesMask() {
    if (dest_memory_nodes_mask_) {
      numa_bitmask_free(dest_memory_nodes_mask_);
      dest_memory_nodes_mask_ = nullptr;
    }

    if (dest_memory_nodes_.size() == 0) {
      return;
    }

    dest_memory_nodes_mask_ = numa_parse_nodestring(dest_memory_nodes_.c_str());
    if (dest_memory_nodes_mask_ == 0) {
      GlobalLogger.Error("Invalid value of `dest_numa_nodes`: %s.\n",
                         dest_memory_nodes_.c_str());
    }
  }
};
#endif  // #ifdef KVDK_WITH_JEMALLOC

// Chunk based simple implementation
// TODO: optimize, implement free
class ChunkBasedAllocator {
 public:
  SpaceEntry Allocate(uint64_t size);
  void Free(const SpaceEntry& entry);

  ChunkBasedAllocator(uint32_t max_access_threads, Allocator* alloc)
      : dalloc_thread_cache_(max_access_threads), alloc_(alloc) {}
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
  Allocator* alloc_;
};
}  // namespace KVDK_NAMESPACE