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

Allocator* global_memory_allocator() {
  return &global_state.system_memory_allocator;
}

static unsigned integer_log2(unsigned v) {
  return (sizeof(unsigned) * 8) - (__builtin_clz(v) + 1);
}

static unsigned round_pow2_up(unsigned v) {
  unsigned v_log2 = integer_log2(v);

  unsigned one = 1;
  if (v != one << v_log2) {
    v = one << (v_log2 + 1);
  }
  return v;
}

// SplitMix64 hash
static uint64_t hash64(uint64_t x) {
  x += 0x9e3779b97f4a7c15;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9;
  x = (x ^ (x >> 27)) * 0x94d049bb133111eb;
  return x ^ (x >> 31);
}

static uintptr_t get_fs_base() { return (uintptr_t)pthread_self(); }

#ifdef KVDK_WITH_JEMALLOC
bool JemallocMemoryAllocator::SetMaxAccessThreads(uint32_t max_access_threads) {
  if (max_access_threads == 0 || arenas_initialized_) {
    return false;
  }

  auto ul = Allocator::AcquireLock();

  if (arenas_initialized_) {
    return false;
  }

  num_arenas_ = round_pow2_up(max_access_threads);
  arena_mask_ = num_arenas_ - 1;

  return Allocator::EnableThreadLocalCounters(max_access_threads);
}

static SpinMutex jemalloc_arena_spin;
static std::map<unsigned, JemallocMemoryAllocator*> arena_allocator_map;

// Allocates size bytes aligned to alignment. Returns NULL if allocation fails.
void* alloc_aligned_slow(size_t size, size_t alignment, unsigned arena_index) {
  JemallocMemoryAllocator* alloc = arena_allocator_map[arena_index];
  if (alloc == nullptr) {
    return NULL;
  }

  size_t extended_size = size + alignment;
  void* ptr;

  ptr = mmap(NULL, extended_size, PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

  if (ptr == MAP_FAILED) {
    return NULL;
  }

  // mbind
  bitmask* mask = alloc->GetMbindNodesMask();
  if (mask) {
    int mode = alloc->GetMbindMode();
    int err = mbind(ptr, extended_size, mode, mask->maskp, mask->size, 0);
    if (KVDK_UNLIKELY(err)) {
      GlobalLogger.Error("syscall mbind() errno: %d.\n", errno);
      munmap(ptr, extended_size);
      return NULL;
    }
  }

  uintptr_t addr = (uintptr_t)ptr;
  uintptr_t aligned_addr = (addr + alignment) & ~(alignment - 1);

  size_t head_len = aligned_addr - addr;
  if (head_len > 0) {
    munmap(ptr, head_len);
  }

  uintptr_t tail = aligned_addr + size;
  size_t tail_len = (addr + extended_size) - (aligned_addr + size);
  if (tail_len > 0) {
    munmap((void*)tail, tail_len);
  }

  return (void*)aligned_addr;
}

static void* arena_extent_alloc(extent_hooks_t*, void* new_addr, size_t size,
                                size_t alignment, bool* zero, bool* commit,
                                unsigned arena_index) {
  JemallocMemoryAllocator* alloc = arena_allocator_map[arena_index];
  if (alloc == nullptr) {
    return NULL;
  }

  void* addr = mmap(new_addr, size, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (addr == MAP_FAILED) {
    return NULL;
  }

  // mbind
  bitmask* mask = alloc->GetMbindNodesMask();
  if (mask) {
    int mode = alloc->GetMbindMode();
    int err = mbind(addr, size, mode, mask->maskp, mask->size, 0);
    if (KVDK_UNLIKELY(err)) {
      GlobalLogger.Error("syscall mbind() errno: %d.\n", errno);
      munmap(addr, size);
      return NULL;
    }
  }

  if (new_addr != NULL && addr != new_addr) {
    /* wrong place */
    munmap(addr, size);
    return NULL;
  }

  if ((uintptr_t)addr & (alignment - 1)) {
    munmap(addr, size);
    addr = alloc_aligned_slow(size, alignment, arena_index);
    if (addr == NULL) {
      return NULL;
    }
  }

  *zero = true;
  *commit = true;

  return addr;
}

static bool arena_extent_dalloc(extent_hooks_t*, void*, size_t, bool,
                                unsigned) {
  return true;
}

static bool arena_extent_commit(extent_hooks_t*, void*, size_t, size_t, size_t,
                                unsigned) {
  return false;
}

static bool arena_extent_decommit(extent_hooks_t*, void*, size_t, size_t,
                                  size_t, unsigned) {
  return true;
}

static bool arena_extent_purge(extent_hooks_t*, void* addr, size_t,
                               size_t offset, size_t length, unsigned) {
  int err = madvise((char*)addr + offset, length, MADV_DONTNEED);
  return (err != 0);
}

static bool arena_extent_split(extent_hooks_t*, void*, size_t, size_t, size_t,
                               bool, unsigned) {
  return false;
}

static bool arena_extent_merge(extent_hooks_t*, void*, size_t, void*, size_t,
                               bool, unsigned) {
  return false;
}

// clang-format off
static extent_hooks_t arena_extent_hooks = {
    arena_extent_alloc,
    arena_extent_dalloc,
    NULL,
    arena_extent_commit,
    arena_extent_decommit,
    arena_extent_purge,
    NULL,
    arena_extent_split,
    arena_extent_merge
};
// clang-format on

unsigned JemallocMemoryAllocator::get_arena_for_current_thread() {
  unsigned int arena_idx;
  arena_idx = hash64(get_fs_base()) & arena_mask_;
  return arena_index_[arena_idx];
}

unsigned JemallocMemoryAllocator::create_an_arena() {
  std::unique_lock<SpinMutex> ul(jemalloc_arena_spin);

  unsigned arena_index;
  size_t unsigned_size = sizeof(unsigned int);
  je_kvdk_mallctl("arenas.create", (void*)&arena_index, &unsigned_size, NULL,
                  0);

  // setup extent_hooks for newly created arena
  char cmd[64];
  extent_hooks_t* hooks = &arena_extent_hooks;
  snprintf(cmd, sizeof(cmd), "arena.%u.extent_hooks", arena_index);
  je_kvdk_mallctl(cmd, NULL, NULL, (void*)&hooks, sizeof(extent_hooks_t*));

  arena_allocator_map[arena_index] = this;
  return arena_index;
}

void JemallocMemoryAllocator::destroy_arenas() {
  if (!arenas_initialized_ || num_arenas_ == 0) {
    return;
  }

  std::unique_lock<SpinMutex> ul(jemalloc_arena_spin);

  for (uint32_t i = 0; i < num_arenas_; i++) {
    char cmd[128];
    unsigned arena_index = arena_index_[i];
    snprintf(cmd, 128, "arena.%u.destroy", arena_index);
    je_kvdk_mallctl(cmd, NULL, NULL, NULL, 0);
    arena_allocator_map.erase(arena_index);
  }

  arena_index_.clear();
  arenas_initialized_ = false;
}

void* JemallocMemoryAllocator::je_kvdk_mallocx_check(size_t size, int flags) {
  if (KVDK_LIKELY(size)) {
    void* ptr = je_kvdk_mallocx(size, flags);
    return ptr;
  }
  return nullptr;
}

int JemallocMemoryAllocator::check_alignment(size_t alignment) {
  int err = 0;
  if ((alignment < sizeof(void*)) || (((alignment - 1) & alignment) != 0)) {
    err = 1;
  }
  return err;
}

#endif  // #ifdef KVDK_WITH_JEMALLOC

}  // namespace KVDK_NAMESPACE