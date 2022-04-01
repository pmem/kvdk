/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <memory>
#include <set>

#include "../alias.hpp"
#include "../allocator.hpp"
#include "../utils/utils.hpp"

namespace KVDK_NAMESPACE {

constexpr uint32_t kFreelistMaxClassifiedBlockSize = 255;
constexpr uint32_t kSpaceMapLockGranularity = 64;

class PMEMAllocator;

// A byte map to record free blocks of PMem space, used for merging adjacent
// free space entries in the free list
class SpaceMap {
 public:
  SpaceMap(uint64_t num_blocks)
      : map_(num_blocks, {false, 0}),
        lock_granularity_(kSpaceMapLockGranularity),
        map_spins_(num_blocks / lock_granularity_ + 1) {}

  uint64_t TestAndUnset(uint64_t offset, uint64_t length);

  uint64_t TryMerge(uint64_t offset, uint64_t max_merge_length,
                    uint64_t min_merge_length);

  void Set(uint64_t offset, uint64_t length);

  uint64_t Size() { return map_.size(); }

 private:
  // The highest 1 bit ot the token indicates if this is the start of a space
  // entry, the lower 7 bits indicate how many free blocks followed
  struct Token {
   public:
    Token(bool is_start, uint8_t size)
        : token_(size | (is_start ? (1 << 7) : 0)) {}
    uint8_t Size() { return token_ & INT8_MAX; }
    void Clear() { token_ = 0; }
    bool Empty() { return Size() == 0; }
    bool IsStart() { return token_ & (1 << 7); }
    void UnStart() { token_ &= INT8_MAX; }

   private:
    uint8_t token_;
  };

  std::vector<Token> map_;
  // every lock_granularity_ bytes share a spin lock
  const uint32_t lock_granularity_;
  std::vector<SpinMutex> map_spins_;
};

// free entry pool consists of three level vectors, the first level
// indicates different block size, each block size consists of several free
// space entry lists (the second level), and each list consists of several
// free space entries (the third level).
//
// For a specific block size, a access thread will move a entry list from the
// pool to its thread cache while no usable free space in the cache, and the
// background thread will move cached entry list to the pool for merge and
// balance resource
//
// Organization of the three level vectors:
//
// block size (1st level)   entry list (2nd level)   entries (3th level)
//     1   -----------------   list1    ------------   entry1
//                    |                         |---   entry2
//                    |-----   list2    ------------   entry1
//                                              |---   entry2
//                                              |---   entry3
//                              ...
//     2   -----------------   list1    ------------   entry1
//                    |                         |---   entry2
//                    |                         |---   entry3
//                    |-----   list2
//                              ...
//    ...
// max_block_size   --------   list1
//                    |-----   list2
class SpaceEntryPool {
 public:
  SpaceEntryPool(uint32_t max_classified_b_size)
      : pool_(max_classified_b_size), spins_(max_classified_b_size) {}

  // move a list of b_size free space entries to pool, "src" will be empty
  // after move
  void MoveEntryList(std::vector<PMemOffsetType>& src, uint32_t b_size) {
    std::lock_guard<SpinMutex> lg(spins_[b_size]);
    assert(b_size < pool_.size());
    pool_[b_size].emplace_back();
    pool_[b_size].back().swap(src);
  }

  // try to fetch b_size free space entries from a entry list of pool to dst
  bool TryFetchEntryList(std::vector<PMemOffsetType>& dst, uint32_t b_size) {
    if (pool_[b_size].size() != 0) {
      std::lock_guard<SpinMutex> lg(spins_[b_size]);
      if (pool_[b_size].size() != 0) {
        dst.swap(pool_[b_size].back());
        pool_[b_size].pop_back();
        return true;
      }
    }
    return false;
  }

 private:
  std::vector<std::vector<std::vector<PMemOffsetType>>> pool_;
  // Entry lists of a same block size share a spin lock
  std::vector<SpinMutex> spins_;
};

class Freelist {
 public:
  Freelist(uint32_t max_classified_b_size, uint64_t num_segment_blocks,
           uint32_t block_size, uint32_t num_threads, uint64_t num_blocks,
           PMEMAllocator* allocator)
      : num_segment_blocks_(num_segment_blocks),
        block_size_(block_size),
        max_classified_b_size_(max_classified_b_size),
        active_pool_(max_classified_b_size),
        merged_pool_(max_classified_b_size),
        space_map_(num_blocks),
        flist_thread_cache_(num_threads, max_classified_b_size),
        pmem_allocator_(allocator) {}

  Freelist(uint64_t num_segment_blocks, uint32_t block_size,
           uint32_t num_threads, uint64_t num_blocks, PMEMAllocator* allocator)
      : Freelist(kFreelistMaxClassifiedBlockSize, num_segment_blocks,
                 block_size, num_threads, num_blocks, allocator) {}

  // Add a space entry
  void Push(const SpaceEntry& entry);

  // Add a batch of space entry to free list entries pool, return pushed size
  uint64_t BatchPush(const std::vector<SpaceEntry>& entries);

  // Request a at least "size" free space entry
  bool Get(uint32_t size, SpaceEntry* space_entry);

  // Try to merge thread-cached free space entries to get a at least "size"
  // entry
  bool MergeGet(uint32_t size, SpaceEntry* space_entry);

  // Merge adjacent free spaces stored in the entry pool into larger one
  //
  // Fetch every free space entry lists from active_pool_, for each entry in the
  // list, try to merge followed free space with it. Then insert merged entries
  // into merged_pool_. After merging, move all entry lists from merged_pool_ to
  // active_pool_ for next run. Calculate the minimal timestamp of free entries
  // in the pool meantime
  // TODO: set a condition to decide if we need to do merging
  void MergeSpaceInPool();

  // Move cached free space list to space entry pool to balance usable space
  // of access threads
  //
  // Iterate every active entry lists of thread caches, move the list to
  // active_pool_, and update minimal timestamp of free entries meantime
  void MoveCachedListsToPool();

  // Origanize free space entries, including merging adjacent space and move
  // thread cached space entries to pool
  void OrganizeFreeSpace();

 private:
  // Each access thread caches some freed space entries in active_entry_offsets
  // to avoid contention. To balance free space entries among threads, if too
  // many entries cached by a thread, newly freed entries will be stored to
  // backup_entries and move to entry pool which shared by all threads.
  struct alignas(64) FlistThreadCache {
    FlistThreadCache(uint32_t max_classified_b_size)
        : active_entry_offsets(max_classified_b_size),
          spins(max_classified_b_size) {}

    FlistThreadCache() = delete;
    FlistThreadCache(FlistThreadCache&&) = delete;
    FlistThreadCache(const FlistThreadCache&) = delete;

    // Offsets of active entries, entry size stored in block unit indicated by
    // Array index
    Array<std::vector<PMemOffsetType>> active_entry_offsets;
    // Protect active_entry_offsets
    Array<SpinMutex> spins;
  };

  class SpaceCmp {
   public:
    bool operator()(const SpaceEntry& s1, const SpaceEntry& s2) const {
      return s1.size > s2.size;
    }
  };

  uint64_t MergeSpace(uint64_t offset, uint64_t max_size,
                      uint64_t min_merge_size) {
    if (min_merge_size > max_size) {
      return 0;
    }
    uint64_t size = space_map_.TryMerge(offset, max_size, min_merge_size);
    return size;
  }

  const uint64_t num_segment_blocks_;
  const uint32_t block_size_;
  const uint32_t max_classified_b_size_;
  SpaceEntryPool active_pool_;
  SpaceEntryPool merged_pool_;
  SpaceMap space_map_;
  Array<FlistThreadCache> flist_thread_cache_;
  PMEMAllocator* pmem_allocator_;
  // Store all large free space entries that larger than max_classified_b_size_
  std::set<SpaceEntry, SpaceCmp> large_entries_;
  SpinMutex large_entries_spin_;
};

}  // namespace KVDK_NAMESPACE
