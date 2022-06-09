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

constexpr uint32_t kMaxSmallBlockSize = 255;
constexpr uint32_t kMaxBlockSizeIndex = 255;
constexpr uint32_t kBlockSizeIndexInterval = 1024;
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
  SpaceEntryPool(uint32_t max_small_entry_b_size,
                 uint32_t max_large_entry_size_index)
      : small_entry_pool_(max_small_entry_b_size),
        large_entry_pool_(max_large_entry_size_index),
        small_entry_spins_(max_small_entry_b_size),
        large_entry_spins_(max_large_entry_size_index) {}

  // move a list of b_size free space entries to pool, "src" will be empty
  // after move
  void MoveEntryList(std::vector<PMemOffsetType>& src, uint32_t b_size) {
    if (src.size() > 0) {
      std::lock_guard<SpinMutex> lg(small_entry_spins_[b_size]);
      assert(b_size < small_entry_pool_.size());
      small_entry_pool_[b_size].emplace_back();
      small_entry_pool_[b_size].back().swap(src);
    }
  }

  // try to fetch b_size free space entries from a entry list of pool to dst
  bool TryFetchEntryList(std::vector<PMemOffsetType>& dst, uint32_t b_size) {
    if (small_entry_pool_[b_size].size() != 0) {
      std::lock_guard<SpinMutex> lg(small_entry_spins_[b_size]);
      if (small_entry_pool_[b_size].size() != 0) {
        dst.swap(small_entry_pool_[b_size].back());
        small_entry_pool_[b_size].pop_back();
        return true;
      }
    }
    return false;
  }

  void MoveLargeEntrySet(std::set<SpaceEntry, SpaceEntry::SpaceCmp>& src,
                         size_t size_index) {
    if (src.size() > 0) {
      kvdk_assert(size_index < large_entry_pool_.size(), "");
      std::lock_guard<SpinMutex> lg(large_entry_spins_[size_index]);
      large_entry_pool_[size_index].emplace_back();
      large_entry_pool_[size_index].back().swap(src);
    }
  }

  // Try to fetch large space entries set with size_index from pool to dst
  bool TryFetchLargeEntrySet(std::set<SpaceEntry, SpaceEntry::SpaceCmp>& dst,
                             size_t size_index) {
    kvdk_assert(size_index < large_entry_pool_.size(), "");
    if (large_entry_pool_[size_index].size() != 0) {
      std::lock_guard<SpinMutex> lg(large_entry_spins_[size_index]);
      if (large_entry_pool_[size_index].size() != 0) {
        dst.swap(large_entry_pool_[size_index].back());
        large_entry_pool_[size_index].pop_back();
        return true;
      }
    }
    return false;
  }

 private:
  // Pool of small space entries, the vector index is block size of space
  // entries in each entry offset list
  std::vector<std::vector<std::vector<PMemOffsetType>>> small_entry_pool_;
  // Pool of large space entries, the vector index is block size index of space
  // entries in each entry set
  std::vector<std::vector<std::set<SpaceEntry, SpaceEntry::SpaceCmp>>>
      large_entry_pool_;
  // Small entry lists of a same block size share a spin lock
  std::vector<SpinMutex> small_entry_spins_;
  // Large entry set of same size index share a spin lock
  std::vector<SpinMutex> large_entry_spins_;
};

class Freelist {
 public:
  Freelist(uint32_t max_small_entry_b_size, uint64_t num_segment_blocks,
           uint32_t block_size, uint32_t num_threads, uint64_t num_blocks,
           PMEMAllocator* allocator)
      : num_segment_blocks_(num_segment_blocks),
        block_size_(block_size),
        max_small_entry_block_size_(max_small_entry_b_size),
        max_block_size_index_(std::min(
            kMaxBlockSizeIndex, blockSizeIndex(num_segment_blocks_) + 1)),
        active_pool_(max_small_entry_b_size, max_block_size_index_),
        merged_pool_(max_small_entry_b_size, max_block_size_index_),
        space_map_(num_blocks),
        flist_thread_cache_(num_threads, max_small_entry_block_size_,
                            max_block_size_index_),
        pmem_allocator_(allocator) {}

  Freelist(uint64_t num_segment_blocks, uint32_t block_size,
           uint32_t num_threads, uint64_t num_blocks, PMEMAllocator* allocator)
      : Freelist(kMaxSmallBlockSize, num_segment_blocks, block_size,
                 num_threads, num_blocks, allocator) {}

  // Add a space entry
  void Push(const SpaceEntry& entry);

  // Add a batch of space entry to free list entries pool, return pushed size
  uint64_t BatchPush(const std::vector<SpaceEntry>& entries);

  // Request a free space entry that equal to or larger than "size"
  bool Get(uint32_t size, SpaceEntry* space_entry);

  // Merge adjacent free spaces stored in the entry pool into larger one
  //
  // Fetch every free space entry lists from active_pool_, for each entry in the
  // list, try to merge followed free space with it. Then insert merged entries
  // into merged_pool_. After merging, move all entry lists from merged_pool_ to
  // active_pool_ for next run. Calculate the minimal timestamp of free entries
  // in the pool meantime
  // TODO: set a condition to decide if we need to do merging
  // Notice: we do not merge large entries for performance
  void MergeSpaceInPool();

  // Move cached free space list to space entry pool to balance usable space
  // of access threads
  //
  // Iterate every active entry lists of thread caches, move the list to
  // active_pool_, and update minimal timestamp of free entries meantime
  void MoveCachedEntriesToPool();

  // Origanize free space entries, including merging adjacent space and move
  // thread cached space entries to pool
  void OrganizeFreeSpace();

 private:
  // Each access thread caches some freed space entries in small_entry_offsets
  // and large_entries according to their size. To balance free space entries
  // among threads, a background thread will regularly move cached entries to
  // entry pool which shared by all threads.
  struct alignas(64) FlistThreadCache {
    FlistThreadCache(uint32_t max_small_entry_b_size,
                     size_t max_large_entry_size_index)
        : small_entry_offsets(max_small_entry_b_size),
          large_entries(max_large_entry_size_index),
          small_entry_spins(max_small_entry_b_size),
          large_entry_spins(max_large_entry_size_index) {}

    FlistThreadCache() = delete;
    FlistThreadCache(FlistThreadCache&&) = delete;
    FlistThreadCache(const FlistThreadCache&) = delete;

    // Offsets of small free space entries whose block size smaller than
    // max_small_entry_b_size. Array index indicates block size of entries
    // stored in the vector
    Array<std::vector<PMemOffsetType>> small_entry_offsets;
    // Store all large free space entries whose block size larger than
    // max_small_entry_b_size. Array index indicates entries stored in the set
    // have block size between "max_small_entry_b_size + index *
    // kBlockSizeIndexInterval" and "max_small_entry_b_size +
    // (index + 1) * kBlockSizeIndexInterval"
    Array<std::set<SpaceEntry, SpaceEntry::SpaceCmp>> large_entries;
    // Protect small_entry_offsets
    Array<SpinMutex> small_entry_spins;
    // Protect large_entries
    Array<SpinMutex> large_entry_spins;
  };

  uint64_t MergeSpace(uint64_t offset, uint64_t max_size,
                      uint64_t min_merge_size) {
    if (min_merge_size > max_size) {
      return 0;
    }
    uint64_t size = space_map_.TryMerge(offset, max_size, min_merge_size);
    return size;
  }

  uint32_t blockSizeIndex(uint32_t block_size) {
    kvdk_assert(block_size <= num_segment_blocks_, "");
    uint32_t ret = block_size < max_small_entry_block_size_
                       ? 0
                       : (block_size - max_small_entry_block_size_) /
                             kBlockSizeIndexInterval;
    return std::min(ret, max_block_size_index_ - 1);
  }

  bool getSmallEntry(uint32_t size, SpaceEntry* space_entry);

  bool getLargeEntry(uint32_t size, SpaceEntry* space_entry);

  const uint64_t num_segment_blocks_;
  const uint32_t block_size_;
  const uint32_t max_small_entry_block_size_;
  const uint32_t max_block_size_index_;
  SpaceEntryPool active_pool_;
  SpaceEntryPool merged_pool_;
  SpaceMap space_map_;
  Array<FlistThreadCache> flist_thread_cache_;
  PMEMAllocator* pmem_allocator_;
  SpinMutex large_entries_spin_;
};

}  // namespace KVDK_NAMESPACE
