/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <memory>
#include <set>

#include "../allocator.hpp"
#include "../utils.hpp"
#include "kvdk/namespace.hpp"

namespace KVDK_NAMESPACE {

class SpaceMap {
public:
  SpaceMap(uint64_t size)
      : map_(size, {false, 0}), lock_granularity_(64),
        spins_(size / lock_granularity_ + 1) {}

  uint64_t TestAndUnset(uint64_t offset, uint64_t length);

  uint64_t TryMerge(uint64_t offset, uint64_t max_merge_length,
                    uint64_t target_merge_length);

  void Set(uint64_t offset, uint64_t length);

  uint64_t Size() { return map_.size(); }

private:
  // The highest 1 bit indicates if this is the start of a space entry, the
  // lower 7 bits indicates how many free blocks followed
  struct MapToken {
  public:
    MapToken(bool is_start, uint8_t size)
        : token(size | (is_start ? (1 << 7) : 0)) {}
    uint8_t Size() { return token & ((1 << 7) - 1); }
    void Clear() { token = 0; }
    bool Empty() { return token == 0; }
    bool IsStart() { return token & (1 << 7); }
    void UnStart() { token &= ((1 << 7) - 1); }

  private:
    uint8_t token;
  };

  // how many blocks share a lock
  const uint32_t lock_granularity_;
  std::vector<MapToken> map_;
  std::vector<SpinMutex> spins_;
};

// free entry pool consists of three level vectors, the first level
// indicates different block size, each block size consists of several free
// space entry lists (the second level), and each list consists of several
// free space entries (the third level).
//
// For a specific block size, a write thread will move a entry list from the
// pool to its thread cache while no usable free space in the cache, or move a
// entry list to the pool while too many entries cached.
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
  SpaceEntryPool(uint32_t max_b_size) : pool_(max_b_size), spins_(max_b_size) {}

  // move a entry list of b_size free space entries to pool, "src" will be empty
  // after move
  void MoveEntryList(std::vector<SpaceEntry> &src, uint32_t b_size) {
    std::lock_guard<SpinMutex> lg(spins_[b_size]);
    assert(b_size < pool_.size());
    pool_[b_size].emplace_back();
    pool_[b_size].back().swap(src);
  }

  // try to fetch b_size free space entries from a entry list of pool to dst
  bool TryFetchEntryList(std::vector<SpaceEntry> &dst, uint32_t b_size) {
    std::lock_guard<SpinMutex> lg(spins_[b_size]);
    if (pool_[b_size].size() != 0) {
      dst.swap(pool_[b_size].back());
      pool_[b_size].pop_back();
      return true;
    }
    return false;
  }

private:
  std::vector<std::vector<std::vector<SpaceEntry>>> pool_;
  std::vector<SpinMutex> spins_;
};

class FreeList {
public:
  FreeList(uint32_t max_b_size, uint64_t num_segment_blocks,
           uint32_t num_threads, std::shared_ptr<SpaceMap> space_map)
      : num_segment_blocks_(num_segment_blocks),
        max_classified_b_size_(max_b_size), active_pool_(max_b_size),
        merged_pool_(max_b_size), space_map_(space_map),
        thread_cache_(num_threads, max_b_size) {}

  FreeList(uint64_t num_segment_blocks, uint32_t num_threads,
           std::shared_ptr<SpaceMap> space_map)
      : FreeList(FREE_LIST_MAX_BLOCK, num_segment_blocks, num_threads,
                 space_map) {}

  void Push(const SizedSpaceEntry &entry);

  bool Get(uint32_t b_size, SizedSpaceEntry *space_entry);

  bool MergeGet(uint32_t b_size, SizedSpaceEntry *space_entry);

  // Move cached free space list to space entry pool to balance usable space
  // of write threads
  //
  // Iterate every backup entry lists of thread caches, and move the list to
  // active_pool_ if more than kMinMovableEntries in it
  void MoveCachedListToPool();

  // Merge adjacent free spaces into a larger one
  //
  // Fetch every free space entry lists from active_pool_, for each entry in the
  // list, try to merge followed free space with it. Then insert merged entries
  // into merged_pool_. After merging, move all entry lists from merged_pool_ to
  // active_pool_ for next run
  // TODO: set a condition to decide if we need to do merging
  void MergeFreeSpace();

private:
  // Each write threads cache some freed space entries in active_entries to
  // avoid contention. To balance free space entries among threads, if too many
  // entries cached by a thread, newly freed entries will be stored to
  // backup_entries and move to entry pool which shared by all threads.
  struct ThreadCache {
    ThreadCache(uint32_t max_b_size)
        : active_entries(max_b_size), backup_entries(max_b_size),
          spins(max_b_size) {}

    std::vector<std::vector<SpaceEntry>> active_entries;
    std::vector<std::vector<SpaceEntry>> backup_entries;
    std::vector<SpinMutex> spins;
  };

  class SpaceCmp {
  public:
    bool operator()(const SizedSpaceEntry &s1, const SizedSpaceEntry &s2) {
      return s1.size > s2.size;
    }
  };

  uint64_t MergeSpace(const SpaceEntry &space_entry, uint64_t max_size,
                      uint64_t target_size) {
    if (target_size > max_size) {
      return false;
    }
    uint64_t size =
        space_map_->TryMerge(space_entry.offset, max_size, target_size);
    return size;
  }

  const uint64_t num_segment_blocks_;
  const uint32_t max_classified_b_size_;
  std::shared_ptr<SpaceMap> space_map_;
  std::set<SizedSpaceEntry, SpaceCmp> large_entries_;
  std::vector<ThreadCache> thread_cache_;
  SpaceEntryPool active_pool_;
  SpaceEntryPool merged_pool_;
  SpinMutex large_entries_spin_;
};

} // namespace KVDK_NAMESPACE
