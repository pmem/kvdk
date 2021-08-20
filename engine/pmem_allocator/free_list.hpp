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
  SpaceMap(uint64_t size, uint32_t block_size)
      : block_size_(block_size), map_(size, {false, 0}), lock_granularity_(64),
        spins_(size / lock_granularity_ + 1) {}

  uint64_t TestAndUnset(uint64_t offset, uint64_t length);

  uint64_t FindAndUnset(uint64_t &start_offset, uint64_t max_length,
                        uint64_t target_length);

  uint64_t MergeAndUnset(uint64_t offset, uint64_t max_length,
                         uint64_t target_length);

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

  private:
    uint8_t token;
  };

  const uint32_t block_size_;
  std::vector<MapToken> map_;
  // how many blocks share a lock
  const uint32_t lock_granularity_;
  std::vector<SpinMutex> spins_;
};

class FreeList {
public:
  FreeList(uint32_t max_b_size, uint32_t num_threads,
           std::shared_ptr<SpaceMap> space_map)
      : space_entry_pool_(max_b_size), space_map_(space_map),
        thread_cache_(num_threads, max_b_size), spins_(max_b_size) {}

  FreeList(std::shared_ptr<SpaceMap> space_map, uint32_t num_threads)
      : FreeList(FREE_LIST_MAX_BLOCK, num_threads, space_map) {}

  void Push(const SizedSpaceEntry &entry);

  bool Get(uint32_t b_size, SizedSpaceEntry *space_entry);

  bool MergeGet(uint32_t b_size, uint64_t segment_blocks,
                SizedSpaceEntry *space_entry);

  void MoveBackupLists();

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
        space_map_->MergeAndUnset(space_entry.offset, max_size, target_size);
    return size;
  }

  std::shared_ptr<SpaceMap> space_map_;
  // space_entry_pool_ consists of three level vectors, the first level
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
  std::vector<std::vector<std::vector<SpaceEntry>>> space_entry_pool_;
  std::set<SizedSpaceEntry, SpaceCmp> large_entries_;
  std::vector<ThreadCache> thread_cache_;
  SpinMutex large_entries_spin_;
  std::vector<SpinMutex> spins_;
};

} // namespace KVDK_NAMESPACE
