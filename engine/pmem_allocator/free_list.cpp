/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "free_list.hpp"

#include "../thread_manager.hpp"
#include "pmem_allocator.hpp"

namespace KVDK_NAMESPACE {

// For balance free entries among threads, move a free list to the background
// pool if more than kMinMovableEntries in the free list
const uint32_t kMinMovableEntries = 1024;
// To avoid background space merge consumes resorces in idle time, do merge only
// if more than kMergeThreshold space entries been freed since last merge
const uint64_t kMergeThreshold = 1024 * 1024;
// Freqently merge a space with a far larger following one will cause high CPU
// overhead, so we merge a following space only if (size of the following space)
// not larger than (kMaxAjacentSpaceSizeInMerge * current space size)
const uint64_t kMaxAjacentSpaceSizeInMerge = 8;

void SpaceMap::Set(uint64_t offset, uint64_t length) {
  assert(offset < map_.size());
  if (length == 0) {
    return;
  }
  auto cur = offset;
  SpinMutex* last_lock = &map_spins_[cur / lock_granularity_];
  std::lock_guard<SpinMutex> lg(*last_lock);
  auto to_set = length > INT8_MAX ? INT8_MAX : length;
  kvdk_assert(map_[cur].Empty(), "");
  map_[cur] = Token(true, to_set);
  length -= to_set;
  if (length > 0) {
    std::unique_ptr<std::lock_guard<SpinMutex>> lg(nullptr);
    while (length > 0) {
      cur += to_set;
      assert(cur < map_.size());
      to_set = length > INT8_MAX ? INT8_MAX : length;
      length -= to_set;
      SpinMutex* next_lock = &map_spins_[cur / lock_granularity_];
      if (next_lock != last_lock) {
        lg.reset(new std::lock_guard<SpinMutex>(*next_lock));
        last_lock = next_lock;
      }
      kvdk_assert(map_[cur].Empty(), "");
      map_[cur] = Token(false, to_set);
    }
  }
  kvdk_assert(map_[cur + to_set].IsStart() || map_[cur + to_set].Empty(), "");
}

uint64_t SpaceMap::TestAndUnset(uint64_t offset, uint64_t length) {
  assert(offset < map_.size());
  if (length == 0) {
    return 0;
  }
  std::lock_guard<SpinMutex> start_lg(map_spins_[offset / lock_granularity_]);
  SpinMutex* last_lock = &map_spins_[offset / lock_granularity_];
  std::unique_ptr<std::lock_guard<SpinMutex>> lg(nullptr);
  uint64_t res = 0;
  if (map_[offset].IsStart()) {
    res += map_[offset].Size();
    map_[offset].Clear();

    while (res < length) {
      uint64_t cur = offset + res;
      kvdk_assert(cur <= map_.size(), "");
      SpinMutex* next_lock = &map_spins_[cur / lock_granularity_];
      if (next_lock != last_lock) {
        last_lock = next_lock;
        lg.reset(new std::lock_guard<SpinMutex>(*next_lock));
      }
      if (cur == map_.size() || map_[cur].Empty()) {
        break;
      }
      kvdk_assert(!map_[cur].IsStart(), "");
      res += map_[cur].Size();
      map_[cur].Clear();
    }
  }
  if (res > 0 && res != length) {
    GlobalLogger.Error("Unset size mismatch %lu and %lu\n", res, length);
    std::abort();
  }
  return res;
}

uint64_t SpaceMap::TryMerge(uint64_t start_offset, uint64_t start_size,
                            uint64_t limit_merge_size) {
  kvdk_assert(start_size > 0, "");
  assert(start_offset < map_.size());
  if (start_offset + start_size > map_.size()) {
    return 0;
  }
  uint64_t end_offset = std::min(start_offset + limit_merge_size, map_.size());
  SpinMutex* last_lock = &map_spins_[start_offset / lock_granularity_];
  std::lock_guard<SpinMutex> lg(*last_lock);
  if (map_[start_offset].Empty() || !map_[start_offset].IsStart()) {
    return 0;
  }

  std::vector<SpinMutex*> locked;
  std::vector<uint64_t> merged_offset;
#if KVDK_DEBUG_LEVEL > 0
  // check start size
  uint64_t debug_size = map_[start_offset].Size();
  uint64_t debug_cur = start_offset + debug_size;
  while (true) {
    SpinMutex* debug_lock = &map_spins_[debug_cur / lock_granularity_];
    if (debug_lock != last_lock) {
      last_lock = debug_lock;
      debug_lock->lock();
      locked.push_back(debug_lock);
    }
    if (map_[debug_cur].IsStart() || map_[debug_cur].Empty()) {
      break;
    } else {
      debug_size += map_[debug_cur].Size();
      debug_cur = start_offset + debug_size;
    }
  }
  if (debug_size != start_size) {
    GlobalLogger.Error("Size mismatch %lu and %lu\n", debug_size, start_size);
  }
  kvdk_assert(debug_size == start_size,
              "TryMerge: start space entry size error");
#endif  // KVDK_DEBUG_LEVEL > 0

  uint64_t merged_size = start_size;
  uint64_t in_merge_offset = start_offset + start_size;
  bool keep_merge = in_merge_offset < end_offset;
  while (keep_merge) {
    SpinMutex* in_merge_lock = &map_spins_[in_merge_offset / lock_granularity_];
    if (in_merge_lock != last_lock) {
      last_lock = in_merge_lock;
      in_merge_lock->lock();
      locked.push_back(in_merge_lock);
    }

    if (map_[in_merge_offset].Empty()) {
      break;
    } else {
      kvdk_assert(map_[in_merge_offset].IsStart(), "");
      uint64_t in_merge_size = map_[in_merge_offset].Size();
      uint64_t cur = in_merge_offset + in_merge_size;
      while (true) {
        if (map_[cur].IsStart() || map_[cur].Empty()) {
          merged_size += in_merge_size;
          merged_offset.push_back(in_merge_offset);
          in_merge_offset = cur;
          keep_merge = in_merge_offset < end_offset;
          break;
        } else if (in_merge_size > kMaxAjacentSpaceSizeInMerge *
                                       merged_size /* avoid merging a large
                                                      space into a small one */
                   || cur >= end_offset) {
          keep_merge = false;
          break;
        } else {
          in_merge_size += map_[cur].Size();
          cur += map_[cur].Size();
        }
      }
    }
  }

  for (uint64_t o : merged_offset) {
    map_[o].UnStart();
  }

  for (SpinMutex* l : locked) {
    l->unlock();
  }
  return merged_size;
}

void Freelist::OrganizeFreeSpace() {
  MoveCachedEntriesToPool();
  if (last_freed_after_merge_.load() > kMergeThreshold) {
    MergeSpaceInPool();
  }
}

void Freelist::MergeSpaceInPool() {
  last_freed_after_merge_.store(0);
  std::vector<PMemOffsetType> merging_list;
  std::vector<std::vector<PMemOffsetType>> merged_small_entry_offsets(
      max_small_entry_block_size_);
  std::vector<std::set<SpaceEntry, SpaceEntry::SpaceCmp>> merged_large_entries(
      max_block_size_index_);

  for (uint32_t b_size = 1; b_size < max_small_entry_block_size_; b_size++) {
    while (active_pool_.TryFetchEntryList(merging_list, b_size)) {
      for (PMemOffsetType& offset : merging_list) {
        assert(offset % block_size_ == 0);
        auto b_offset = offset / block_size_;
        uint64_t merged_blocks_ =
            MergeSpace(b_offset, b_size,
                       num_segment_blocks_ - b_offset % num_segment_blocks_);

        if (merged_blocks_ > 0) {
          // Persist merged free entry on PMem
          if (merged_blocks_ > b_size) {
            pmem_allocator_->persistSpaceEntry(offset,
                                               merged_blocks_ * block_size_);
          }

          // large space entries
          if (merged_blocks_ >= merged_small_entry_offsets.size()) {
            size_t size_index = blockSizeIndex(merged_blocks_);
            merged_large_entries[size_index].emplace(
                offset, merged_blocks_ * block_size_);
            if (merged_large_entries[size_index].size() >= kMinMovableEntries) {
              // move merged entries to merging pool to avoid redundant
              // merging
              merged_pool_.MoveLargeEntrySet(merged_large_entries[size_index],
                                             size_index);
            }
          } else {
            merged_small_entry_offsets[merged_blocks_].emplace_back(offset);
            if (merged_small_entry_offsets[merged_blocks_].size() >=
                kMinMovableEntries) {
              // move merged entries to merging pool to avoid redundant
              // merging
              merged_pool_.MoveEntryList(
                  merged_small_entry_offsets[merged_blocks_], merged_blocks_);
            }
          }
        }
      }
    }
  }

  for (uint32_t b_size = 1; b_size < merged_small_entry_offsets.size();
       b_size++) {
    if (merged_small_entry_offsets[b_size].size() > 0) {
      active_pool_.MoveEntryList(merged_small_entry_offsets[b_size], b_size);
    }
    while (merged_pool_.TryFetchEntryList(merged_small_entry_offsets[b_size],
                                          b_size)) {
      active_pool_.MoveEntryList(merged_small_entry_offsets[b_size], b_size);
    }
  }

  for (size_t size_index = 0; size_index < merged_large_entries.size();
       size_index++) {
    if (merged_large_entries[size_index].size() > 0) {
      active_pool_.MoveLargeEntrySet(merged_large_entries[size_index],
                                     size_index);
    }
    while (merged_pool_.TryFetchLargeEntrySet(merged_large_entries[size_index],
                                              size_index)) {
      active_pool_.MoveLargeEntrySet(merged_large_entries[size_index],
                                     size_index);
    }
  }
}

void Freelist::Push(const SpaceEntry& entry) {
  assert(entry.size > 0);
  assert(entry.size % block_size_ == 0);
  assert(entry.offset % block_size_ == 0);
  auto b_size = entry.size / block_size_;
  auto b_offset = entry.offset / block_size_;
  space_map_.Set(b_offset, b_size);

  auto& flist_thread_cache = flist_thread_cache_[access_thread.id];
  if (b_size >= flist_thread_cache.small_entry_offsets.size()) {
    size_t size_index = blockSizeIndex(b_size);
    std::lock_guard<SpinMutex> lg(
        flist_thread_cache.large_entry_spins[size_index]);
    flist_thread_cache.large_entries[size_index].insert(entry);
  } else {
    std::lock_guard<SpinMutex> lg(flist_thread_cache.small_entry_spins[b_size]);
    flist_thread_cache.small_entry_offsets[b_size].emplace_back(entry.offset);
  }
  flist_thread_cache.recently_freed.fetch_add(1, std::memory_order_relaxed);
}

uint64_t Freelist::BatchPush(const std::vector<SpaceEntry>& entries) {
  uint64_t pushed_size = 0;
  std::vector<std::vector<PMemOffsetType>> moving_small_entry_list(
      max_small_entry_block_size_);
  std::vector<std::set<SpaceEntry, SpaceEntry::SpaceCmp>>
      moving_large_entry_set(max_block_size_index_);
  for (const SpaceEntry& entry : entries) {
    kvdk_assert(entry.size % block_size_ == 0,
                "batch freed entry size is not aligned to block size");
    uint32_t b_size = entry.size / block_size_;
    uint64_t b_offset = entry.offset / block_size_;
    space_map_.Set(b_offset, b_size);
    if (b_size < max_small_entry_block_size_) {
      moving_small_entry_list[b_size].emplace_back(entry.offset);
      if (moving_small_entry_list[b_size].size() == kMinMovableEntries) {
        active_pool_.MoveEntryList(moving_small_entry_list[b_size], b_size);
      }
    } else {
      size_t size_index = blockSizeIndex(b_size);
      moving_large_entry_set[size_index].insert(entry);
      if (moving_large_entry_set[size_index].size() == kMinMovableEntries) {
        active_pool_.MoveLargeEntrySet(moving_large_entry_set[size_index],
                                       size_index);
      }
    }
    pushed_size += entry.size;
  }

  for (uint32_t b_size = 1; b_size < moving_small_entry_list.size(); b_size++) {
    if (moving_small_entry_list[b_size].size() > 0) {
      active_pool_.MoveEntryList(moving_small_entry_list[b_size], b_size);
    }
  }
  for (uint32_t size_index = 0; size_index < moving_large_entry_set.size();
       size_index++) {
    if (moving_large_entry_set[size_index].size() > 0) {
      active_pool_.MoveLargeEntrySet(moving_large_entry_set[size_index],
                                     size_index);
    }
  }
  last_freed_after_merge_.fetch_add(entries.size(), std::memory_order_relaxed);
  return pushed_size;
}

bool Freelist::Get(uint32_t size, SpaceEntry* space_entry) {
  assert(size % block_size_ == 0);
  if (getSmallEntry(size, space_entry)) {
    return true;
  }
  return getLargeEntry(size, space_entry);
}

void Freelist::MoveCachedEntriesToPool() {
  std::vector<PMemOffsetType> moving_small_entry_list;
  std::set<SpaceEntry, SpaceEntry::SpaceCmp> moving_large_entry_set;
  for (uint64_t i = 0; i < flist_thread_cache_.size(); i++) {
    auto& tc = flist_thread_cache_[i];
    last_freed_after_merge_.fetch_add(
        tc.recently_freed.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    tc.recently_freed.store(0, std::memory_order_relaxed);

    for (size_t b_size = 1; b_size < tc.small_entry_offsets.size(); b_size++) {
      moving_small_entry_list.clear();
      {
        std::lock_guard<SpinMutex> lg(tc.small_entry_spins[b_size]);
        if (tc.small_entry_offsets[b_size].size() > 0) {
          moving_small_entry_list.swap(tc.small_entry_offsets[b_size]);
        }
      }

      active_pool_.MoveEntryList(moving_small_entry_list, b_size);
    }

    for (size_t size_index = 0; size_index < tc.large_entries.size();
         size_index++) {
      moving_large_entry_set.clear();
      {
        std::lock_guard<SpinMutex> lg(tc.large_entry_spins[size_index]);
        if (tc.large_entries[size_index].size() > 0) {
          moving_large_entry_set.swap(tc.large_entries[size_index]);
        }
      }

      active_pool_.MoveLargeEntrySet(moving_large_entry_set, size_index);
    }
  }
}

bool Freelist::getSmallEntry(uint32_t size, SpaceEntry* space_entry) {
  auto b_size = size / block_size_;
  auto& flist_thread_cache = flist_thread_cache_[access_thread.id];
  for (uint32_t i = b_size; i < flist_thread_cache.small_entry_offsets.size();
       i++) {
  search_entry:
    std::lock_guard<SpinMutex> lg(flist_thread_cache.small_entry_spins[i]);
    if (flist_thread_cache.small_entry_offsets[i].size() == 0 &&
        !active_pool_.TryFetchEntryList(
            flist_thread_cache.small_entry_offsets[i], i) &&
        !merged_pool_.TryFetchEntryList(
            flist_thread_cache.small_entry_offsets[i], i)) {
      // no usable b_size free space entry
      continue;
    }

    if (flist_thread_cache.small_entry_offsets[i].size() > 0) {
      space_entry->offset = flist_thread_cache.small_entry_offsets[i].back();
      flist_thread_cache.small_entry_offsets[i].pop_back();
      assert(space_entry->offset % block_size_ == 0);
      auto b_offset = space_entry->offset / block_size_;
      if (space_map_.TestAndUnset(b_offset, i) == i) {
        space_entry->size = i * block_size_;
        return true;
      }
      goto search_entry;
    }
  }
  return false;
}

bool Freelist::getLargeEntry(uint32_t size, SpaceEntry* space_entry) {
  auto b_size = size / block_size_;
  auto& flist_thread_cache = flist_thread_cache_[access_thread.id];

  auto size_index = blockSizeIndex(b_size);
  for (size_t i = size_index; i < flist_thread_cache.large_entries.size();
       i++) {
    std::lock_guard<SpinMutex> lg(flist_thread_cache.large_entry_spins[i]);
    if (flist_thread_cache.large_entries[i].size() == 0) {
      if (!active_pool_.TryFetchLargeEntrySet(
              flist_thread_cache.large_entries[i], i) &&
          !merged_pool_.TryFetchLargeEntrySet(
              flist_thread_cache.large_entries[i], i)) {
        // no suitable free space entry in this size
        continue;
      }
    }

    auto iter = flist_thread_cache.large_entries[i].begin();
    while (iter != flist_thread_cache.large_entries[i].end()) {
      if (iter->size < size) {
        break;
      }
      auto b_offset = iter->offset / block_size_;
      auto b_size = iter->size / block_size_;
      if (space_map_.TestAndUnset(b_offset, b_size) == b_size) {
        *space_entry = *iter;
        iter = flist_thread_cache.large_entries[i].erase(iter);
        return true;
      } else {
        iter = flist_thread_cache.large_entries[i].erase(iter);
      }
    }
  }
  return false;
}

}  // namespace KVDK_NAMESPACE