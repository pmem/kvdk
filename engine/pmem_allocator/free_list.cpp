/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "free_list.hpp"

#include "../thread_manager.hpp"
#include "pmem_allocator.hpp"

namespace KVDK_NAMESPACE {

const uint32_t kMinMovableEntries = 8;

void SpaceMap::Set(uint64_t offset, uint64_t length) {
  assert(offset < map_.size());
  if (length == 0) {
    return;
  }
  auto cur = offset;
  SpinMutex* last_lock = &map_spins_[cur / lock_granularity_];
  std::lock_guard<SpinMutex> lg(*last_lock);
  auto to_set = length > INT8_MAX ? INT8_MAX : length;
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
      map_[cur] = Token(false, to_set);
    }
  }
}

uint64_t SpaceMap::TestAndUnset(uint64_t offset, uint64_t length) {
  assert(offset < map_.size());
  if (length == 0) {
    return 0;
  }
  uint64_t res = 0;
  uint64_t cur = offset;
  std::lock_guard<SpinMutex> start_lg(map_spins_[cur / lock_granularity_]);
  SpinMutex* last_lock = &map_spins_[cur / lock_granularity_];
  std::unique_ptr<std::lock_guard<SpinMutex>> lg(nullptr);
  if (map_[offset].IsStart()) {
    while (1) {
      if (cur >= map_.size() || map_[cur].Empty()) {
        break;
      } else {
        res += map_[cur].Size();
        map_[cur].Clear();
        cur = offset + res;
      }
      if (res < length) {
        SpinMutex* next_lock = &map_spins_[cur / lock_granularity_];
        if (next_lock != last_lock) {
          last_lock = next_lock;
          lg.reset(new std::lock_guard<SpinMutex>(*next_lock));
        }
      } else {
        break;
      }
    }
  }
  return res;
}

uint64_t SpaceMap::TryMerge(uint64_t offset, uint64_t max_merge_length,
                            uint64_t min_merge_length) {
  assert(offset < map_.size());
  if (offset + min_merge_length > map_.size()) {
    return 0;
  }
  uint64_t cur = offset;
  uint64_t end_offset = std::min(offset + max_merge_length, map_.size());
  SpinMutex* last_lock = &map_spins_[cur / lock_granularity_];
  uint64_t merged = 0;
  std::lock_guard<SpinMutex> lg(*last_lock);
  if (map_[cur].Empty() || !map_[cur].IsStart()) {
    return merged;
  }
  std::vector<SpinMutex*> locked;
  std::vector<uint64_t> merged_offset;
  while (cur < end_offset) {
    if (map_[cur].Empty()) {
      break;
    } else {
      merged += map_[cur].Size();
      if (cur != offset && map_[cur].IsStart()) {
        merged_offset.push_back(cur);
      }
      cur = offset + merged;
    }

    SpinMutex* next_lock = &map_spins_[cur / lock_granularity_];
    if (next_lock != last_lock) {
      last_lock = next_lock;
      next_lock->lock();
      locked.push_back(next_lock);
    }
  }
  if (merged >= min_merge_length) {
    for (uint64_t o : merged_offset) {
      map_[o].UnStart();
    }
  } else {
    merged = 0;
  }
  for (SpinMutex* l : locked) {
    l->unlock();
  }
  return merged;
}

void Freelist::OrganizeFreeSpace() {
  MoveCachedListsToPool();
  MergeSpaceInPool();
}

void Freelist::MergeSpaceInPool() {
  std::vector<PMemOffsetType> merging_list;
  std::vector<std::vector<PMemOffsetType>> merged_entry_list(
      max_classified_b_size_);

  for (uint32_t b_size = 1; b_size < max_classified_b_size_; b_size++) {
    while (active_pool_.TryFetchEntryList(merging_list, b_size)) {
      for (PMemOffsetType& offset : merging_list) {
        assert(offset % block_size_ == 0);
        auto b_offset = offset / block_size_;
        uint64_t merged_blocks = MergeSpace(
            b_offset, num_segment_blocks_ - b_offset % num_segment_blocks_,
            b_size);

        if (merged_blocks > 0) {
          // Persist merged free entry on PMem
          if (merged_blocks > b_size) {
            pmem_allocator_->persistSpaceEntry(offset,
                                               merged_blocks * block_size_);
          }

          // large space entries
          if (merged_blocks >= merged_entry_list.size()) {
            std::lock_guard<SpinMutex> lg(large_entries_spin_);
            large_entries_.emplace(offset, merged_blocks * block_size_);
            // move merged entries to merging pool to avoid redundant merging
          } else {
            merged_entry_list[merged_blocks].emplace_back(offset);
            if (merged_entry_list[merged_blocks].size() >= kMinMovableEntries) {
              merged_pool_.MoveEntryList(merged_entry_list[merged_blocks],
                                         merged_blocks);
            }
          }
        }
      }
    }
  }

  std::vector<PMemOffsetType> merged_list;
  for (uint32_t b_size = 1; b_size < max_classified_b_size_; b_size++) {
    while (merged_pool_.TryFetchEntryList(merged_list, b_size)) {
      active_pool_.MoveEntryList(merged_list, b_size);
    }

    if (merged_entry_list[b_size].size() > 0) {
      active_pool_.MoveEntryList(merged_entry_list[b_size], b_size);
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
  if (b_size >= flist_thread_cache.active_entry_offsets.size()) {
    std::lock_guard<SpinMutex> lg(large_entries_spin_);
    large_entries_.emplace(entry);
  } else {
    std::lock_guard<SpinMutex> lg(flist_thread_cache.spins[b_size]);
    flist_thread_cache.active_entry_offsets[b_size].emplace_back(entry.offset);
  }
}

uint64_t Freelist::BatchPush(const std::vector<SpaceEntry>& entries) {
  uint64_t pushed_size = 0;
  Array<std::vector<PMemOffsetType>> moving_list(max_classified_b_size_);
  for (const SpaceEntry& entry : entries) {
    kvdk_assert(entry.size % block_size_ == 0,
                "batch freed entry size is not aligned to block size");
    uint32_t b_size = entry.size / block_size_;
    uint64_t b_offset = entry.offset / block_size_;
    space_map_.Set(b_offset, b_size);
    if (b_size < max_classified_b_size_) {
      moving_list[b_size].emplace_back(entry.offset);
      if (moving_list[b_size].size() == kMinMovableEntries) {
        active_pool_.MoveEntryList(moving_list[b_size], b_size);
      }
    } else {
      std::lock_guard<SpinMutex> lg(large_entries_spin_);
      large_entries_.emplace(entry);
    }
    pushed_size += entry.size;
  }

  for (uint32_t b_size = 1; b_size < moving_list.size(); b_size++) {
    if (moving_list[b_size].size() > 0) {
      active_pool_.MoveEntryList(moving_list[b_size], b_size);
    }
  }
  return pushed_size;
}

bool Freelist::Get(uint32_t size, SpaceEntry* space_entry) {
  assert(size % block_size_ == 0);
  auto b_size = size / block_size_;
  auto& flist_thread_cache = flist_thread_cache_[access_thread.id];
  for (uint32_t i = b_size; i < flist_thread_cache.active_entry_offsets.size();
       i++) {
    bool found = false;
  search_entry : {
    std::lock_guard<SpinMutex> lg(flist_thread_cache.spins[i]);
    if (flist_thread_cache.active_entry_offsets[i].size() == 0) {
      if (!active_pool_.TryFetchEntryList(
              flist_thread_cache.active_entry_offsets[i], i) &&
          !merged_pool_.TryFetchEntryList(
              flist_thread_cache.active_entry_offsets[i], i)) {
        // no usable b_size free space entry
        continue;
      }
    }

    if (flist_thread_cache.active_entry_offsets[i].size() != 0) {
      space_entry->offset = flist_thread_cache.active_entry_offsets[i].back();
      flist_thread_cache.active_entry_offsets[i].pop_back();
      found = true;
    }
  }

    if (found) {
      assert(space_entry->offset % block_size_ == 0);
      auto b_offset = space_entry->offset / block_size_;
      if (space_map_.TestAndUnset(b_offset, i) == i) {
        space_entry->size = i * block_size_;
        return true;
      }
      goto search_entry;
    }
  }

  if (!large_entries_.empty()) {
    std::lock_guard<SpinMutex> lg(large_entries_spin_);
    while (!large_entries_.empty()) {
      auto large_entry = large_entries_.begin();
      auto entry_size = large_entry->size;
      auto entry_offset = large_entry->offset;
      assert(entry_size % block_size_ == 0);
      assert(entry_offset % block_size_ == 0);
      auto entry_b_size = entry_size / block_size_;
      auto entry_b_offset = entry_offset / block_size_;
      if (entry_b_size >= b_size) {
        space_entry->offset = large_entry->offset;
        large_entries_.erase(large_entry);
        if (space_map_.TestAndUnset(entry_b_offset, entry_b_size) ==
            entry_b_size) {
          space_entry->size = entry_size;
          return true;
        }
      } else {
        break;
      }
    }
  }
  return false;
}

void Freelist::MoveCachedListsToPool() {
  std::vector<PMemOffsetType> moving_list;
  for (uint64_t i = 0; i < flist_thread_cache_.size(); i++) {
    auto& tc = flist_thread_cache_[i];

    for (size_t b_size = 1; b_size < tc.active_entry_offsets.size(); b_size++) {
      moving_list.clear();
      {
        std::lock_guard<SpinMutex> lg(tc.spins[b_size]);
        if (tc.active_entry_offsets[b_size].size() > 0) {
          moving_list.swap(tc.active_entry_offsets[b_size]);
        }
      }

      if (moving_list.size() > 0) {
        active_pool_.MoveEntryList(moving_list, b_size);
      }
    }
  }
}

bool Freelist::MergeGet(uint32_t size, SpaceEntry* space_entry) {
  assert(size % block_size_ == 0);
  auto b_size = size / block_size_;
  auto& cache_list = flist_thread_cache_[access_thread.id].active_entry_offsets;
  for (uint32_t i = 1; i < max_classified_b_size_; i++) {
    size_t j = 0;
    while (j < cache_list[i].size()) {
      assert(cache_list[i][j] % block_size_ == 0);
      auto b_offset = cache_list[i][j] / block_size_;
      uint64_t merged_blocks = MergeSpace(
          b_offset, num_segment_blocks_ - b_offset % num_segment_blocks_,
          b_size);
      if (merged_blocks >= b_size) {
        space_entry->offset = cache_list[i][j];
        std::swap(cache_list[i][j], cache_list[i].back());
        cache_list[i].pop_back();
        if (space_map_.TestAndUnset(b_offset, merged_blocks) == merged_blocks) {
          space_entry->size = merged_blocks * block_size_;
          return true;
        }
      } else {
        j++;
      }
    }
  }
  return false;
}

}  // namespace KVDK_NAMESPACE