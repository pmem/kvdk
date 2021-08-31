/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "free_list.hpp"
#include "../thread_manager.hpp"
#include "pmem_allocator.hpp"
#include <libpmem.h>

namespace KVDK_NAMESPACE {

const uint32_t kMaxCacheEntries = 16;
const uint32_t kMinMovableEntries = 8;

void SpaceMap::Set(uint64_t offset, uint64_t length) {
  auto cur = offset;
  SpinMutex *last_lock = &map_spins_[cur / lock_granularity_];
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
      SpinMutex *next_lock = &map_spins_[cur / lock_granularity_];
      if (next_lock != last_lock) {
        lg.reset(new std::lock_guard<SpinMutex>(*next_lock));
        last_lock = next_lock;
      }
      map_[cur] = Token(false, to_set);
    }
  }
}

uint64_t SpaceMap::TestAndUnset(uint64_t offset, uint64_t length) {
  uint64_t res = 0;
  uint64_t cur = offset;
  std::lock_guard<SpinMutex> start_lg(map_spins_[cur / lock_granularity_]);
  SpinMutex *last_lock = &map_spins_[cur / lock_granularity_];
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
        SpinMutex *next_lock = &map_spins_[cur / lock_granularity_];
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
  uint64_t cur = offset;
  uint64_t end_offset = offset + max_merge_length;
  SpinMutex *last_lock = &map_spins_[cur / lock_granularity_];
  uint64_t merged = 0;
  std::lock_guard<SpinMutex> lg(*last_lock);
  if (map_[cur].Empty() || !map_[cur].IsStart()) {
    return merged;
  }
  std::vector<SpinMutex *> locked;
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

    SpinMutex *next_lock = &map_spins_[cur / lock_granularity_];
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
  for (SpinMutex *l : locked) {
    l->unlock();
  }
  return merged;
}

void Freelist::HandleDelayedFreeEntries() {
  std::vector<SizedSpaceEntry> unfreed_entries;
  std::vector<std::vector<SpaceEntry>> merged_entry_list(
      max_classified_b_size_);
  for (auto &list : delayed_free_entries_) {
    for (auto &&entry : list) {
      if (entry.space_entry.info < min_timestamp_of_entries_) {
        if (entry.size < max_classified_b_size_) {
          merged_entry_list[entry.size].emplace_back(entry.space_entry);
          if (merged_entry_list[entry.size].size() >= kMinMovableEntries) {
            merged_pool_.MoveEntryList(merged_entry_list[entry.size],
                                       entry.size);
          }
        } else {
          std::lock_guard<SpinMutex> lg(large_entries_spin_);
          large_entries_.insert(entry);
        }
      } else {
        unfreed_entries.emplace_back(entry);
      }
    }
  }

  for (uint32_t b_size = 1; b_size < max_classified_b_size_; b_size++) {
    if (merged_entry_list[b_size].size() > 0) {
      active_pool_.MoveEntryList(merged_entry_list[b_size], b_size);
    }
  }

  delayed_free_entries_.clear();
  delayed_free_entries_.emplace_back(std::move(unfreed_entries));
}

void Freelist::OrganizeFreeSpace() {
  // Notice: we should move cached list to pool after merge entries in the pool,
  // otherwise we may miss some entries during minimal timestamp checking
  MergeFreeSpaceInPool();
  MoveCachedListsToPool();
  HandleDelayedFreeEntries();
}

void Freelist::MergeFreeSpaceInPool() {
  uint64_t min_timestamp = UINT64_MAX;
  std::vector<SpaceEntry> merging_list;
  std::vector<std::vector<SpaceEntry>> merged_entry_list(
      max_classified_b_size_);

  for (uint32_t b_size = 1; b_size < max_classified_b_size_; b_size++) {
    if (active_pool_.TryFetchEntryList(merging_list, b_size)) {
      for (SpaceEntry &se : merging_list) {
        uint64_t merged_size = MergeSpace(
            se, num_segment_blocks_ - se.offset % num_segment_blocks_, b_size);

        if (merged_size > 0) {
          // Persist merged free entry on PMem
          if (merged_size > b_size) {
            DataHeader header(0, merged_size);
            pmem_memcpy_persist(pmem_allocator_->offset2addr(se.offset),
                                &header, sizeof(DataHeader));
            // As we marked new size on PMem, it contains no valid data so we
            // can set it's ts to 0
            se.info = 0;
          }

          if (se.info > 0 && min_timestamp > se.info) {
            min_timestamp = se.info;
          }

          // large space entries
          if (merged_size >= merged_entry_list.size()) {
            std::lock_guard<SpinMutex> lg(large_entries_spin_);
            large_entries_.insert(
                SizedSpaceEntry(se.offset, merged_size, se.info));
            // move merged entries to merging pool to avoid redundant merging
          } else {
            merged_entry_list[merged_size].emplace_back(std::move(se));
            if (merged_entry_list[merged_size].size() >= kMinMovableEntries) {
              merged_pool_.MoveEntryList(merged_entry_list[merged_size],
                                         merged_size);
            }
          }
        }
      }
    }
  }

  std::vector<SpaceEntry> merged_list;
  for (uint32_t b_size = 1; b_size < max_classified_b_size_; b_size++) {
    while (merged_pool_.TryFetchEntryList(merged_list, b_size)) {
      active_pool_.MoveEntryList(merged_list, b_size);
    }

    if (merged_entry_list[b_size].size() > 0) {
      active_pool_.MoveEntryList(merged_entry_list[b_size], b_size);
    }
  }

  min_timestamp_of_entries_ = min_timestamp;
}

void Freelist::DelayPush(const SizedSpaceEntry &entry) {
  auto &thread_cache = thread_cache_[write_thread.id];
  std::lock_guard<SpinMutex> lg(thread_cache.spins.back());
  thread_cache.delayed_free_entries.emplace_back(entry);
}

void Freelist::Push(const SizedSpaceEntry &entry) {
  space_map_->Set(entry.space_entry.offset, entry.size);
  auto &thread_cache = thread_cache_[write_thread.id];
  if (entry.size >= thread_cache.active_entries.size()) {
    std::lock_guard<SpinMutex> lg(large_entries_spin_);
    large_entries_.insert(entry);
  } else {
    std::lock_guard<SpinMutex> lg(thread_cache.spins[entry.size]);
    thread_cache.active_entries[entry.size].emplace_back(entry.space_entry);
  }
}

bool Freelist::Get(uint32_t b_size, SizedSpaceEntry *space_entry) {
  auto &thread_cache = thread_cache_[write_thread.id];
  for (uint32_t i = b_size; i < thread_cache.active_entries.size(); i++) {
    bool found = false;
    {
      std::lock_guard<SpinMutex> lg(thread_cache.spins[i]);
      if (thread_cache.active_entries[i].size() == 0) {
        if (!active_pool_.TryFetchEntryList(thread_cache.active_entries[i],
                                            i) &&
            !merged_pool_.TryFetchEntryList(thread_cache.active_entries[i],
                                            i)) {
          // no usable b_size free space entry
          continue;
        }
      }

      if (thread_cache.active_entries[i].size() != 0) {
        space_entry->space_entry = thread_cache.active_entries[i].back();
        thread_cache.active_entries[i].pop_back();
        found = true;
      }
    }

    if (found &&
        space_map_->TestAndUnset(space_entry->space_entry.offset, i) == i) {
      space_entry->size = i;
      return true;
    }
  }

  if (!large_entries_.empty()) {
    std::lock_guard<SpinMutex> lg(large_entries_spin_);
    while (!large_entries_.empty()) {
      auto space = large_entries_.begin();
      if (space->size >= b_size) {
        auto size = space->size;
        space_entry->space_entry = space->space_entry;
        large_entries_.erase(space);
        if (space_map_->TestAndUnset(space_entry->space_entry.offset, size) ==
            size) {
          space_entry->size = size;
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
  std::vector<SpaceEntry> moving_list;
  uint64_t min_ts = min_timestamp_of_entries_;
  for (auto &tc : thread_cache_) {
    for (size_t i = 1; i < tc.active_entries.size(); i++) {
      moving_list.clear();
      {
        std::lock_guard<SpinMutex> lg(tc.spins[i]);
        if (tc.active_entries[i].size() > 0) {
          moving_list.swap(tc.active_entries[i]);
        }
      }

      if (moving_list.size() > 0) {
        for (auto &se : moving_list) {
          if (se.info < min_ts) {
            min_ts = se.info;
          }
        }

        active_pool_.MoveEntryList(moving_list, i);
      }
    }

    std::lock_guard<SpinMutex> lg(
        tc.spins.back() /* delayed free entries lock*/);
    delayed_free_entries_.emplace_back(std::move(tc.delayed_free_entries));
    tc.delayed_free_entries.clear();
  }
  min_timestamp_of_entries_ = min_ts;
}

bool Freelist::MergeGet(uint32_t b_size, SizedSpaceEntry *space_entry) {
  auto &cache_list = thread_cache_[write_thread.id].active_entries;
  for (uint32_t i = 1; i < max_classified_b_size_; i++) {
    size_t j = 0;
    while (j < cache_list[i].size()) {
      uint64_t size = MergeSpace(cache_list[i][j],
                                 num_segment_blocks_ - cache_list[i][j].offset %
                                                           num_segment_blocks_,
                                 b_size);
      if (size >= b_size) {
        space_entry->space_entry = cache_list[i][j];
        space_entry->size = size;
        std::swap(cache_list[i][j], cache_list[i].back());
        cache_list[i].pop_back();
        if (space_map_->TestAndUnset(space_entry->space_entry.offset, size) ==
            size) {
          return true;
        }
      } else {
        j++;
      }
    }
  }
  return false;
}

} // namespace KVDK_NAMESPACE