/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "free_list.hpp"
#include "../thread_manager.hpp"

namespace KVDK_NAMESPACE {

const uint32_t kMaxCacheEntries = 16;

void SpaceMap::Set(uint64_t offset, uint64_t length) {
  auto cur = offset;
  SpinMutex *last_lock = &spins_[cur / block_size_];
  std::lock_guard<SpinMutex> lg(*last_lock);
  auto to_set = length > INT8_MAX ? INT8_MAX : length;
  map_[cur] = MapToken(true, to_set);
  length -= to_set;
  if (length > 0) {
    std::unique_ptr<std::lock_guard<SpinMutex>> lg(nullptr);
    while (length > 0) {
      cur += to_set;
      assert(cur < map_.size());
      to_set = length > INT8_MAX ? INT8_MAX : length;
      length -= to_set;
      SpinMutex *next_lock = &spins_[cur / block_size_];
      if (next_lock != last_lock) {
        lg.reset(new std::lock_guard<SpinMutex>(*next_lock));
        last_lock = next_lock;
      }
      map_[cur] = MapToken(false, to_set);
    }
  }
}

uint64_t SpaceMap::TestAndUnset(uint64_t offset, uint64_t length) {
  uint64_t res = 0;
  uint64_t cur = offset;
  std::lock_guard<SpinMutex> start_lg(spins_[cur / block_size_]);
  SpinMutex *last_lock = &spins_[cur / block_size_];
  std::unique_ptr<std::lock_guard<SpinMutex>> lg(nullptr);
  while (map_[offset].IsStart()) {
    if (cur >= map_.size() || map_[cur].Empty()) {
      break;
    } else {
      res += map_[cur].Size();
      map_[cur].Clear();
      cur = offset + res;
    }
    if (res < length) {
      SpinMutex *next_lock = &spins_[cur / block_size_];
      if (next_lock != last_lock) {
        last_lock = next_lock;
        lg.reset(new std::lock_guard<SpinMutex>(*next_lock));
      }
    } else {
      break;
    }
  }
  return res;
}

uint64_t SpaceMap::FindAndUnset(uint64_t &start_offset, uint64_t max_length,
                                uint64_t target_length) {
  uint64_t cur = start_offset;
  uint64_t end_offset = start_offset + max_length;
  SpinMutex *start_lock = &spins_[cur];
  std::unique_ptr<std::lock_guard<SpinMutex>> lg(
      new std::lock_guard<SpinMutex>(*start_lock));
  while (cur < map_.size()) {
    if (&spins_[cur / block_size_] != start_lock) {
      start_lock = &spins_[cur / block_size_];
      lg.reset(new std::lock_guard<SpinMutex>(*start_lock));
    }
    SpinMutex *last_lock = start_lock;
    start_offset = cur;
    bool done = false;
    uint64_t length = 0;
    std::vector<uint64_t> merged_offset;
    std::vector<SpinMutex *> locked;
    while (1) {
      if (cur >= end_offset || (end_offset - cur + length < target_length) ||
          map_[cur].Empty()) {
        if (length >= target_length) {
          for (auto o : merged_offset) {
            map_[o].Clear();
          }
          done = true;
        }
        for (auto s : locked) {
          s->unlock();
        }
        break;
      }
      length += map_[cur].Size();
      merged_offset.push_back(cur);
      cur = start_offset + length;

      SpinMutex *next_lock = &spins_[cur / block_size_];
      if (next_lock != last_lock) {
        next_lock->lock();
        locked.push_back(next_lock);
        last_lock = next_lock;
      }
    }
    if (done) {
      return length;
    } else {
      cur++;
    }
  }
  return 0;
}

uint64_t SpaceMap::MergeAndUnset(uint64_t offset, uint64_t max_length,
                                 uint64_t target_length) {
  uint64_t cur = offset;
  uint64_t end_offset = offset + max_length;
  SpinMutex *last_lock = &spins_[cur / block_size_];
  uint64_t res = 0;
  std::lock_guard<SpinMutex> lg(*last_lock);
  std::vector<SpinMutex *> locked;
  std::vector<uint64_t> merged_offset;
  while (cur < end_offset) {
    if (map_[cur].Empty()) {
      break;
    } else {
      res += map_[cur].Size();
      merged_offset.push_back(cur);
      cur = offset + res;
    }

    SpinMutex *next_lock = &spins_[cur / block_size_];
    if (next_lock != last_lock) {
      last_lock = next_lock;
      next_lock->lock();
      locked.push_back(next_lock);
    }
  }
  if (res >= target_length) {
    for (uint64_t o : merged_offset) {
      map_[o].Clear();
    }
  } else {
    res = 0;
  }
  for (SpinMutex *l : locked) {
    l->unlock();
  }
  return res;
}

void FreeList::Push(const SizedSpaceEntry &entry) {
  space_map_->Set(entry.space_entry.offset, entry.size);
  auto &thread_cache = thread_cache_[write_thread.id];
  if (entry.size >= thread_cache.active_entries.size()) {
    std::lock_guard<SpinMutex> lg(large_entries_spin_);
    large_entries_.insert(entry);
  } else {
    if (thread_cache.active_entries[entry.size].size() >= kMaxCacheEntries) {
      std::lock_guard<SpinMutex> lg(thread_cache.spins[entry.size]);
      thread_cache.backup_entries[entry.size].emplace_back(entry.space_entry);
      if (thread_cache.backup_entries[entry.size].size() >= kMaxCacheEntries) {
        std::lock_guard<SpinMutex> lg(spins_[entry.size]);
        space_entry_pool_[entry.size].emplace_back();
        space_entry_pool_[entry.size].back().swap(
            thread_cache.backup_entries[entry.size]);
      }
    } else {
      thread_cache.active_entries[entry.size].emplace_back(entry.space_entry);
    }
  }
}

bool FreeList::Get(uint32_t b_size, SizedSpaceEntry *space_entry) {
  auto &thread_cache = thread_cache_[write_thread.id];
  for (uint32_t i = b_size; i < thread_cache.active_entries.size(); i++) {
    if (thread_cache.active_entries[i].size() == 0) {
      if (thread_cache.backup_entries[i].size() != 0) {
        std::lock_guard<SpinMutex> lg(thread_cache.spins[i]);
        thread_cache.active_entries[i].swap(thread_cache.backup_entries[i]);
      } else if (space_entry_pool_[i].size() != 0) {
        std::lock_guard<SpinMutex> lg(spins_[i]);
        if (space_entry_pool_[i].size() != 0) {
          thread_cache.active_entries[i].swap(space_entry_pool_[i].back());
          space_entry_pool_[i].pop_back();
        }
      }
    }

    if (thread_cache.active_entries[i].size() != 0) {
      space_entry->space_entry = thread_cache.active_entries[i].back();
      thread_cache.active_entries[i].pop_back();
      if (space_map_->TestAndUnset(space_entry->space_entry.offset, i) == i) {
        space_entry->size = i;
        return true;
      }
    }
  }

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
  return false;
}

bool FreeList::MergeGet(uint32_t b_size, uint64_t segment_blocks,
                        SizedSpaceEntry *space_entry) {
  auto &cache_list = thread_cache_[write_thread.id].active_entries;
  for (uint32_t i = 1; i < FREE_LIST_MAX_BLOCK; i++) {
    auto iter = cache_list[i].begin();
    while (iter != cache_list[i].end()) {
      uint64_t size = MergeSpace(
          *iter, segment_blocks - iter->offset % segment_blocks, b_size);
      if (size >= b_size) {
        space_entry->space_entry = *iter;
        space_entry->size = size;
        cache_list[i].erase(iter);
        return true;
      }
      iter++;
    }
  }
  return false;
}

} // namespace KVDK_NAMESPACE