/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <thread>

#include "data_entry.hpp"
#include "libpmem.h"
#include "pmem_allocator.hpp"
#include "thread_manager.hpp"

namespace KVDK_NAMESPACE {

uint64_t SpaceMap::TestAndUnset(uint64_t offset, uint64_t length) {
  uint64_t res = 0;
  uint64_t cur = offset;
  std::lock_guard<SpinMutex> start_lg(spins_[cur / 64]);
  SpinMutex *last_lock = &spins_[cur / 64];
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
      SpinMutex *next_lock = &spins_[cur / 64];
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

uint64_t SpaceMap::MergeAndUnset(uint64_t offset, uint64_t max_length,
                                 uint64_t target_length) {
  uint64_t cur = offset;
  uint64_t end_offset = offset + max_length;
  SpinMutex *last_lock = &spins_[cur / 64];
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

    SpinMutex *next_lock = &spins_[cur / 64];
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

uint64_t SpaceMap::FindAndUnset(uint64_t &start_offset, uint64_t max_length,
                                uint64_t target_length) {
  uint64_t cur = start_offset;
  uint64_t end_offset = start_offset + max_length;
  SpinMutex *start_lock = &spins_[cur];
  std::unique_ptr<std::lock_guard<SpinMutex>> lg(
      new std::lock_guard<SpinMutex>(*start_lock));
  while (cur < map_.size()) {
    if (&spins_[cur / 64] != start_lock) {
      start_lock = &spins_[cur / 64];
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

      SpinMutex *next_lock = &spins_[cur / 64];
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

void SpaceMap::Set(uint64_t offset, uint64_t length) {
  auto cur = offset;
  SpinMutex *last_lock = &spins_[cur / 64];
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
      SpinMutex *next_lock = &spins_[cur / 64];
      if (next_lock != last_lock) {
        lg.reset(new std::lock_guard<SpinMutex>(*next_lock));
        last_lock = next_lock;
      }
      map_[cur] = MapToken(false, to_set);
    }
  }
}

bool FreeList::Get(uint32_t b_size, SizedSpaceEntry *space_entry) {
  for (uint32_t i = b_size; i < FREE_LIST_MAX_BLOCK; i++) {
    if (offsets_[i].size() != 0) {
      space_entry->space_entry = offsets_[i].back();
      offsets_[i].pop_back();
      if (space_map_->TestAndUnset(space_entry->space_entry.offset, i) == i) {
        space_entry->size = i;
        return true;
      }
    }
  }

  if (!large_entries_.empty()) {
    auto space = large_entries_.begin();
    if (space->size >= b_size) {
      auto size = space->size;
      space_entry->space_entry = space->space_entry;
      large_entries_.erase(space);
      if (space_map_->TestAndUnset(space_entry->space_entry.offset, size) ==
          size) {
        space_entry->size = size;
      }
      return true;
    }
  }
  return false;
}

bool FreeList::MergeGet(uint32_t b_size, uint64_t segment_blocks,
                        SizedSpaceEntry *space_entry) {
  for (uint32_t i = 1; i < FREE_LIST_MAX_BLOCK; i++) {
    auto iter = offsets_[i].begin();
    while (iter != offsets_[i].end()) {
      uint64_t size = MergeSpace(
          *iter, segment_blocks - iter->offset % segment_blocks, b_size);
      if (size >= b_size) {
        space_entry->space_entry = *iter;
        space_entry->size = size;
        offsets_[i].erase(iter);
        return true;
      }
      iter++;
    }
  }

  auto iter = large_entries_.begin();
  while (iter != large_entries_.end()) {
    uint64_t size = MergeSpace(
        iter->space_entry,
        segment_blocks - iter->space_entry.offset % segment_blocks, b_size);
    if (size >= b_size) {
      space_entry->space_entry = iter->space_entry;
      space_entry->size = size;
      large_entries_.erase(iter);
      return true;
    }
    iter++;
  }
  return false;
  /* full merge
  uint64_t offset = 0;
  while (offset < space_map->Size()) {
    uint64_t size = space_map->FindAndUnset(
        offset, std::min(offset + segment_blocks, space_map->Size()), b_size);
    if (size >= b_size) {
      space_entry->space_entry = SpaceEntry(offset, nullptr, nullptr);
      space_entry->size = size;
      return true;
    }
    offset += segment_blocks;
  }
  return false;
  */
}

void FreeList::Push(const SizedSpaceEntry &entry) {
  space_map_->Set(entry.space_entry.offset, entry.size);
  if (entry.size >= FREE_LIST_MAX_BLOCK) {
    large_entries_.insert(entry);
  } else {
    offsets_[entry.size].emplace_back(entry.space_entry);
  }
}

void PMEMAllocator::Free(const SizedSpaceEntry &entry) {
  thread_cache_[local_thread.id].freelist.Push(entry);
}

void PMEMAllocator::PopulateSpace() {
  GlobalLogger.Log("Populating PMEM space ...\n");
  std::vector<std::thread> ths;
  for (int i = 0; i < 16; i++) {
    ths.emplace_back([=]() {
      pmem_memset(pmem_space_ + mapped_size_ * i / 16, 0, mapped_size_ / 16,
                  PMEM_F_MEM_NONTEMPORAL);
    });
  }
  for (auto &t : ths) {
    t.join();
  }
  GlobalLogger.Log("Populating done\n");
}

PMEMAllocator::~PMEMAllocator() { pmem_unmap(pmem_space_, mapped_size_); }

PMEMAllocator::PMEMAllocator(const std::string &pmem_file, uint64_t map_size,
                             uint64_t num_segment_blocks, uint32_t block_size,
                             uint32_t num_write_threads)
    : num_segment_blocks_(num_segment_blocks), block_size_(block_size),
      offset_head_(0) {
  int is_pmem;
  GlobalLogger.Log("Initializing PMEM size %lu in file %s\n", map_size,
                   pmem_file.c_str());
  if ((pmem_space_ =
           (char *)pmem_map_file(pmem_file.c_str(), map_size, PMEM_FILE_CREATE,
                                 0666, &mapped_size_, &is_pmem)) == nullptr) {
    GlobalLogger.Error("Pmem map file %s failed: %s\n", pmem_file.c_str(),
                       strerror(errno));
    exit(1);
  }
  if (!is_pmem) {
    GlobalLogger.Error("%s is not a pmem path\n", pmem_file.c_str());
    exit(1);
  }
  if (mapped_size_ != map_size) {
    GlobalLogger.Error("Pmem map file %s size %lu less than expected %lu\n",
                       pmem_file.c_str(), mapped_size_, map_size);
  }
  max_offset_ = mapped_size_ / block_size_;
  space_map_ = std::make_shared<SpaceMap>(max_offset_);
  GlobalLogger.Log("Map pmem space done\n");
  thread_cache_.resize(num_write_threads, space_map_);
  init_data_size_2_b_size();
}

bool PMEMAllocator::FreeAndFetchSegment(SizedSpaceEntry *segment_space_entry) {
  assert(segment_space_entry);
  if (segment_space_entry->size == num_segment_blocks_) {
    thread_cache_[local_thread.id].segment_offset =
        segment_space_entry->space_entry.offset;
    thread_cache_[local_thread.id].segment_usable_blocks =
        segment_space_entry->size;
    return false;
  }

  if (segment_space_entry->size > 0) {
    Free(*segment_space_entry);
  }

  segment_space_entry->space_entry.offset =
      offset_head_.fetch_add(num_segment_blocks_, std::memory_order_relaxed);
  if (segment_space_entry->space_entry.offset >= max_offset_) {
    return false;
  }
  segment_space_entry->size = num_segment_blocks_;
  return true;
}

SizedSpaceEntry PMEMAllocator::Allocate(unsigned long size) {
  SizedSpaceEntry space_entry;
  auto b_size = size_2_b_size(size);
  // Now the requested block size should smaller than segment size
  // TODO: handle this
  if (b_size > num_segment_blocks_) {
    return space_entry;
  }
  auto &thread_cache = thread_cache_[local_thread.id];
  bool full_segment = thread_cache.segment_usable_blocks < b_size;
  while (full_segment) {
    while (1) {
      // allocate from free list space
      if (thread_cache.free_entry.size >= b_size) {
        // Padding remaining space
        auto extra_space = thread_cache.free_entry.size - b_size;
        // TODO optimize, do not write PMEM
        if (extra_space >= FREE_SPACE_PADDING_BLOCK) {
          DataHeader header(0, extra_space);
          pmem_memcpy_persist(
              offset2addr(thread_cache.free_entry.space_entry.offset + b_size),
              &header, sizeof(DataHeader));
        } else {
          b_size = thread_cache.free_entry.size;
        }

        space_entry = thread_cache.free_entry;
        space_entry.size = b_size;
        thread_cache.free_entry.size -= b_size;
        thread_cache.free_entry.space_entry.offset += b_size;
        return space_entry;
      }
      if (thread_cache.free_entry.size > 0) {
        Free(thread_cache.free_entry);
        thread_cache.free_entry.size = 0;
      }

      // allocate from free list
      if (thread_cache.freelist.Get(b_size, &thread_cache.free_entry)) {
        continue;
      }
      break;
    }

    // allocate a new segment, add remainning space of the old one
    // to the free list
    if (thread_cache.segment_usable_blocks > 0) {
      Free(SizedSpaceEntry(thread_cache.segment_offset,
                           thread_cache.segment_usable_blocks));
    }

    thread_cache.segment_offset =
        offset_head_.fetch_add(num_segment_blocks_, std::memory_order_relaxed);
    if (thread_cache.segment_offset >= max_offset_ - b_size) {
      if (thread_cache.freelist.MergeGet(b_size, num_segment_blocks_,
                                         &thread_cache.free_entry)) {
        continue;
      }
      GlobalLogger.Error("PMEM OVERFLOW!\n");
      return space_entry;
    }
    thread_cache.segment_usable_blocks = std::min(
        num_segment_blocks_, max_offset_ - thread_cache.segment_offset);
    full_segment = false;
  }
  space_entry.size = b_size;
  space_entry.space_entry.offset = thread_cache.segment_offset;
  thread_cache.segment_offset += space_entry.size;
  thread_cache.segment_usable_blocks -= space_entry.size;
  return space_entry;
}

} // namespace KVDK_NAMESPACE