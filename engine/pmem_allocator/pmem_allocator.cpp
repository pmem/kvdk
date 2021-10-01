/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <thread>

#include "../thread_manager.hpp"
#include "libpmem.h"
#include "pmem_allocator.hpp"

namespace KVDK_NAMESPACE {

void PMEMAllocator::Free(const SizedSpaceEntry &entry) {
  free_list_->Push(entry);
}

void PMEMAllocator::DelayFree(const SizedSpaceEntry &entry) {
  free_list_->DelayPush(entry);
}

void PMEMAllocator::PopulateSpace() {
  GlobalLogger.Info("Populating PMem space ...\n");
  std::vector<std::thread> ths;

  int pu = get_usable_pu();
  if (pu <= 0) {
    pu = 1;
  } else if (pu > 16) {
    // 16 is a moderate concurrent number for writing PMem.
    pu = 16;
  }
  for (int i = 0; i < pu; i++) {
    ths.emplace_back([=]() {
      uint64_t offset = pmem_size_ * i / pu;
      // To cover the case that mapped_size_ is not divisible by pu.
      uint64_t len = std::min(pmem_size_ / pu, pmem_size_ - offset);
      pmem_memset(pmem_ + offset, 0, len, PMEM_F_MEM_NONTEMPORAL);
    });
  }
  for (auto &t : ths) {
    t.join();
  }
  GlobalLogger.Info("Populating done\n");
}

PMEMAllocator::~PMEMAllocator() { pmem_unmap(pmem_, pmem_size_); }

PMEMAllocator::PMEMAllocator(const std::string &pmem_file, uint64_t pmem_space,
                             uint64_t num_segment_blocks, uint32_t block_size,
                             uint32_t num_write_threads)
    : thread_cache_(num_write_threads), num_segment_blocks_(num_segment_blocks),
      block_size_(block_size), offset_head_(0) {
  int is_pmem;
  GlobalLogger.Info("Initializing PMem size %lu in file %s\n", pmem_space,
                    pmem_file.c_str());
  if ((pmem_ = (char *)pmem_map_file(pmem_file.c_str(), pmem_space,
                                     PMEM_FILE_CREATE, 0666, &pmem_size_,
                                     &is_pmem)) == nullptr) {
    GlobalLogger.Error("Pmem map file %s failed: %s\n", pmem_file.c_str(),
                       strerror(errno));
    exit(1);
  }
  if (!is_pmem) {
    GlobalLogger.Error("%s is not a pmem path\n", pmem_file.c_str());
    exit(1);
  }
  if (pmem_size_ != pmem_space) {
    GlobalLogger.Error("Pmem map file %s size %lu less than expected %lu\n",
                       pmem_file.c_str(), pmem_size_, pmem_space);
  }
  max_block_offset_ =
      pmem_size_ / block_size_ / num_segment_blocks_ * num_segment_blocks_;
  // num_segment_blocks and block_size are persisted and never changes.
  // No need to worry user modify those parameters so that records may be
  // skipped.
  size_t sz_wasted = pmem_size_ % (block_size_ * num_segment_blocks_);
  if (sz_wasted != 0)
    GlobalLogger.Error(
        "Pmem file size not aligned with segment size, %llu space is wasted.\n",
        sz_wasted);
  free_list_ = std::make_shared<Freelist>(
      num_segment_blocks, num_write_threads,
      std::make_shared<SpaceMap>(max_block_offset_), this);
  GlobalLogger.Info("Map pmem space done\n");
  init_data_size_2_block_size();
}

bool PMEMAllocator::FreeAndFetchSegment(SizedSpaceEntry *segment_space_entry) {
  assert(segment_space_entry);
  if (segment_space_entry->size == num_segment_blocks_) {
    thread_cache_[write_thread.id].segment_entry = *segment_space_entry;
    return false;
  }

  if (segment_space_entry->size > 0) {
    Free(*segment_space_entry);
  }

  segment_space_entry->space_entry.offset =
      offset_head_.fetch_add(num_segment_blocks_, std::memory_order_relaxed);
  // Don't fetch block that may excess PMem file boundary
  if (segment_space_entry->space_entry.offset >
      max_block_offset_ - num_segment_blocks_)
    return false;
  segment_space_entry->size = num_segment_blocks_;
  return true;
}

void PMEMAllocator::FetchSegmentSpace(SizedSpaceEntry *segment_entry) {
  uint64_t offset;
  while (1) {
    offset = offset_head_.load(std::memory_order_relaxed);
    if (offset < max_block_offset_) {
      if (offset_head_.compare_exchange_strong(offset,
                                               offset + num_segment_blocks_)) {
        Free(*segment_entry);
        *segment_entry = SizedSpaceEntry{offset, num_segment_blocks_, 0};
        assert(segment_entry->space_entry.offset <=
                   max_block_offset_ - num_segment_blocks_ &&
               "Block may excess PMem file boundary");
        break;
      }
      continue;
    }
    break;
  }
}

SizedSpaceEntry PMEMAllocator::Allocate(unsigned long size) {
  SizedSpaceEntry space_entry;
  auto b_size = size_2_block_size(size);
  // Now the requested block size should smaller than segment size
  // TODO: handle this
  if (b_size > num_segment_blocks_) {
    return space_entry;
  }
  auto &thread_cache = thread_cache_[write_thread.id];
  bool full_segment = thread_cache.segment_entry.size < b_size;
  while (full_segment) {
    while (1) {
      // allocate from free list space
      if (thread_cache.free_entry.size >= b_size) {
        // Padding remaining space
        auto extra_space = thread_cache.free_entry.size - b_size;
        // TODO optimize, do not write PMem
        if (extra_space >= kMinPaddingBlockSize) {
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
      if (free_list_->Get(b_size, &thread_cache.free_entry)) {
        continue;
      }
      break;
    }

    // allocate a new segment, add remainning space of the old one
    // to the free list
    FetchSegmentSpace(&thread_cache.segment_entry);

    if (thread_cache.segment_entry.size < b_size) {
      GlobalLogger.Error("PMem OVERFLOW!\n");
      return space_entry;
    }
    full_segment = false;
  }
  space_entry = thread_cache.segment_entry;
  space_entry.size = b_size;
  thread_cache.segment_entry.space_entry.offset += space_entry.size;
  thread_cache.segment_entry.size -= space_entry.size;
  return space_entry;
}

} // namespace KVDK_NAMESPACE
