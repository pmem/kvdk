/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <thread>

#include "../data_entry.hpp"
#include "../thread_manager.hpp"
#include "libpmem.h"
#include "pmem_allocator.hpp"

namespace KVDK_NAMESPACE {

void PMEMAllocator::Free(const SizedSpaceEntry &entry) {
  free_list_->Push(entry);
}

void PMEMAllocator::PopulateSpace() {
  GlobalLogger.Log("Populating PMem space ...\n");
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
      uint64_t len = pmem_size_ / pu;
      // Re-calculate the length of last chunk to cover the
      // case that mapped_size_ is not divisible by pu.
      if (i == pu - 1)
        len = pmem_size_ - (pu - 1) * len;
      pmem_memset(pmem_ + pmem_size_ * i / pu, 0, len, PMEM_F_MEM_NONTEMPORAL);
    });
  }
  for (auto &t : ths) {
    t.join();
  }
  GlobalLogger.Log("Populating done\n");
}

PMEMAllocator::~PMEMAllocator() { pmem_unmap(pmem_, pmem_size_); }

PMEMAllocator::PMEMAllocator(const std::string &pmem_file, uint64_t pmem_space,
                             uint64_t num_segment_blocks, uint32_t block_size,
                             uint32_t num_write_threads)
    : num_segment_blocks_(num_segment_blocks), block_size_(block_size),
      offset_head_(0) {
  int is_pmem;
  GlobalLogger.Log("Initializing PMem size %lu in file %s\n", pmem_space,
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
  max_block_offset_ = pmem_size_ / block_size_;
  free_list_ =
      std::make_shared<Freelist>(num_segment_blocks, num_write_threads,
                                 std::make_shared<SpaceMap>(max_block_offset_));
  GlobalLogger.Log("Map pmem space done\n");
  thread_cache_.resize(num_write_threads);
  init_data_size_2_block_size();
}

bool PMEMAllocator::FreeAndFetchSegment(SizedSpaceEntry *segment_space_entry) {
  assert(segment_space_entry);
  if (segment_space_entry->size == num_segment_blocks_) {
    thread_cache_[write_thread.id].segment_offset =
        segment_space_entry->space_entry.offset;
    thread_cache_[write_thread.id].segment_usable_blocks =
        segment_space_entry->size;
    return false;
  }

  if (segment_space_entry->size > 0) {
    Free(*segment_space_entry);
  }

  segment_space_entry->space_entry.offset =
      offset_head_.fetch_add(num_segment_blocks_, std::memory_order_relaxed);
  if (segment_space_entry->space_entry.offset >= max_block_offset_) {
    return false;
  }
  segment_space_entry->size = num_segment_blocks_;
  return true;
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
  bool full_segment = thread_cache.segment_usable_blocks < b_size;
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
    if (thread_cache.segment_usable_blocks > 0) {
      Free(SizedSpaceEntry(thread_cache.segment_offset,
                           thread_cache.segment_usable_blocks));
    }

    thread_cache.segment_offset =
        offset_head_.fetch_add(num_segment_blocks_, std::memory_order_relaxed);
    thread_cache.segment_usable_blocks =
        thread_cache.segment_offset >= max_block_offset_
            ? 0
            : std::min(num_segment_blocks_,
                       max_block_offset_ - thread_cache.segment_offset);
    if (thread_cache.segment_offset >= max_block_offset_ - b_size) {
      if (free_list_->MergeGet(b_size, &thread_cache.free_entry)) {
        continue;
      }
      GlobalLogger.Error("PMem OVERFLOW!\n");
      return space_entry;
    }
    full_segment = false;
  }
  space_entry.size = b_size;
  space_entry.space_entry.offset = thread_cache.segment_offset;
  thread_cache.segment_offset += space_entry.size;
  thread_cache.segment_usable_blocks -= space_entry.size;
  return space_entry;
}

} // namespace KVDK_NAMESPACE
