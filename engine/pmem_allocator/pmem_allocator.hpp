/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <memory>
#include <set>
#include <vector>

#include "../allocator.hpp"
#include "../structures.hpp"
#include "free_list.hpp"
#include "kvdk/namespace.hpp"

namespace KVDK_NAMESPACE {

constexpr uint64_t kNullPmemOffset = UINT64_MAX;
constexpr uint64_t kMinPaddingBlockSize = 8;

// Manage allocation/de-allocation of PMem space
//
// PMem space consists of several segment, and a segment is consists of
// several blocks, a block is the minimal allocation unit of PMem space. The
// maximum allocated data size should smaller than a segment.
class PMEMAllocator : public Allocator {
public:
  PMEMAllocator(const std::string &pmem_file, uint64_t pmem_space,
                uint64_t num_segment_blocks, uint32_t block_size,
                uint32_t num_write_threads);

  ~PMEMAllocator();

  virtual SizedSpaceEntry Allocate(uint64_t size) override;

  void Free(const SizedSpaceEntry &entry) override;

  void DelayFree(const SizedSpaceEntry &entry);

  // transfer block_offset of allocated space to address
  inline char *offset2addr(uint64_t block_offset) {
    if (block_offset == kNullPmemOffset) {
      return nullptr;
    } else {
      return pmem_ + block_offset * block_size_;
    }
  }

  // transfer address of allocated space to block_offset
  inline uint64_t addr2offset(void *addr) {
    if (addr) {
      return ((char *)addr - pmem_) / block_size_;
    } else {
      return kNullPmemOffset;
    }
  }

  // Populate PMem space so the following access can be faster
  // Warning! this will zero the entire PMem space
  void PopulateSpace();

  // Free space_entry and fetch a new segment in space_entry, unless
  // segment_space_entry is a full segment
  bool FreeAndFetchSegment(SizedSpaceEntry *segment_space_entry);

  // Regularly execute by background thread of KVDK
  void BackgroundWork() {
    free_list_->MoveCachedListToPool();
    free_list_->MergeFreeSpaceInPool();
  }

private:
  // Write threads cache a dedicated PMem segment and a free space to
  // avoid contention
  struct ThreadCache {
    // Space got from free list
    SizedSpaceEntry free_entry;
    // Space fetched from head of PMem segments
    SizedSpaceEntry segment_entry;
    char padding[64 - sizeof(SizedSpaceEntry) * 2];
  };

  void FetchSegmentSpace(SizedSpaceEntry *segment_entry);

  void init_data_size_2_block_size() {
    data_size_2_block_size_.resize(4096);
    for (size_t i = 0; i < data_size_2_block_size_.size(); i++) {
      data_size_2_block_size_[i] =
          (i / block_size_) + (i % block_size_ == 0 ? 0 : 1);
    }
  }

  inline uint32_t size_2_block_size(uint32_t data_size) {
    if (data_size < data_size_2_block_size_.size()) {
      return data_size_2_block_size_[data_size];
    }
    return data_size / block_size_ + (data_size % block_size_ == 0 ? 0 : 1);
  }

  std::vector<ThreadCache> thread_cache_;
  const uint64_t num_segment_blocks_;
  const uint32_t block_size_;
  std::atomic<uint64_t> offset_head_;
  char *pmem_;
  uint64_t pmem_size_;
  uint64_t max_block_offset_;
  std::shared_ptr<Freelist> free_list_;
  // For quickly get corresponding block size of a requested data size
  std::vector<uint16_t> data_size_2_block_size_;
};
} // namespace KVDK_NAMESPACE
