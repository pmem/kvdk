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

class PMEMAllocator : public Allocator {
public:
  PMEMAllocator(const std::string &pmem_file, uint64_t map_size,
                uint64_t num_segment_blocks, uint32_t block_size,
                uint32_t num_write_threads);

  ~PMEMAllocator();

  virtual SizedSpaceEntry Allocate(uint64_t size) override;

  void Free(const SizedSpaceEntry &entry) override;

  inline char *offset2addr(uint64_t offset) {
    if (offset == NULL_PMEM_OFFSET) {
      return nullptr;
    } else {
      return pmem_space_ + offset * block_size_;
    }
  }

  inline uint64_t addr2offset(void *addr) {
    if (addr) {
      return ((char *)addr - pmem_space_) / block_size_;
    } else {
      return NULL_PMEM_OFFSET;
    }
  }
  // Warning! this will zero the pmem space
  void PopulateSpace();

  // Free space_entry and fetch a new segment in space_entry
  bool FreeAndFetchSegment(SizedSpaceEntry *segment_space_entry);

  // Regularly execute by background thread of KVDK
  void BackgroundWork() { free_list_->MoveCachedListToPool(); }

private:
  struct ThreadCache {
    alignas(64) uint64_t segment_offset = 0;
    uint64_t segment_usable_blocks = 0;
    SizedSpaceEntry free_entry;
  };

  void init_data_size_2_b_size() {
    data_size_2_b_size_.resize(4096);
    for (size_t i = 0; i < data_size_2_b_size_.size(); i++) {
      data_size_2_b_size_[i] =
          (i / block_size_) + (i % block_size_ == 0 ? 0 : 1);
    }
  }

  inline uint32_t size_2_b_size(uint32_t data_size) {
    if (data_size < data_size_2_b_size_.size()) {
      return data_size_2_b_size_[data_size];
    }
    return data_size / block_size_ + (data_size % block_size_ == 0 ? 0 : 1);
  }

  const uint64_t num_segment_blocks_;
  const uint32_t block_size_;
  uint64_t mapped_size_;
  uint64_t max_offset_;
  std::vector<ThreadCache> thread_cache_;
  std::atomic<uint64_t> offset_head_;
  char *pmem_space_;
  // quickly get required block size of data size
  std::vector<uint16_t> data_size_2_b_size_;
  std::shared_ptr<FreeList> free_list_;
};
} // namespace KVDK_NAMESPACE
