/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <memory>
#include <set>
#include <vector>

#include "allocator.hpp"
#include "kvdk/namespace.hpp"
#include "stdint.h"
#include "structures.hpp"

namespace KVDK_NAMESPACE {

class SpaceMap {
public:
  SpaceMap(uint64_t size) : map_(size, {false, 0}), spins_(size / 64 + 1) {}

  uint64_t TestAndUnset(uint64_t offset, uint64_t length);

  uint64_t FindAndUnset(uint64_t &start_offset, uint64_t max_length,
                        uint64_t target_length);

  uint64_t MergeAndUnset(uint64_t offset, uint64_t max_length,
                         uint64_t target_length);

  void Set(uint64_t offset, uint64_t length);

  uint64_t Size() { return map_.size(); }

private:
  struct MapToken {
  public:
    MapToken(bool is_start, uint8_t size)
        : token(size | (is_start ? (1 << 7) : 0)) {}
    uint8_t Size() { return token & (INT8_MAX); }
    void Clear() { token = 0; }
    bool Empty() { return token == 0; }
    bool IsStart() { return token & (1 << 7); }

  private:
    uint8_t token;
  };

  std::vector<MapToken> map_;
  std::vector<SpinMutex> spins_;
};

class FreeList {
public:
  FreeList(uint32_t max_b_size, std::shared_ptr<SpaceMap> s)
      : offsets_(max_b_size), space_map_(s) {}

  FreeList(std::shared_ptr<SpaceMap> s)
      : offsets_(FREE_LIST_MAX_BLOCK), space_map_(s) {}

  void Push(const SizedSpaceEntry &entry);

  bool Get(uint32_t b_size, SizedSpaceEntry *space_entry);

  bool MergeGet(uint32_t b_size, uint64_t segment_blocks,
                SizedSpaceEntry *space_entry);

private:
  uint64_t MergeSpace(const SpaceEntry &space_entry, uint64_t max_size,
                      uint64_t target_size) {
    if (target_size > max_size) {
      return false;
    }
    uint64_t size =
        space_map_->MergeAndUnset(space_entry.offset, max_size, target_size);
    return size;
  }
  class SpaceCmp {
  public:
    bool operator()(const SizedSpaceEntry &s1, const SizedSpaceEntry &s2) {
      return s1.size > s2.size;
    }
  };

  std::vector<std::vector<SpaceEntry>> offsets_;
  std::set<SizedSpaceEntry, SpaceCmp> large_entries_;
  std::shared_ptr<SpaceMap> space_map_;
};

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

private:
  struct ThreadCache {
    ThreadCache(std::shared_ptr<SpaceMap> space_map) : freelist(space_map) {}

    alignas(64) uint64_t segment_offset = 0;
    uint64_t segment_usable_blocks = 0;
    SizedSpaceEntry free_entry;
    FreeList freelist;
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
  std::shared_ptr<SpaceMap> space_map_;
};
} // namespace KVDK_NAMESPACE