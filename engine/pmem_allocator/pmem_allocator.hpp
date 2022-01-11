/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <fcntl.h>
#include <sys/mman.h>

#include <memory>
#include <set>
#include <vector>

#include "../allocator.hpp"
#include "../data_record.hpp"
#include "../structures.hpp"
#include "free_list.hpp"
#include "kvdk/namespace.hpp"

namespace KVDK_NAMESPACE {

constexpr PMemOffsetType kPmemNullOffset = UINT64_MAX;
constexpr uint64_t kMinPaddingBlocks = 8;

// Manage allocation/de-allocation of PMem space at block unit
//
// PMem space consists of several segment, and a segment is consists of
// several blocks, a block is the minimal allocation unit of PMem space. The
// maximum allocated data size should smaller than a segment.
class PMEMAllocator : public Allocator {
public:
  ~PMEMAllocator();

  static PMEMAllocator *
  NewPMEMAllocator(const std::string &pmem_file, uint64_t pmem_size,
                   uint64_t num_segment_blocks, uint32_t block_size,
                   uint32_t num_write_threads, bool populate_space_on_new_file,
                   bool use_devdax_mode);

  // Allocate a PMem space, return offset and actually allocated space in bytes
  SpaceEntry Allocate(uint64_t size) override;

  // Free a PMem space entry. The entry should be allocated by this allocator
  void Free(const SpaceEntry &entry) override;

  // translate block_offset of allocated space to address
  inline void *offset2addr_checked(uint64_t offset) {
    assert(validate_offset(offset) && "Trying to access invalid offset");
    return pmem_ + offset;
  }

  inline void *offset2addr(uint64_t offset) {
    if (validate_offset(offset)) {
      return pmem_ + offset;
    }
    return nullptr;
  }

  template <typename T> inline T *offset2addr_checked(uint64_t offset) {
    return static_cast<T *>(offset2addr_checked(offset));
  }

  template <typename T> inline T *offset2addr(uint64_t block_offset) {
    return static_cast<T *>(offset2addr(block_offset));
  }

  // translate address of allocated space to block_offset
  inline uint64_t addr2offset_checked(const void *addr) {
    assert((char *)addr >= pmem_);
    uint64_t offset = (char *)addr - pmem_;
    assert(validate_offset(offset) && "Trying to create invalid offset");
    return offset;
  }

  inline uint64_t addr2offset(const void *addr) {
    if (addr) {
      uint64_t offset = (char *)addr - pmem_;
      if (validate_offset(offset)) {
        return offset;
      }
    }
    return kPmemNullOffset;
  }

  inline bool validate_offset(uint64_t offset) {
    return offset < pmem_size_ && offset != kPmemNullOffset;
  }

  // Free segment_space_entry and fetch an allocated segment to
  // segment_space_entry, until reach the end of allocated space
  bool FreeAndFetchSegment(SpaceEntry *segment_space_entry);

  // Regularly execute by background thread of KVDK
  void BackgroundWork() { free_list_.OrganizeFreeSpace(); }

  void LogAllocation(size_t tid, size_t sz) {
    palloc_thread_cache_[tid].allocated_sz += sz;
  }
  void LogDeallocation(size_t tid, size_t sz) {
    palloc_thread_cache_[tid].allocated_sz -= sz;
  }
  size_t PMemUsageInBytes();

private:
  friend Freelist;

  PMEMAllocator(char *pmem, uint64_t pmem_size, uint64_t num_segment_blocks,
                uint32_t block_size, uint32_t num_write_threads);
  // Write threads cache a dedicated PMem segment and a free space to
  // avoid contention
  struct alignas(64) PAllocThreadCache {
    // Space got from free list, the size is aligned to block_size_
    SpaceEntry free_entry;
    // Space fetched from head of PMem segments, the size is aligned to
    // block_size_
    SpaceEntry segment_entry;
    size_t allocated_sz{};
  };

  // Populate PMem space so the following access can be faster
  // Warning! this will zero the entire PMem space
  void populateSpace();

  bool allocateSegmentSpace(SpaceEntry *segment_entry);

  static bool checkDevDaxAndGetSize(const char *path, uint64_t *size);

  // Mark and persist a space entry on PMem
  void persistSpaceEntry(PMemOffsetType offset, uint64_t size);

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

  // Protect PMem offset head
  SpinMutex offset_head_lock_;
  uint64_t offset_head_;
  std::vector<PAllocThreadCache, AlignedAllocator<PAllocThreadCache>>
      palloc_thread_cache_;
  const uint32_t block_size_;
  const uint64_t segment_size_;
  char *pmem_;
  uint64_t pmem_size_;
  Freelist free_list_;
  // For quickly get corresponding block size of a requested data size
  std::vector<uint16_t> data_size_2_block_size_;
};
} // namespace KVDK_NAMESPACE
