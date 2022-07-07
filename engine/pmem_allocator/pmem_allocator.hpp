/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <fcntl.h>
#include <sys/mman.h>

#include <memory>
#include <set>
#include <vector>

#include "../alias.hpp"
#include "../allocator.hpp"
#include "../data_record.hpp"
#include "../structures.hpp"
#include "../version/version_controller.hpp"
#include "free_list.hpp"

namespace KVDK_NAMESPACE {

constexpr PMemOffsetType kNullPMemOffset = UINT64_MAX;
constexpr uint64_t kMinPaddingBlocks = 8;

// Manage allocation/de-allocation of PMem space at block unit
//
// PMem space consists of several segment, and a segment is consists of
// several blocks, a block is the minimal allocation unit of PMem space. The
// maximum allocated data size should smaller than a segment.
class PMEMAllocator : public Allocator {
 public:
  virtual ~PMEMAllocator();

  static PMEMAllocator* NewPMEMAllocator(
      const std::string& pmem_file, uint64_t pmem_size,
      uint64_t num_segment_blocks, uint32_t block_size,
      uint32_t max_access_threads, bool populate_pmem_space_on_new_file,
      bool use_devdax_mode, VersionController* version_controller);

  // Allocate a PMem space, return offset and actually allocated space in bytes
  SpaceEntry Allocate(uint64_t size) override;

  // Free a PMem space entry. The entry should be allocated by this allocator
  void Free(const SpaceEntry& entry) override;

  // Purge a kvdk data record and free it
  template <typename T>
  void PurgeAndFree(T* pmem_record) {
    static_assert(std::is_same<T, DLRecord>::value ||
                      std::is_same<T, StringRecord>::value,
                  "");
    pmem_record->Destroy();
    Free(SpaceEntry(addr2offset_checked(pmem_record),
                    pmem_record->GetRecordSize()));
  }

  // translate block_offset of allocated space to address
  inline void* offset2addr_checked(PMemOffsetType offset) const {
    assert(validate_offset(offset) && "Trying to access invalid offset");
    return pmem_ + offset;
  }

  inline void* offset2addr(PMemOffsetType offset) const {
    if (validate_offset(offset)) {
      return pmem_ + offset;
    }
    return nullptr;
  }

  template <typename T>
  inline T* offset2addr_checked(PMemOffsetType offset) const {
    return static_cast<T*>(offset2addr_checked(offset));
  }

  template <typename T>
  inline T* offset2addr(PMemOffsetType block_offset) const {
    return static_cast<T*>(offset2addr(block_offset));
  }

  // translate address of allocated space to block_offset
  inline PMemOffsetType addr2offset_checked(const void* addr) const {
    assert((char*)addr >= pmem_);
    PMemOffsetType offset = (char*)addr - pmem_;
    assert(validate_offset(offset) && "Trying to create invalid offset");
    return offset;
  }

  inline PMemOffsetType addr2offset(const void* addr) const {
    if (addr) {
      PMemOffsetType offset = (char*)addr - pmem_;
      if (validate_offset(offset)) {
        return offset;
      }
    }
    return kNullPMemOffset;
  }

  inline bool validate_offset(uint64_t offset) const {
    return offset < pmem_size_ && offset != kNullPMemOffset;
  }

  // Try to fetch an used segment to segment_space_entry, until reach the a
  // never used segment or end of pmem space
  //
  // Notice: Please only use this function in recovery
  bool FetchSegment(SpaceEntry* segment_space_entry);

  // Regularly execute by background thread of KVDK
  void BackgroundWork() { free_list_.OrganizeFreeSpace(); }

  void BatchFree(const std::vector<SpaceEntry>& entries) {
    if (entries.size() > 0) {
      uint64_t freed = free_list_.BatchPush(entries);
      LogDeallocation(access_thread.id, freed);
    }
  }

  void LogAllocation(int tid, size_t sz) {
    if (tid == -1) {
      global_allocated_size_.fetch_add(sz);
    } else {
      assert(tid >= 0);
      palloc_thread_cache_[tid].allocated_sz += sz;
    }
  }

  void LogDeallocation(int tid, size_t sz) {
    if (tid == -1) {
      global_allocated_size_.fetch_sub(sz);
    } else {
      assert(tid >= 0);
      palloc_thread_cache_[tid].allocated_sz -= sz;
    }
  }

  std::int64_t PMemUsageInBytes();

  // Notice: This function is only for unit test
  Freelist* GetFreeList() { return &free_list_; }

 private:
  friend Freelist;

  PMEMAllocator(char* pmem, uint64_t pmem_size, uint64_t num_segment_blocks,
                uint32_t block_size, uint32_t max_access_threads,
                VersionController* version_controller);
  // Access threads cache a dedicated PMem segment and a free space to
  // avoid contention
  struct alignas(64) PAllocThreadCache {
    // Space got from free list, the size is aligned to block_size_
    SpaceEntry free_entry;
    // Space fetched from head of PMem segments, the size is aligned to
    // block_size_
    SpaceEntry segment_entry;
    std::int64_t allocated_sz{};
  };

  bool allocateSegmentSpace(SpaceEntry* segment_entry);

  static bool checkDevDaxAndGetSize(const char* path, uint64_t* size);

  // Populate PMem space so the following access can be faster
  // Warning! this will zero the entire PMem space
  void populateSpace();

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

  // Mark and persist a space entry on PMem
  void persistSpaceEntry(PMemOffsetType offset, uint64_t size);

  char* pmem_;
  std::vector<PAllocThreadCache, AlignedAllocator<PAllocThreadCache>>
      palloc_thread_cache_;
  const uint32_t block_size_;
  const uint64_t segment_size_;
  // Protect PMem offset head
  SpinMutex offset_head_lock_;
  uint64_t offset_head_;
  uint64_t pmem_size_;
  Freelist free_list_;
  // For quickly get corresponding block size of a requested data size
  std::vector<uint16_t> data_size_2_block_size_;
  VersionController* version_controller_;
  std::atomic<std::int64_t> global_allocated_size_{0};
};
}  // namespace KVDK_NAMESPACE
