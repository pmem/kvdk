/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <sys/sysmacros.h>
#include <thread>

#include "../thread_manager.hpp"
#include "libpmem.h"
#include "pmem_allocator.hpp"

namespace KVDK_NAMESPACE {

PMEMAllocator::PMEMAllocator(char *pmem, uint64_t pmem_size,
                             uint64_t num_segment_blocks, uint32_t block_size,
                             uint32_t num_write_threads)
    : pmem_(pmem), palloc_thread_cache_(num_write_threads),
      block_size_(block_size), segment_size_(num_segment_blocks * block_size),
      offset_head_(0), pmem_size_(pmem_size),
      free_list_(num_segment_blocks, block_size, num_write_threads,
                 pmem_size / block_size / num_segment_blocks *
                     num_segment_blocks /*num blocks*/,
                 this) {
  init_data_size_2_block_size();
}

void PMEMAllocator::Free(const SpaceEntry &entry) {
  if (entry.size > 0) {
    assert(entry.size % block_size_ == 0);
    free_list_.Push(entry);
    LogDeallocation(write_thread.id, entry.size);
  }
}

std::int64_t PMEMAllocator::PMemUsageInBytes() {
  std::int64_t total = 0;
  for (auto const &ptcache : palloc_thread_cache_) {
    total += ptcache.allocated_sz;
  }
  return total;
}

void PMEMAllocator::populateSpace() {
  GlobalLogger.Info("Populating PMem space ...\n");
  assert((pmem_ - static_cast<char *>(nullptr)) % 64 == 0);
  assert(pmem_size_ % 64 == 0);
  for (size_t i = 0; i < pmem_size_ / 64; i++) {
    _mm512_stream_si512(reinterpret_cast<__m512i *>(pmem_) + i,
                        _mm512_set1_epi64(0ULL));
  }
  _mm_mfence();
  GlobalLogger.Info("Populating done\n");
}

PMEMAllocator::~PMEMAllocator() { pmem_unmap(pmem_, pmem_size_); }

PMEMAllocator *PMEMAllocator::NewPMEMAllocator(const std::string &pmem_file,
                                               uint64_t pmem_size,
                                               uint64_t num_segment_blocks,
                                               uint32_t block_size,
                                               uint32_t num_write_threads,
                                               bool populate_space_on_new_file,
                                               bool use_devdax_mode) {
  int is_pmem;
  uint64_t mapped_size;
  char *pmem;
  // TODO jiayu: Should we clear map failed file?
  bool pmem_file_exist = file_exist(pmem_file);
  if (!use_devdax_mode) {
    if ((pmem = (char *)pmem_map_file(pmem_file.c_str(), pmem_size,
                                      PMEM_FILE_CREATE, 0666, &mapped_size,
                                      &is_pmem)) == nullptr) {
      GlobalLogger.Error("PMem map file %s failed: %s\n", pmem_file.c_str(),
                         strerror(errno));
      return nullptr;
    }

    if (!is_pmem) {
      GlobalLogger.Error("%s is not a pmem path\n", pmem_file.c_str());
      return nullptr;
    }
  } else {
    if (!checkDevDaxAndGetSize(pmem_file.c_str(), &mapped_size)) {
      GlobalLogger.Error(
          "checkDevDaxAndGetSize %s failed device %s faild: %s\n",
          pmem_file.c_str(), strerror(errno));
      return nullptr;
    }

    int flags = PROT_READ | PROT_WRITE;
    int fd = open(pmem_file.c_str(), O_RDWR, 0666);
    if (fd < 0) {
      GlobalLogger.Error("Open devdax device %s faild: %s\n", pmem_file.c_str(),
                         strerror(errno));
      return nullptr;
    }

    if ((pmem = (char *)mmap(nullptr, pmem_size, flags, MAP_SHARED, fd, 0)) ==
        nullptr) {
      GlobalLogger.Error("Mmap devdax device %s faild: %s\n", pmem_file.c_str(),
                         strerror(errno));
      return nullptr;
    }
  }

  if (mapped_size != pmem_size) {
    GlobalLogger.Error(
        "Pmem map file %s size %lu is not same as expected %lu\n",
        pmem_file.c_str(), mapped_size, pmem_size);
    return nullptr;
  }

  PMEMAllocator *allocator = nullptr;
  // We need to allocate a byte map in pmem allocator which require a large
  // memory, so we catch exception here
  try {
    allocator = new PMEMAllocator(pmem, pmem_size, num_segment_blocks,
                                  block_size, num_write_threads);
  } catch (std::bad_alloc &err) {
    GlobalLogger.Error("Error while initialize PMEMAllocator: %s\n",
                       err.what());
    return nullptr;
  }

  // num_segment_blocks and block_size are persisted and never changes.
  // No need to worry user modify those parameters so that records may be
  // skipped.
  size_t sz_wasted = pmem_size % (block_size * num_segment_blocks);
  if (sz_wasted != 0)
    GlobalLogger.Error(
        "Pmem file size not aligned with segment size, %llu space is wasted.\n",
        sz_wasted);
  GlobalLogger.Info("Map pmem space done\n");

  if (!pmem_file_exist && populate_space_on_new_file) {
    allocator->populateSpace();
  }

  return allocator;
}

bool PMEMAllocator::FreeAndFetchSegment(SpaceEntry *segment_space_entry) {
  assert(segment_space_entry);
  kvdk_assert(write_thread.id >= 0, "Call PMEMAllocator::FreeAndFetchSegment "
                                    "with a un-initialized write thread\n");
  if (segment_space_entry->size == segment_size_) {
    persistSpaceEntry(segment_space_entry->offset, segment_size_);
    palloc_thread_cache_[write_thread.id].segment_entry = *segment_space_entry;
    LogDeallocation(write_thread.id, segment_size_);
    return false;
  }

  std::lock_guard<SpinMutex> lg(offset_head_lock_);
  if (offset_head_ <= pmem_size_ - segment_size_) {
    Free(*segment_space_entry);
    *segment_space_entry = SpaceEntry{offset_head_, segment_size_};
    offset_head_ += segment_size_;
    LogAllocation(write_thread.id, segment_size_);
    return true;
  }
  return false;
}

bool PMEMAllocator::allocateSegmentSpace(SpaceEntry *segment_entry) {
  std::lock_guard<SpinMutex> lg(offset_head_lock_);
  if (offset_head_ <= pmem_size_ - segment_size_) {
    *segment_entry = SpaceEntry{offset_head_, segment_size_};
    persistSpaceEntry(offset_head_, segment_size_);
    offset_head_ += segment_size_;
    return true;
  }
  return false;
}

bool PMEMAllocator::checkDevDaxAndGetSize(const char *path, uint64_t *size) {
  char spath[PATH_MAX];
  char npath[PATH_MAX];
  char *rpath;
  FILE *sfile;
  struct stat st;

  if (stat(path, &st) < 0) {
    GlobalLogger.Error("stat file %s failed %s\n", path, strerror(errno));
    return false;
  }

  snprintf(spath, PATH_MAX, "/sys/dev/char/%d:%d/subsystem", major(st.st_rdev),
           minor(st.st_rdev));
  // Get the real path of the /sys/dev/char/major:minor/subsystem
  if ((rpath = realpath(spath, npath)) == 0) {
    GlobalLogger.Error("realpath on file %s failed %s\n", spath,
                       strerror(errno));
    return false;
  }

  // Checking the rpath is DAX device by compare
  if (strcmp("/sys/class/dax", rpath)) {
    return false;
  }

  snprintf(spath, PATH_MAX, "/sys/dev/char/%d:%d/size", major(st.st_rdev),
           minor(st.st_rdev));

  sfile = fopen(spath, "r");
  if (!sfile) {
    GlobalLogger.Error("fopen on file %s failed %s\n", spath, strerror(errno));
    return false;
  }

  if (fscanf(sfile, "%lu", size) < 0) {
    GlobalLogger.Error("fscanf on file %s failed %s\n", spath, strerror(errno));
    fclose(sfile);
    return false;
  }

  fclose(sfile);
  return true;
}

SpaceEntry PMEMAllocator::Allocate(uint64_t size) {
  SpaceEntry space_entry;
  uint32_t b_size = size_2_block_size(size);
  uint32_t aligned_size = b_size * block_size_;
  // Now the requested block size should smaller than segment size
  // TODO: handle this
  if (aligned_size > segment_size_) {
    return space_entry;
  }
  auto &palloc_thread_cache = palloc_thread_cache_[write_thread.id];
  while (palloc_thread_cache.segment_entry.size < aligned_size) {
    while (1) {
      // allocate from free list space
      if (palloc_thread_cache.free_entry.size >= aligned_size) {
        // Padding remaining space
        auto extra_space = palloc_thread_cache.free_entry.size - aligned_size;
        // TODO optimize, do not write PMem
        if (extra_space >= kMinPaddingBlocks * block_size_) {
          assert(extra_space % block_size_ == 0);
          DataEntry padding(0, static_cast<uint32_t>(extra_space), 0,
                            RecordType::Padding, 0, 0);
          pmem_memcpy_persist(
              offset2addr(palloc_thread_cache.free_entry.offset + aligned_size),
              &padding, sizeof(DataEntry));
        } else {
          aligned_size = palloc_thread_cache.free_entry.size;
        }

        space_entry = palloc_thread_cache.free_entry;
        space_entry.size = aligned_size;
        palloc_thread_cache.free_entry.size -= aligned_size;
        palloc_thread_cache.free_entry.offset += aligned_size;
        LogAllocation(write_thread.id, aligned_size);
        return space_entry;
      }
      if (palloc_thread_cache.free_entry.size > 0) {
        // Not a true free
        LogAllocation(write_thread.id, palloc_thread_cache.free_entry.size);
        Free(palloc_thread_cache.free_entry);
        palloc_thread_cache.free_entry.size = 0;
      }

      // allocate from free list
      if (free_list_.Get(aligned_size, &palloc_thread_cache.free_entry)) {
        continue;
      }
      break;
    }

    LogAllocation(write_thread.id, palloc_thread_cache.segment_entry.size);
    Free(palloc_thread_cache.segment_entry);
    // allocate a new segment, add remainning space of the old one
    // to the free list
    if (!allocateSegmentSpace(&palloc_thread_cache.segment_entry)) {
      GlobalLogger.Error("PMem OVERFLOW!\n");
      return space_entry;
    }
  }
  space_entry = palloc_thread_cache.segment_entry;
  space_entry.size = aligned_size;
  palloc_thread_cache.segment_entry.offset += aligned_size;
  palloc_thread_cache.segment_entry.size -= aligned_size;
  LogAllocation(write_thread.id, aligned_size);
  return space_entry;
}

void PMEMAllocator::persistSpaceEntry(PMemOffsetType offset, uint64_t size) {
  DataHeader header(0, size);
  pmem_memcpy_persist(offset2addr(offset), &header, sizeof(DataHeader));
}

} // namespace KVDK_NAMESPACE
