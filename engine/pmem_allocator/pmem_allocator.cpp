/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "pmem_allocator.hpp"

#include <sys/sysmacros.h>

#include <thread>

#include "../thread_manager.hpp"
#include "libpmem.h"

namespace KVDK_NAMESPACE {

PMEMAllocator::PMEMAllocator(char* pmem, uint64_t pmem_size,
                             uint64_t num_segment_blocks, uint32_t block_size,
                             uint32_t max_access_threads,
                             VersionController* version_controller)
    : pmem_(pmem),
      palloc_thread_cache_(max_access_threads),
      block_size_(block_size),
      segment_size_(num_segment_blocks * block_size),
      offset_head_(0),
      pmem_size_(pmem_size),
      free_list_(num_segment_blocks, block_size, max_access_threads,
                 pmem_size / block_size / num_segment_blocks *
                     num_segment_blocks /*num blocks*/,
                 this),
      version_controller_(version_controller) {
  init_data_size_2_block_size();
}

void PMEMAllocator::Free(const SpaceEntry& space_entry) {
  if (space_entry.size > 0) {
    assert(space_entry.size % block_size_ == 0);
    free_list_.Push(space_entry);
    LogDeallocation(access_thread.id, space_entry.size);
  }
}

std::int64_t PMEMAllocator::PMemUsageInBytes() {
  std::int64_t total = 0;
  for (auto const& ptcache : palloc_thread_cache_) {
    total += ptcache.allocated_sz;
  }
  total += global_allocated_size_.load();
  return total;
}

void PMEMAllocator::populateSpace() {
  GlobalLogger.Info("Populating PMem space ...\n");
  assert((pmem_ - static_cast<char*>(nullptr)) % 64 == 0);
  for (size_t i = 0; i < pmem_size_ / 64; i++) {
    _mm512_stream_si512(reinterpret_cast<__m512i*>(pmem_) + i,
                        _mm512_set1_epi64(0ULL));
  }
  _mm_mfence();
  GlobalLogger.Info("Populating done\n");
}

PMEMAllocator::~PMEMAllocator() { pmem_unmap(pmem_, pmem_size_); }

PMEMAllocator* PMEMAllocator::NewPMEMAllocator(
    const std::string& pmem_file, uint64_t pmem_size,
    uint64_t num_segment_blocks, uint32_t block_size,
    uint32_t max_access_threads, bool populate_space_on_new_file,
    bool use_devdax_mode, VersionController* version_controller) {
  int is_pmem;
  uint64_t mapped_size;
  char* pmem;
  // TODO jiayu: Should we clear map failed file?
  bool pmem_file_exist = file_exist(pmem_file);
  if (!use_devdax_mode) {
    if ((pmem = (char*)pmem_map_file(pmem_file.c_str(), pmem_size,
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

    if ((pmem = (char*)mmap(nullptr, pmem_size, flags, MAP_SHARED, fd, 0)) ==
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

  PMEMAllocator* allocator = nullptr;
  // We need to allocate a byte map in pmem allocator which require a large
  // memory, so we catch exception here
  try {
    allocator =
        new PMEMAllocator(pmem, pmem_size, num_segment_blocks, block_size,
                          max_access_threads, version_controller);
  } catch (std::bad_alloc& err) {
    GlobalLogger.Error("Error while initialize PMEMAllocator: %s\n",
                       err.what());
    return nullptr;
  }

  // num_segment_blocks and block_size are persisted and never changes.
  // No need to worry user modify those parameters so that records may be
  // skipped.
  size_t sz_wasted = pmem_size % (block_size * num_segment_blocks);
  if (sz_wasted != 0) {
    GlobalLogger.Error(
        "Pmem file size not aligned with segment size, %llu space is wasted.\n",
        sz_wasted);
  }
  GlobalLogger.Info("Map pmem space done\n");

  if (!pmem_file_exist && populate_space_on_new_file) {
    allocator->populateSpace();
  }

  return allocator;
}

bool PMEMAllocator::FetchSegment(SpaceEntry* segment_space_entry) {
  assert(segment_space_entry);

  std::lock_guard<SpinMutex> lg(offset_head_lock_);
  if (offset_head_ <= pmem_size_ - segment_size_ &&
      offset2addr<DataHeader>(offset_head_)->record_size != 0) {
    *segment_space_entry = SpaceEntry{offset_head_, segment_size_};
    offset_head_ += segment_size_;
    LogAllocation(-1, segment_size_);
    return true;
  }
  return false;
}

bool PMEMAllocator::allocateSegmentSpace(SpaceEntry* segment_entry) {
  std::lock_guard<SpinMutex> lg(offset_head_lock_);
  if (offset_head_ <= pmem_size_ - segment_size_) {
    *segment_entry = SpaceEntry{offset_head_, segment_size_};
    offset_head_ += segment_size_;
    persistSpaceEntry(segment_entry->offset, segment_size_);
    return true;
  }
  return false;
}

bool PMEMAllocator::checkDevDaxAndGetSize(const char* path, uint64_t* size) {
  char spath[PATH_MAX];
  char npath[PATH_MAX];
  char* rpath;
  FILE* sfile;
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
  auto& palloc_thread_cache = palloc_thread_cache_[access_thread.id];
  while (palloc_thread_cache.segment_entry.size < aligned_size) {
    while (!backup_processing /* we do not allocate space from free space entry if there is a backup thread processing */) {
      // allocate from free list space
      if (palloc_thread_cache.free_entry.size >= aligned_size) {
        // Padding remaining space
        auto extra_space = palloc_thread_cache.free_entry.size - aligned_size;
        if (extra_space >= kMinPaddingBlocks * block_size_) {
          assert(extra_space % block_size_ == 0);
          // Mark splited space entry size on PMem, we should firstly mark the
          // 2st part for correctness in recovery
          persistSpaceEntry(
              palloc_thread_cache.free_entry.offset + aligned_size,
              extra_space);
        } else {
          aligned_size = palloc_thread_cache.free_entry.size;
        }

        space_entry = palloc_thread_cache.free_entry;
        space_entry.size = aligned_size;
        if (offset2addr_checked<DataEntry>(space_entry.offset)
                ->header.record_size != space_entry.size) {
          // TODO (jiayu): Avoid persist metadata on PMem in allocation
          persistSpaceEntry(space_entry.offset, space_entry.size);
        }
        palloc_thread_cache.free_entry.size -= aligned_size;
        palloc_thread_cache.free_entry.offset += aligned_size;
        LogAllocation(access_thread.id, aligned_size);
        return space_entry;
      }
      if (palloc_thread_cache.free_entry.size > 0) {
        // Not a true free
        LogAllocation(access_thread.id, palloc_thread_cache.free_entry.size);
        Free(palloc_thread_cache.free_entry);
        palloc_thread_cache.free_entry.size = 0;
      }

      // allocate from free list
      if (free_list_.Get(aligned_size, &palloc_thread_cache.free_entry)) {
        continue;
      }
      break;
    }

    LogAllocation(access_thread.id, palloc_thread_cache.segment_entry.size);
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

  // Persist size of space entry on PMem
  // TODO (jiayu): Avoid persist metadata on PMem in allocation
  persistSpaceEntry(space_entry.offset, space_entry.size);
  palloc_thread_cache.segment_entry.offset += space_entry.size;
  palloc_thread_cache.segment_entry.size -= space_entry.size;
  LogAllocation(access_thread.id, space_entry.size);
  return space_entry;
}

void PMEMAllocator::persistSpaceEntry(PMemOffsetType offset, uint64_t size) {
  std::uint32_t sz = static_cast<std::uint32_t>(size);
  kvdk_assert(size == static_cast<std::uint64_t>(sz), "Integer Overflow!");
  DataEntry padding{0, sz, TimeStampType{}, RecordType::Padding, 0, 0};
  pmem_memcpy_persist(offset2addr_checked(offset), &padding, sizeof(DataEntry));
}

Status PMEMAllocator::Backup(const std::string& backup_file_path) {
  int fd = open(backup_file_path.c_str(), O_CREAT | O_RDWR, 0666);
  if (fd < 0) {
    GlobalLogger.Error("Open backup file %s error in PMemAllocator::Backup\n",
                       backup_file_path.c_str());
    return Status::IOError;
  }

  int res = ftruncate64(fd, pmem_size_);
  if (res != 0) {
    GlobalLogger.Error(
        "Truncate backup file %s to pmem file size %lu error in "
        "PMemAllocator::Backup\n",
        backup_file_path.c_str(), pmem_size_);
    return Status::IOError;
  }

  void* backup_file =
      mmap64(nullptr, pmem_size_, PROT_WRITE, MAP_SHARED, fd, 0);

  if (backup_file == nullptr) {
    GlobalLogger.Error("map backup file %s error in PMemAllocator::Backup\n",
                       backup_file_path.c_str());
    return Status::IOError;
  }

  auto multi_thread_memcpy = [&](char* dst, char* src, size_t len,
                                 uint64_t threads) {
    size_t per_thread = len / threads;
    size_t extra = len % threads;
    std::vector<std::thread> ths;
    for (size_t i = 0; i < threads; i++) {
      ths.emplace_back([&]() {
        memcpy(dst + i * per_thread, src + i * per_thread, per_thread);
      });
    }
    memcpy(dst + len - extra, src + len - extra, extra);

    GlobalLogger.Info("%lu threads memcpy\n", ths.size());
    for (auto& t : ths) {
      t.join();
    }
    GlobalLogger.Info("%lu threads memcpy done\n", ths.size());
  };

  std::lock_guard<std::mutex> lg(backup_lock);
  backup_processing = true;
  defer(backup_processing = false);
  // We prohibit allocate space from free space entry during backup, meanwhile,
  // the copying data maybe updated and write to new allocated segment, we need
  // these updated data for recovery, so we copy data twice
  //
  // There may be ongoing writing to just allocated space while we start backup,
  // so we also wait for holding snapshot of access threads changed
  uint64_t copy_offset_1st_end = 0;
  uint64_t copy_offset_2nd_start = 0;
  uint64_t copy_offset_2nd_end = 0;
  std::vector<TimeStampType> thread_holding_snapshot_ts(
      palloc_thread_cache_.size());
  for (size_t i = 0; i < palloc_thread_cache_.size(); i++) {
    thread_holding_snapshot_ts[i] =
        version_controller_->GetLocalSnapshot(i).GetTimestamp();
    copy_offset_1st_end = std::max(
        copy_offset_1st_end,
        palloc_thread_cache_[i].segment_entry.offset +
            (segment_size_ -
             palloc_thread_cache_[i].segment_entry.offset % segment_size_));
    copy_offset_2nd_start = std::min(
        copy_offset_2nd_start, palloc_thread_cache_[i].segment_entry.offset);
  }

  for (size_t i = 0; i < palloc_thread_cache_.size(); i++) {
    if (thread_holding_snapshot_ts[i] != kMaxTimestamp &&
        thread_holding_snapshot_ts[i] ==
            version_controller_->GetLocalSnapshot(i).GetTimestamp()) {
      sleep(1);
    }
  }

  memcpy(backup_file, pmem_, copy_offset_1st_end);
  //  multi_thread_memcpy((char *)backup_file, pmem_, copy_offset_1st, 4);
  {
    std::lock_guard<SpinMutex> lg(offset_head_lock_);
    copy_offset_2nd_end = offset_head_;
  }
  memcpy((char*)backup_file + copy_offset_2nd_start,
         pmem_ + copy_offset_2nd_start,
         copy_offset_2nd_end - copy_offset_2nd_start);
  //  multi_thread_memcpy((char *)backup_file + copy_offset_1st,
  //                      pmem_ + copy_offset_1st,
  //                      copy_offset_2nd - copy_offset_1st, 4);

  msync(backup_file, copy_offset_2nd_end, MS_SYNC);
  close(fd);
  return Status::Ok;
}

}  // namespace KVDK_NAMESPACE
