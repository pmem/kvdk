/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cassert>
#include <vector>

#include "alias.hpp"
#include "thread_manager.hpp"

namespace KVDK_NAMESPACE {

struct DLRecord;
struct StringRecord;

constexpr MemoryOffsetType kNullMemoryOffset = UINT64_MAX;

struct SpaceEntry {
  class SpaceCmp {
   public:
    bool operator()(const SpaceEntry& s1, const SpaceEntry& s2) const {
      if (s1.size > s2.size) return true;
      if (s1.size == s2.size && s1.offset < s2.offset) return true;
      return false;
    }
  };
  SpaceEntry() = default;

  SpaceEntry(uint64_t _offset, uint64_t _size) : offset(_offset), size(_size) {}
  uint64_t offset;
  uint64_t size = 0;
};

class Allocator {
 public:
  virtual SpaceEntry Allocate(uint64_t size) = 0;
  virtual SpaceEntry AllocateAligned(size_t alignment, uint64_t size) = 0;
  virtual void Free(const SpaceEntry& entry) = 0;
  virtual std::string AllocatorName() = 0;

  virtual void BatchFree(const std::vector<SpaceEntry>& entries) {
    for (const SpaceEntry& entry : entries) {
      this->Free(entry);
    }
  }

  Allocator(char* base_addr, uint64_t max_offset)
      : base_addr_(base_addr), max_offset_(max_offset) {}

  // Purge a kvdk data record and free it
  template <typename T>
  void PurgeAndFree(T* data_record) {
    static_assert(std::is_same<T, DLRecord>::value ||
                      std::is_same<T, StringRecord>::value,
                  "");
    data_record->Destroy();
    Free(SpaceEntry(addr2offset_checked(data_record),
                    data_record->GetRecordSize()));
  }

  /**
   * @brief Enable thread local memory usage counters.
   * This helps to avoid possible bottleneck when multiple threads
   * access the std::atomic `global_allocated_size_`.
   *
   * @param max_access_threads
   * @return true Success
   * @return false Failure
   */
  bool EnableThreadLocalCounters(uint32_t max_access_threads) {
    if (max_access_threads <= 1) {
      return true;
    }

    // Should not be enabled more than once
    if (!thread_local_counter_enabled_) {
      std::lock_guard<std::mutex> lg(allocator_mu_);
      if (!thread_local_counter_enabled_) {
        thread_local_counter_enabled_ = true;
        thread_allocated_sizes_.resize(max_access_threads);
        return true;
      }
    }

    return false;
  }

  void LogAllocation(int tid, size_t sz) {
    if (tid == -1 || !thread_local_counter_enabled_) {
      global_allocated_size_.fetch_add(sz);
    } else {
      assert(tid >= 0);
      thread_allocated_sizes_[tid] += sz;
    }
  }

  void LogDeallocation(int tid, size_t sz) {
    if (tid == -1 || !thread_local_counter_enabled_) {
      global_allocated_size_.fetch_sub(sz);
    } else {
      assert(tid >= 0);
      thread_allocated_sizes_[tid] -= sz;
    }
  }

  std::int64_t BytesAllocated() const {
    std::int64_t total = 0;
    for (auto const& thread_size : thread_allocated_sizes_) {
      total += thread_size;
    }
    total += global_allocated_size_.load();
    return total;
  }

  // translate offset of allocated space to address
  inline void* offset2addr_checked(MemoryOffsetType offset) const {
    assert(validate_offset(offset) && "Trying to access invalid offset");
    return base_addr_ + offset;
  }

  inline void* offset2addr(MemoryOffsetType offset) const {
    if (validate_offset(offset)) {
      return base_addr_ + offset;
    }
    return nullptr;
  }

  template <typename T>
  inline T* offset2addr_checked(MemoryOffsetType offset) const {
    return static_cast<T*>(offset2addr_checked(offset));
  }

  template <typename T>
  inline T* offset2addr(MemoryOffsetType offset) const {
    return static_cast<T*>(offset2addr(offset));
  }

  // translate address of allocated space to offset
  inline MemoryOffsetType addr2offset_checked(const void* addr) const {
    assert((char*)addr >= base_addr_);
    MemoryOffsetType offset = (char*)addr - base_addr_;
    assert(validate_offset(offset) && "Trying to create invalid offset");
    return offset;
  }

  inline MemoryOffsetType addr2offset(const void* addr) const {
    if (addr) {
      MemoryOffsetType offset = (char*)addr - base_addr_;
      if (validate_offset(offset)) {
        return offset;
      }
    }
    return kNullMemoryOffset;
  }

  inline bool validate_offset(uint64_t offset) const {
    return offset < max_offset_;
  }

 private:
  char* base_addr_;
  uint64_t max_offset_;

  bool thread_local_counter_enabled_{};
  std::vector<std::int64_t> thread_allocated_sizes_;
  std::atomic<int64_t> global_allocated_size_{0};

  std::mutex allocator_mu_;
};

// Global default memory allocator
Allocator* global_memory_allocator();

/// Caution: AlignedPoolAllocator is not thread-safe
template <typename T>
class AlignedPoolAllocator {
  static_assert(alignof(T) <= 1024,
                "Alignment greater than 1024B not supported");

 public:
  using value_type = T;

  explicit inline AlignedPoolAllocator() : pools_{}, pos_{TrunkSize} {}

  inline AlignedPoolAllocator(AlignedPoolAllocator const&)
      : AlignedPoolAllocator{} {}
  AlignedPoolAllocator(AlignedPoolAllocator&&) = delete;
  ~AlignedPoolAllocator() {
    for (auto p : pools_) {
      free(p);
    }
  }

  inline T* allocate(size_t n) {
    if (pools_.capacity() < 64) {
      pools_.reserve(64);
    }

    if (pos_ + n <= TrunkSize) {
      size_t old_pos = pos_;
      pos_ += n;
      return &pools_.back()[old_pos];
    } else if (n <= TrunkSize) {
      allocate_trunk();
      pos_ = n;
      return &pools_.back()[0];
    } else {
      allocate_trunk(n);
      pos_ = TrunkSize;
      return &pools_.back()[0];
    }
  }

  inline void deallocate(T*, size_t) noexcept { return; }

 private:
  inline void allocate_trunk(size_t sz = TrunkSize) {
    pools_.emplace_back(
        static_cast<T*>(aligned_alloc(alignof(T), sizeof(T) * sz)));
    if (pools_.back() == nullptr || alignof(pools_.back()[0]) != alignof(T)) {
      throw std::bad_alloc{};
    }
  }

  static constexpr size_t TrunkSize = 1024;
  std::vector<T*> pools_;
  size_t pos_;
};

// Thread safety guaranteed by aligned_alloc
template <typename T>
class AlignedAllocator {
 public:
  using value_type = T;

  inline T* allocate(size_t n) {
    static_assert(sizeof(T) % alignof(T) == 0);
    T* p = static_cast<T*>(aligned_alloc(alignof(T), n * sizeof(T)));
    if (p == nullptr) {
      throw std::bad_alloc{};
    }
    return p;
  }

  inline void deallocate(T* p, size_t) noexcept { free(p); }
};

template <typename T>
class Array {
 public:
  template <typename... Args>
  explicit Array(uint64_t size, Args&&... args) : size_(size) {
    static_assert(sizeof(T) % alignof(T) == 0);
    allocated = alloc_->AllocateAligned(alignof(T), size * sizeof(T));
    if (allocated.size == 0) {
      throw std::bad_alloc{};
    }

    data_ = alloc_->offset2addr<T>(allocated.offset);
    for (uint64_t i = 0; i < size; i++) {
      new (data_ + i) T{std::forward<Args>(args)...};
    }
  }

  Array(const Array&) = delete;
  Array& operator=(const Array&) = delete;
  Array(Array&&) = delete;

  Array() : size_(0), data_(nullptr){};

  ~Array() {
    if (data_ != nullptr) {
      for (uint64_t i = 0; i < size_; i++) {
        data_[i].~T();
      }
      alloc_->Free(allocated);
    }
  }

  T& back() {
    assert(size_ > 0);
    return data_[size_ - 1];
  }

  T& front() {
    assert(size_ > 0);
    return data_[0];
  }

  T& operator[](uint64_t index) {
    if (index >= size_) {
      throw std::out_of_range("array out of range");
    }
    return data_[index];
  }

  uint64_t size() { return size_; }

 private:
  uint64_t const size_;
  T* data_{nullptr};
  Allocator* alloc_ = global_memory_allocator();
  SpaceEntry allocated;
};

}  // namespace KVDK_NAMESPACE