/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <emmintrin.h>
#include <smmintrin.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <x86intrin.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <vector>

#define XXH_INLINE_ALL
#include "xxhash.h"
#undef XXH_INLINE_ALL

#include "../alias.hpp"
#include "../macros.hpp"
#include "codec.hpp"

namespace KVDK_NAMESPACE {

// A tool struct that call f on destroy
// use the macro defer(code) to do so
template <typename F>
struct Defer {
  F f;
  Defer(F f) : f(f) {}
  ~Defer() { f(); }
};
template <typename F>
Defer<F> defer_func(F f) {
  return Defer<F>(f);
}
#define DEFER_1(x, y) x##y
#define DEFER_2(x, y) DEFER_1(x, y)
#define DEFER_3(x) DEFER_2(x, __COUNTER__)
#define defer(code) auto DEFER_3(_defer_) = defer_func([&]() { code; })

inline void atomic_memcpy_16(void* dst, void* src) {
  ((std::atomic<__uint128_t>*)dst)
      ->store(((std::atomic<__uint128_t>*)src)->load());
}

inline uint64_t hash_str(const char* str, uint64_t size) {
  return XXH3_64bits(str, size);
}

inline uint64_t get_checksum(const void* data, uint64_t size) {
  return XXH3_64bits(data, size);
}

inline unsigned long long get_seed() {
  unsigned long long seed = 0;
  while (seed == 0 && _rdseed64_step(&seed) != 1) {
  }
  return seed;
}

inline uint64_t fast_random_64() {
  static std::mt19937_64 generator;
  thread_local uint64_t seed = 0;
  if (seed == 0) {
    seed = generator();
  }
  uint64_t x = seed; /* The state must be seeded with a nonzero value. */
  x ^= x >> 12;      // a
  x ^= x << 25;      // b
  x ^= x >> 27;      // c
  seed = x;
  return x * 0x2545F4914F6CDD1D;
}

inline uint64_t rdtsc() {
  uint32_t lo, hi;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((uint64_t)lo) | (((uint64_t)hi) << 32);
}

inline void atomic_load_16(void* dst, const void* src) {
  (*(__uint128_t*)dst) = __atomic_load_16(src, std::memory_order_relaxed);
}

inline void atomic_store_16(void* dst, const void* src) {
  __atomic_store_16(dst, (*(__uint128_t*)src), std::memory_order_relaxed);
}

inline void memcpy_16(void* dst, const void* src) {
  __m128i m0 = _mm_loadu_si128(((const __m128i*)src) + 0);
  _mm_storeu_si128(((__m128i*)dst) + 0, m0);
}

inline void memcpy_8(void* dst, const void* src) {
  *((uint64_t*)dst) = *((uint64_t*)src);
}

inline void memcpy_4(void* dst, const void* src) {
  *((uint32_t*)dst) = *((uint32_t*)src);
}

inline void memcpy_2(void* dst, const void* src) {
  *((uint16_t*)dst) = *((uint16_t*)src);
}

inline void memcpy_1(void* dst, const void* src) {
  *((uint8_t*)dst) = *((uint8_t*)src);
}

inline std::string format_dir_path(const std::string& dir) {
  return dir.back() == '/' ? dir : dir + "/";
}

inline bool file_exist(const std::string& name) {
  bool exist = access(name.c_str(), 0) == 0;
  return exist;
}

inline int create_dir_if_missing(const std::string& name) {
  int res = mkdir(name.c_str(), 0755) != 0;
  if (res != 0) {
    if (errno != EEXIST) {
      return res;
    } else {
      struct stat s;
      if (stat(name.c_str(), &s) == 0) {
        return S_ISDIR(s.st_mode) ? 0 : res;
      }
    }
  }
  return res;
}

inline std::string string_view_2_string(const StringView& src) {
  return std::string(src.data(), src.size());
}

inline int compare_string_view(const StringView& src,
                               const StringView& target) {
  auto size = std::min(src.size(), target.size());
  for (uint32_t i = 0; i < size; i++) {
    if (src[i] != target[i]) {
      return src[i] - target[i];
    }
  }
  return src.size() - target.size();
}

inline bool equal_string_view(const StringView& src, const StringView& target) {
  if (src.size() == target.size()) {
    return compare_string_view(src, target) == 0;
  }
  return false;
}

class SpinMutex {
 private:
  std::atomic_flag locked = ATOMIC_FLAG_INIT;

 public:
  SpinMutex() = default;

  void lock() {
    while (locked.test_and_set(std::memory_order_acquire)) {
      asm volatile("pause");
    }
  }

  void unlock() { locked.clear(std::memory_order_release); }

  bool try_lock() {
    if (locked.test_and_set(std::memory_order_acquire)) {
      return false;
    }
    return true;
  }

  SpinMutex(const SpinMutex& s) = delete;
  SpinMutex(SpinMutex&& s) = delete;
  SpinMutex& operator=(const SpinMutex& s) = delete;
};

class RWLock {
  static constexpr std::int64_t reader_val{1};
  static constexpr std::int64_t writer_val{
      std::numeric_limits<std::int64_t>::min()};

  // device.load() > 0 indicates only reader exists
  // device.load() == 0 indicates no reader and no writer
  // device.load() == writer_val indicates only writer exists, block readers
  // Otherwise, writer has registered and is waiting for readers to leave
  std::atomic_int64_t device{0};

 public:
  bool try_lock_shared() {
    std::int64_t old = device.load();
    if (old < 0 || !device.compare_exchange_strong(old, old + reader_val)) {
      // Other writer has acquired lock.
      return false;
    }
    return true;
  }

  void lock_shared() {
    while (!try_lock_shared()) {
      pause();
    }
    return;
  }

  void unlock_shared() {
    device.fetch_sub(reader_val);
    return;
  }

  bool try_lock() {
    std::int64_t old = device.load();
    if (old < 0 || !device.compare_exchange_strong(old, old + writer_val)) {
      // Other writer has acquired lock.
      return false;
    }
    while (device.load() != writer_val) {
      // Block until all readers have left.
      pause();
    }
    return true;
  }

  void lock() {
    while (!try_lock()) {
      pause();
    }
    return;
  }

  void unlock() {
    device.fetch_sub(writer_val);
    return;
  }

 private:
  void pause() {
    for (size_t i = 0; i < 64; i++) {
      _mm_pause();
    }
  }
};

/// Caution: AlignedPoolAllocator is not thread-safe
template <typename T>
class AlignedPoolAllocator {
  static_assert(alignof(T) <= 1024,
                "Alignment greater than 1024B not supported");

 private:
  static constexpr size_t TrunkSize = 1024;

  std::vector<T*> pools_;
  size_t pos_;

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

template <typename T, typename Alloc = AlignedAllocator<T>>
class Array {
 public:
  template <typename... Args>
  explicit Array(uint64_t size, Args&&... args) : size_(size) {
    data_ = alloc_.allocate(size_);
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
      alloc_.deallocate(data_, size_);
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
  Alloc alloc_{};
};

class Slice {
 public:
  Slice() : _data(nullptr), _size(0) {}
  Slice(const char* data) : _data(data) { _size = strlen(_data); }
  Slice(const char* data, uint64_t size) : _data(data), _size(size) {}

  Slice(const std::string& str) : _data(str.data()), _size(str.size()) {}
  Slice(const StringView& sv) : _data(sv.data()), _size(sv.size()) {}

  const char* data() const { return _data; }

  uint64_t& size() { return _size; }

  uint64_t size() const { return _size; }

  bool operator==(const Slice& b) {
    if (b.size() == this->_size &&
        memcmp(this->_data, b.data(), b.size()) == 0) {
      return true;
    } else {
      return false;
    }
  }

  static int compare(const Slice& src, const Slice& target) {
    auto size = std::min(src.size(), target.size());
    for (uint32_t i = 0; i < size; i++) {
      if (src.data()[i] != target.data()[i]) {
        return src.data()[i] - target.data()[i];
      }
    }
    return src.size() - target.size();
  }

  std::string to_string() { return std::string(_data, _size); }

  std::string to_string() const { return std::string(_data, _size); }

 private:
  const char* _data;
  uint64_t _size;
};

template <typename T>
void compare_excange_if_larger(std::atomic<T>& num, T target) {
  while (true) {
    T n = num.load(std::memory_order_relaxed);
    if (n <= target) {
      if (!num.compare_exchange_strong(n, target)) {
        continue;
      }
    }
    break;
  }
}

// Return the number of process unit (PU) that are bound to the kvdk instance
int get_usable_pu(void);

namespace TimeUtils {
/* Return the UNIX time in microseconds */
inline UnixTimeType unix_time(void) {
  struct timeval tv;
  UnixTimeType ust;

  gettimeofday(&tv, NULL);
  ust = ((UnixTimeType)tv.tv_sec) * 1000000;
  ust += tv.tv_usec;
  return ust;
}

inline int64_t millisecond_time() { return unix_time() / 1000; }

inline bool CheckIsExpired(ExpireTimeType expired_time) {
  if (expired_time >= 0 && expired_time <= millisecond_time()) {
    return true;
  }
  return false;
}

inline bool CheckTTL(TTLType ttl_time, UnixTimeType base_time) {
  if (ttl_time != kPersistTime &&
      /* check overflow*/ ttl_time > INT64_MAX - base_time) {
    return false;
  }
  return true;
}

inline ExpireTimeType TTLToExpireTime(
    TTLType ttl_time, UnixTimeType base_time = millisecond_time()) {
  return ttl_time == kPersistTime ? kPersistTime : ttl_time + base_time;
}

inline TTLType ExpireTimeToTTL(ExpireTimeType expire_time,
                               UnixTimeType base_time = millisecond_time()) {
  return expire_time == kPersistTime ? kPersistTime : expire_time - base_time;
}

}  // namespace TimeUtils
}  // namespace KVDK_NAMESPACE
