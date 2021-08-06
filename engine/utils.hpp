/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#define XXH_INLINE_ALL

#include <assert.h>
#include <atomic>
#include <cstdint>
#include <emmintrin.h>
#include <random>
#include <smmintrin.h>
#include <string>
#include <sys/stat.h>
#include <sys/time.h>
#include <vector>

#include "kvdk/namespace.hpp"
#include "xxhash.h"

namespace KVDK_NAMESPACE {

#define NULL_PMEM_OFFSET UINT64_MAX
#define MAX_WRITE_BATCH_SIZE (1 << 20)
#define MAX_SKIPLIST_LEVEL 32
#define FREE_LIST_MAX_BLOCK 255
#define FREE_SPACE_PADDING_BLOCK 8

inline uint64_t hash_str(const char *str, uint64_t size) {
  return XXH3_64bits(str, size);
}

inline uint64_t get_checksum(const void *data, uint64_t size) {
  return XXH3_64bits(data, size);
}

inline uint64_t fast_random() {
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

inline int memcmp_16(const void *a, const void *b) {
  register __m128i xmm0, xmm1;
  xmm0 = _mm_loadu_si128((__m128i *)(a));
  xmm1 = _mm_loadu_si128((__m128i *)(b));
  __m128i diff = _mm_xor_si128(xmm0, xmm1);
  if (_mm_testz_si128(diff, diff))
    return 0; // equal
  else
    return 1; // non-equal
}

inline void memcpy_16(void *dst, const void *src) {
  __m128i m0 = _mm_loadu_si128(((const __m128i *)src) + 0);
  _mm_storeu_si128(((__m128i *)dst) + 0, m0);
}

inline void memcpy_8(void *dst, const void *src) {
  *((uint64_t *)dst) = *((uint64_t *)src);
}

inline void memcpy_4(void *dst, const void *src) {
  *((uint32_t *)dst) = *((uint32_t *)src);
}

inline void memcpy_2(void *dst, const void *src) {
  *((uint16_t *)dst) = *((uint16_t *)src);
}

inline void memcpy_1(void *dst, const void *src) {
  *((uint8_t *)dst) = *((uint8_t *)src);
}

inline std::string format_dir_path(const std::string &dir) {
  return dir.back() == '/' ? dir : dir + "/";
}

inline int create_dir_if_missing(const std::string &name) {
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

class Slice {
public:
  Slice() : _data(nullptr), _size(0) {}
  Slice(const char *data) : _data(data) { _size = strlen(_data); }
  Slice(const char *data, uint64_t size) : _data(data), _size(size) {}

  Slice(const std::string &str) : _data(str.data()), _size(str.size()) {}

  const char *data() const { return _data; }

  uint64_t &size() { return _size; }

  uint64_t size() const { return _size; }

  bool operator==(const Slice &b) {
    if (b.size() == this->_size &&
        memcmp(this->_data, b.data(), b.size()) == 0) {
      return true;
    } else {
      return false;
    }
  }

  static int compare(const Slice &src, const Slice &target) {
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
  const char *_data;
  uint64_t _size;
};

template <typename T>
void compare_excange_if_larger(std::atomic<T> &num, T target) {
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

class SpinMutex {
private:
  std::atomic_flag locked = ATOMIC_FLAG_INIT;
  //  int owner = -1;

public:
  void lock() {
    while (locked.test_and_set(std::memory_order_acquire)) {
      asm volatile("pause");
    }
    //    owner = local_thread.id;
  }

  void unlock() {
    //    owner = -1;
    locked.clear(std::memory_order_release);
  }

  bool try_lock() {
    if (locked.test_and_set(std::memory_order_acquire)) {
      return false;
    }
    //    owner = local_thread.id;
    return true;
  }

  //  bool hold() { return owner == local_thread.id; }

  SpinMutex(const SpinMutex &s) : locked(ATOMIC_FLAG_INIT) {}

  SpinMutex(const SpinMutex &&s) : locked(ATOMIC_FLAG_INIT) {}

  SpinMutex() : locked(ATOMIC_FLAG_INIT) {}
};
} // namespace KVDK_NAMESPACE