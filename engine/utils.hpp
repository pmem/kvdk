/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <assert.h>
#include <emmintrin.h>
#include <random>
#include <smmintrin.h>
#include <string>
#include <sys/time.h>

#include "pmemdb/namespace.hpp"
#include <atomic>
#include <vector>

#define XXH_INLINE_ALL
#include "xxhash.h"

#define DO_LOG 1
// #define DO_STATS 1

namespace PMEMDB_NAMESPACE {

extern thread_local int t_id;

enum OP_CODE : unsigned char {
  OP_SET,
  OP_GET,
  OP_DELETE,
};

enum DATA_ENTRY_TYPE : unsigned char {
  HASH_DATA_RECORD = 1,
  HASH_DELETE_RECORD = 1 << 1,
  SORTED_DATA_RECORD = 1 << 2,
  SORTED_DELETE_RECORD = 1 << 3,
  SORTED_HEADER_RECORD = 1 << 4,
};

#define NULL_PMEM_OFFSET UINT64_MAX

#define MAX_SKIPLIST_LEVEL 32

#define DRAM_CHUNKS_ALLOCATION_NUM (1 << 16)

#define PMEM_HEADER_SIZE 8 // // high | checksum(4B) | b_size(4B) | low
#define PMEM_FREE_LIST_SLOT_NUM 255

inline uint64_t hash_key(const char *key, uint32_t key_size) {
  return XXH3_64bits(key, key_size);
}

inline uint64_t get_checksum(const void *value, uint16_t v_size,
                             uint64_t key_hash_value) {
  return XXH3_64bits_withSeed(value, v_size, key_hash_value);
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

inline void memcpy_6(void *dst, const void *src) {
  unsigned char *dd = ((unsigned char *)dst) + 6;
  const unsigned char *ss = ((const unsigned char *)src) + 6;
  *((uint32_t *)(dd - 6)) = *((uint32_t *)(ss - 6));
  *((uint16_t *)(dd - 2)) = *((uint16_t *)(ss - 2));
}

inline void memcpy_4(void *dst, const void *src) {
  *((uint32_t *)dst) = *((uint32_t *)src);
}

inline void memcpy_2(void *dst, const void *src) {
  *((uint16_t *)dst) = *((uint16_t *)src);
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
    //    uint64_t k;
    //    uint64_t t;
    //    memcpy_8(&k, src.data());
    //    memcpy_8(&t, target.data());
    //    fprintf(stderr, "compare %lu and %lu\n", k, t);
    //    return k<t?-1:k>t?1:0;
    //    return src.to_string().compare(target.to_string());
    ///*
    uint32_t size = std::min(src.size(), target.size());
    for (uint32_t i = 0; i < size; i++) {
      if (src.data()[i] < target.data()[i]) {
        return -1;
      }
      if (src.data()[i] > target.data()[i]) {
        return 1;
      }
    }

    return src.size() == size ? 0 : src.size() > size ? 1 : -1;
    //    */
  }

  std::string to_string() { return std::string(_data, _size); }

  std::string to_string() const { return std::string(_data, _size); }

private:
  const char *_data;
  uint64_t _size;
};

class SpinMutex {
private:
  std::atomic_flag locked = ATOMIC_FLAG_INIT;
  //  int owner = -1;

public:
  void lock() {
    while (locked.test_and_set(std::memory_order_acquire)) {
      asm volatile("pause");
    }
    //    owner = t_id;
  }

  void unlock() {
    //    owner = -1;
    locked.clear(std::memory_order_release);
  }

  bool try_lock() {
    if (locked.test_and_set(std::memory_order_acquire)) {
      return false;
    }
    //    owner = t_id;
    return true;
  }

  //  bool hold() { return owner == t_id; }

  SpinMutex(const SpinMutex &s) : locked(ATOMIC_FLAG_INIT) {}

  SpinMutex(const SpinMutex &&s) : locked(ATOMIC_FLAG_INIT) {}

  SpinMutex() : locked(ATOMIC_FLAG_INIT) {}
};

#ifdef DO_STATS

class StopWatch {
public:
  StopWatch(uint64_t &s) : stats(s) { timer.Start(); }
  ~StopWatch() { stats += timer.End(); }

private:
  Timer timer;
  uint64_t &stats;
};

struct Stats {
  uint64_t get_pmem{0};
  uint64_t get_offset{0};
  uint64_t get_value{0};
  uint64_t get_lru{0};
  uint64_t search_hash_in_get{0};
  uint64_t set_lru_hash_table_in_get{0};
  uint64_t search_lru_hash_table_in_get{0};
  uint64_t set_nvm{0};
  uint64_t set_lru{0};
  uint64_t set_lru_hash_table_in_set{0};
  uint64_t set_pmem{0};
  uint64_t write_value{0};
  uint64_t search_hash_in_set{0};
  uint64_t search_free_list{0};
  void Print();
};

#endif
} // namespace PMEMDB_NAMESPACE