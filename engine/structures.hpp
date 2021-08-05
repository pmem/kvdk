/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "logger.hpp"
#include "pmemdb/namespace.hpp"
#include "safestringlib/include/safe_lib.h"
#include "utils.hpp"

namespace PMEMDB_NAMESPACE {

struct DataEntry {
  DataEntry(uint32_t c, uint32_t bs, uint64_t v, uint8_t t, uint16_t ks,
            uint32_t vs)
      : checksum(c), b_size(bs), version(v), type(t), k_size(ks), v_size(vs) {}
  DataEntry() = default;
  // header, it can be atomically written to pmem
  uint32_t checksum;
  uint32_t b_size;
  // meta
  uint64_t version;
  uint8_t type;
  uint16_t k_size;
  uint32_t v_size;

  uint32_t Checksum(uint64_t hash_value) {
    return get_checksum(&version,
                        sizeof(DataEntry) - PMEM_HEADER_SIZE + k_size + v_size,
                        hash_value);
  }

  // make sure there is data followed
  Slice Key() { return Slice((char *)this + sizeof(DataEntry), k_size); }

  Slice Value() {
    return Slice((char *)this + sizeof(DataEntry) + k_size, v_size);
  }
};

struct SortedDataEntry : public DataEntry {
  SortedDataEntry() = default;
  SortedDataEntry(uint32_t c, uint32_t bs, uint64_t v, uint8_t t, uint16_t ks,
                  uint32_t vs, uint64_t pr, uint64_t ne)
      : DataEntry(c, bs, v, t, ks, vs), prev(pr), next(ne) {}

  uint64_t prev;
  uint64_t next;

  uint32_t Checksum(uint64_t hash_value) {
    return get_checksum(&version, sizeof(DataEntry) - PMEM_HEADER_SIZE,
                        hash_value) +
           get_checksum(this + 1, v_size + k_size, hash_value);
  }

  // make sure there is data followed
  Slice Key() { return Slice((char *)this + sizeof(SortedDataEntry), k_size); }

  Slice Value() {
    return Slice((char *)this + sizeof(SortedDataEntry) + k_size, v_size);
  }
};

static uint64_t data_entry_size(uint8_t type) {
  if (type &
      (SORTED_DATA_RECORD | SORTED_DELETE_RECORD | SORTED_HEADER_RECORD)) {
    return sizeof(SortedDataEntry);
  } else {
    return sizeof(DataEntry);
  }
}

struct HashEntry {
  HashEntry() = default;
  HashEntry(uint32_t kp, uint16_t r, uint8_t t, uint64_t bo)
      : key_prefix(kp), reference(r), type(t), offset(bo) {}

  uint32_t key_prefix;
  uint16_t reference;
  uint8_t type;
  uint64_t offset;

  static void CopyHeader(HashEntry *dst, HashEntry *src) {
    memcpy_s(dst, sizeof(HashEntry), src, 8);
  }

  static void CopyOffset(HashEntry *dst, HashEntry *src) {
    dst->offset = src->offset;
  }
};

struct HashCache {
  HashEntry *entry_base = nullptr;
};

struct Slot {
  HashCache hash_cache;
  SpinMutex spin;
};

// Free pmem blocks
struct SpaceEntry {
  int64_t block_offset = -1;
  HashEntry *hash_entry_reference = nullptr;
  SpinMutex *hash_entry_mutex = nullptr;

  SpaceEntry() = default;
  SpaceEntry(uint64_t bo, HashEntry *r, SpinMutex *mutex)
      : block_offset(bo), hash_entry_reference(r), hash_entry_mutex(mutex) {}

  void MaybeDeref() {
    if (hash_entry_reference != nullptr) {
      if (hash_entry_mutex != nullptr) {
        std::lock_guard<SpinMutex> lg(*hash_entry_mutex);
        hash_entry_reference->reference--;
      }
    }
  }
};

class FreeList {
public:
  FreeList(uint32_t max_b_size) : offsets(max_b_size) {}

  FreeList() : offsets(PMEM_FREE_LIST_SLOT_NUM) {}

  void Push(uint64_t b_off, uint32_t b_size, HashEntry *hash_entry_reference,
            SpinMutex *mutex) {
#ifdef DO_LOG
    if (b_size >= PMEM_FREE_LIST_SLOT_NUM) {
      // TODO: support large block
      GlobalLogger.Error("block size too large for the free list\n");
      exit(1);
    }
#endif
    offsets[b_size].emplace_back(b_off, hash_entry_reference, mutex);
  }

  Status Get(uint32_t &b_size, SpaceEntry *space_entry) {
    for (uint32_t i = b_size; i < PMEM_FREE_LIST_SLOT_NUM; i++) {
      if (offsets[i].size() != 0) {
        *space_entry = offsets[i].back();
        b_size = i;
        offsets[i].pop_back();
        return Status::Ok;
      }
    }
    return Status::NotFound;
  }

private:
  std::vector<std::vector<SpaceEntry>> offsets;
};

// should align to 8 bytes
struct ThreadSpace {
  uint64_t pmem_blocks_offset;
  uint64_t usable_blocks;
  uint64_t newest_restored_version = 0;
  uint32_t dram_chunks_offset;
  uint32_t usable_chunks = 0;
  FreeList free_list;

  char padding[64 - 32 - sizeof(FreeList)]; // align to 64 for cache coherence
};

struct Node {
  SortedDataEntry *data_entry; // data entry on pmem
  char key[8];
  std::atomic<Node *> next[0];

  Node *Next(int l) { return next[l].load(std::memory_order_acquire); }

  bool CASNext(int l, Node *expected, Node *x) {
    assert(l >= 0);
    return (next[l].compare_exchange_strong(expected, x));
  }

  Node *RelaxedNext(int l) { return next[l].load(std::memory_order_relaxed); }

  void SetNext(int l, Node *x) {
    assert(l >= 0);
    (&next[0] + l)->store(x, std::memory_order_release);
  }

  void RelaxedSetNext(int l, Node *x) {
    assert(l >= 0);
    next[l].store(x, std::memory_order_relaxed);
  }
};
} // namespace PMEMDB_NAMESPACE