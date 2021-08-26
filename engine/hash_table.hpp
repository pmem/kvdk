/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdio>
#include <vector>

#include "data_entry.hpp"
#include "dram_allocator.hpp"
#include "kvdk/engine.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "structures.hpp"

namespace KVDK_NAMESPACE {
enum class HashEntryStatus : uint16_t {
  Normal = 1,
  // A deletion type entry of a key which has no older data entry existing, so
  // it can be safely used by a new key
  Clean,
  // New created hash entry for inserting a new key
  Initializing,
  // A entry being updated by the same key, or a CLEAN entry being used by a new
  // key
  Updating,
  // A NORMAL deletion type entry that is reusing by a new key
  // TODO: handle recycle of delete record of reused hash entries in runtime
  BeingReused,
};

struct HashHeader {
  uint32_t key_prefix;
  uint16_t type;
  HashEntryStatus status;
};

struct HashEntry {
  HashEntry() = default;
  HashEntry(uint32_t kp, uint16_t t, uint64_t bo)
      : header({kp, t, HashEntryStatus::Normal}), offset(bo) {}

  HashHeader header;
  uint64_t offset;

  static void CopyHeader(HashEntry *dst, HashEntry *src) { memcpy_8(dst, src); }
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

class HashTable {
public:
  struct KeyHashHint {
    uint64_t key_hash_value;
    uint32_t bucket;
    uint32_t slot;
    SpinMutex *spin;
  };

  enum class SearchPurpose : uint8_t {
    Read = 0,
    // More read only purpose here

    Write,
    Recover,
    // More write purpose here
  };

  HashTable(uint64_t hash_bucket_num, uint32_t hash_bucket_size,
            uint32_t num_buckets_per_slot,
            const std::shared_ptr<PMEMAllocator> &pmem_allocator,
            uint32_t write_threads)
      : hash_bucket_num_(hash_bucket_num),
        num_buckets_per_slot_(num_buckets_per_slot),
        hash_bucket_size_(hash_bucket_size),
        dram_allocator_(new DRAMAllocator(write_threads)),
        pmem_allocator_(pmem_allocator),
        num_entries_per_bucket_((hash_bucket_size_ - 8 /* next pointer */) /
                                sizeof(HashEntry)) {
    main_buckets_ = dram_allocator_->offset2addr(
        (dram_allocator_->Allocate(hash_bucket_size * hash_bucket_num)
             .space_entry.offset));
    //    memset(main_buckets_, 0, hash_bucket_size * hash_bucket_num);
    slots_.resize(hash_bucket_num / num_buckets_per_slot);
    hash_bucket_entries_.resize(hash_bucket_num, 0);
  }

  KeyHashHint GetHint(const Slice &key) {
    KeyHashHint hint;
    hint.key_hash_value = hash_str(key.data(), key.size());
    hint.bucket = get_bucket_num(hint.key_hash_value);
    hint.slot = get_slot_num(hint.bucket);
    hint.spin = &slots_[hint.slot].spin;
    return hint;
  }

  Status Search(const KeyHashHint &hint, const Slice &key, uint16_t type_mask,
                HashEntry *hash_entry, DataEntry *data_entry,
                HashEntry **entry_base, SearchPurpose purpose);

  void Insert(const KeyHashHint &hint, HashEntry *entry_base, uint16_t type,
              uint64_t offset);

private:
  inline uint32_t get_bucket_num(uint64_t key_hash_value) {
    return key_hash_value & (hash_bucket_num_ - 1);
  }

  inline uint32_t get_slot_num(uint32_t bucket) {
    return bucket / num_buckets_per_slot_;
  }

  bool MatchHashEntry(const Slice &key, uint32_t hash_k_prefix,
                      uint16_t target_type, const HashEntry *hash_entry,
                      void *data_entry);

  std::vector<uint64_t> hash_bucket_entries_;
  const uint64_t hash_bucket_num_;
  const uint32_t num_buckets_per_slot_;
  const uint32_t hash_bucket_size_;
  const uint64_t num_entries_per_bucket_;
  std::vector<Slot> slots_;
  std::shared_ptr<PMEMAllocator> pmem_allocator_;
  std::unique_ptr<DRAMAllocator> dram_allocator_;
  char *main_buckets_;
};
} // namespace KVDK_NAMESPACE
