/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <atomic>
#include <cstdio>
#include <limits>
#include <vector>

#include "data_entry.hpp"
#include "dram_allocator.hpp"
#include "kvdk/engine.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "structures.hpp"

namespace KVDK_NAMESPACE {
enum class HashEntryStatus : uint8_t {
  // Hash entry in hash table should always being initialized, it should never
  // be 0
  Invalid = 0,
  Normal = 1,
  // New created hash entry for inserting a new key
  Initializing = 1 << 1,
  // A entry being updated by the same key, or a CleanReusable hash entry being
  // updated by a new key
  Updating = 1 << 2,
  // A Normal hash entry of a delete record that is reusing by a new key, it's
  // unknown if there are older version data of the same key existing so we can
  // not free corresponding PMem data entry
  DirtyReusable = 1 << 3,
  // A hash entry of a delete record which has no older version data of the same
  // key exsiting on PMem, so the delete record can be safely freed after the
  // hash entry updated by a new key
  CleanReusable = 1 << 4,
  // A empty hash entry that points to nothing
  Empty = 1 << 5
};

enum class HashOffsetType : uint8_t {
  // Value initialized considered as Invalid
  Invalid = 0,
  // Offset is PMem offset of a data entry
  DataEntry = 1,
  // Offset is PMem offset of a double linked data entry
  DLDataEntry = 2,
  // Offset is pointer to a dram skiplist node
  SkiplistNode = 3,
  // Offset is pointer to a dram skiplist struct
  Skiplist = 4,
  // Offset field contains pointer to UnorderedCollection object on DRAM
  UnorderedCollection = 5,
  // Offset field contains PMem pointer to element of UnorderedCollection
  UnorderedCollectionElement = 6
};

struct HashHeader {
  uint32_t key_prefix;
  uint16_t data_type;
  HashOffsetType offset_type;
  HashEntryStatus status;
};

class UnorderedCollection;

struct HashEntry {
public:
  HashEntry() = default;

  HashEntry(uint32_t key_hash_prefix, uint16_t data_entry_type, uint64_t offset,
            HashOffsetType offset_type)
      : header({key_hash_prefix, data_entry_type, offset_type,
                HashEntryStatus::Normal}),
        offset(offset) {}

  HashEntry(uint32_t kp, uint16_t t, uint64_t offset, HashEntryStatus status,
            HashOffsetType offset_type)
      : header({kp, t, offset_type, status}), offset(offset) {}

  HashHeader header;
  union {
    uint64_t offset;
    UnorderedCollection *p_unordered_collection;
  };

  static void CopyHeader(HashEntry *dst, HashEntry *src) { memcpy_8(dst, src); }
  static void CopyOffset(HashEntry *dst, HashEntry *src) {
    dst->offset = src->offset;
  }

  bool Reusable() {
    return (uint8_t)header.status & ((uint8_t)HashEntryStatus::CleanReusable |
                                     (uint8_t)HashEntryStatus::DirtyReusable |
                                     (uint8_t)HashEntryStatus::Empty);
  }

  bool Empty() { return header.status == HashEntryStatus::Empty; }

  // Make this hash entry reusable while its content been deleted
  void Clear() { header.status = HashEntryStatus::Empty; }
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

  static HashTable *
  NewHashTable(uint64_t hash_bucket_num, uint32_t hash_bucket_size,
               uint32_t num_buckets_per_slot,
               const std::shared_ptr<PMEMAllocator> &pmem_allocator,
               uint32_t write_threads);

  KeyHashHint GetHint(const pmem::obj::string_view &key) {
    KeyHashHint hint;
    hint.key_hash_value = hash_str(key.data(), key.size());
    hint.bucket = get_bucket_num(hint.key_hash_value);
    hint.slot = get_slot_num(hint.bucket);
    hint.spin = &slots_[hint.slot].spin;
    return hint;
  }

  // Search key in hash table for read operations
  //
  // type_mask: which data types to search
  // entry_base: store hash entry position of "key" if found
  // hash_entry_snap: store a hash entry copy of searching key for lock-free
  // read, as hash entry maybe modified by write operations
  // data_entry_meta: store a copy of data entry metadata part of searching key
  Status SearchForRead(const KeyHashHint &hint,
                       const pmem::obj::string_view &key, uint16_t type_mask,
                       HashEntry **entry_base, HashEntry *hash_entry_snap,
                       DataEntry *data_entry_meta);

  // Search key in hash table for write operations
  //
  // type_mask: which data types to search
  // entry_base: store hash entry position to write. It's either hash entry
  // position of "key" to update if it's existing, or a clear position to insert
  // new hash entry
  // hash_entry_snap: store a hash entry copy of searching key
  // data_entry_meta: store a copy of data entry metadata part of searching key
  // in_recovery: whether called during recovery of kvdk instance
  Status SearchForWrite(const KeyHashHint &hint,
                        const pmem::obj::string_view &key, uint16_t type_mask,
                        HashEntry **entry_base, HashEntry *hash_entry_snap,
                        DataEntry *data_entry_meta, bool in_recovery = false);

  // Insert a hash entry to hash table
  //
  // entry_base: position to insert, it's get from SearchForWrite()
  void Insert(const KeyHashHint &hint, HashEntry *entry_base, uint16_t type,
              uint64_t offset, HashOffsetType offset_type);

private:
  HashTable(uint64_t hash_bucket_num, uint32_t hash_bucket_size,
            uint32_t num_buckets_per_slot,
            const std::shared_ptr<PMEMAllocator> &pmem_allocator,
            uint32_t write_threads)
      : hash_bucket_num_(hash_bucket_num),
        num_buckets_per_slot_(num_buckets_per_slot),
        hash_bucket_size_(hash_bucket_size),
        dram_allocator_(ChunkBasedAllocator(write_threads)),
        pmem_allocator_(pmem_allocator),
        num_entries_per_bucket_((hash_bucket_size_ - 8 /* next pointer */) /
                                sizeof(HashEntry)),
        slots_(hash_bucket_num / num_buckets_per_slot),
        hash_bucket_entries_(hash_bucket_num, 0) {}

  inline uint32_t get_bucket_num(uint64_t key_hash_value) {
    return key_hash_value & (hash_bucket_num_ - 1);
  }

  inline uint32_t get_slot_num(uint32_t bucket) {
    return bucket / num_buckets_per_slot_;
  }

  // Check if "key" of data type "target_type" is indexed by "hash_entry". If
  // matches, copy metadata of data entry of "key" to "data_entry_metadata" and
  // return true, otherwise return false.
  bool MatchHashEntry(const pmem::obj::string_view &key, uint32_t hash_k_prefix,
                      uint16_t target_type, const HashEntry *hash_entry,
                      void *data_entry_metadata);

  std::vector<uint64_t> hash_bucket_entries_;
  const uint64_t hash_bucket_num_;
  const uint32_t num_buckets_per_slot_;
  const uint32_t hash_bucket_size_;
  const uint64_t num_entries_per_bucket_;
  std::vector<Slot> slots_;
  std::shared_ptr<PMEMAllocator> pmem_allocator_;
  ChunkBasedAllocator dram_allocator_;
  char *main_buckets_;
};
} // namespace KVDK_NAMESPACE
