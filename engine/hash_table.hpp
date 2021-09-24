/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <atomic>
#include <cstdio>
#include <vector>
#include <limits>

#include "data_entry.hpp"
#include "dram_allocator.hpp"
#include "kvdk/engine.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "structures.hpp"

namespace KVDK_NAMESPACE {
enum class HashEntryStatus : uint8_t {
  Empty = 0,
  Normal = 1,
  // A hash entry of a delete record which has no older version data of the same
  // key exsiting on PMem, so the delete record can be safely freed after the
  // hash entry updated by a new key
  Clean,
  // New created hash entry for inserting a new key
  Initializing,
  // A entry being updated by the same key, or a CLEAN hash entry being updated
  // by a new key
  Updating,
  // A Normal hash entry of a delete record that is reusing by a new key
  BeingReused,
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

// Monitor to keep track of readers of a resource
// Multiple readers may hold the same resource
// When a writer comes, it may mark the resource as dirty
// This will preventing any future reader from acquiring the resource
// 
class RWMonitor
{
private:
  // _counter_ registers the number of readers
  std::atomic_int16_t _counter_;
  constexpr static int16_t int16_min = std::numeric_limits<std::int16_t>::min();

public:
  RWMonitor() : _counter_{0}
  {
  }

  // Add a reader to resource. 
  // If resource marked dirty, fail and return false.
  inline bool RegisterReader()
  {
    std::int16_t old = _counter_.fetch_add(1);
    if (old < 0)
    {
      UnregisterReader();
      return false;
    }
    else
    {
      return true;
    }
  }

  // Remove a reader from resource.
  inline void UnregisterReader()
  {
    _counter_.fetch_sub(1);
  }

  // Mark the resource dirty, prevent further reader from entering.
  inline void MarkDirty()
  {
    if(_counter_.load() >= 0)
      _counter_.fetch_add(int16_min);
  }

  // Register a writer only if all readers have left
  // and no writer have registered yet.
  // After first writer having registered,
  // Other writers are blocked out.
  inline bool RegisterWriter()
  {
    std::int16_t old = _counter_.load();
    if (old < 0)
    {
      // Already marked dirty, no more Readers can enter
      if (old == int16_min)
      {
        old = _counter_.fetch_add(1);
        if (old == int16_min)
        {
          // Successfully registered as writer
          return true;
        }
        else
        {
          // Resource acquired by other Writer
          return false;
        }
      } 
      else
      {
        // Resource not released by readers or already acquired by other writer
        return false;
      }     
    }
    else
    {
      // Resource not marked dirty first, no writer should register
      return false;
    }
  }
};
static_assert(sizeof(RWMonitor) == sizeof(std::int16_t));

struct HashHeader {
  uint32_t key_prefix;
  uint16_t data_type;
  HashOffsetType offset_type;
  HashEntryStatus status;
};

class UnorderedCollection;

struct HashEntry {
private: 

public:
  HashEntry() = default;
  HashEntry(uint32_t key_hash_prefix, uint16_t data_entry_type, uint64_t offset,
            HashOffsetType offset_type)
      : header({key_hash_prefix, data_entry_type, offset_type, HashEntryStatus::Normal}), offset(offset) {}

  HashHeader header;
  union
  {
    uint64_t offset;
    UnorderedCollection* p_uncoll;
  };

  

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
        dram_allocator_(new ChunkBasedAllocator(write_threads)),
        pmem_allocator_(pmem_allocator),
        num_entries_per_bucket_((hash_bucket_size_ - 8 /* next pointer */) /
                                sizeof(HashEntry)) {
    main_buckets_ = dram_allocator_->offset2addr(
        (dram_allocator_->Allocate(hash_bucket_size * hash_bucket_num)
             .space_entry.offset));
    slots_.resize(hash_bucket_num / num_buckets_per_slot);
    hash_bucket_entries_.resize(hash_bucket_num, 0);
  }

  KeyHashHint GetHint(const pmem::obj::string_view &key) {
    KeyHashHint hint;
    hint.key_hash_value = hash_str(key.data(), key.size());
    hint.bucket = get_bucket_num(hint.key_hash_value);
    hint.slot = get_slot_num(hint.bucket);
    hint.spin = &slots_[hint.slot].spin;
    return hint;
  }

  Status Search(const KeyHashHint &hint, const pmem::obj::string_view &key,
                uint16_t type_mask, HashEntry *hash_entry,
                DataEntry *data_entry, HashEntry **entry_base,
                SearchPurpose purpose);

  void Insert(const KeyHashHint &hint, HashEntry *entry_base, uint16_t type,
              uint64_t offset, HashOffsetType offset_type);

  bool MatchImpl2(pmem::obj::string_view key, HashEntry matching_entry);

  HashEntry* SearchImpl2(KeyHashHint hint, pmem::obj::string_view key, bool (*type_matcher)(DataEntryType));

  void InsertImpl2(HashEntry* const where, HashEntry new_hash_entry);

  std::mutex _mutex_htable{};

private:
  inline uint32_t get_bucket_num(uint64_t key_hash_value) {
    return key_hash_value & (hash_bucket_num_ - 1);
  }

  inline uint32_t get_slot_num(uint32_t bucket) {
    return bucket / num_buckets_per_slot_;
  }

  bool MatchHashEntry(const pmem::obj::string_view &key, uint32_t hash_k_prefix,
                      uint16_t target_type, const HashEntry *hash_entry,
                      void *data_entry);

  std::vector<uint64_t> hash_bucket_entries_;
  const uint64_t hash_bucket_num_;
  const uint32_t num_buckets_per_slot_;
  const uint32_t hash_bucket_size_;
  const uint64_t num_entries_per_bucket_;
  std::vector<Slot> slots_;
  std::shared_ptr<PMEMAllocator> pmem_allocator_;
  std::unique_ptr<ChunkBasedAllocator> dram_allocator_;
  char *main_buckets_;
};
} // namespace KVDK_NAMESPACE
