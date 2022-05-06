/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <atomic>
#include <cstdio>
#include <limits>
#include <vector>

#include "alias.hpp"
#include "data_record.hpp"
#include "dram_allocator.hpp"
#include "kvdk/engine.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "structures.hpp"

namespace KVDK_NAMESPACE {
enum class KeyStatus : uint8_t {
  Persist = 0,
  Volatile = 1 << 0,  // expirable
  Expired = 1 << 1,
};
struct HashHeader {
  uint32_t key_prefix;
  RecordType record_type;
  PointerType index_type;
  KeyStatus entry_status;
};

class Skiplist;
class SkiplistNode;
struct BucketIterator;

template <RecordType ListType, RecordType DataType>
class GenericList;

using List = GenericList<RecordType::ListRecord, RecordType::ListElem>;
using HashList = GenericList<RecordType::HashRecord, RecordType::HashElem>;

struct alignas(16) HashEntry {
 public:
  friend class HashTable;
  HashEntry& operator=(const HashEntry&) = delete;
  HashEntry(const HashEntry& hash_entry) { atomic_load_16(this, &hash_entry); }
  union Index {
    Index(void* _ptr) : ptr(_ptr) {}
    Index() = default;
    void* ptr;
    SkiplistNode* skiplist_node;
    StringRecord* string_record;
    DLRecord* dl_record;
    Skiplist* skiplist;
    List* list;
    HashList* hlist;
  };
  static_assert(sizeof(Index) == 8);

  HashEntry() = default;

  HashEntry(uint32_t key_hash_prefix, RecordType record_type, void* _index,
            PointerType index_type, KeyStatus entry_status)
      : index_(_index),
        header_({key_hash_prefix, record_type, index_type, entry_status}) {}

  bool Empty() { return header_.index_type == PointerType::Empty; }

  Index GetIndex() const { return index_; }

  PointerType GetIndexType() const { return header_.index_type; }

  RecordType GetRecordType() const { return header_.record_type; }

  bool IsExpiredStatus() { return header_.entry_status == KeyStatus::Expired; }

  bool IsTTLStatus() { return header_.entry_status == KeyStatus::Volatile; }

  // Check if "key" of data type "target_type" is indexed by "this". If
  // matches, copy data entry of data record of "key" to "data_entry_metadata"
  // and return true, otherwise return false.
  bool Match(const StringView& key, uint32_t hash_k_prefix,
             uint16_t target_type, DataEntry* data_entry_metadata);

 private:
  // Make this hash entry empty while its content been deleted
  void clear() { header_.index_type = PointerType::Empty; }

  void updateEntryStatus(KeyStatus entry_status) {
    header_.entry_status = entry_status;
  }

 private:
  Index index_;
  HashHeader header_;
};
static_assert(sizeof(HashEntry) == 16);

// Size of each hash bucket
//
// It should be larger than hans entry size (which is 16) plus 8 (the pointer
// to next bucket). It is recommended to set it align to cache line
constexpr size_t kHashBucketSize = 128;
constexpr size_t kNumEntryPerBucket =
    (kHashBucketSize - sizeof(void*)) / sizeof(HashEntry);
struct HashBucket {
  HashBucket() {
    memset(hash_entries, 0, sizeof(HashEntry) * kNumEntryPerBucket);
  }
  HashEntry hash_entries[kNumEntryPerBucket];
  HashBucket* next{nullptr};
  char padding[kHashBucketSize - sizeof(hash_entries) - sizeof(next)];
};
static_assert(sizeof(HashBucket) == kHashBucketSize);

struct HashCache {
  HashEntry* entry_ptr = nullptr;
};

struct Slot {
  HashCache hash_cache;
  SpinMutex spin;
};

struct HashTableIterator;

class HashTable {
 public:
  friend class HashTableIterator;
  friend class BucketIterator;
  struct KeyHashHint {
    uint64_t key_hash_value;
    uint32_t bucket;
    uint32_t slot;
    SpinMutex* spin;
  };

  static HashTable* NewHashTable(uint64_t hash_bucket_num,
                                 uint32_t num_buckets_per_slot,
                                 const PMEMAllocator* pmem_allocator,
                                 uint32_t max_access_threads);

  KeyHashHint GetHint(const StringView& key) {
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
  // entry_ptr: store hash entry position of "key" if found
  // hash_entry_snap: store a hash entry copy of searching key for lock-free
  // read, as hash entry maybe modified by write operations
  // data_entry_meta: store a copy of data entry metadata part of searching key
  Status SearchForRead(const KeyHashHint& hint, const StringView& key,
                       uint16_t type_mask, HashEntry** entry_ptr,
                       HashEntry* hash_entry_snap, DataEntry* data_entry_meta);

  // Search key in hash table for write operations
  //
  // type_mask: which data types to search
  // entry_ptr: store hash entry position to write. It's either hash entry
  // position of "key" to update if it's existing, or a clear position to insert
  // new hash entry
  // hash_entry_snap: store a hash entry copy of searching key
  // data_entry_meta: store a copy of data entry metadata part of searching key
  // hint: make sure hint.spin is hold
  Status SearchForWrite(const KeyHashHint& hint, const StringView& key,
                        uint16_t type_mask, HashEntry** entry_ptr,
                        HashEntry* hash_entry_snap, DataEntry* data_entry_meta);

  // Insert a hash entry to hash table
  // @param entry_ptr: position to insert, it's get from SearchForWrite()
  // TODO: remove the default param.
  void Insert(const KeyHashHint& hint, HashEntry* entry_ptr, RecordType type,
              void* index, PointerType index_type,
              KeyStatus entry_status = KeyStatus::Persist);

  // Erase a hash entry so it can be reused in future
  void Erase(HashEntry* entry_ptr) {
    assert(entry_ptr != nullptr);
    entry_ptr->clear();
  }

  void UpdateEntryStatus(HashEntry* entry_ptr, KeyStatus entry_status) {
    assert(entry_ptr != nullptr);
    entry_ptr->updateEntryStatus(entry_status);
  }

  std::unique_lock<SpinMutex> AcquireLock(StringView const& key) {
    return std::unique_lock<SpinMutex>{*GetHint(key).spin};
  }

  HashTableIterator GetIterator();

 private:
  HashTable(uint64_t hash_bucket_num, uint32_t num_buckets_per_slot,
            const PMEMAllocator* pmem_allocator, uint32_t max_access_threads)
      : num_hash_buckets_(hash_bucket_num),
        num_buckets_per_slot_(num_buckets_per_slot),
        pmem_allocator_(pmem_allocator),
        dram_allocator_(max_access_threads),
        slots_(hash_bucket_num / num_buckets_per_slot),
        hash_bucket_entries_(hash_bucket_num, 0),
        hash_buckets_(num_hash_buckets_) {}

  inline uint32_t get_bucket_num(uint64_t key_hash_value) {
    return key_hash_value & (num_hash_buckets_ - 1);
  }

  inline uint32_t get_slot_num(uint32_t bucket) {
    return bucket / num_buckets_per_slot_;
  }

  Status allocateEntry(BucketIterator& bucket_iter);

  const uint64_t num_hash_buckets_;
  const uint32_t num_buckets_per_slot_;
  const PMEMAllocator* pmem_allocator_;
  ChunkBasedAllocator dram_allocator_;
  Array<Slot> slots_;
  std::vector<uint64_t> hash_bucket_entries_;
  Array<HashBucket> hash_buckets_;
  void* main_buckets_;
};

class BucketIterator {
 private:
  friend class HashTable;
  HashTable* hash_table_;
  uint64_t bucket_idx_;
  uint64_t entry_idx_;
  HashBucket* bucket_ptr_;

 public:
  BucketIterator(HashTable* hash_table /* should be non-null */,
                 uint64_t bucket_idx)
      : hash_table_(hash_table),
        bucket_idx_(bucket_idx),
        entry_idx_(0),
        bucket_ptr_(nullptr) {
    if (bucket_idx_ < hash_table_->hash_buckets_.size()) {
      bucket_ptr_ = &hash_table_->hash_buckets_[bucket_idx_];
      _mm_prefetch(bucket_ptr_, _MM_HINT_T0);
    }
  }

  BucketIterator(const BucketIterator&) = default;

  bool Valid() {
    return bucket_ptr_ != nullptr &&
           entry_idx_ < hash_table_->hash_bucket_entries_[bucket_idx_];
  }

  HashEntry& operator*() {
    return bucket_ptr_->hash_entries[entry_idx_ % kNumEntryPerBucket];
  }

  HashEntry* operator->() { return &operator*(); }

  BucketIterator& operator++() {
    next();
    return *this;
  }

  BucketIterator operator++(int) {
    BucketIterator tmp{*this};
    this->operator++();
    return tmp;
  }

 private:
  void next() {
    if (Valid()) {
      entry_idx_++;
      if (entry_idx_ % kNumEntryPerBucket == 0 && Valid()) {
        bucket_ptr_ = bucket_ptr_->next;
        _mm_prefetch(bucket_ptr_, _MM_HINT_T0);
      }
    }
  }
};

// Iterate all slots in a hashtable
struct HashTableIterator {
 private:
  // lock current access slot
  std::unique_lock<SpinMutex> iter_lock_slot_;
  // current slot id
  HashTable* hash_table_;
  uint64_t current_slot_id_;

 public:
  class SlotIterator {
   private:
    HashTable* hash_table_;
    uint64_t start_bucket_;
    uint64_t end_bucket_;
    uint64_t current_bucket_;
    BucketIterator bucket_iter_;

   public:
    SlotIterator(HashTable* hash_table /* should be non null */,
                 uint64_t slot_idx)
        : hash_table_(hash_table),
          start_bucket_(hash_table_->num_buckets_per_slot_ * slot_idx),
          end_bucket_(start_bucket_ + hash_table_->num_buckets_per_slot_),
          current_bucket_(start_bucket_),
          bucket_iter_(hash_table_, current_bucket_) {
      getBucket();
    }

    HashEntry& operator*() { return *bucket_iter_; }

    HashEntry* operator->() { return &operator*(); }

    SlotIterator& operator++() {
      Next();
      return *this;
    }

    SlotIterator operator++(int) {
      auto tmp = *this;
      this->operator++();
      return tmp;
    }

    bool Valid() {
      return bucket_iter_.Valid() && current_bucket_ < end_bucket_ &&
             current_bucket_ >= start_bucket_;
    }

   private:
    // Locate to bucket with hash entries from current
    void getBucket() {
      if (!bucket_iter_.Valid()) {
        current_bucket_++;
        while (current_bucket_ < end_bucket_) {
          bucket_iter_ = BucketIterator(hash_table_, current_bucket_);
          if (bucket_iter_.Valid()) {
            return;
          }
          current_bucket_++;
        }
      }
    }

    void Next() {
      if (Valid()) {
        bucket_iter_++;
        if (!bucket_iter_.Valid()) {
          getBucket();
        }
      }
    }
  };

  HashTableIterator(HashTable* hash_table)
      : hash_table_(hash_table), current_slot_id_(0) {}

  std::unique_lock<SpinMutex> AcquireSlotLock() {
    SpinMutex* slot_lock = GetSlotLock();
    return std::unique_lock<SpinMutex>(*slot_lock);
  }

  void Next() {
    if (Valid()) {
      current_slot_id_++;
    }
  }

  bool Valid() { return current_slot_id_ < hash_table_->slots_.size(); }

  SlotIterator Slot() { return SlotIterator{hash_table_, current_slot_id_}; }

  SpinMutex* GetSlotLock() {
    return &hash_table_->slots_[current_slot_id_].spin;
  }
};
}  // namespace KVDK_NAMESPACE
