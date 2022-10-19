/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <atomic>
#include <cstdio>
#include <limits>
#include <vector>

#include "alias.hpp"
#include "allocator.hpp"
#include "data_record.hpp"
#include "dram_allocator.hpp"
#include "kvdk/volatile/engine.hpp"
#include "structures.hpp"

namespace KVDK_NAMESPACE {

class Skiplist;
class SkiplistNode;
struct HashBucketIterator;

struct List;
struct HashList;

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

  HashEntry(uint32_t key_hash_prefix, RecordType record_type,
            RecordStatus record_status, void* _index, PointerType index_type)
      : index_(_index),
        header_({key_hash_prefix, record_type, record_status, index_type}) {}

  bool Empty() { return header_.index_type == PointerType::Empty; }

  // Make this hash entry empty while its content been deleted
  void Clear() { header_.index_type = PointerType::Empty; }

  bool Allocated() { return header_.index_type == PointerType::Allocated; }

  void MarkAsAllocated() { header_.index_type = PointerType::Allocated; }

  Index GetIndex() const { return index_; }

  PointerType GetIndexType() const { return header_.index_type; }

  RecordType GetRecordType() const { return header_.record_type; }

  RecordStatus GetRecordStatus() const { return header_.record_status; }

  // Check if "key" of data type "target_type" is indexed by "this". If
  // matches, copy data entry of data record of "key" to "data_entry_metadata"
  // and return true, otherwise return false.
  //
  // Args:
  // * target_type: a mask of RecordType, search all masked types
  bool Match(const StringView& key, uint32_t hash_k_prefix, uint8_t target_type,
             DataEntry* data_entry_metadata);

 private:
  struct EntryHeader {
    uint32_t key_prefix;
    RecordType record_type;
    RecordStatus record_status;
    PointerType index_type;
  };

  Index index_;
  EntryHeader header_;
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
  friend class HashSlotIterator;
  friend class HashBucketIterator;
  struct LookupResult {
   public:
    Status s{Status::Ok};
    HashEntry entry{};
    HashEntry* entry_ptr{nullptr};

    LookupResult& operator=(LookupResult const& other) {
      s = other.s;
      memcpy_16(&entry, &other.entry);
      entry_ptr = other.entry_ptr;
      key_hash_prefix = other.key_hash_prefix;
      return *this;
    }

   private:
    friend class HashTable;
    uint32_t key_hash_prefix;
  };

  static HashTable* NewHashTable(uint64_t hash_bucket_num,
                                 uint32_t num_buckets_per_slot,
                                 const Allocator* kv_allocator,
                                 Allocator* new_bucket_allocator,
                                 uint32_t max_access_threads);

  // Look up key in hashtable
  // Store a copy of hash entry in LookupResult::entry, and a pointer to the
  // hash entry on hash table in LookupResult::entry_ptr
  // If may_insert is true and key not found, then store
  // pointer of a free-to-write hash entry in LookupResult::entry_ptr.
  //
  // * type_mask: which data types to search
  //
  // return status:
  // Status::NotFound is key is not found.
  // Status::MemoryOverflow if may_insert is true but
  // failed to allocate new hash entry
  // Status::Ok on success.
  //
  // Notice: key should be locked if set may_insert to true
  template <bool may_insert>
  LookupResult Lookup(const StringView& key, uint8_t type_mask);

  // Insert a hash entry to hash table
  // * insert_position: indicate the the postion to insert new entry, it should
  // be return of Lookup of the inserting key
  void Insert(const LookupResult& insert_position, RecordType type,
              RecordStatus status, void* index, PointerType index_type);

  // Lookup and insert a hash entry of key to hash table, return lookup result
  LookupResult Insert(const StringView& key, RecordType type,
                      RecordStatus status, void* index, PointerType index_type);

  // Erase a hash entry so it can be reused in future
  void Erase(HashEntry* entry_ptr) {
    assert(entry_ptr != nullptr);
    entry_ptr->Clear();
  }

  std::unique_lock<SpinMutex> AcquireLock(StringView const& key) {
    return std::unique_lock<SpinMutex>{*getHint(key).spin};
  }

  HashTableIterator GetIterator(uint64_t start_slot_idx, uint64_t end_slot_idx);

  size_t GetSlotsNum() { return slots_.size(); }

  // StringAlike is std::string or StringView
  template <typename StringAlike>
  std::vector<std::unique_lock<SpinMutex>> RangeLock(
      std::vector<StringAlike> const& keys) {
    std::vector<SpinMutex*> spins;
    for (auto const& key : keys) {
      spins.push_back(getHint(key).spin);
    }
    std::sort(spins.begin(), spins.end());
    auto end = std::unique(spins.begin(), spins.end());

    std::vector<std::unique_lock<SpinMutex>> guard;
    for (auto iter = spins.begin(); iter != end; ++iter) {
      guard.emplace_back(**iter);
    }
    return guard;
  }

 private:
  HashTable(uint64_t hash_bucket_num, uint32_t num_buckets_per_slot,
            const Allocator* kv_allocator, Allocator* new_bucket_allocator,
            uint32_t max_access_threads)
      : num_hash_buckets_(hash_bucket_num),
        num_buckets_per_slot_(num_buckets_per_slot),
        kv_allocator_(kv_allocator),
        chunk_based_bucket_allocator_{max_access_threads, new_bucket_allocator},
        slots_(hash_bucket_num / num_buckets_per_slot),
        hash_bucket_entries_(hash_bucket_num, 0),
        hash_buckets_(num_hash_buckets_) {}

  struct KeyHashHint {
    uint32_t bucket;
    uint32_t slot;
    // hash value stored on hash entry
    uint32_t key_hash_prefix;
    SpinMutex* spin;
  };

  KeyHashHint getHint(const StringView& key) {
    KeyHashHint hint;
    uint64_t hash_val = hash_str(key.data(), key.size());
    hint.key_hash_prefix = hash_val >> 32;
    hint.bucket = get_bucket_num(hash_val);
    hint.slot = get_slot_num(hint.bucket);
    hint.spin = &slots_[hint.slot].spin;
    return hint;
  }

  inline uint32_t get_bucket_num(uint64_t key_hash_value) {
    return key_hash_value & (num_hash_buckets_ - 1);
  }

  inline uint32_t get_slot_num(uint32_t bucket) {
    return bucket / num_buckets_per_slot_;
  }

  Status allocateEntry(HashBucketIterator& bucket_iter);

  const uint64_t num_hash_buckets_;
  const uint32_t num_buckets_per_slot_;
  const Allocator* kv_allocator_;
  ChunkBasedAllocator chunk_based_bucket_allocator_;
  Array<Slot> slots_;
  std::vector<uint64_t> hash_bucket_entries_;
  Array<HashBucket> hash_buckets_;
  void* main_buckets_;
};

// Iterator all hash entries in a hash table bucket
class HashBucketIterator {
 public:
  HashBucketIterator(HashTable* hash_table /* should be non-null */,
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

  HashBucketIterator(const HashBucketIterator&) = default;

  bool Valid() {
    return bucket_ptr_ != nullptr &&
           entry_idx_ < hash_table_->hash_bucket_entries_[bucket_idx_];
  }

  HashEntry& operator*() {
    return bucket_ptr_->hash_entries[entry_idx_ % kNumEntryPerBucket];
  }

  HashEntry* operator->() { return &operator*(); }

  HashBucketIterator& operator++() {
    next();
    return *this;
  }

  HashBucketIterator operator++(int) {
    HashBucketIterator tmp{*this};
    this->operator++();
    return tmp;
  }

 private:
  friend class HashTable;

  void next() {
    if (Valid()) {
      entry_idx_++;
      if (entry_idx_ % kNumEntryPerBucket == 0 && Valid()) {
        bucket_ptr_ = bucket_ptr_->next;
        _mm_prefetch(bucket_ptr_, _MM_HINT_T0);
      }
    }
  }

  HashTable* hash_table_;
  uint64_t bucket_idx_;
  uint64_t entry_idx_;
  HashBucket* bucket_ptr_;
};

// Iterator all hash entries in a hash table slot
class HashSlotIterator {
 public:
  HashSlotIterator(HashTable* hash_table /* should be non null */,
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

  HashSlotIterator& operator++() {
    next();
    return *this;
  }

  HashSlotIterator operator++(int) {
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
        bucket_iter_ = HashBucketIterator(hash_table_, current_bucket_);
        if (bucket_iter_.Valid()) {
          return;
        }
        current_bucket_++;
      }
    }
  }

  void next() {
    if (Valid()) {
      bucket_iter_++;
      if (!bucket_iter_.Valid()) {
        getBucket();
      }
    }
  }

  HashTable* hash_table_;
  uint64_t start_bucket_;
  uint64_t end_bucket_;
  uint64_t current_bucket_;
  HashBucketIterator bucket_iter_;
};

// Iterate all slots in a hashtable
struct HashTableIterator {
 public:
  HashTableIterator(HashTable* hash_table, uint64_t start_slot_idx,
                    uint64_t end_slot_idx)
      : hash_table_(hash_table),
        current_slot_idx_(start_slot_idx),
        end_slot_idx_(end_slot_idx) {}

  std::unique_lock<SpinMutex> AcquireSlotLock() {
    SpinMutex* slot_lock = GetSlotLock();
    return std::unique_lock<SpinMutex>(*slot_lock);
  }

  void Next() {
    if (Valid()) {
      current_slot_idx_++;
    }
  }

  bool Valid() { return current_slot_idx_ < end_slot_idx_; }

  HashSlotIterator Slot() {
    return HashSlotIterator{hash_table_, current_slot_idx_};
  }

  SpinMutex* GetSlotLock() {
    return &hash_table_->slots_[current_slot_idx_].spin;
  }

 private:
  // lock current access slot
  std::unique_lock<SpinMutex> iter_lock_slot_;
  // current slot id
  HashTable* hash_table_;
  uint64_t current_slot_idx_;
  uint64_t end_slot_idx_;
};
}  // namespace KVDK_NAMESPACE
