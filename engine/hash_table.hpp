/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <atomic>
#include <cstdio>
#include <limits>
#include <vector>

#include "data_record.hpp"
#include "dram_allocator.hpp"
#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "structures.hpp"

namespace KVDK_NAMESPACE {
enum class HashIndexType : uint8_t {
  // Value initialized considered as Invalid
  Invalid = 0,
  // Index is PMem offset of a string record
  StringRecord = 1,
  // Index is PMem offset of a doubly linked record
  DLRecord = 2,
  // Index is pointer to a dram skiplist node
  SkiplistNode = 3,
  // Index is pointer to a dram skiplist struct
  Skiplist = 4,
  // Index field contains pointer to UnorderedCollection object on DRAM
  UnorderedCollection = 5,
  // Index field contains PMem pointer to element of UnorderedCollection
  UnorderedCollectionElement = 6,
  // Index field contains pointer to Queue object on DRAM
  Queue = 7,
  // Index is empty which point to nothing
  Empty = 8,
};

struct HashHeader {
  uint32_t key_prefix;
  RecordType record_type;
  HashIndexType index_type;
  bool is_persist;
};

class Skiplist;
class SkiplistNode;
class UnorderedCollection;
class Queue;

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
    UnorderedCollection* p_unordered_collection;
    Queue* queue_ptr;
  };
  static_assert(sizeof(Index) == 8);

  HashEntry() = default;

  HashEntry(uint32_t key_hash_prefix, RecordType record_type, void* _index,
            HashIndexType index_type, bool is_persist)
      : header_({key_hash_prefix, record_type, index_type, is_persist}),
        index_(_index) {}

  bool Empty() { return header_.index_type == HashIndexType::Empty; }

  Index GetIndex() const { return index_; }

  HashIndexType GetIndexType() const { return header_.index_type; }

  RecordType GetRecordType() const { return header_.record_type; }

  bool IsPersist() { return header_.is_persist; }

  // Check if "key" of data type "target_type" is indexed by "this". If
  // matches, copy data entry of data record of "key" to "data_entry_metadata"
  // and return true, otherwise return false.
  bool Match(const StringView& key, uint32_t hash_k_prefix,
             uint16_t target_type, DataEntry* data_entry_metadata);

 private:
  // Make this hash entry empty while its content been deleted
  void Clear() { header_.index_type = HashIndexType::Empty; }

  bool SetExpiredFlag() { header_.is_persist = false; }

 private:
  Index index_;
  HashHeader header_;
};
static_assert(sizeof(HashEntry) == 16);

struct HashCache {
  HashEntry* entry_ptr = nullptr;
};

struct Slot {
  HashCache hash_cache;
  SpinMutex spin;
};

struct SlotIterator;

class HashTable {
 public:
  friend class SlotIterator;
  struct KeyHashHint {
    uint64_t key_hash_value;
    uint32_t bucket;
    uint32_t slot;
    SpinMutex* spin;
  };

  static HashTable* NewHashTable(uint64_t hash_bucket_num,
                                 uint32_t hash_bucket_size,
                                 uint32_t num_buckets_per_slot,
                                 std::shared_ptr<PMEMAllocator> pmem_allocator,
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
  //
  // entry_ptr: position to insert, it's get from SearchForWrite()
  void Insert(const KeyHashHint& hint, HashEntry* entry_ptr, RecordType type,
              void* index, HashIndexType index_type, bool is_persist = true);

  // Erase a hash entry so it can be reused in future
  void Erase(HashEntry* entry_ptr) {
    assert(entry_ptr != nullptr);
    entry_ptr->Clear();
  }

  void SetExpiredFlag(HashEntry* entry_ptr) {
    assert(entry_ptr != nullptr);
    entry_ptr->SetExpiredFlag();
  }

  SlotIterator GetSlotIterator();

 private:
  HashTable(uint64_t hash_bucket_num, uint32_t hash_bucket_size,
            uint32_t num_buckets_per_slot,
            std::shared_ptr<PMEMAllocator> pmem_allocator,
            uint32_t max_access_threads)
      : hash_bucket_num_(hash_bucket_num),
        num_buckets_per_slot_(num_buckets_per_slot),
        hash_bucket_size_(hash_bucket_size),
        dram_allocator_(max_access_threads),
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

  std::vector<uint64_t> hash_bucket_entries_;
  const uint64_t hash_bucket_num_;
  const uint32_t num_buckets_per_slot_;
  const uint32_t hash_bucket_size_;
  const uint64_t num_entries_per_bucket_;
  Array<Slot> slots_;
  std::shared_ptr<PMEMAllocator> pmem_allocator_;
  ChunkBasedAllocator dram_allocator_;
  void* main_buckets_;
};

struct SlotIterator {
 private:
  // The range of bucket id is [0, hash_bucket_num_).The range of bucket id
  // corresponding to the current slot is [slot_id*num_buckets_per_slot_,
  // (slot_id+1)*num_buckets_per_slot_).
  uint64_t iter_start_bucket_idx_;
  uint64_t iter_end_bucket_idx_;
  // lock current access slot
  std::unique_lock<SpinMutex> iter_lock_slot_;
  // current slot id
  uint64_t current_slot_id;
  HashTable* hash_table_;

  void GetBucketRangeAndLockSlot() {
    // release prev slot lock.
    iter_lock_slot_ = std::unique_lock<SpinMutex>(
        hash_table_->slots_[current_slot_id].spin, std::defer_lock);
    iter_lock_slot_.lock();
    iter_start_bucket_idx_ =
        current_slot_id * hash_table_->num_buckets_per_slot_;
    iter_end_bucket_idx_ =
        (current_slot_id + 1) * hash_table_->num_buckets_per_slot_;
  }

 public:
  class BucketIterator {
   private:
    SlotIterator* slot_iter_;
    uint64_t bucket_idx_;
    uint64_t entry_idx_;
    char* bucket_ptr_;

   public:
    BucketIterator(SlotIterator* slot_iter, uint64_t bucket_idx)
        : slot_iter_(slot_iter), bucket_idx_(bucket_idx), entry_idx_(0) {
      GetBucket();
    }

    HashEntry& operator*() {
      return *(reinterpret_cast<HashEntry*>(bucket_ptr_) +
               entry_idx_ % slot_iter_->hash_table_->num_entries_per_bucket_);
    }

    HashEntry* operator->() { return &operator*(); }

    BucketIterator& operator++() {
      Next();
      return *this;
    }

    BucketIterator operator++(int) {
      auto tmp = *this;
      ++*this;
      return tmp;
    }

    friend bool operator==(const BucketIterator& a, const BucketIterator& b) {
      return a.slot_iter_ && b.slot_iter_ && a.bucket_idx_ == b.bucket_idx_ &&
             a.entry_idx_ == b.entry_idx_;
    }

    friend bool operator!=(const BucketIterator& a, const BucketIterator& b) {
      return !(a == b);
    }

   private:
    // Get valid bucket, which has hash entries.
    void GetBucket() {
      auto hash_table = slot_iter_->hash_table_;
      while (bucket_idx_ < slot_iter_->iter_end_bucket_idx_ &&
             !hash_table->hash_bucket_entries_[bucket_idx_]) {
        bucket_idx_++;
      }
      if (bucket_idx_ == slot_iter_->iter_end_bucket_idx_) {
        bucket_ptr_ = nullptr;
        return;
      }
      bucket_ptr_ = reinterpret_cast<char*>(hash_table->main_buckets_) +
                    bucket_idx_ * hash_table->hash_bucket_size_;
      _mm_prefetch(bucket_ptr_, _MM_HINT_T0);
    }

    void Next() {
      auto hash_table = slot_iter_->hash_table_;
      if (entry_idx_ < hash_table->hash_bucket_entries_[bucket_idx_]) {
        entry_idx_++;
        if (entry_idx_ > 0 &&
            entry_idx_ % hash_table->num_entries_per_bucket_ == 0) {
          bucket_ptr_ = bucket_ptr_ + hash_table->hash_bucket_size_ - 8;
          _mm_prefetch(bucket_ptr_, _MM_HINT_T0);
        }
      }
      if (entry_idx_ == hash_table->hash_bucket_entries_[bucket_idx_]) {
        entry_idx_ = 0;
        bucket_idx_++;
        GetBucket();
      }
    }
  };

  SlotIterator(HashTable* hash_table)
      : hash_table_(hash_table), current_slot_id(0) {
    GetBucketRangeAndLockSlot();
  }
  void Next() {
    current_slot_id++;
    if (current_slot_id < hash_table_->slots_.size()) {
      GetBucketRangeAndLockSlot();
    }
  }

  bool Valid() { return current_slot_id < hash_table_->slots_.size(); }

  BucketIterator Begin() {
    return BucketIterator{this, iter_start_bucket_idx_};
  }

  BucketIterator End() { return BucketIterator{this, iter_end_bucket_idx_}; }
};

}  // namespace KVDK_NAMESPACE
