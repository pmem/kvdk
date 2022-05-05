/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "hash_table.hpp"

#include "hash_list.hpp"
#include "simple_list.hpp"
#include "sorted_collection/skiplist.hpp"
#include "thread_manager.hpp"

namespace KVDK_NAMESPACE {
HashTable* HashTable::NewHashTable(uint64_t hash_bucket_num,
                                   uint32_t hash_bucket_size,
                                   uint32_t num_buckets_per_slot,
                                   const PMEMAllocator* pmem_allocator,
                                   uint32_t max_access_threads) {
  HashTable* table;
  // We catch exception here as we may need to allocate large memory for hash
  // table here
  try {
    table =
        new HashTable(hash_bucket_num, hash_bucket_size, num_buckets_per_slot,
                      pmem_allocator, max_access_threads);
    return table;
  } catch (std::bad_alloc& b) {
    GlobalLogger.Error("No enough dram to create global hash table: b\n",
                       b.what());
    table = nullptr;
  }

  return table;
}

bool HashEntry::Match(const StringView& key, uint32_t hash_k_prefix,
                      uint16_t target_type, DataEntry* data_entry_metadata) {
  if ((target_type & header_.record_type) &&
      hash_k_prefix == header_.key_prefix) {
    void* pmem_record = nullptr;
    StringView data_entry_key;

    switch (header_.index_type) {
      case PointerType::Empty: {
        return false;
      }
      case PointerType::StringRecord: {
        pmem_record = index_.string_record;
        data_entry_key = index_.string_record->Key();
        break;
      }
      case PointerType::HashElem:
      case PointerType::DLRecord: {
        pmem_record = index_.dl_record;
        data_entry_key = index_.dl_record->Key();
        break;
      }
      case PointerType::List: {
        data_entry_key = index_.list->Name();
        break;
      }
      case PointerType::HashList: {
        data_entry_key = index_.hlist->Name();
        break;
      }
      case PointerType::SkiplistNode: {
        SkiplistNode* dram_node = index_.skiplist_node;
        pmem_record = dram_node->record;
        data_entry_key = dram_node->record->Key();
        break;
      }
      case PointerType::Skiplist: {
        Skiplist* skiplist = index_.skiplist;
        pmem_record = skiplist->HeaderRecord();
        data_entry_key = skiplist->Name();
        break;
      }
      default: {
        GlobalLogger.Error("Not supported hash index type: %u\n",
                           header_.index_type);
        assert(false && "Trying to use invalid PointerType!");
        return false;
      }
    }

    if (data_entry_metadata != nullptr) {
      memcpy(data_entry_metadata, pmem_record, sizeof(DataEntry));
    }

    if (equal_string_view(key, data_entry_key)) {
      return true;
    }
  }
  return false;
}

Status HashTable::SearchForWrite(const KeyHashHint& hint, const StringView& key,
                                 uint16_t type_mask, HashEntry** entry_ptr,
                                 HashEntry* hash_entry_snap,
                                 DataEntry* data_entry_meta) {
  assert(entry_ptr);
  assert((*entry_ptr) == nullptr);
  HashEntry* reusable_entry = nullptr;

  HashBucket* bucket_ptr = &hash_buckets_[hint.bucket];
  _mm_prefetch(bucket_ptr, _MM_HINT_T0);
  uint32_t key_hash_prefix = hint.key_hash_value >> 32;
  uint64_t entries = hash_bucket_entries_[hint.bucket];

  // search cache
  *entry_ptr = slots_[hint.slot].hash_cache.entry_ptr;
  if (*entry_ptr != nullptr) {
    atomic_load_16(hash_entry_snap, *entry_ptr);
    if (hash_entry_snap->Match(key, key_hash_prefix, type_mask,
                               data_entry_meta)) {
      return Status::Ok;
    }
  }

  // iterate hash entrys in the bucket
  HashTableBucketIterator iter(this, hint.bucket);
  while (iter.Valid()) {
    *entry_ptr = &*iter;
    atomic_load_16(hash_entry_snap, *entry_ptr);
    if (hash_entry_snap->Match(key, key_hash_prefix, type_mask,
                               data_entry_meta)) {
      slots_[hint.slot].hash_cache.entry_ptr = *entry_ptr;
      return Status::Ok;
    }
    if ((*entry_ptr)->Empty()) {
      reusable_entry = *entry_ptr;
    }
    iter++;
  }

  if (reusable_entry == nullptr) {
    allocate(iter);
    assert(iter.Valid());
    *entry_ptr = &(*iter);
  } else {
    *entry_ptr = reusable_entry;
  }

  return Status::NotFound;
}

Status HashTable::SearchForRead(const KeyHashHint& hint, const StringView& key,
                                uint16_t type_mask, HashEntry** entry_ptr,
                                HashEntry* hash_entry_snap,
                                DataEntry* data_entry_meta) {
  assert(entry_ptr);
  *entry_ptr = nullptr;

  HashBucket* bucket_ptr = &hash_buckets_[hint.bucket];
  _mm_prefetch(bucket_ptr, _MM_HINT_T0);
  uint32_t key_hash_prefix = hint.key_hash_value >> 32;

  // search cache
  *entry_ptr = slots_[hint.slot].hash_cache.entry_ptr;
  if (*entry_ptr != nullptr) {
    atomic_load_16(hash_entry_snap, *entry_ptr);
    if (hash_entry_snap->Match(key, key_hash_prefix, type_mask,
                               data_entry_meta)) {
      return Status::Ok;
    }
  }

  // iterate hash entrys in the bucket
  HashTableBucketIterator iter(this, hint.bucket);
  while (iter.Valid()) {
    *entry_ptr = &(*iter);
    atomic_load_16(hash_entry_snap, *entry_ptr);
    if (hash_entry_snap->Match(key, key_hash_prefix, type_mask,
                               data_entry_meta)) {
      slots_[hint.slot].hash_cache.entry_ptr = *entry_ptr;
      return Status::Ok;
    }
    iter++;
  }

  return Status::NotFound;
}

void HashTable::Insert(const KeyHashHint& hint, HashEntry* entry_ptr,
                       RecordType type, void* index, PointerType index_type,
                       KeyStatus entry_status) {
  HashEntry new_hash_entry(hint.key_hash_value >> 32, type, index, index_type,
                           entry_status);
  atomic_store_16(entry_ptr, &new_hash_entry);
}

Status HashTable::allocate(HashTableBucketIterator& bucket_iter) {
  kvdk_assert(bucket_iter.hash_table_ == this &&
                  bucket_iter.entry_idx_ ==
                      hash_bucket_entries_[bucket_iter.bucket_idx_],
              "Only allocate new hash entry at end of hash bucket");
  assert(bucket_iter.bucket_ptr_ != nullptr);
  if (hash_bucket_entries_[bucket_iter.bucket_idx_] > 0 &&
      hash_bucket_entries_[bucket_iter.bucket_idx_] % kNumEntryPerBucket == 0) {
    auto space = dram_allocator_.Allocate(128);
    if (space.size == 0) {
      GlobalLogger.Error("MemoryOverflow!\n");
      return Status::MemoryOverflow;
    }
    bucket_iter.bucket_ptr_->next =
        dram_allocator_.offset2addr<HashBucket>(space.offset);
    bucket_iter.bucket_ptr_ = bucket_iter.bucket_ptr_->next;
  }
  bucket_iter.entry_idx_ = hash_bucket_entries_[bucket_iter.bucket_idx_]++;
  bucket_iter->clear();
  kvdk_assert(bucket_iter.Valid(), "");
  return Status::Ok;
}

SlotIterator HashTable::GetSlotIterator() { return SlotIterator{this}; }

}  // namespace KVDK_NAMESPACE
