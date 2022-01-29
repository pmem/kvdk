/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "hash_table.hpp"

#include "queue.hpp"
#include "skiplist.hpp"
#include "thread_manager.hpp"
#include "unordered_collection.hpp"

namespace KVDK_NAMESPACE {
HashTable* HashTable::NewHashTable(
    uint64_t hash_bucket_num, uint32_t hash_bucket_size,
    uint32_t num_buckets_per_slot,
    std::shared_ptr<PMEMAllocator> pmem_allocator,
    uint32_t max_access_threads) {
  HashTable* table = new (std::nothrow)
      HashTable(hash_bucket_num, hash_bucket_size, num_buckets_per_slot,
                pmem_allocator, max_access_threads);
  if (table) {
    auto main_buckets_space =
        table->dram_allocator_.Allocate(hash_bucket_size * hash_bucket_num);
    if (main_buckets_space.size == 0) {
      GlobalLogger.Error("No enough dram to create global hash table\n");
      delete table;
      table = nullptr;
    } else {
      table->main_buckets_ =
          table->dram_allocator_.offset2addr(main_buckets_space.offset);
    }
  }
  return table;
}

bool HashTable::MatchHashEntry(const StringView& key, uint32_t hash_k_prefix,
                               uint16_t target_type,
                               const HashEntry* hash_entry,
                               DataEntry* data_entry_metadata) {
  if ((target_type & hash_entry->header.data_type) &&
      hash_k_prefix == hash_entry->header.key_prefix) {
    void* pmem_record;
    StringView data_entry_key;

    switch (hash_entry->header.index_type) {
      case HashIndexType::Empty: {
        return false;
      }
      case HashIndexType::StringRecord: {
        pmem_record = hash_entry->index.string_record;
        data_entry_key = hash_entry->index.string_record->Key();
        break;
      }
      case HashIndexType::UnorderedCollectionElement:
      case HashIndexType::DLRecord: {
        pmem_record = hash_entry->index.dl_record;
        data_entry_key = hash_entry->index.dl_record->Key();
        break;
      }
      case HashIndexType::UnorderedCollection: {
        UnorderedCollection* p_collection =
            hash_entry->index.p_unordered_collection;
        data_entry_key = p_collection->Name();
        break;
      }
      case HashIndexType::Queue: {
        Queue* p_collection = hash_entry->index.queue_ptr;
        data_entry_key = p_collection->Name();
        break;
      }
      case HashIndexType::SkiplistNode: {
        SkiplistNode* dram_node = hash_entry->index.skiplist_node;
        pmem_record = dram_node->record;
        data_entry_key = dram_node->record->Key();
        break;
      }
      case HashIndexType::Skiplist: {
        Skiplist* skiplist = hash_entry->index.skiplist;
        pmem_record = skiplist->header()->record;
        data_entry_key = skiplist->Name();
        break;
      }
      default: {
        GlobalLogger.Error("Not supported hash offset type: %u\n",
                           hash_entry->header.index_type);
        assert(false && "Trying to use invalid HashIndexType!");
        return false;
      }
    }

    if (__glibc_likely(data_entry_metadata != nullptr)) {
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
  char* bucket_ptr =
      (char*)main_buckets_ + (uint64_t)hint.bucket * hash_bucket_size_;
  _mm_prefetch(bucket_ptr, _MM_HINT_T0);

  uint32_t key_hash_prefix = hint.key_hash_value >> 32;
  uint64_t entries = hash_bucket_entries_[hint.bucket];
  bool found = false;

  // search cache
  *entry_ptr = slots_[hint.slot].hash_cache.entry_ptr;
  if (*entry_ptr != nullptr) {
    memcpy_16(hash_entry_snap, *entry_ptr);
    if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry_snap,
                       data_entry_meta)) {
      found = true;
    }
  }

  if (!found) {
    // iterate hash entries
    *entry_ptr = (HashEntry*)bucket_ptr;
    uint64_t i = 0;
    for (i = 0; i < entries; i++) {
      if (i > 0 && i % num_entries_per_bucket_ == 0) {
        // next bucket
        memcpy_8(&bucket_ptr, bucket_ptr + hash_bucket_size_ - 8);
        _mm_prefetch(bucket_ptr, _MM_HINT_T0);
      }
      *entry_ptr = (HashEntry*)bucket_ptr + (i % num_entries_per_bucket_);

      memcpy_16(hash_entry_snap, *entry_ptr);
      if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry_snap,
                         data_entry_meta)) {
        slots_[hint.slot].hash_cache.entry_ptr = *entry_ptr;
        found = true;
        break;
      }

      if ((*entry_ptr)->Empty()) {
        reusable_entry = *entry_ptr;
      }
    }

    if (!found) {
      // reach end of buckets, reuse entry or allocate a new bucket
      if (i > 0 && i % num_entries_per_bucket_ == 0) {
        if (reusable_entry != nullptr) {
          *entry_ptr = reusable_entry;
        } else {
          auto space = dram_allocator_.Allocate(hash_bucket_size_);
          if (space.size == 0) {
            GlobalLogger.Error("Memory overflow!\n");
            return Status::MemoryOverflow;
          }
          void* next_off = dram_allocator_.offset2addr(space.offset);
          memset(next_off, 0, space.size);
          memcpy_8(bucket_ptr + hash_bucket_size_ - 8, &next_off);
          *entry_ptr = (HashEntry*)next_off;
        }
      } else {
        *entry_ptr = (HashEntry*)bucket_ptr + (i % num_entries_per_bucket_);
      }
    }
  }

  if (!found && (*entry_ptr) != reusable_entry) {
    (*entry_ptr)->Clear();
    hash_bucket_entries_[hint.bucket]++;
  }

  return found ? Status::Ok : Status::NotFound;
}

Status HashTable::SearchForRead(const KeyHashHint& hint, const StringView& key,
                                uint16_t type_mask, HashEntry** entry_ptr,
                                HashEntry* hash_entry_snap,
                                DataEntry* data_entry_meta) {
  assert(entry_ptr);
  assert((*entry_ptr) == nullptr);
  char* bucket_ptr =
      (char*)main_buckets_ + (uint64_t)hint.bucket * hash_bucket_size_;
  _mm_prefetch(bucket_ptr, _MM_HINT_T0);

  uint32_t key_hash_prefix = hint.key_hash_value >> 32;
  uint64_t entries = hash_bucket_entries_[hint.bucket];

  // search cache
  *entry_ptr = slots_[hint.slot].hash_cache.entry_ptr;
  if (*entry_ptr != nullptr) {
    memcpy_16(hash_entry_snap, *entry_ptr);
    if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry_snap,
                       data_entry_meta)) {
      return Status::Ok;
    }
  }

  // iterate hash entrys
  for (uint64_t i = 0; i < entries; i++) {
    if (i > 0 && i % num_entries_per_bucket_ == 0) {
      // next bucket
      memcpy_8(&bucket_ptr, bucket_ptr + hash_bucket_size_ - 8);
      _mm_prefetch(bucket_ptr, _MM_HINT_T0);
    }
    *entry_ptr = (HashEntry*)bucket_ptr + (i % num_entries_per_bucket_);
    while (1) {
      memcpy_16(hash_entry_snap, *entry_ptr);
      if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry_snap,
                         data_entry_meta)) {
        slots_[hint.slot].hash_cache.entry_ptr = *entry_ptr;
        return Status::Ok;
      } else {
        // check if hash entry modified by another write thread during
        // MatchHashEntry
        if (memcmp(hash_entry_snap, *(entry_ptr), sizeof(HashEntry)) == 0) {
          break;
        }
      }
    }
  }
  return Status::NotFound;
}

void HashTable::Insert(const KeyHashHint& hint, HashEntry* entry_ptr,
                       uint16_t type, void* index, HashIndexType index_type) {
  assert(access_thread.id >= 0);

  HashEntry new_hash_entry(hint.key_hash_value >> 32, type, index, index_type);

  memcpy_16(entry_ptr, &new_hash_entry);
}

}  // namespace KVDK_NAMESPACE
