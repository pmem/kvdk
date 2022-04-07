/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "hash_table.hpp"

#include "simple_list.hpp"
#include "sorted_collection/skiplist.hpp"
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
      memset(table->main_buckets_, 0, hash_bucket_size * hash_bucket_num);
      kvdk_assert((uint64_t)table->main_buckets_ % sizeof(HashEntry) == 0,
                  "hash table bucket address should aligned to hash entry size "
                  "in create new hash table");
    }
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
      case PointerType::UnorderedCollectionElement:
      case PointerType::DLRecord: {
        pmem_record = index_.dl_record;
        data_entry_key = index_.dl_record->Key();
        break;
      }
      case PointerType::List: {
        data_entry_key = index_.list->Name();
        break;
      }
      case PointerType::UnorderedCollection: {
        data_entry_key = index_.p_unordered_collection->Name();
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
        pmem_record = skiplist->Header()->record;
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
  char* bucket_ptr =
      (char*)main_buckets_ + (uint64_t)hint.bucket * hash_bucket_size_;
  _mm_prefetch(bucket_ptr, _MM_HINT_T0);

  uint32_t key_hash_prefix = hint.key_hash_value >> 32;
  uint64_t entries = hash_bucket_entries_[hint.bucket];
  bool found = false;

  // search cache
  *entry_ptr = slots_[hint.slot].hash_cache.entry_ptr;
  if (*entry_ptr != nullptr) {
    atomic_load_16(hash_entry_snap, *entry_ptr);
    if (hash_entry_snap->Match(key, key_hash_prefix, type_mask,
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

      atomic_load_16(hash_entry_snap, *entry_ptr);
      if (hash_entry_snap->Match(key, key_hash_prefix, type_mask,
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
          kvdk_assert((uint64_t)next_off % sizeof(HashEntry) == 0,
                      "hash table bucket address should aligned to hash entry "
                      "size in allocate new bucket");
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
    (*entry_ptr)->clear();
    hash_bucket_entries_[hint.bucket]++;
  }

  return found ? Status::Ok : Status::NotFound;
}

Status HashTable::SearchForRead(const KeyHashHint& hint, const StringView& key,
                                uint16_t type_mask, HashEntry** entry_ptr,
                                HashEntry* hash_entry_snap,
                                DataEntry* data_entry_meta) {
  assert(entry_ptr);
  *entry_ptr = nullptr;
  char* bucket_ptr =
      (char*)main_buckets_ + (uint64_t)hint.bucket * hash_bucket_size_;
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

  // iterate hash entrys
  for (uint64_t i = 0; i < entries; i++) {
    if (i > 0 && i % num_entries_per_bucket_ == 0) {
      // next bucket
      memcpy_8(&bucket_ptr, bucket_ptr + hash_bucket_size_ - 8);
      _mm_prefetch(bucket_ptr, _MM_HINT_T0);
    }
    *entry_ptr = (HashEntry*)bucket_ptr + (i % num_entries_per_bucket_);
    while (1) {
      atomic_load_16(hash_entry_snap, *entry_ptr);
      if (hash_entry_snap->Match(key, key_hash_prefix, type_mask,
                                 data_entry_meta)) {
        slots_[hint.slot].hash_cache.entry_ptr = *entry_ptr;
        return Status::Ok;
      } else {
        // check if hash entry modified by another write thread during
        // match hash entry
        if (memcmp(hash_entry_snap, *(entry_ptr), sizeof(HashEntry)) == 0) {
          break;
        }
      }
    }
  }
  return Status::NotFound;
}

void HashTable::Insert(
    const KeyHashHint& hint, HashEntry* entry_ptr, RecordType type, void* index,
    PointerType index_type,
    HashEntryStatus entry_status /*= HashEntryStatus::Persist*/) {
  HashEntry new_hash_entry(hint.key_hash_value >> 32, type, index, index_type,
                           entry_status);
  atomic_store_16(entry_ptr, &new_hash_entry);
}

SlotIterator HashTable::GetSlotIterator() { return SlotIterator{this}; }

}  // namespace KVDK_NAMESPACE
