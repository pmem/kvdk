/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "hash_table.hpp"
#include "skiplist.hpp"
#include "thread_manager.hpp"
#include "unordered_collection.hpp"

namespace KVDK_NAMESPACE {
HashTable *
HashTable::NewHashTable(uint64_t hash_bucket_num, uint32_t hash_bucket_size,
                        uint32_t num_buckets_per_slot,
                        const std::shared_ptr<PMEMAllocator> &pmem_allocator,
                        uint32_t write_threads) {
  HashTable *table = new (std::nothrow)
      HashTable(hash_bucket_num, hash_bucket_size, num_buckets_per_slot,
                pmem_allocator, write_threads);
  if (table) {
    auto main_buckets_space =
        table->dram_allocator_.Allocate(hash_bucket_size * hash_bucket_num);
    if (main_buckets_space.size == 0) {
      GlobalLogger.Error("No enough dram to create global hash table\n");
      delete table;
      table = nullptr;
    } else {
      table->main_buckets_ = table->dram_allocator_.offset2addr(
          main_buckets_space.space_entry.offset);
    }
  }
  return table;
}

bool HashTable::MatchHashEntry(const pmem::obj::string_view &key,
                               uint32_t hash_k_prefix, uint16_t target_type,
                               const HashEntry *hash_entry,
                               DataEntry *data_entry_metadata) {
  if (hash_entry->header.status == HashEntryStatus::Empty) {
    return false;
  }
  if ((target_type & hash_entry->header.data_type) &&
      hash_k_prefix == hash_entry->header.key_prefix) {

    void *pmem_record;
    pmem::obj::string_view data_entry_key;

    switch (hash_entry->header.offset_type) {
    case HashOffsetType::StringRecord: {
      pmem_record = pmem_allocator_->offset2addr(hash_entry->offset);
      data_entry_key = static_cast<StringRecord *>(pmem_record)->Key();
      break;
    }
    case HashOffsetType::UnorderedCollectionElement:
    case HashOffsetType::DLRecord: {
      pmem_record = pmem_allocator_->offset2addr(hash_entry->offset);
      data_entry_key = static_cast<DLRecord *>(pmem_record)->Key();
      break;
    }
    case HashOffsetType::UnorderedCollection: {
      UnorderedCollection *p_collection = hash_entry->p_unordered_collection;
      data_entry_key = p_collection->Name();
      break;
    }
    case HashOffsetType::SkiplistNode: {
      SkiplistNode *dram_node = (SkiplistNode *)hash_entry->offset;
      pmem_record = dram_node->record;
      data_entry_key = static_cast<DLRecord *>(pmem_record)->Key();
      break;
    }
    case HashOffsetType::Skiplist: {
      Skiplist *skiplist = (Skiplist *)hash_entry->offset;
      pmem_record = skiplist->header()->record;
      data_entry_key = skiplist->name();
      break;
    }
    default: {
      GlobalLogger.Error("Not supported hash offset type: %u\n",
                         hash_entry->header.offset_type);
      assert(false && "Trying to use invalid HashOffsetType!");
      return false;
    }
    }

    if (__glibc_likely(data_entry_metadata != nullptr)) {
      memcpy(data_entry_metadata, pmem_record, sizeof(DataEntry));
    }

    if (compare_string_view(key, data_entry_key) == 0) {
      return true;
    }
  }
  return false;
}

Status HashTable::SearchForWrite(const KeyHashHint &hint,
                                 const pmem::obj::string_view &key,
                                 uint16_t type_mask, HashEntry **entry_ptr,
                                 HashEntry *hash_entry_snap,
                                 DataEntry *data_entry_meta, bool in_recovery) {
  assert(entry_ptr);
  assert((*entry_ptr) == nullptr);
  HashEntry *reusable_entry = nullptr;
  char *bucket_ptr =
      (char *)main_buckets_ + (uint64_t)hint.bucket * hash_bucket_size_;
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
      (*entry_ptr)->header.status = HashEntryStatus::Updating;
      found = true;
    }
  }

  if (!found) {
    // iterate hash entries
    *entry_ptr = (HashEntry *)bucket_ptr;
    uint64_t i = 0;
    for (i = 0; i < entries; i++) {
      if (i > 0 && i % num_entries_per_bucket_ == 0) {
        // next bucket
        memcpy_8(&bucket_ptr, bucket_ptr + hash_bucket_size_ - 8);
        _mm_prefetch(bucket_ptr, _MM_HINT_T0);
      }
      *entry_ptr = (HashEntry *)bucket_ptr + (i % num_entries_per_bucket_);

      memcpy_16(hash_entry_snap, *entry_ptr);
      if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry_snap,
                         data_entry_meta)) {
        slots_[hint.slot].hash_cache.entry_ptr = *entry_ptr;
        found = true;
        break;
      }

      if (!in_recovery /* we don't reused hash entry in
                                             recovering */
          && (*entry_ptr)->Reusable()) {
        reusable_entry = *entry_ptr;
      }
    }

    if (!found) {
      // reach end of buckets, reuse entry or allocate a new bucket
      if (i > 0 && i % num_entries_per_bucket_ == 0) {
        if (reusable_entry != nullptr) {
          if (data_entry_meta && !reusable_entry->Empty()) {
            memcpy(data_entry_meta,
                   pmem_allocator_->offset2addr(reusable_entry->offset),
                   sizeof(DataEntry));
          }
          *entry_ptr = reusable_entry;
        } else {
          auto space = dram_allocator_.Allocate(hash_bucket_size_);
          if (space.size == 0) {
            GlobalLogger.Error("Memory overflow!\n");
            return Status::MemoryOverflow;
          }
          void *next_off =
              dram_allocator_.offset2addr(space.space_entry.offset);
          memset(next_off, 0, space.size);
          memcpy_8(bucket_ptr + hash_bucket_size_ - 8, &next_off);
          *entry_ptr = (HashEntry *)next_off;
        }
      } else {
        *entry_ptr = (HashEntry *)bucket_ptr + (i % num_entries_per_bucket_);
      }
    }
  }

  // set status of writing position, see comments of HashEntryStatus
  if (found) {
    (*entry_ptr)->header.status = HashEntryStatus::Updating;
  } else {
    if ((*entry_ptr) == reusable_entry) {
      if ((*entry_ptr)->header.status == HashEntryStatus::CleanReusable) {
        (*entry_ptr)->header.status = HashEntryStatus::Updating;
      }
    } else {
      (*entry_ptr)->header.status = HashEntryStatus::Initializing;
    }
  }

  return found ? Status::Ok : Status::NotFound;
}

Status HashTable::SearchForRead(const KeyHashHint &hint,
                                const pmem::obj::string_view &key,
                                uint16_t type_mask, HashEntry **entry_ptr,
                                HashEntry *hash_entry_snap,
                                DataEntry *data_entry_meta) {
  assert(entry_ptr);
  assert((*entry_ptr) == nullptr);
  char *bucket_ptr =
      (char *)main_buckets_ + (uint64_t)hint.bucket * hash_bucket_size_;
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
    // iterate hash entrys
    for (uint64_t i = 0; i < entries; i++) {
      if (i > 0 && i % num_entries_per_bucket_ == 0) {
        // next bucket
        memcpy_8(&bucket_ptr, bucket_ptr + hash_bucket_size_ - 8);
        _mm_prefetch(bucket_ptr, _MM_HINT_T0);
      }
      *entry_ptr = (HashEntry *)bucket_ptr + (i % num_entries_per_bucket_);

      memcpy_16(hash_entry_snap, *entry_ptr);
      if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry_snap,
                         data_entry_meta)) {
        slots_[hint.slot].hash_cache.entry_ptr = *entry_ptr;
        found = true;
        break;
      }
    }
  }
  return found ? Status::Ok : Status::NotFound;
}

void HashTable::Insert(const KeyHashHint &hint, HashEntry *entry_ptr,
                       uint16_t type, uint64_t offset,
                       HashOffsetType offset_type) {
  assert(write_thread.id >= 0);

  HashEntry new_hash_entry(hint.key_hash_value >> 32, type, offset,
                           (type == StringDeleteRecord)
                               ? HashEntryStatus::DirtyReusable
                               : HashEntryStatus::Normal,
                           offset_type);

  bool new_entry = entry_ptr->header.status == HashEntryStatus::Initializing;
  memcpy_16(entry_ptr, &new_hash_entry);
  if (new_entry) { // new allocated
    hash_bucket_entries_[hint.bucket]++;
  }
}

} // namespace KVDK_NAMESPACE
