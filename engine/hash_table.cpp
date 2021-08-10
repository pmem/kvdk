/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "hash_table.hpp"
#include "skiplist.hpp"
#include "thread_manager.hpp"

namespace KVDK_NAMESPACE {
bool HashTable::MatchHashEntry(const Slice &key, uint32_t hash_k_prefix,
                               uint16_t target_type,
                               const HashEntry *hash_entry, void *data_entry) {
  if ((target_type & hash_entry->header.type) &&
      hash_k_prefix == hash_entry->header.key_prefix) {

    void *data_entry_pmem;
    Slice data_entry_key;

    if (hash_entry->header.type & StringDataEntryType) {
      data_entry_pmem = pmem_allocator_->offset2addr(hash_entry->offset);
      data_entry_key = ((DataEntry *)data_entry_pmem)->Key();
    } else if (hash_entry->header.type &
               (SORTED_DATA_RECORD | SORTED_DELETE_RECORD)) {
      SkiplistNode *node = (SkiplistNode *)hash_entry->offset;
      data_entry_pmem = node->data_entry;
      data_entry_key = ((DLDataEntry *)data_entry_pmem)->Key();
    } else if (hash_entry->header.type & SORTED_HEADER_RECORD) {
      Skiplist *skiplist = (Skiplist *)hash_entry->offset;
      data_entry_pmem = skiplist->header()->data_entry;
      data_entry_key = skiplist->name();
    } else {
      // not supported yet
      return false;
    }

    if (__glibc_likely(data_entry != nullptr)) {
      memcpy(data_entry, data_entry_pmem,
             data_entry_size(hash_entry->header.type));
    }
    if (Slice::compare(key, data_entry_key) == 0) {
      return true;
    }
  }
  return false;
}

Status HashTable::Search(const KeyHashHint &hint, const Slice &key,
                         uint16_t type_mask, HashEntry *hash_entry,
                         DataEntry *data_entry, HashEntry **entry_base,
                         bool search_for_write) {
  assert(!search_for_write || local_thread.id >= 0);
  assert(entry_base);
  assert((*entry_base) == nullptr);
  HashEntry *reusable_entry = nullptr;
  char *bucket_base = main_buckets_ + (uint64_t)hint.bucket * hash_bucket_size_;
  _mm_prefetch(bucket_base, _MM_HINT_T0);

  bool is_found = false;

  uint32_t key_hash_prefix = hint.key_hash_value >> 32;
  uint64_t entries = hash_bucket_entries_[hint.bucket];

  // search cache
  *entry_base = slots_[hint.slot].hash_cache.entry_base;
  if (*entry_base != nullptr) {
    memcpy_16(hash_entry, *entry_base);
    if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry,
                       data_entry)) {
      return Status::Ok;
    }
  }

  // iterate hash entrys
  *entry_base = (HashEntry *)bucket_base;
  uint64_t i = 0;
  while (i < entries) {
    memcpy_16(hash_entry, *entry_base);
    if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry,
                       data_entry)) {
      slots_[hint.slot].hash_cache.entry_base = *entry_base;
      return Status::Ok;
    }

    if (search_for_write &&
        (*entry_base)->header.type == STRING_DELETE_RECORD &&
        (*entry_base)->header.reference == 0 &&
        (*entry_base)->header.key_prefix != key_hash_prefix) {
      reusable_entry = *entry_base;
    }

    i++;

    // next bucket
    if (i % num_entries_per_bucket_ == 0) {
      char *next_off;
      if (i == entries) {
        if (search_for_write) {
          if (reusable_entry != nullptr) {
            *entry_base = reusable_entry;
            break;
          } else {
            auto space = dram_allocator_->Allocate(hash_bucket_size_);
            if (space.size == 0) {
              GlobalLogger.Error("Memory overflow!\n");
              return Status::MemoryOverflow;
            }
            next_off = dram_allocator_->offset2addr(space.space_entry.offset);
            memcpy_8(bucket_base + hash_bucket_size_ - 8, &next_off);
          }
        } else {
          break;
        }
      } else {
        memcpy_8(&next_off, bucket_base + hash_bucket_size_ - 8);
      }
      bucket_base = next_off;
      _mm_prefetch(bucket_base, _MM_HINT_T0);
    }
    (*entry_base) = (HashEntry *)bucket_base + (i % num_entries_per_bucket_);
  }
  return Status::NotFound;
}

void HashTable::Insert(const KeyHashHint &hint, HashEntry *entry_base,
                       uint16_t type, uint64_t offset, bool is_update) {
  assert(local_thread.id >= 0);

  uint32_t new_existing_pmem_records =
      (type & DeleteDataEntryType)
          ? (is_update ? entry_base->header.reference : 0)
          : (is_update ? (entry_base->header.reference + 1) : 1);
  HashEntry new_hash_entry(hint.key_hash_value >> 32, new_existing_pmem_records,
                           type, offset);
  bool reused_entry = false;
  if (!is_update && entry_base->header.type == STRING_DELETE_RECORD) {
    DataEntry *reused_data_entry =
        (DataEntry *)(pmem_allocator_->offset2addr(entry_base->offset));
    pmem_allocator_->Free(SizedSpaceEntry(entry_base->offset,
                                          reused_data_entry->header.b_size,
                                          nullptr, nullptr));
    reused_entry = true;
  }

  if (!is_update) {
    memcpy_16(entry_base, &new_hash_entry);
    if (!reused_entry) { // new allocated
      hash_bucket_entries_[hint.bucket]++;
    }
  } else {
    HashEntry::CopyOffset(entry_base, &new_hash_entry);
    HashEntry::CopyHeader(entry_base, &new_hash_entry);
  }
}
} // namespace KVDK_NAMESPACE