/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "hash_table.hpp"
#include "skiplist.hpp"
#include "thread_manager.hpp"

namespace KVDK_NAMESPACE {
bool HashTable::MatchHashEntry(const pmem::obj::string_view &key,
                               uint32_t hash_k_prefix, uint16_t target_type,
                               const HashEntry *hash_entry, void *data_entry) {
  if (hash_entry->header.status == HashEntryStatus::Clean) {
    return false;
  }
  if ((target_type & hash_entry->header.data_type) &&
      hash_k_prefix == hash_entry->header.key_prefix) {

    void *data_entry_pmem;
    pmem::obj::string_view data_entry_key;

    switch (hash_entry->header.offset_type) {
    case HashOffsetType::DataEntry: {
      data_entry_pmem = pmem_allocator_->offset2addr(hash_entry->offset);
      data_entry_key = ((DataEntry *)data_entry_pmem)->Key();
      break;
    }
    case HashOffsetType::DLDataEntry: {
      data_entry_pmem = pmem_allocator_->offset2addr(hash_entry->offset);
      data_entry_key = ((DLDataEntry *)data_entry_pmem)->Key();
      break;
    }
    case HashOffsetType::SkiplistNode: {
      SkiplistNode *dram_node = (SkiplistNode *)hash_entry->offset;
      data_entry_pmem = dram_node->data_entry;
      data_entry_key = ((DLDataEntry *)data_entry_pmem)->Key();
      break;
    }
    case HashOffsetType::Skiplist: {
      Skiplist *skiplist = (Skiplist *)hash_entry->offset;
      data_entry_pmem = skiplist->header()->data_entry;
      data_entry_key = skiplist->name();
      break;
    }
    default: {
      GlobalLogger.Error("Not supported hash offset type: %u\n",
                         hash_entry->header.offset_type);
      return false;
    }
    }

    if (__glibc_likely(data_entry != nullptr)) {
      memcpy(data_entry, data_entry_pmem,
             data_entry_size(hash_entry->header.data_type));
    }

    if (compare_string_view(key, data_entry_key) == 0) {
      return true;
    }
  }
  return false;
}

Status HashTable::Search(const KeyHashHint &hint,
                         const pmem::obj::string_view &key, uint16_t type_mask,
                         HashEntry *hash_entry, DataEntry *data_entry,
                         HashEntry **entry_base, SearchPurpose purpose) {
  assert(purpose == SearchPurpose::Read || write_thread.id >= 0);
  assert(entry_base);
  assert((*entry_base) == nullptr);
  HashEntry *reusable_entry = nullptr;
  char *bucket_base = main_buckets_ + (uint64_t)hint.bucket * hash_bucket_size_;
  _mm_prefetch(bucket_base, _MM_HINT_T0);

  uint32_t key_hash_prefix = hint.key_hash_value >> 32;
  uint64_t entries = hash_bucket_entries_[hint.bucket];
  bool found = false;

  // search cache
  *entry_base = slots_[hint.slot].hash_cache.entry_base;
  if (*entry_base != nullptr) {
    memcpy_16(hash_entry, *entry_base);
    if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry,
                       data_entry)) {
      if (purpose >= SearchPurpose::Write) {
        (*entry_base)->header.status = HashEntryStatus::Updating;
      }
      found = true;
    }
  }

  if (!found) {
    // iterate hash entrys
    *entry_base = (HashEntry *)bucket_base;
    uint64_t i = 0;
    while (i < entries) {
      memcpy_16(hash_entry, *entry_base);
      if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry,
                         data_entry)) {
        slots_[hint.slot].hash_cache.entry_base = *entry_base;
        found = true;
        break;
      }

      if (purpose == SearchPurpose::Write /* we don't reused hash entry in
                                             recovering */
          && (*entry_base)->Reusable()) {
        reusable_entry = *entry_base;
      }

      i++;

      // next bucket
      if (i % num_entries_per_bucket_ == 0) {
        char *next_off;
        if (i == entries) {
          if (purpose >= SearchPurpose::Write) {
            if (reusable_entry != nullptr) {
              if (data_entry &&
                  reusable_entry->header.status != HashEntryStatus::Clean) {
                memcpy(data_entry,
                       pmem_allocator_->offset2addr(reusable_entry->offset),
                       sizeof(DataEntry));
              }
              *entry_base = reusable_entry;
              break;
            } else {
              auto space = dram_allocator_->Allocate(hash_bucket_size_);
              if (space.size == 0) {
                GlobalLogger.Error("Memory overflow!\n");
                return Status::MemoryOverflow;
              }
              next_off = dram_allocator_->offset2addr(space.space_entry.offset);
              memset(next_off, 0, space.size);
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
  }

  if (purpose >= SearchPurpose::Write) {
    if (found) {
      (*entry_base)->header.status = HashEntryStatus::Updating;
    } else {
      if ((*entry_base) == reusable_entry) {
        if ((*entry_base)->header.status == HashEntryStatus::CleanReusable) {
          // Reuse a clean reusable hash entry is as same as updating
          (*entry_base)->header.status = HashEntryStatus::Updating;
        }
      } else {
        (*entry_base)->header.status = HashEntryStatus::Initializing;
      }
    }
  }

  return found ? Status::Ok : Status::NotFound;
}

void HashTable::Insert(const KeyHashHint &hint, HashEntry *entry_base,
                       uint16_t type, uint64_t offset,
                       HashOffsetType offset_type) {
  assert(write_thread.id >= 0);

  HashEntry new_hash_entry(hint.key_hash_value >> 32, type, offset,
                           type == StringDeleteRecord
                               ? HashEntryStatus::DirtyReusable
                               : HashEntryStatus::Normal,
                           offset_type);

  bool new_entry = entry_base->header.status == HashEntryStatus::Initializing;
  memcpy_16(entry_base, &new_hash_entry);
  if (new_entry) { // new allocated
    hash_bucket_entries_[hint.bucket]++;
  }
}
} // namespace KVDK_NAMESPACE
