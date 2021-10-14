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
                               void *data_entry_metadata) {
  if (hash_entry->header.status == HashEntryStatus::Empty) {
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
    case HashOffsetType::UnorderedCollectionElement:
    case HashOffsetType::DLDataEntry: {
      data_entry_pmem = pmem_allocator_->offset2addr(hash_entry->offset);
      data_entry_key = ((DLDataEntry *)data_entry_pmem)->Key();
      break;
    }
    case HashOffsetType::UnorderedCollection: {
      UnorderedCollection *p_collection = hash_entry->p_unordered_collection;
      data_entry_key = p_collection->Name();
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
      assert(false && "Trying to use invalid HashOffsetType!");
      return false;
    }
    }

    if (__glibc_likely(data_entry_metadata != nullptr)) {
      memcpy(data_entry_metadata, data_entry_pmem,
             data_entry_size(hash_entry->header.data_type));
    }

    if (compare_string_view(key, data_entry_key) == 0) {
      return true;
    }
  }
  return false;
}

Status HashTable::SearchForWrite(const KeyHashHint &hint,
                                 const pmem::obj::string_view &key,
                                 uint16_t type_mask, HashEntry **entry_base,
                                 HashEntry *hash_entry_snap,
                                 DataEntry *data_entry_meta, bool in_recovery) {
  assert(entry_base);
  assert((*entry_base) == nullptr);
  HashEntry *reusable_entry = nullptr;
  char *bucket_base = main_buckets_ + (uint64_t)hint.bucket * hash_bucket_size_;
  _mm_prefetch(bucket_base, _MM_HINT_T0);

  uint32_t key_hash_prefix = hint.key_hash_value >> 32;
  uint64_t entries = hash_bucket_entries_[hint.bucket];
  // if (entries > 10)
  // {
  //   std::cerr
  //     << "Too many entries in one bucket!\n"
  //     << "Bucket: " << hint.bucket << "\t"
  //     << "Hash: " << hint.key_hash_value << "\t"
  //     << "Entries: " << entries << "\t"
  //     << "Type: " << type_mask << "\t"
  //     << "Key: " << key << std::endl;

  //   // char* const p = main_buckets_ + (uint64_t)hint.bucket * hash_bucket_size_;
  //   // for (HashEntry const* p_bucket = reinterpret_cast<HashEntry const*>(p); i < count; i++)
  //   // {
  //   //   /* code */
  //   // }
     
  //   // std::abort();
  // }
  
  bool found = false;

  // search cache
  *entry_base = slots_[hint.slot].hash_cache.entry_base;
  if (*entry_base != nullptr) {
    memcpy_16(hash_entry_snap, *entry_base);
    if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry_snap,
                       data_entry_meta)) {
      (*entry_base)->header.status = HashEntryStatus::Updating;
      found = true;
    }
  }

  if (!found) {
    // iterate hash entries
    *entry_base = (HashEntry *)bucket_base;
    uint64_t i = 0;
    for (i = 0; i < entries; i++) {
      if (i > 0 && i % num_entries_per_bucket_ == 0) {
        // next bucket
        memcpy_8(&bucket_base, bucket_base + hash_bucket_size_ - 8);
        _mm_prefetch(bucket_base, _MM_HINT_T0);
      }
      *entry_base = (HashEntry *)bucket_base + (i % num_entries_per_bucket_);

      memcpy_16(hash_entry_snap, *entry_base);
      if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry_snap,
                         data_entry_meta)) {
        slots_[hint.slot].hash_cache.entry_base = *entry_base;
        found = true;
        break;
      }

      if (!in_recovery /* we don't reused hash entry in
                                             recovering */
          && (*entry_base)->Reusable()) {
        reusable_entry = *entry_base;
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
          *entry_base = reusable_entry;
        } else {
          auto space = dram_allocator_.Allocate(hash_bucket_size_);
          if (space.size == 0) {
            GlobalLogger.Error("Memory overflow!\n");
            return Status::MemoryOverflow;
          }
          char *next_off;
          next_off = dram_allocator_.offset2addr(space.space_entry.offset);
          memset(next_off, 0, space.size);
          memcpy_8(bucket_base + hash_bucket_size_ - 8, &next_off);
          *entry_base = (HashEntry *)next_off;
        }
      } else {
        *entry_base = (HashEntry *)bucket_base + (i % num_entries_per_bucket_);
      }
    }
  }

  // set status of writing position, see comments of HashEntryStatus
  if (found) {
    (*entry_base)->header.status = HashEntryStatus::Updating;
  } else {
    if ((*entry_base) == reusable_entry) {
      if ((*entry_base)->header.status == HashEntryStatus::CleanReusable) {
        (*entry_base)->header.status = HashEntryStatus::Updating;
      }
    } else {
      (*entry_base)->header.status = HashEntryStatus::Initializing;
    }
  }

  return found ? Status::Ok : Status::NotFound;
}

Status HashTable::SearchForRead(const KeyHashHint &hint,
                                const pmem::obj::string_view &key,
                                uint16_t type_mask, HashEntry **entry_base,
                                HashEntry *hash_entry_snap,
                                DataEntry *data_entry_meta) {
  assert(entry_base);
  assert((*entry_base) == nullptr);
  char *bucket_base = main_buckets_ + (uint64_t)hint.bucket * hash_bucket_size_;
  _mm_prefetch(bucket_base, _MM_HINT_T0);

  uint32_t key_hash_prefix = hint.key_hash_value >> 32;
  uint64_t entries = hash_bucket_entries_[hint.bucket];
  bool found = false;

  // search cache
  *entry_base = slots_[hint.slot].hash_cache.entry_base;
  if (*entry_base != nullptr) {
    memcpy_16(hash_entry_snap, *entry_base);
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
        memcpy_8(&bucket_base, bucket_base + hash_bucket_size_ - 8);
        _mm_prefetch(bucket_base, _MM_HINT_T0);
      }
      *entry_base = (HashEntry *)bucket_base + (i % num_entries_per_bucket_);

      memcpy_16(hash_entry_snap, *entry_base);
      if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry_snap,
                         data_entry_meta)) {
        slots_[hint.slot].hash_cache.entry_base = *entry_base;
        found = true;
        break;
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
                           (type == StringDeleteRecord)
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
