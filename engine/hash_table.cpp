/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "hash_table.hpp"

#include "hash_collection/hash_list.hpp"
#include "list_collection/list.hpp"
#include "sorted_collection/skiplist.hpp"
#include "thread_manager.hpp"

namespace KVDK_NAMESPACE {
HashTable* HashTable::NewHashTable(uint64_t hash_bucket_num,
                                   uint32_t num_buckets_per_slot,
                                   const PMEMAllocator* pmem_allocator,
                                   uint32_t max_access_threads) {
  HashTable* table;
  // We catch exception here as we may need to allocate large memory for hash
  // table here
  try {
    table = new HashTable(hash_bucket_num, num_buckets_per_slot, pmem_allocator,
                          max_access_threads);
  } catch (std::bad_alloc& b) {
    GlobalLogger.Error("No enough dram to create global hash table: b\n",
                       b.what());
    table = nullptr;
  }

  return table;
}

bool HashEntry::Match(const StringView& key, uint32_t hash_k_prefix,
                      uint8_t target_type, DataEntry* data_entry_metadata) {
  if ((target_type & header_.record_type) &&
      hash_k_prefix == header_.key_prefix) {
    void* pmem_record = nullptr;
    StringView data_entry_key;

    switch (header_.index_type) {
      case PointerType::Empty:
      case PointerType::Allocated: {
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

template <bool may_insert>
HashTable::LookupResult HashTable::Lookup(const StringView& key,
                                          uint8_t type_mask) {
  LookupResult ret;
  HashEntry* empty_entry = nullptr;
  auto hint = GetHint(key);
  ret.key_hash_prefix = hint.key_hash_prefix;

  HashBucket* bucket_ptr = &hash_buckets_[hint.bucket];
  _mm_prefetch(bucket_ptr, _MM_HINT_T0);

  // search cache
  ret.entry_ptr = slots_[hint.slot].hash_cache.entry_ptr;
  if (ret.entry_ptr != nullptr) {
    atomic_load_16(&ret.entry, ret.entry_ptr);
    if (ret.entry.Match(key, hint.key_hash_prefix, type_mask, nullptr)) {
      return ret;
    }
  }

  // iterate hash entries in the bucket
  HashBucketIterator iter(this, hint.bucket);
  while (iter.Valid()) {
    ret.entry_ptr = &*iter;
    atomic_load_16(&ret.entry, ret.entry_ptr);
    if (ret.entry.Match(key, hint.key_hash_prefix, type_mask, nullptr)) {
      slots_[hint.slot].hash_cache.entry_ptr = ret.entry_ptr;
      return ret;
    }
    if (ret.entry_ptr->Empty()) {
      empty_entry = ret.entry_ptr;
    }
    iter++;
  }

  if (may_insert) {
    if (empty_entry == nullptr) {
      ret.s = allocateEntry(iter);
      if (ret.s != Status::Ok) {
        kvdk_assert(ret.s == Status::MemoryOverflow, "");
        return ret;
      }
      kvdk_assert(
          iter.Valid(),
          "HashBucketIterator should be valid after allocate new entry");
      kvdk_assert(iter->Empty(), "newly allocated hash entry should be empty");
      ret.entry_ptr = &(*iter);
    } else {
      ret.entry_ptr = empty_entry;
    }
  }

  ret.s = NotFound;
  if (may_insert) {
    ret.entry_ptr->MarkAsAllocated();
  }
  return ret;
}

template HashTable::LookupResult HashTable::Lookup<true>(const StringView&,
                                                         uint8_t);
template HashTable::LookupResult HashTable::Lookup<false>(const StringView&,
                                                          uint8_t);

void HashTable::Insert(const LookupResult& insert_position, RecordType type,
                       RecordStatus status, void* index,
                       PointerType index_type) {
  HashEntry new_hash_entry(insert_position.key_hash_prefix, type, status, index,
                           index_type);
  atomic_store_16(insert_position.entry_ptr, &new_hash_entry);
}

HashTable::LookupResult HashTable::Insert(const StringView& key,
                                          RecordType type, RecordStatus status,
                                          void* index, PointerType index_type) {
  auto lookup_result = Lookup<true>(key, type);
  if (lookup_result.s == Status::Ok || lookup_result.s == Status::NotFound) {
    Insert(lookup_result, type, status, index, index_type);
  }
  return lookup_result;
}

Status HashTable::allocateEntry(HashBucketIterator& bucket_iter) {
  kvdk_assert(bucket_iter.hash_table_ == this &&
                  bucket_iter.entry_idx_ ==
                      hash_bucket_entries_[bucket_iter.bucket_idx_],
              "Only allocate new hash entry at end of hash bucket");
  assert(bucket_iter.bucket_ptr_ != nullptr);
  if (hash_bucket_entries_[bucket_iter.bucket_idx_] > 0 &&
      hash_bucket_entries_[bucket_iter.bucket_idx_] % kNumEntryPerBucket == 0) {
    auto space = dram_allocator_.Allocate(kHashBucketSize);
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

HashTableIterator HashTable::GetIterator(uint64_t start_slot_idx,
                                         uint64_t end_slot_idx) {
  return HashTableIterator{this, start_slot_idx, end_slot_idx};
}

}  // namespace KVDK_NAMESPACE
