/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "hash_table.hpp"
#include "skiplist.hpp"
#include "thread_manager.hpp"
#include "unordered_collection.hpp"

namespace KVDK_NAMESPACE {
bool HashTable::MatchHashEntry(const pmem::obj::string_view &key,
                               uint32_t hash_k_prefix, uint16_t target_type,
                               const HashEntry *hash_entry, void *data_entry) {
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
    case HashOffsetType::UnorderedCollection:
    {
      UnorderedCollection* p_uncoll = hash_entry->p_uncoll;
      data_entry_key = p_uncoll->Name();
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
  char *bucket_base2 = main_buckets_ + (uint64_t)hint.bucket * hash_bucket_size_;
  _mm_prefetch(bucket_base, _MM_HINT_T0);


  uint32_t key_hash_prefix = hint.key_hash_value >> 32;
  uint64_t entries = hash_bucket_entries_[hint.bucket];
  bool found = false;

  if (entries == 7)
  {
    int breakpoint = 10;
  }
  

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
    HashEntry* p_hash_bucket_scanning = reinterpret_cast<HashEntry*>(bucket_base);
    uint64_t i = 0;
    while (i < entries) {

      HashEntry* p_hash_entry_scanning = reinterpret_cast<HashEntry*>(*entry_base);
      assert(*entry_base);
      memcpy_16(hash_entry, *entry_base);
      if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry,
                         data_entry)) {
        slots_[hint.slot].hash_cache.entry_base = *entry_base;
        found = true;
        break;
      }
      assert(*entry_base);

      if (purpose == SearchPurpose::Write /* we don't reused hash entry in
                                             recovering */
          && (*entry_base)->header.data_type == StringDeleteRecord) {
        reusable_entry = *entry_base;
      }

      i++;

      assert(*entry_base);
      // next bucket
      if (i % num_entries_per_bucket_ == 0) {
        char *next_off;
        if (i == entries) {
          if (purpose >= SearchPurpose::Write) {
            if (reusable_entry != nullptr) {
              assert(false);
              if (data_entry) {
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
              assert(next_off);
              memset(next_off, 0, space.size);
              memcpy_8(bucket_base + hash_bucket_size_ - 8, &next_off);
            }
          } else {
            break;
          }
        } else {
          memcpy_8(&next_off, bucket_base + hash_bucket_size_ - 8);
          assert(next_off);
        }
        assert(next_off);
        bucket_base = next_off;
        _mm_prefetch(bucket_base, _MM_HINT_T0);
        assert(bucket_base);
      }
      (*entry_base) = (HashEntry *)bucket_base + (i % num_entries_per_bucket_);
      assert(*entry_base);
    }
  }

  if (purpose >= SearchPurpose::Write) {
    if (found) {
      (*entry_base)->header.status = HashEntryStatus::Updating;
    } else {
      if ((*entry_base) == reusable_entry) {
        if ((*entry_base)->header.status == HashEntryStatus::Clean) {
          (*entry_base)->header.status = HashEntryStatus::Updating;
        } else {
          (*entry_base)->header.status = HashEntryStatus::BeingReused;
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

  if (hash_bucket_entries_[hint.bucket] == 7)
  {
    int breakpoint = 10;
  }

  assert(write_thread.id >= 0);

  HashEntry new_hash_entry(hint.key_hash_value >> 32, type, offset,
                           offset_type);

  if (entry_base->header.status == HashEntryStatus::Updating) {
    HashEntry::CopyOffset(entry_base, &new_hash_entry);
    HashEntry::CopyHeader(entry_base, &new_hash_entry);
  } else {
    bool new_entry = entry_base->header.status == HashEntryStatus::Initializing;
    memcpy_16(entry_base, &new_hash_entry);
    if (new_entry) { // new allocated
      hash_bucket_entries_[hint.bucket]++;
    }
  }
}

bool HashTable::MatchImpl2(pmem::obj::string_view key, HashEntry matching_entry)
{
  pmem::obj::string_view record_key;
  switch (matching_entry.header.offset_type) {
  case HashOffsetType::DataEntry: {
    DataEntry* pmp_record = reinterpret_cast<DataEntry*>(pmem_allocator_->offset2addr(matching_entry.offset));
    record_key = pmp_record->Key();
    break;
  }
  case HashOffsetType::UnorderedCollectionElement:
  case HashOffsetType::DLDataEntry: {
    DLDataEntry* pmp_record = reinterpret_cast<DLDataEntry*>(pmem_allocator_->offset2addr(matching_entry.offset));
    record_key = pmp_record->Key();
    break;
  }
  case HashOffsetType::UnorderedCollection:
  {
    std::shared_ptr<UnorderedCollection> un_coll = matching_entry.p_uncoll->shared_from_this();
  }
  case HashOffsetType::SkiplistNode: {
    SkiplistNode *dram_node = (SkiplistNode *)matching_entry.offset;
    record_key = ((DLDataEntry *)dram_node->data_entry)->Key();
    break;
  }
  case HashOffsetType::Skiplist: {
    Skiplist *skiplist = (Skiplist *)matching_entry.offset;
    record_key = skiplist->name();
    break;
  }
  case HashOffsetType::Invalid:
  default: {
    GlobalLogger.Error("Not supported hash offset type: %u\n",
                        matching_entry.header.offset_type);
    assert(false && "Invalid HashOffsetType!");
    return false;
  }
  }

  return (compare_string_view(key, record_key) == 0);
}

// If existing HashEntry indexing a key is found, its address is returned
// Else return address suitable to position a HashEntry that matches the key
// User should Check the content of this address himself or herself 
// by looking at HashEntry::DataEntryType.
// If it is DataEntryType::Empty, then user should assume the search found nothing.
// Otherwise a valid HashEntry* is returned and can be used to retrieve data
// from PMem
HashEntry* HashTable::SearchImpl2(KeyHashHint hint, pmem::obj::string_view key, bool (*type_matcher)(DataEntryType))
{
  HashEntry *p_bucket_chain = reinterpret_cast<HashEntry*>(main_buckets_ + (uint64_t)hint.bucket * hash_bucket_size_);
  _mm_prefetch(p_bucket_chain, _MM_HINT_T0);

  uint32_t key_hash_prefix = hint.key_hash_value >> 32;

  // Scan cache
  HashEntry *p_entry_scanning = slots_[hint.slot].hash_cache.entry_base;
  if (p_entry_scanning != nullptr &&
      p_entry_scanning->header.key_prefix == key_hash_prefix &&
      type_matcher((DataEntryType)(p_entry_scanning->header.data_type)) &&
      MatchImpl2(key, *p_entry_scanning))
    return p_entry_scanning;
      
  // Match cache failed, scanning whole bucket
  size_t n_entries_in_bucket_chain = hash_bucket_entries_[hint.bucket];
  HashEntry* p_bucket = p_bucket_chain;
  p_entry_scanning = p_bucket_chain;
  for (size_t i = 0; i < n_entries_in_bucket_chain; ++i)
  {
    // Scan a HashEntry
    if (p_entry_scanning->header.key_prefix == key_hash_prefix &&
        type_matcher((DataEntryType)(p_entry_scanning->header.data_type)) &&
        MatchImpl2(key, *p_entry_scanning)){}
      return p_entry_scanning;

    // Else, not matching, goto next HashEntry,
    // Which can be in current bucket or next bucket
    ++p_entry_scanning;
    if(i % num_entries_per_bucket_ != num_entries_per_bucket_-1)
    {
      // Goto next HashEntry in current bucket
      continue;
    }
    else
    {
      // Reach last entry of bucket, may jump to next bucket or initialize new bucket
      p_bucket = nullptr;
      memcpy(&p_bucket, p_entry_scanning, sizeof(HashEntry*));
      // Bucket end is pointer to next bucket
      if (i < n_entries_in_bucket_chain)
      {
        // reach end of current bucket, jump to next bucket
        p_entry_scanning = p_bucket;
        assert(next_bucket && "Should not have reach the end of bucket chain!");
        continue;
      }
      else
      {
        // reach end of current bucket and also end of bucket chain
        // If the last HashEntry, treated as HashEntry*, is nullptr
        // Then we need to allocates new bucket
        if (p_bucket == nullptr)
        {
          auto space = dram_allocator_->Allocate(hash_bucket_size_);
          if (space.size == 0) {
            GlobalLogger.Error("Memory overflow!\n");
            throw std::bad_alloc{};
          }
          p_bucket = reinterpret_cast<HashEntry*>(dram_allocator_->offset2addr(space.space_entry.offset));

          p_entry_scanning = p_bucket;
          memset(p_bucket, 0, hash_bucket_size_);

          return p_entry_scanning;
        }
        // New bucket allocated but no HashEntry has been put in
        else
        {
          p_entry_scanning = p_bucket;
          return p_entry_scanning;
        }        
      }        
    }     
  }
}

void HashTable::InsertImpl2(HashEntry* const where, HashEntry new_hash_entry)
{
  *where = new_hash_entry;
}


} // namespace KVDK_NAMESPACE
