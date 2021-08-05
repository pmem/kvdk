/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "mempool.hpp"
#include "pmemdb/db.hpp"
#include "structures.hpp"
#include <vector>

namespace PMEMDB_NAMESPACE {

class HashTable {
public:
  struct HashHint {
    uint32_t bucket;
    uint32_t slot;
    SpinMutex *spin;
  };

  HashTable(uint64_t max_memory_usage, uint64_t hash_bucket_num,
            uint32_t hash_bucket_size, uint32_t slot_grain,
            char *pmem_value_log, uint32_t pmem_block_size,
            std::vector<ThreadSpace> &t)
      : hash_bucket_num_(hash_bucket_num), slot_grain_(slot_grain),
        hash_bucket_size_(hash_bucket_size), thread_space_(t),
        pmem_value_log_(pmem_value_log), pmem_block_size_(pmem_block_size),
        mempool_(hash_bucket_size, max_memory_usage) {
    slots_.resize(hash_bucket_num / slot_grain);
    hash_bucket_entries_.resize(hash_bucket_num, 0);
    mempool_.GetChunk(hash_bucket_num, dram_hash_map_chunk_offset_);
  }

  HashHint GetHint(uint64_t key_hash_value) {
    uint32_t bucket = get_bucket_num(key_hash_value);
    uint32_t slot = get_slot_num(bucket);
    return std::move(HashHint{bucket, slot, &slots_[slot].spin});
  }

  bool Search(int thread_id, const Slice &key, uint64_t key_hash_value,
              uint8_t type_mask, HashEntry *hash_entry, DataEntry *data_entry,
              HashEntry **entry_base, bool search_for_write, HashHint *hint) {
    assert(entry_base);
    assert((*entry_base) == nullptr);

    uint32_t bucket = hint ? (hint->bucket) : get_bucket_num(key_hash_value);
    uint32_t slot = hint ? (hint->slot) : get_slot_num(bucket);
    HashEntry *reusable_entry = nullptr;
    char *bucket_base =
        mempool_.GetAddress(dram_hash_map_chunk_offset_ + bucket);

    _mm_prefetch(bucket_base, _MM_HINT_T0);
    _mm_prefetch(bucket_base + 64, _MM_HINT_T0);

    bool is_found = false;
    uint64_t hash_bucket_entry_num =
        (hash_bucket_size_ - 4) / sizeof(HashEntry);

    uint32_t key_hash_prefix = key_hash_value >> 32;

    uint64_t entries = hash_bucket_entries_[bucket];
    {
      //       search cache
      if (!search_for_write) {
        *entry_base = slots_[slot].hash_cache.entry_base;
        if (*entry_base != nullptr) {
          memcpy_s(hash_entry, sizeof(HashEntry), *entry_base,
                   sizeof(HashEntry));
          if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry,
                             data_entry)) {
            return true;
          }
        }
      }

      if (!is_found) {
        *entry_base = (HashEntry *)bucket_base;
        for (uint32_t i = 0; i < entries; i++) {
          memcpy_s(hash_entry, sizeof(HashEntry), *entry_base,
                   sizeof(HashEntry));
          if (MatchHashEntry(key, key_hash_prefix, type_mask, hash_entry,
                             data_entry)) {
            slots_[slot].hash_cache.entry_base = *entry_base;
            is_found = true;
            break;
          }

          if (search_for_write && (*entry_base)->type == HASH_DELETE_RECORD &&
              (*entry_base)->reference == 0) {
            reusable_entry = *entry_base;
          }

          if (!search_for_write && i == entries - 1) {
            break;
          }

          if ((i + 1) % hash_bucket_entry_num == 0) {
            uint32_t next_off;
            // allocate new bucket for write
            if (i + 1 == entries) {
              if (search_for_write && reusable_entry == nullptr) {
                if (thread_space_[thread_id].usable_chunks == 0) {
                  mempool_.GetChunk(
                      DRAM_CHUNKS_ALLOCATION_NUM,
                      thread_space_[thread_id].dram_chunks_offset);
                  thread_space_[thread_id].usable_chunks =
                      DRAM_CHUNKS_ALLOCATION_NUM;
                }
                next_off = thread_space_[thread_id].dram_chunks_offset++;
                thread_space_[thread_id].usable_chunks--;
                memcpy_4(bucket_base + hash_bucket_size_ - 4, &next_off);
              } else {
                break;
              }
            } else {
              memcpy_4(&next_off, bucket_base + hash_bucket_size_ - 4);
            }

            bucket_base = mempool_.GetAddress(next_off);
            _mm_prefetch(bucket_base, _MM_HINT_T0);
            _mm_prefetch(bucket_base + 64, _MM_HINT_T0);

            *entry_base = (HashEntry *)bucket_base;
          } else {
            (*entry_base)++;
          }
        }
      }
    }
    if (!is_found && search_for_write && reusable_entry) {
      *entry_base = reusable_entry;
    }
    return is_found;
  }

  void Insert(int thread_id, HashEntry *entry_base, uint64_t key_hash_value,
              uint8_t type, uint64_t offset, DataEntry *updated_data_entry,
              HashHint *hint) {
    uint32_t bucket = hint ? hint->bucket : get_bucket_num(key_hash_value);
    uint32_t slot = hint ? hint->slot : get_slot_num(bucket);

    uint32_t new_existing_pmem_records =
        (type == HASH_DELETE_RECORD)
            ? (updated_data_entry ? entry_base->reference : 0)
            : (updated_data_entry ? (entry_base->reference + 1) : 1);
    HashEntry new_hash_entry(key_hash_value >> 32, new_existing_pmem_records,
                             type, offset);
    bool reused_entry = false;
    if (!updated_data_entry &&
        entry_base->type == HASH_DELETE_RECORD) { // reused hash entry
      DataEntry *reused_data_entry =
          (DataEntry *)(pmem_value_log_ +
                        entry_base->offset * pmem_block_size_);
      thread_space_[thread_id].free_list.Push(
          entry_base->offset, reused_data_entry->b_size, nullptr, nullptr);
      reused_entry = true;
    }

    if (!updated_data_entry) {
      memcpy_s(entry_base, sizeof(HashEntry), &new_hash_entry,
               sizeof(HashEntry));
      if (!reused_entry) { // new allocated
        hash_bucket_entries_[bucket]++;
      }
    } else {
      auto old_entry(*entry_base);
      HashEntry::CopyOffset(entry_base, &new_hash_entry);
      HashEntry::CopyHeader(entry_base, &new_hash_entry);
      if (old_entry.type != HASH_DELETE_RECORD) {
        thread_space_[thread_id].free_list.Push(
            (old_entry.type & HASH_DATA_RECORD)
                ? old_entry.offset
                : ((char *)((Node *)old_entry.offset)->data_entry -
                   pmem_value_log_) /
                      pmem_block_size_,
            updated_data_entry->b_size, entry_base, &slots_[slot].spin);
      } else {
        thread_space_[thread_id].free_list.Push(
            old_entry.offset, updated_data_entry->b_size, nullptr, nullptr);
      }
    }
  }

private:
  inline uint32_t get_bucket_num(uint64_t key_hash_value) {
    return key_hash_value & (hash_bucket_num_ - 1);
  }

  inline uint32_t get_slot_num(uint32_t bucket) { return bucket / slot_grain_; }

  inline bool MatchHashEntry(const Slice &key, uint32_t hash_k_prefix,
                             uint8_t target_type, const HashEntry *hash_entry,
                             void *data_entry) {
    if ((target_type & hash_entry->type) &&
        hash_k_prefix == hash_entry->key_prefix) {

      void *data_entry_pmem;
      if (hash_entry->type & (SORTED_DATA_RECORD | SORTED_DELETE_RECORD)) {
        Node *node = (Node *)hash_entry->offset;
        data_entry_pmem = node->data_entry;
      } else if (hash_entry->type & (HASH_DELETE_RECORD | HASH_DATA_RECORD)) {
        data_entry_pmem =
            (pmem_value_log_ + hash_entry->offset * pmem_block_size_);
      } else {
        return false;
      }
      memcpy_s(data_entry, data_entry_size(hash_entry->type), data_entry_pmem,
               data_entry_size(hash_entry->type));

      if (((DataEntry *)data_entry)->k_size == key.size() &&
          memcmp(key.data(),
                 (char *)data_entry_pmem + data_entry_size(hash_entry->type),
                 key.size()) == 0) {
        return true;
      }
    }
    return false;
  }

  std::vector<uint64_t> hash_bucket_entries_;
  const uint64_t hash_bucket_num_;
  const uint32_t slot_grain_;
  const uint32_t hash_bucket_size_;
  const uint32_t pmem_block_size_;
  uint32_t dram_hash_map_chunk_offset_;
  MemoryPool mempool_;
  char *pmem_value_log_;
  std::vector<Slot> slots_;
  std::vector<ThreadSpace> &thread_space_;
};
} // namespace PMEMDB_NAMESPACE