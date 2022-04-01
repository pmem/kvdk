/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "old_records_cleaner.hpp"

#include "../kv_engine.hpp"
#include "../sorted_collection/skiplist.hpp"

namespace KVDK_NAMESPACE {
void OldRecordsCleaner::PushToCache(const OldDataRecord& old_data_record) {
  kvdk_assert(
      static_cast<DataEntry*>(old_data_record.pmem_data_record)->meta.type &
          (StringDataRecord | SortedDataRecord),
      "Wrong type in OldRecordsCleaner::Push");
  kvdk_assert(access_thread.id >= 0,
              "call OldRecordsCleaner::Push with uninitialized access thread");

  auto& tc = cleaner_thread_cache_[access_thread.id];
  std::lock_guard<SpinMutex> lg(tc.old_records_lock);
  tc.old_data_records.emplace_back(old_data_record);
}

void OldRecordsCleaner::PushToCache(const OldDeleteRecord& old_delete_record) {
  kvdk_assert(
      static_cast<DataEntry*>(old_delete_record.pmem_delete_record)->meta.type &
          (StringDeleteRecord | SortedDeleteRecord | ExpirableRecordType),
      "Wrong type in OldRecordsCleaner::Push");
  kvdk_assert(access_thread.id >= 0,
              "call OldRecordsCleaner::Push with uninitialized access thread");
  kvdk_assert(access_thread.id >= 0,
              "call OldRecordsCleaner::Push with uninitialized access thread");

  auto& tc = cleaner_thread_cache_[access_thread.id];
  std::lock_guard<SpinMutex> lg(tc.old_records_lock);
  tc.old_delete_records.emplace_back(old_delete_record);
}

void OldRecordsCleaner::PushToGlobal(
    const std::deque<OldDeleteRecord>& old_delete_records) {
  global_old_delete_records_.emplace_back(old_delete_records);
}

void OldRecordsCleaner::TryGlobalClean() {
  std::vector<SpaceEntry> space_to_free;
  // records that can't be freed this time
  std::deque<OldDataRecord> data_record_refered;
  std::deque<OldDeleteRecord> delete_record_refered;
  PendingFreeSpaceEntries space_pending;
  // Update recorded oldest snapshot up to state so we can know which records
  // can be freed
  kv_engine_->version_controller_.UpdatedOldestSnapshot();
  TimeStampType oldest_snapshot_ts =
      kv_engine_->version_controller_.OldestSnapshotTS();

  // Fetch thread cached old records
  // Notice: As we can purge old delete records only after the older data
  // records are purged for recovery, so we must fetch cached old delete records
  // before cached old data records, and purge old data records before purge old
  // delete records here
  for (size_t i = 0; i < cleaner_thread_cache_.size(); i++) {
    auto& cleaner_thread_cache = cleaner_thread_cache_[i];
    // As clean delete record is costly, we prefer to amortize the overhead to
    // TryCleanCachedOldRecords(), otherwise this procedure will cost a longn
    // time. Only if there is to delete records waiting to be cleaned in a
    // thread cache, then we help to clean them here
    if (cleaner_thread_cache.old_delete_records.size() >
        kLimitCachedDeleteRecords) {
      std::lock_guard<SpinMutex> lg(cleaner_thread_cache.old_records_lock);
      global_old_delete_records_.emplace_back();
      global_old_delete_records_.back().swap(
          cleaner_thread_cache.old_delete_records);
      break;
    }
  }

  for (size_t i = 0; i < cleaner_thread_cache_.size(); i++) {
    auto& cleaner_thread_cache = cleaner_thread_cache_[i];
    if (cleaner_thread_cache.old_data_records.size() > 0) {
      std::lock_guard<SpinMutex> lg(cleaner_thread_cache.old_records_lock);
      global_old_data_records_.emplace_back();
      global_old_data_records_.back().swap(
          cleaner_thread_cache.old_data_records);
    }
  }

  // Find free-able data records
  for (auto& data_records : global_old_data_records_) {
    for (auto& record : data_records) {
      if (record.release_time <= oldest_snapshot_ts) {
        space_to_free.emplace_back(purgeOldDataRecord(record));
      } else {
        data_record_refered.emplace_back(record);
      }
    }
  }

  clean_all_data_record_ts_ = oldest_snapshot_ts;

  // Find purge-able delete records
  // To avoid access invalid data, an old delete record can be freed only if
  // no holding snapshot is older than its purging time
  for (auto& delete_records : global_old_delete_records_) {
    for (auto& record : delete_records) {
      if (record.release_time <= clean_all_data_record_ts_) {
        space_pending.entries.emplace_back(purgeOldDeleteRecord(record));
      } else {
        delete_record_refered.emplace_back(record);
      }
    }
  }

  if (space_pending.entries.size() > 0) {
    space_pending.release_time =
        kv_engine_->version_controller_.GetCurrentTimestamp();
    global_pending_free_space_entries_.emplace_back(space_pending);
  }

  auto iter = global_pending_free_space_entries_.begin();
  while (iter != global_pending_free_space_entries_.end()) {
    if (iter->release_time < oldest_snapshot_ts) {
      kv_engine_->pmem_allocator_->BatchFree(iter->entries);
      iter++;
    } else {
      break;
    }
  }
  global_pending_free_space_entries_.erase(
      global_pending_free_space_entries_.begin(), iter);

  if (space_to_free.size() > 0) {
    kv_engine_->pmem_allocator_->BatchFree(space_to_free);
  }

  global_old_data_records_.clear();
  global_old_data_records_.emplace_back(data_record_refered);
  global_old_delete_records_.clear();
  global_old_delete_records_.emplace_back(delete_record_refered);
}

void OldRecordsCleaner::TryCleanCachedOldRecords(size_t num_limit_clean) {
  kvdk_assert(access_thread.id >= 0,
              "call KVEngine::handleThreadLocalPendingFreeRecords in a "
              "un-initialized access thread");
  auto& tc = cleaner_thread_cache_[access_thread.id];
  if (tc.old_data_records.size() > 0 || tc.old_delete_records.size() > 0) {
    maybeUpdateOldestSnapshot();
    std::unique_lock<SpinMutex> ul(tc.old_records_lock);
    for (int limit = num_limit_clean;
         tc.old_delete_records.size() > 0 &&
         tc.old_delete_records.front().release_time <
             clean_all_data_record_ts_ &&
         limit > 0;
         limit--) {
      // To avoid access invalid data, an old delete record can be freed only if
      // no holding snapshot is older than its purging time
      tc.pending_free_space_entries.emplace_back(PendingFreeSpaceEntry{
          purgeOldDeleteRecord(tc.old_delete_records.front()),
          kv_engine_->version_controller_.GetCurrentTimestamp()});
      tc.old_delete_records.pop_front();
    }

    TimeStampType oldest_refer_ts =
        kv_engine_->version_controller_.OldestSnapshotTS();
    for (int limit = num_limit_clean;
         tc.old_data_records.size() > 0 &&
         tc.old_data_records.front().release_time < oldest_refer_ts &&
         limit > 0;
         limit--) {
      kv_engine_->pmem_allocator_->Free(
          purgeOldDataRecord(tc.old_data_records.front()));
      tc.old_data_records.pop_front();
    }

    for (int limit = num_limit_clean;
         tc.pending_free_space_entries.size() > 0 &&
         tc.pending_free_space_entries.front().release_time < oldest_refer_ts &&
         limit > 0;
         limit--) {
      kv_engine_->pmem_allocator_->Free(
          tc.pending_free_space_entries.front().entry);
      tc.pending_free_space_entries.pop_front();
    }
  }
}

void OldRecordsCleaner::maybeUpdateOldestSnapshot() {
  // To avoid too many records pending free, we upadte global smallest
  // snapshot regularly. We update it every kUpdateSnapshotRound to mitigate
  // the overhead
  constexpr size_t kUpdateSnapshotRound = 10000;
  thread_local size_t round = 0;
  if ((++round) % kUpdateSnapshotRound == 0) {
    kv_engine_->version_controller_.UpdatedOldestSnapshot();
  }
}

SpaceEntry OldRecordsCleaner::purgeOldDataRecord(
    const OldDataRecord& old_data_record) {
  DataEntry* data_entry =
      static_cast<DataEntry*>(old_data_record.pmem_data_record);
  switch (data_entry->meta.type) {
    case StringDataRecord:
    case SortedDataRecord: {
      data_entry->Destroy();
      return SpaceEntry(kv_engine_->pmem_allocator_->addr2offset(data_entry),
                        data_entry->header.record_size);
    }
    default:
      std::abort();
  }
}

SpaceEntry OldRecordsCleaner::purgeOldDeleteRecord(
    OldDeleteRecord& old_delete_record) {
  DataEntry* data_entry =
      static_cast<DataEntry*>(old_delete_record.pmem_delete_record);
  switch (data_entry->meta.type) {
    case StringDataRecord: {
      kvdk_assert(old_delete_record.record_index.hash_entry.RawPointer()
                      ->IsExpiredStatus(),
                  "ths string data record should be expired.");
      [[gnu::fallthrough]];
    }
    case StringDeleteRecord: {
      kvdk_assert(
          old_delete_record.record_index.hash_entry.RawPointer() != nullptr &&
              old_delete_record.key_lock != nullptr &&
              old_delete_record.record_index.hash_entry.GetTag() ==
                  PointerType::HashEntry,
          "hash index not stored in old delete record of string");
      HashEntry* hash_entry_ptr = static_cast<HashEntry*>(
          old_delete_record.record_index.hash_entry.RawPointer());
      if (hash_entry_ptr->GetIndex().string_record ==
          old_delete_record.pmem_delete_record) {
        std::lock_guard<SpinMutex> lg(*old_delete_record.key_lock);
        if (hash_entry_ptr->GetIndex().string_record ==
            old_delete_record.pmem_delete_record) {
          kv_engine_->hash_table_->Erase(hash_entry_ptr);
        }
      }
      // we don't need to purge a delete record
      return SpaceEntry(kv_engine_->pmem_allocator_->addr2offset(data_entry),
                        data_entry->header.record_size);
    }
    case SortedDeleteRecord: {
    handle_sorted_delete_record : {
      std::lock_guard<SpinMutex> lg(*old_delete_record.key_lock);
      auto delete_record_index_type =
          old_delete_record.record_index.skiplist_node.GetTag();
      SkiplistNode* dram_node = nullptr;
      bool need_purge = false;
      switch (delete_record_index_type) {
        case PointerType::HashEntry: {
          DLRecord* hash_indexed_pmem_record;
          HashEntry* hash_entry_ref = static_cast<HashEntry*>(
              old_delete_record.record_index.hash_entry.RawPointer());
          auto hash_index_type = hash_entry_ref->GetIndexType();
          if (hash_index_type == PointerType::DLRecord) {
            if (hash_entry_ref->GetIndex().dl_record ==
                old_delete_record.pmem_delete_record) {
              hash_indexed_pmem_record = hash_entry_ref->GetIndex().dl_record;
            }
          } else if (hash_index_type == PointerType::SkiplistNode) {
            dram_node = hash_entry_ref->GetIndex().skiplist_node;
            hash_indexed_pmem_record = dram_node->record;
          } else {
            // Hash entry of this key already been cleaned. This happens if
            // another delete record of this key inserted in hash table and
            // cleaned before this record
            break;
          }

          if (hash_indexed_pmem_record ==
              old_delete_record.pmem_delete_record) {
            need_purge = true;
            kv_engine_->hash_table_->Erase(hash_entry_ref);
          }
          break;
        }
        case PointerType::SkiplistNode: {
          dram_node = static_cast<SkiplistNode*>(
              old_delete_record.record_index.skiplist_node.RawPointer());
          if (dram_node->record == old_delete_record.pmem_delete_record) {
            need_purge = true;
          }
          break;
        }
        case PointerType::Empty: {
          // This record is not indexed by hash table and skiplist node, so we
          // check linkage to determine if its already been purged
          if (Skiplist::CheckRecordLinkage(
                  static_cast<DLRecord*>(old_delete_record.pmem_delete_record),
                  kv_engine_->pmem_allocator_.get())) {
            need_purge = true;
          }
          break;
        }
        default: {
          kvdk_assert(false, "never should reach");
        }
      }
      if (need_purge) {
        while (!Skiplist::Purge(
            static_cast<DLRecord*>(old_delete_record.pmem_delete_record),
            old_delete_record.key_lock, dram_node,
            kv_engine_->pmem_allocator_.get(), kv_engine_->hash_table_.get())) {
          // lock conflict, retry
          goto handle_sorted_delete_record;
        }
      }
      return SpaceEntry(kv_engine_->pmem_allocator_->addr2offset(data_entry),
                        data_entry->header.record_size);
    }
    }
    default: {
      std::abort();
    }
  }
}

}  // namespace KVDK_NAMESPACE