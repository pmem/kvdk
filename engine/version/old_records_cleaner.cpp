/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "old_records_cleaner.hpp"
#include "../kv_engine.hpp"
#include "../skiplist.hpp"

namespace KVDK_NAMESPACE {
void OldRecordsCleaner::Push(OldDataRecord &&old_data_record) {
  kvdk_assert(access_thread.id >= 0,
              "call OldRecordsCleaner::Push with uninitialized access thread");

  auto &tc = thread_cache_[access_thread.id];
  std::lock_guard<SpinMutex> lg(tc.old_records_lock);
  tc.old_data_records.emplace_back(
      std::forward<OldDataRecord>(old_data_record));
  // To avoid too many cached old records pending clean, we try to clean cached
  // records while pushing new one
  TryCleanCachedOldRecords();
}

void OldRecordsCleaner::Push(OldDeleteRecord &&old_delete_record) {
  kvdk_assert(access_thread.id >= 0,
              "call OldRecordsCleaner::Push with uninitialized access thread");

  auto &tc = thread_cache_[access_thread.id];
  std::lock_guard<SpinMutex> lg(tc.old_records_lock);
  tc.old_delete_records.emplace_back(
      std::forward<OldDeleteRecord>(old_delete_record));
  // To avoid too many cached old records pending clean, we try to clean cached
  // records while pushing new one
  TryCleanCachedOldRecords();
}

void OldRecordsCleaner::TryCleanAll() {
  // records that can't be freed this time
  std::deque<OldDataRecord> data_record_refered;
  std::deque<OldDeleteRecord> delete_record_refered;
  PendingFreeSpaceEntries space_pending;

  std::vector<SpaceEntry> space_to_free;

  // Update recorded oldest snapshot up to state so we can know which records
  // can be freed
  kv_engine_->version_controller_.UpdatedOldestSnapshot();
  TimestampType oldest_snapshot_ts =
      kv_engine_->version_controller_.OldestSnapshotTS();

  // Fetch thread cached pending free records
  for (size_t i = 0; i < thread_cache_.size(); i++) {
    auto &thread_cache = thread_cache_[i];
    if (thread_cache.old_data_records.size() > 0 ||
        thread_cache.old_delete_records.size() > 0) {
      std::lock_guard<SpinMutex> lg(thread_cache.old_records_lock);

      if (thread_cache.old_data_records.size() > 0) {
        global_old_data_records_.emplace_back();
        global_old_data_records_.back().swap(thread_cache.old_data_records);
      }

      if (thread_cache.old_delete_records.size() > 0) {
        global_old_delete_records_.emplace_back();
        global_old_delete_records_.back().swap(thread_cache.old_delete_records);
      }
    }
  }

  // Find free-able data records
  for (auto &data_records : global_old_data_records_) {
    for (auto &record : data_records) {
      if (record.newer_version_timestamp <= oldest_snapshot_ts) {
        space_to_free.emplace_back(purgeOldDataRecord(record));
      } else {
        data_record_refered.emplace_back(std::move(record));
      }
    }
  }

  // Find free-able delete records
  for (auto &delete_records : global_old_delete_records_) {
    for (auto &record : delete_records) {
      if (record.newer_version_timestamp <= oldest_snapshot_ts) {
        space_pending.entries.emplace_back(purgeOldDeleteRecord(record));
      } else {
        delete_record_refered.emplace_back(std::move(record));
      }
    }
  }

  if (space_pending.entries.size() > 0) {
    space_pending.free_ts =
        kv_engine_->version_controller_.GetCurrentTimestamp();
    pending_free_space_entries_.emplace_back(std::move(space_pending));
  }

  auto iter = pending_free_space_entries_.begin();
  for (auto iter = pending_free_space_entries_.begin();
       iter != pending_free_space_entries_.end(); iter++) {
    if (iter->free_ts < oldest_snapshot_ts) {
      kv_engine_->pmem_allocator_->BatchFree(iter->entries);
    } else {
      break;
    }
  }
  pending_free_space_entries_.erase(pending_free_space_entries_.begin(), iter);

  if (space_to_free.size() > 0) {
    kv_engine_->pmem_allocator_->BatchFree(space_to_free);
  }

  global_old_data_records_.clear();
  global_old_data_records_.emplace_back(std::move(data_record_refered));
  global_old_delete_records_.clear();
  global_old_delete_records_.emplace_back(std::move(delete_record_refered));
}

void OldRecordsCleaner::TryCleanCachedOldRecords() {
  constexpr size_t kLimitForegroundFree = 1;
  kvdk_assert(access_thread.id >= 0,
              "call KVEngine::handleThreadLocalPendingFreeRecords in a "
              "un-initialized access thread");
  auto &tc = thread_cache_[access_thread.id];
  size_t limit_free = kLimitForegroundFree;
  maybeUpdateOldestSnapshot();
  TimestampType oldest_refer_ts =
      kv_engine_->version_controller_.OldestSnapshotTS();
  while (tc.old_data_records.size() > 0 &&
         tc.old_data_records.front().newer_version_timestamp <
             oldest_refer_ts &&
         limit_free-- > 0) {
    kv_engine_->pmem_allocator_->Free(
        purgeOldDataRecord(tc.old_data_records.front()));
    tc.old_data_records.pop_front();
  }
}

void OldRecordsCleaner::maybeUpdateOldestSnapshot() {
  // To avoid too many records pending free, we upadte global smallest
  // snapshot regularly. We update it every kUpdateSnapshotRound to mitigate
  // the overhead
  static size_t kUpdateSnapshotRound = 10000;
  thread_local size_t round = 0;
  if ((++round) % kUpdateSnapshotRound == 0) {
    kv_engine_->version_controller_.UpdatedOldestSnapshot();
  }
}

SpaceEntry
OldRecordsCleaner::purgeOldDataRecord(const OldDataRecord &old_data_record) {
  DataEntry *data_entry =
      static_cast<DataEntry *>(old_data_record.pmem_data_record);
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
    const OldDeleteRecord &old_delete_record) {
  DataEntry *data_entry =
      static_cast<DataEntry *>(old_delete_record.pmem_delete_record);
  switch (data_entry->meta.type) {
  case StringDeleteRecord: {
    if (old_delete_record.hash_entry_ref->index.string_record ==
        old_delete_record.pmem_delete_record) {
      std::lock_guard<SpinMutex> lg(*old_delete_record.hash_entry_lock);
      if (old_delete_record.hash_entry_ref->index.string_record ==
          old_delete_record.pmem_delete_record) {
        old_delete_record.hash_entry_ref->Clear();
      }
    }
    // we don't need to purge a delete record
    return SpaceEntry(kv_engine_->pmem_allocator_->addr2offset(data_entry),
                      data_entry->header.record_size);
  }
  case SortedDeleteRecord: {
    while (1) {
      HashEntry *hash_entry_ref = old_delete_record.hash_entry_ref;
      SpinMutex *hash_entry_lock = old_delete_record.hash_entry_lock;
      std::lock_guard<SpinMutex> lg(*hash_entry_lock);
      DLRecord *hash_indexed_pmem_record = nullptr;
      SkiplistNode *dram_node = nullptr;
      switch (hash_entry_ref->header.offset_type) {
      case HashOffsetType::DLRecord:
        hash_indexed_pmem_record = hash_entry_ref->index.dl_record;
        break;
      case HashOffsetType::SkiplistNode:
        dram_node = hash_entry_ref->index.skiplist_node;
        hash_indexed_pmem_record = dram_node->record;
        break;
      default:
        GlobalLogger.Error(
            "Wrong time in handle pending free skiplist delete record\n");
        std::abort();
      }

      if (hash_indexed_pmem_record == old_delete_record.pmem_delete_record) {
        if (!Skiplist::Purge(
                static_cast<DLRecord *>(old_delete_record.pmem_delete_record),
                hash_entry_lock, dram_node, kv_engine_->pmem_allocator_.get(),
                kv_engine_->hash_table_.get())) {
          continue;
        }
        hash_entry_ref->Clear();
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

} // namespace KVDK_NAMESPACE