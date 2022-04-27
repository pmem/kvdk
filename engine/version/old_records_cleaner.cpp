/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "old_records_cleaner.hpp"

#include "../kv_engine.hpp"
#include "../sorted_collection/skiplist.hpp"

namespace KVDK_NAMESPACE {

void OldRecordsCleaner::PushToPendingFree(void* addr, TimeStampType ts) {
  kvdk_assert(
      static_cast<DLRecord*>(addr)->entry.meta.type &
          (ListDirtyElem | ListDirtyRecord | HashDirtyElem | HashDirtyRecord),
      "");
  kvdk_assert(access_thread.id >= 0, "");
  auto& tc = cleaner_thread_cache_[access_thread.id];
  std::lock_guard<SpinMutex> guard{tc.old_records_lock};
  DataEntry* data_entry = static_cast<DataEntry*>(addr);
  SpaceEntry entry{kv_engine_->pmem_allocator_->addr2offset_checked(addr),
                   data_entry->header.record_size};
  tc.pending_free_space_entries.push_back(PendingFreeSpaceEntry{entry, ts});

  /// TODO: a thread may quit before all pending free entries are Free()d
  /// GlobalClean() should collect those entries and Free() them.
  maybeUpdateOldestSnapshot();
  TimeStampType earliest_ts =
      kv_engine_->version_controller_.OldestSnapshotTS();
  constexpr size_t kMaxFreePending = 16;
  for (size_t i = 0; i < kMaxFreePending; i++) {
    if (tc.pending_free_space_entries.empty()) {
      break;
    }
    if (tc.pending_free_space_entries.front().release_time >= earliest_ts) {
      break;
    }
    kv_engine_->pmem_allocator_->Free(
        tc.pending_free_space_entries.front().entry);
    tc.pending_free_space_entries.pop_front();
  }
}

bool OldRecordsCleaner::TryFreePendingSpace(
    const PendingFreeSpaceEntries& pending_free_space_entries) {
  kv_engine_->version_controller_.UpdatedOldestSnapshot();
  TimeStampType oldest_snapshot_ts =
      kv_engine_->version_controller_.OldestSnapshotTS();
  if (pending_free_space_entries.release_time <= oldest_snapshot_ts) {
    kv_engine_->pmem_allocator_->BatchFree(pending_free_space_entries.entries);
    return true;
  }
  return false;
}

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

void OldRecordsCleaner::TryGlobalClean(TimeStampType oldest_snapshot_ts) {
  clean_all_data_record_ts_ = oldest_snapshot_ts;
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
}

void OldRecordsCleaner::TryGlobalCleanDataRecords(
    TimeStampType oldest_snapshot_ts) {
  std::vector<SpaceEntry> space_to_free;
  std::deque<OldDataRecord> data_record_refered;

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

  if (space_to_free.size() > 0) {
    kv_engine_->pmem_allocator_->BatchFree(space_to_free);
  }
  global_old_data_records_.clear();
  global_old_data_records_.emplace_back(data_record_refered);
}

void OldRecordsCleaner::TryCleanCachedOldRecords(size_t num_limit_clean) {
  kvdk_assert(access_thread.id >= 0,
              "call KVEngine::handleThreadLocalPendingFreeRecords in a "
              "un-initialized access thread");
  auto& tc = cleaner_thread_cache_[access_thread.id];
  if (tc.old_data_records.empty()) {
    return;
  }

  maybeUpdateOldestSnapshot();
  std::unique_lock<SpinMutex> ul(tc.old_records_lock);

  TimeStampType oldest_refer_ts =
      kv_engine_->version_controller_.OldestSnapshotTS();
  for (int limit = num_limit_clean;
       tc.old_data_records.size() > 0 &&
       tc.old_data_records.front().release_time < oldest_refer_ts && limit > 0;
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

SpaceEntry OldRecordsCleaner::PurgeOutDatedRecord(HashEntry* hash_entry,
                                                  const SpinMutex* key_lock) {
}

}  // namespace KVDK_NAMESPACE