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
      kv_engine_->version_controller_.LocalOldestSnapshotTS();
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

void OldRecordsCleaner::TryGlobalClean() {
  std::vector<SpaceEntry> space_to_free;
  // Update recorded oldest snapshot up to state so we can know which records
  // can be freed
  kv_engine_->version_controller_.UpdatedOldestSnapshot();
  TimeStampType oldest_snapshot_ts =
      kv_engine_->version_controller_.LocalOldestSnapshotTS();

  std::vector<SpaceEntry> free_entries;
  for (size_t i = 0; i < cleaner_thread_cache_.size(); i++) {
    auto& cleaner_thread_cache = cleaner_thread_cache_[i];
    if (cleaner_thread_cache.pending_free_space_entries.size() > 0) {
      std::lock_guard<SpinMutex> lg(cleaner_thread_cache.old_records_lock);
      for (auto& space_entry :
           cleaner_thread_cache.pending_free_space_entries) {
        if (space_entry.release_time < oldest_snapshot_ts) {
          free_entries.emplace_back(space_entry.entry);
        } else {
          global_pending_free_space_entries_.emplace_back(space_entry);
        }
      }
      cleaner_thread_cache.pending_free_space_entries.clear();
    }
  }

  auto iter = global_pending_free_space_entries_.begin();
  while (iter != global_pending_free_space_entries_.end()) {
    if (iter->release_time < oldest_snapshot_ts) {
      free_entries.emplace_back(iter->entry);
      iter++;
    } else {
      break;
    }
  }
  global_pending_free_space_entries_.erase(
      global_pending_free_space_entries_.begin(), iter);

  if (free_entries.size() > 0) {
    kv_engine_->pmem_allocator_->BatchFree(space_to_free);
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

}  // namespace KVDK_NAMESPACE