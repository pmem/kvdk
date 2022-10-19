/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "old_records_cleaner.hpp"

#include "../kv_engine.hpp"
#include "../sorted_collection/skiplist.hpp"

namespace KVDK_NAMESPACE {

OldRecordsCleaner::~OldRecordsCleaner() {
  // Clean twice to ensure all resources are released.
  TryGlobalClean();
  TryGlobalClean();
  for (size_t i = 0; i < cleaner_thread_cache_.size(); i++) {
    auto& tc = cleaner_thread_cache_[i];
    std::lock_guard<SpinMutex> lg(tc.old_records_lock);
    if (!tc.pending_free_space_entries.empty())
      GlobalLogger.Error("PendingFree leaked!");
    for (auto const& q : tc.local_queues_) {
      if (!q.empty()) GlobalLogger.Error("DRAM leaked!");
    }
  }
  if (!global_pending_free_space_entries_.empty())
    GlobalLogger.Error("PendingFree leaked!");
}

void OldRecordsCleaner::RegisterDelayDeleter(IDeleter& deleter) {
  size_t idx = global_queues_.size();
  delay_deleters_[&deleter] = idx;
  global_queues_.emplace_back();
  for (size_t i = 0; i < cleaner_thread_cache_.size(); i++)
    cleaner_thread_cache_[i].local_queues_.emplace_back();
}

void OldRecordsCleaner::DelayDelete(IDeleter& deleter, void* obj) {
  kvdk_assert(this_thread.id >= 0, "");
  auto& tc = cleaner_thread_cache_[this_thread.id];
  std::lock_guard<SpinMutex> guard{tc.old_records_lock};

  TimestampType ts = kv_engine_->version_controller_.GetCurrentTimestamp();

  size_t idx = delay_deleters_.at(&deleter);
  tc.local_queues_[idx].emplace_back(ts, obj);

  constexpr size_t kMaxFreePending = 16;
  tryPurge(deleter, tc.local_queues_[idx], kMaxFreePending);
}

void OldRecordsCleaner::tryPurge(IDeleter& deleter, PendingQueue& pending_kvs,
                                 size_t lim) {
  maybeUpdateOldestSnapshot();
  TimestampType acc_ts =
      kv_engine_->version_controller_.LocalOldestSnapshotTS();
  for (size_t i = 0; i < lim && !pending_kvs.empty(); i++) {
    auto const& pair = pending_kvs.front();
    if (pair.first >= acc_ts) break;
    deleter.Delete(pair.second);
    pending_kvs.pop_front();
  }
}

void OldRecordsCleaner::TryGlobalClean() {
  std::vector<SpaceEntry> space_to_free;
  // Update recorded oldest snapshot up to state so we can know which records
  // can be freed
  kv_engine_->version_controller_.UpdateLocalOldestSnapshot();
  TimestampType oldest_snapshot_ts =
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

    std::lock_guard<SpinMutex> lg(cleaner_thread_cache.old_records_lock);
    for (auto const& del_idx : delay_deleters_)
      tryPurge(*del_idx.first,
               cleaner_thread_cache.local_queues_[del_idx.second], -1U);
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
    kv_engine_->version_controller_.UpdateLocalOldestSnapshot();
  }
}

}  // namespace KVDK_NAMESPACE