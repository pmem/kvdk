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
          (StringDataRecord | SortedElem | SortedHeader),
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
          (DeleteRecordType | ExpirableRecordType),
      "Wrong type in OldRecordsCleaner::Push");
  kvdk_assert(access_thread.id >= 0,
              "call OldRecordsCleaner::Push with uninitialized access thread");

  auto& tc = cleaner_thread_cache_[access_thread.id];
  std::lock_guard<SpinMutex> lg(tc.old_records_lock);
  tc.old_delete_records.emplace_back(old_delete_record);
}

void OldRecordsCleaner::PushToCache(
    const OutdatedCollection& outdated_collection) {
  kvdk_assert(access_thread.id >= 0,
              "call OldRecordsCleaner::Push with uninitialized access thread");
  auto& tc = cleaner_thread_cache_[access_thread.id];
  std::lock_guard<SpinMutex> lg(tc.old_records_lock);
  tc.outdated_collections.emplace_back(outdated_collection);
}

void OldRecordsCleaner::PushToGlobal(
    const std::deque<OldDeleteRecord>& old_delete_records) {
  std::lock_guard<SpinMutex> lg(lock_);
  global_old_delete_records_.emplace_back(old_delete_records);
}

void OldRecordsCleaner::PushToGlobal(
    std::deque<OutdatedCollection>&& outdated_collections) {
  std::lock_guard<SpinMutex> lg(lock_);
  global_outdated_collections_.emplace_back(
      std::forward<std::deque<OutdatedCollection>>(outdated_collections));
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

  std::lock_guard<SpinMutex> lg(lock_);

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

  // Fetch thread cached outdated collections, the workflow is as same as old
  // delete record
  for (size_t i = 0; i < cleaner_thread_cache_.size(); i++) {
    auto& cleaner_thread_cache = cleaner_thread_cache_[i];
    if (cleaner_thread_cache.outdated_collections.size() > 0) {
      std::lock_guard<SpinMutex> lg(cleaner_thread_cache.old_records_lock);
      global_outdated_collections_.emplace_back();
      global_outdated_collections_.back().swap(
          cleaner_thread_cache.outdated_collections);
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

  // Destroy deleted skiplists
  for (auto& outdated_collections : global_outdated_collections_) {
    auto oc_iter = outdated_collections.begin();
    while (oc_iter != outdated_collections.end()) {
      if (oc_iter->release_time < clean_all_data_record_ts_) {
        // For now, we only use this for skiplist
        CollectionIDType id = oc_iter->collection->ID();
        oc_iter->collection->Destroy();
        kv_engine_->removeSkiplist(id);
        oc_iter++;
      } else {
        break;
      }
    }
    outdated_collections.erase(outdated_collections.begin(), oc_iter);
  }

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
  if (tc.old_data_records.empty() && tc.old_delete_records.empty()) {
    return;
  }

  maybeUpdateOldestSnapshot();
  std::unique_lock<SpinMutex> ul(tc.old_records_lock);
  for (int limit = num_limit_clean;
       tc.old_delete_records.size() > 0 &&
       tc.old_delete_records.front().release_time < clean_all_data_record_ts_ &&
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
    case SortedHeader:
    case StringDataRecord:
    case SortedElem: {
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
    case SortedElemDelete: {
    handle_sorted_delete_record : {
      std::unique_lock<SpinMutex> ul(*old_delete_record.key_lock);
      // We check linkage to determine if the delete record already been
      // unlinked by updates. We only check the next linkage, as the record is
      // already been locked, its next record will not be changed.
      bool record_on_list = Skiplist::CheckReocrdNextLinkage(
          static_cast<DLRecord*>(old_delete_record.pmem_delete_record),
          kv_engine_->pmem_allocator_.get());
      if (record_on_list) {
        // record still on list, we clear its index and unlink it from list
        auto delete_record_index_type =
            old_delete_record.record_index.skiplist_node.GetTag();
        SkiplistNode* dram_node = nullptr;
        HashEntry* hash_entry_ref = nullptr;
        switch (delete_record_index_type) {
          case PointerType::HashEntry: {
            hash_entry_ref = static_cast<HashEntry*>(
                old_delete_record.record_index.hash_entry.RawPointer());
            auto hash_index_type = hash_entry_ref->GetIndexType();
            if (hash_index_type == PointerType::SkiplistNode) {
              dram_node = hash_entry_ref->GetIndex().skiplist_node;
              kvdk_assert(
                  dram_node->record == old_delete_record.pmem_delete_record,
                  "On-list old delete record of skiplist no pointed by its "
                  "dram node");
            } else {
              kvdk_assert(hash_index_type == PointerType::DLRecord,
                          "Wrong hash index type in cleaner");
              kvdk_assert(
                  hash_entry_ref->GetIndex().dl_record ==
                      old_delete_record.pmem_delete_record,
                  "On-list old delete record of skiplist no pointed by its "
                  "hash entry")
            }
            break;
          }

          case PointerType::SkiplistNode: {
            dram_node = static_cast<SkiplistNode*>(
                old_delete_record.record_index.skiplist_node.RawPointer());
            kvdk_assert(
                dram_node->record == old_delete_record.pmem_delete_record,
                "On-list old delete record of skiplist no pointed by its dram "
                "node");
            break;
          }

          case PointerType::Empty: {
            break;  // nothing to do
          }

          default: {
            kvdk_assert(false, "never should reach");
          }
        }

        if (!Skiplist::Purge(
                static_cast<DLRecord*>(old_delete_record.pmem_delete_record),
                dram_node, kv_engine_->pmem_allocator_.get(),
                kv_engine_->hash_table_.get(),
                kv_engine_->skiplist_locks_.get())) {
          // lock conflict, retry
          goto handle_sorted_delete_record;
        } else if (hash_entry_ref) {
          // Erase hash entry after successfully purge record
          kv_engine_->hash_table_->Erase(hash_entry_ref);
        }
      }
      return SpaceEntry(
          kv_engine_->pmem_allocator_->addr2offset_checked(data_entry),
          data_entry->header.record_size);
    }
    }
    default: {
      std::abort();
    }
  }
}

}  // namespace KVDK_NAMESPACE