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

void OldRecordsCleaner::PushToGlobal(
    std::deque<OutdatedCollection>&& outdated_collections) {
  global_outdated_collections_.emplace_back(
      std::forward<std::deque<OutdatedCollection>>(outdated_collections));
}

void OldRecordsCleaner::TryGlobalClean() {
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

  // Destroy deleted skiplists
  for (auto& outdated_collections : global_outdated_collections_) {
    auto oc_iter = outdated_collections.begin();
    for (; oc_iter != outdated_collections.end(); ++oc_iter) {
      if (oc_iter->release_time < clean_all_data_record_ts_) {
        auto outdated_collection = oc_iter->collection;
        oc_iter = outdated_collections.erase(oc_iter);
        thread_pool_.commit(
            [&](PointerWithTag<Collection, PointerType> outdated_collection) {
              Collection* collection_ptr = outdated_collection.RawPointer();
              switch (outdated_collection.GetTag()) {
                case PointerType::Skiplist: {
                  static_cast<Skiplist*>(collection_ptr)->Destroy();
                  kv_engine_->removeSkiplist(collection_ptr->ID());
                  break;
                }
                case PointerType::List:
                  break;
                case PointerType::HashList:
                  break;
                default:
                  break;
              }
            },
            outdated_collection);
      }
    }
  }

  auto iter = global_pending_free_space_entries_.begin();
  while (iter != global_pending_free_space_entries_.end()) {
    if (iter->release_time < clean_all_data_record_ts_) {
      kv_engine_->pmem_allocator_->BatchFree(iter->entries);
      iter++;
    } else {
      break;
    }
  }
  global_pending_free_space_entries_.erase(
      global_pending_free_space_entries_.begin(), iter);
}

void OldRecordsCleaner::PushToCache(
    const OutdatedCollection& outdated_collection) {
  kvdk_assert(access_thread.id >= 0,
              "call OldRecordsCleaner::Push with uninitialized access thread");
  auto& tc = cleaner_thread_cache_[access_thread.id];
  std::lock_guard<SpinMutex> lg(tc.old_records_lock);
  tc.outdated_collections.emplace_back(outdated_collection);
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

SpaceEntry OldRecordsCleaner::PurgeStringRecord(void* pmem_record) {
  DataEntry* data_entry = static_cast<DataEntry*>(pmem_record);
  kvdk_assert(data_entry->meta.type == StringDataRecord ||
                  data_entry->meta.type == StringDeleteRecord,
              "the outdated string record type should be `StringDataRecord` "
              "and `StringDeleteRecord`");
  return SpaceEntry(kv_engine_->pmem_allocator_->addr2offset(data_entry),
                    data_entry->header.record_size);
}

SpaceEntry OldRecordsCleaner::PurgeSortedRecord(SkiplistNode* dram_node,
                                                void* pmem_record) {
  DataEntry* data_entry = static_cast<DataEntry*>(pmem_record);
  auto hint = kv_engine_->hash_table_->GetHint(
      static_cast<DLRecord*>(pmem_record)->Key());
handle_sorted_delete_record : {
  std::unique_lock<SpinMutex> ul(*hint.spin);
  // We check linkage to determine if the delete record already been
  // unlinked by updates. We only check the next linkage, as the record is
  // already been locked, its next record will not be changed.
  bool record_on_list = Skiplist::CheckReocrdNextLinkage(
      static_cast<DLRecord*>(pmem_record), kv_engine_->pmem_allocator_.get());
  if (record_on_list) {
    if (!Skiplist::Purge(static_cast<DLRecord*>(pmem_record), hint.spin,
                         dram_node, kv_engine_->pmem_allocator_.get(),
                         kv_engine_->hash_table_.get())) {
      goto handle_sorted_delete_record;
    }
  }
  return SpaceEntry(
      kv_engine_->pmem_allocator_->addr2offset_checked(data_entry),
      data_entry->header.record_size);
}
}

void OldRecordsCleaner::PushToTaskQueue(
    const std::vector<std::pair<void*, PointerType>>& outdated_records) {
  thread_pool_.commit(
      [&](const std::vector<std::pair<void*, PointerType>>& records) {
        PendingFreeSpaceEntries space_entries;
        for (auto& record_pair : records) {
          switch (record_pair.second) {
            case PointerType::StringRecord:
              space_entries.entries.emplace_back(
                  PurgeStringRecord(record_pair.first));
              break;
            case PointerType::SkiplistNode: {
              SkiplistNode* skiplist_node =
                  static_cast<SkiplistNode*>(record_pair.first);
              space_entries.entries.emplace_back(
                  PurgeSortedRecord(skiplist_node, skiplist_node->record));
              break;
            }
            case PointerType::DLRecord: {
              space_entries.entries.emplace_back(
                  PurgeSortedRecord(nullptr, record_pair.first));
              break;
            }
            default:
              break;
          }
        }
        space_entries.release_time =
            kv_engine_->version_controller_.GetCurrentTimestamp();

        {
          std::unique_lock<SpinMutex> lock_space_pool(lock_);
          global_pending_free_space_entries_.emplace_back(
              std::move(space_entries));
        }
      },
      outdated_records);
}

void OldRecordsCleaner::TryCleanDataRecords() {
  std::vector<SpaceEntry> space_to_free;
  // records that can't be freed this time
  std::deque<OldDataRecord> data_record_refered;
  PendingFreeSpaceEntries space_pending;
  // Update recorded oldest snapshot up to state so we can know which records
  // can be freed
  kv_engine_->version_controller_.UpdatedOldestSnapshot();
  TimeStampType oldest_snapshot_ts =
      kv_engine_->version_controller_.OldestSnapshotTS();

  std::lock_guard<SpinMutex> lg(lock_);

  for (size_t i = 0; i < cleaner_thread_cache_.size(); i++) {
    auto& cleaner_thread_cache = cleaner_thread_cache_[i];
    if (cleaner_thread_cache.old_data_records.size() > 0) {
      std::lock_guard<SpinMutex> lg(cleaner_thread_cache.old_records_lock);
      global_old_data_records_.emplace_back();
      global_old_data_records_.back().swap(
          cleaner_thread_cache.old_data_records);
    }
  }

  clean_all_data_record_ts_ = oldest_snapshot_ts;
}
}  // namespace KVDK_NAMESPACE