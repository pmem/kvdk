/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */
#include <algorithm>

#include "kv_engine.hpp"
#include "utils/sync_point.hpp"

namespace KVDK_NAMESPACE {

constexpr uint64_t kForegroundUpdateSnapshotInterval = 1000;

template <typename T>
void KVEngine::removeOutdatedCollection(T* collection) {
  static_assert(std::is_same<T, Skiplist>::value ||
                std::is_same<T, List>::value ||
                std::is_same<T, HashList>::value);
  auto cur_id = collection->ID();
  auto cur_head_record = collection->HeaderRecord();
  while (cur_head_record) {
    auto old_head_record =
        pmem_allocator_->offset2addr<DLRecord>(cur_head_record->old_version);
    if (old_head_record) {
      auto old_collection_id = T::FetchID(old_head_record);
      if (old_collection_id != cur_id) {
        if (getSkiplist(old_collection_id) == nullptr &&
            getHashlist(old_collection_id) == nullptr &&
            getList(old_collection_id) == nullptr) {
          GlobalLogger.Debug("Collection already been destroyed\n");
        }
        kvdk_assert(getSkiplist(old_collection_id) != nullptr ||
                        getHashlist(old_collection_id) != nullptr ||
                        getList(old_collection_id) != nullptr,
                    "collection should not be destroyed yet!");
        cur_head_record->PersistOldVersion(kNullPMemOffset);
        cur_id = old_collection_id;
      }
    }
    cur_head_record = old_head_record;
  }
}

template <typename T>
void KVEngine::removeAndCacheOutdatedVersion(T* record) {
  static_assert(std::is_same<T, StringRecord>::value ||
                std::is_same<T, DLRecord>::value);
  kvdk_assert(access_thread.id >= 0, "");
  auto& tc = cleaner_thread_cache_[access_thread.id];
  if (std::is_same<T, StringRecord>::value) {
    StringRecord* old_record = removeOutDatedVersion<StringRecord>(
        (StringRecord*)record, version_controller_.GlobalOldestSnapshotTs());
    if (old_record) {
      std::lock_guard<SpinMutex> lg(tc.mtx);
      tc.outdated_string_records.emplace_back(
          old_record, version_controller_.GetCurrentTimestamp());
    }
  } else {
    DLRecord* old_record = removeOutDatedVersion<DLRecord>(
        (DLRecord*)record, version_controller_.GlobalOldestSnapshotTs());
    if (old_record) {
      std::lock_guard<SpinMutex> lg(tc.mtx);
      tc.outdated_dl_records.emplace_back(
          old_record, version_controller_.GetCurrentTimestamp());
    }
  }
}

template void KVEngine::removeAndCacheOutdatedVersion<StringRecord>(
    StringRecord* record);
template void KVEngine::removeAndCacheOutdatedVersion<DLRecord>(
    DLRecord* record);

template <typename T>
void KVEngine::cleanOutdatedRecordImpl(T* old_record) {
  static_assert(std::is_same<T, StringRecord>::value ||
                std::is_same<T, DLRecord>::value);
  while (old_record) {
    T* next = pmem_allocator_->offset2addr<T>(old_record->old_version);
    auto record_size = old_record->GetRecordSize();
    if (old_record->GetRecordStatus() == RecordStatus::Normal) {
      old_record->Destroy();
    }
    pmem_allocator_->Free(SpaceEntry(
        pmem_allocator_->addr2offset_checked(old_record), record_size));
    old_record = next;
  }
}

void KVEngine::tryCleanCachedOutdatedRecord() {
  kvdk_assert(access_thread.id >= 0, "");
  auto& tc = cleaner_thread_cache_[access_thread.id];
  // Regularly update local oldest snapshot
  thread_local uint64_t round = 0;
  if (++round % kForegroundUpdateSnapshotInterval == 0) {
    version_controller_.UpdateLocalOldestSnapshot();
  }
  auto release_time = version_controller_.LocalOldestSnapshotTS();
  if (!tc.outdated_string_records.empty()) {
    std::unique_lock<SpinMutex> ul(tc.mtx);
    if (!tc.outdated_string_records.empty() &&
        tc.outdated_string_records.front().release_time < release_time) {
      auto to_clean = tc.outdated_string_records.front();
      tc.outdated_string_records.pop_front();
      ul.unlock();
      cleanOutdatedRecordImpl<StringRecord>(to_clean.record);
      return;
    }
  }

  if (!tc.outdated_dl_records.empty()) {
    std::unique_lock<SpinMutex> ul(tc.mtx);
    if (!tc.outdated_dl_records.empty() &&
        tc.outdated_dl_records.front().release_time < release_time) {
      auto to_clean = tc.outdated_dl_records.front();
      tc.outdated_dl_records.pop_front();
      ul.unlock();
      cleanOutdatedRecordImpl<DLRecord>(to_clean.record);
    }
  }
}

void KVEngine::purgeAndFreeStringRecords(
    const std::vector<StringRecord*>& old_records) {
  std::vector<SpaceEntry> entries;
  for (auto old_record : old_records) {
    while (old_record) {
      StringRecord* next =
          pmem_allocator_->offset2addr<StringRecord>(old_record->old_version);
      if (old_record->GetRecordStatus() == RecordStatus::Normal) {
        old_record->Destroy();
      }
      entries.emplace_back(pmem_allocator_->addr2offset(old_record),
                           old_record->GetRecordSize());
      old_record = next;
    }
  }
  pmem_allocator_->BatchFree(entries);
}

void KVEngine::purgeAndFreeDLRecords(
    const std::vector<DLRecord*>& old_records) {
  std::vector<SpaceEntry> entries;
  std::vector<CollectionIDType> outdated_skiplists;
  for (auto pmem_record : old_records) {
    while (pmem_record) {
      DLRecord* next_record =
          pmem_allocator_->offset2addr<DLRecord>(pmem_record->old_version);
      RecordType type = pmem_record->GetRecordType();
      RecordStatus record_status = pmem_record->GetRecordStatus();
      switch (type) {
        case RecordType::HashElem:
        case RecordType::SortedElem:
        case RecordType::ListElem: {
          entries.emplace_back(pmem_allocator_->addr2offset(pmem_record),
                               pmem_record->GetRecordSize());
          if (record_status == RecordStatus::Normal) {
            pmem_record->Destroy();
          }
          break;
        }
        case RecordType::SortedRecord: {
          if (record_status != RecordStatus::Outdated &&
              !pmem_record->HasExpired()) {
            entries.emplace_back(
                pmem_allocator_->addr2offset_checked(pmem_record),
                pmem_record->GetRecordSize());
            pmem_record->Destroy();
          } else {
            auto skiplist_id = Skiplist::FetchID(pmem_record);
            kvdk_assert(skiplists_.find(skiplist_id) != skiplists_.end(),
                        "Skiplist should not be removed.");
            auto head_record = getSkiplist(skiplist_id)->HeaderRecord();
            if (head_record != pmem_record) {
              entries.emplace_back(
                  pmem_allocator_->addr2offset_checked(pmem_record),
                  pmem_record->GetRecordSize());
              pmem_record->Destroy();
            } else {
              pmem_record->PersistOldVersion(kNullPMemOffset);
            }
          }
          break;
        }
        case RecordType::HashRecord: {
          if (record_status != RecordStatus::Outdated &&
              !pmem_record->HasExpired()) {
            entries.emplace_back(
                pmem_allocator_->addr2offset_checked(pmem_record),
                pmem_record->GetRecordSize());
            pmem_record->Destroy();
          } else {
            auto hash_id = HashList::FetchID(pmem_record);
            kvdk_assert(hlists_.find(hash_id) != hlists_.end(),
                        "Hashlist should not be removed.");
            auto head_record = getHashlist(hash_id)->HeaderRecord();
            if (head_record != pmem_record) {
              entries.emplace_back(
                  pmem_allocator_->addr2offset_checked(pmem_record),
                  pmem_record->GetRecordSize());
              pmem_record->Destroy();
            } else {
              pmem_record->PersistOldVersion(kNullPMemOffset);
            }
          }
          break;
        }
        case RecordType::ListRecord: {
          if (record_status != RecordStatus::Outdated &&
              !pmem_record->HasExpired()) {
            entries.emplace_back(
                pmem_allocator_->addr2offset_checked(pmem_record),
                pmem_record->GetRecordSize());
            pmem_record->Destroy();
          } else {
            auto list_id = List::FetchID(pmem_record);
            kvdk_assert(lists_.find(list_id) != lists_.end(),
                        "Hashlist should not be removed.");
            auto header_record = getList(list_id)->HeaderRecord();
            if (header_record != pmem_record) {
              entries.emplace_back(
                  pmem_allocator_->addr2offset_checked(pmem_record),
                  pmem_record->GetRecordSize());
              pmem_record->Destroy();
            } else {
              pmem_record->PersistOldVersion(kNullPMemOffset);
            }
          }
          break;
        }
        default:
          GlobalLogger.Error("Cleaner abort on record type %u\n", type);
          std::abort();
      }
      pmem_record = next_record;
    }
  }
  pmem_allocator_->BatchFree(entries);
}

void KVEngine::cleanNoHashIndexedSkiplist(
    Skiplist* skiplist, std::vector<DLRecord*>& purge_dl_records) {
  auto prev_node = skiplist->HeaderNode();
  auto iter = skiplist->GetDLList()->GetRecordIterator();
  iter->SeekToFirst();
  while (iter->Valid() && !closing_) {
    DLRecord* cur_record = iter->Record();
    iter->Next();
    auto min_snapshot_ts = version_controller_.GlobalOldestSnapshotTs();
    auto ul = hash_table_->AcquireLock(cur_record->Key());
    // iter old version list
    auto old_record =
        removeOutDatedVersion<DLRecord>(cur_record, min_snapshot_ts);
    if (old_record) {
      purge_dl_records.emplace_back(old_record);
    }

    // check record has dram skiplist node and update skiplist node;
    SkiplistNode* dram_node = nullptr;
    SkiplistNode* cur_node = prev_node->Next(1).RawPointer();
    while (cur_node) {
      if (cur_node->Next(1).GetTag() == SkiplistNode::NodeStatus::Deleted) {
        // cur_node already been deleted
        cur_node = cur_node->Next(1).RawPointer();
      } else {
        kvdk_assert(cur_node->record->GetRecordType() == RecordType::SortedElem,
                    "");
        if (skiplist->Compare(cur_node->UserKey(),
                              Skiplist::UserKey(cur_record)) < 0) {
          prev_node = cur_node;
          cur_node = cur_node->Next(1).RawPointer();
        } else {
          break;
        }
      }
    }

    if (cur_node && cur_node->record == cur_record) {
      dram_node = cur_node;
    }

    if (cur_record->GetRecordType() == RecordType::SortedElem &&
        cur_record->GetRecordStatus() == RecordStatus::Outdated &&
        cur_record->GetTimestamp() < min_snapshot_ts) {
      TEST_SYNC_POINT(
          "KVEngine::BackgroundCleaner::IterSkiplist::"
          "UnlinkDeleteRecord");
      /* Notice: a user thread firstly update this key, its old version
       * record is delete record(cur_record). So the cur_record is not in
       * this skiplist, `Remove` function returns false. Nothing to do for
       * this cur_record which will be purged and freed in the next
       * iteration.
       */
      if (Skiplist::Remove(cur_record, dram_node, pmem_allocator_.get(),
                           dllist_locks_.get())) {
        purge_dl_records.emplace_back(cur_record);
      }
    }
  }
}

void KVEngine::cleanList(List* list, std::vector<DLRecord*>& purge_dl_records) {
  auto iter = list->GetDLList()->GetRecordIterator();
  iter->SeekToFirst();
  while (iter->Valid() && !closing_) {
    DLRecord* cur_record = iter->Record();
    iter->Next();
    auto ul = list->AcquireLock();
    auto min_snapshot_ts =
        std::min(version_controller_.GlobalOldestSnapshotTs(),
                 version_controller_.LocalOldestSnapshotTS());
    auto old_record =
        removeOutDatedVersion<DLRecord>(cur_record, min_snapshot_ts);
    bool clean_cur = false;
    if (cur_record->GetRecordType() == RecordType::ListElem &&
        cur_record->GetRecordStatus() == RecordStatus::Outdated &&
        cur_record->GetTimestamp() < min_snapshot_ts) {
      TEST_SYNC_POINT(
          "KVEngine::BackgroundCleaner::IterList::"
          "UnlinkDeleteRecord");
      if (list->GetDLList()->Remove(cur_record)) {
        clean_cur = true;
      }
    }
    ul.unlock();
    if (old_record) {
      purge_dl_records.emplace_back(old_record);
    }
    if (clean_cur) {
      purge_dl_records.emplace_back(cur_record);
    }
  }
}

void KVEngine::purgeAndFree(PendingCleanRecords& pending_clean_records) {
  {  // purge and free pending string records
    while (!pending_clean_records.pending_purge_strings.empty()) {
      auto& pending_strings =
          pending_clean_records.pending_purge_strings.front();
      if (pending_strings.release_time <
          version_controller_.LocalOldestSnapshotTS()) {
        purgeAndFreeStringRecords(pending_strings.records);
        pending_clean_records.pending_purge_strings.pop_front();
      } else {
        break;
      }
    }
  }

  {  // Destroy skiplist
    while (!pending_clean_records.outdated_skiplists.empty()) {
      auto& ts_skiplist = pending_clean_records.outdated_skiplists.front();
      auto skiplist = ts_skiplist.second;
      if (ts_skiplist.first < version_controller_.LocalOldestSnapshotTS() &&
          skiplist->TryCleaningLock()) {
        skiplist->DestroyAll();
        removeSkiplist(skiplist->ID());
        skiplist->ReleaseCleaningLock();
        pending_clean_records.outdated_skiplists.pop_front();
      } else {
        break;
      }
    }
  }

  {  // Destroy list
    while (!pending_clean_records.outdated_lists.empty()) {
      auto& ts_list = pending_clean_records.outdated_lists.front();
      auto list = ts_list.second;
      if (ts_list.first < version_controller_.LocalOldestSnapshotTS() &&
          list->TryCleaningLock()) {
        {
          auto ul = list->AcquireLock();
          list->DestroyAll();
        }
        removeList(list->ID());
        list->ReleaseCleaningLock();
        pending_clean_records.outdated_lists.pop_front();
      } else {
        break;
      }
    }
  }

  {  // Destroy hash
    while (!pending_clean_records.outdated_hlists.empty()) {
      auto& ts_hlist = pending_clean_records.outdated_hlists.front();
      auto hlist = ts_hlist.second;
      if (ts_hlist.first < version_controller_.LocalOldestSnapshotTS() &&
          hlist->TryCleaningLock()) {
        hlist->DestroyAll();
        removeHashlist(hlist->ID());
        hlist->ReleaseCleaningLock();
        pending_clean_records.outdated_hlists.pop_front();
      } else {
        break;
      }
    }
  }

  {  // purge and free pending old dl records
    while (!pending_clean_records.pending_purge_dls.empty()) {
      auto& pending_dls = pending_clean_records.pending_purge_dls.front();
      if (pending_dls.release_time <
          version_controller_.LocalOldestSnapshotTS()) {
        purgeAndFreeDLRecords(pending_dls.records);
        pending_clean_records.pending_purge_dls.pop_front();
      } else {
        break;
      }
    }
  }
}

void KVEngine::fetchCachedOutdatedVersion(
    PendingCleanRecords& pending_clean_records,
    std::vector<StringRecord*>& purge_string_records,
    std::vector<DLRecord*>& purge_dl_records) {
  size_t i = round_robin_id_.fetch_add(1) % configs_.max_access_threads;
  auto& tc = cleaner_thread_cache_[i];
  std::deque<CleanerThreadCache::OutdatedRecord<StringRecord>>
      outdated_string_records;
  std::deque<CleanerThreadCache::OutdatedRecord<DLRecord>> outdated_dl_records;
  if (tc.outdated_dl_records.size() > kMaxCachedOldRecords ||
      tc.outdated_string_records.size() > kMaxCachedOldRecords) {
    std::lock_guard<SpinMutex> lg(tc.mtx);
    if (tc.outdated_string_records.size() > kMaxCachedOldRecords) {
      outdated_string_records.swap(tc.outdated_string_records);
    }
    if (tc.outdated_dl_records.size() > kMaxCachedOldRecords) {
      outdated_dl_records.swap(tc.outdated_dl_records);
    }
  }
  if (outdated_string_records.size() > 0) {
    std::vector<StringRecord*> to_free_strings;
    version_controller_.UpdateLocalOldestSnapshot();
    auto release_time = version_controller_.LocalOldestSnapshotTS();
    auto iter = outdated_string_records.begin();
    while (iter != outdated_string_records.end()) {
      if (iter->release_time < release_time) {
        to_free_strings.emplace_back(iter->record);
      } else {
        purge_string_records.push_back(iter->record);
      }
      iter++;
    }
    pending_clean_records.pending_purge_strings.emplace_front(
        std::move(to_free_strings), release_time);
  }
  if (outdated_dl_records.size() > 0) {
    std::vector<DLRecord*> to_free_dls;
    version_controller_.UpdateLocalOldestSnapshot();
    auto release_time = version_controller_.LocalOldestSnapshotTS();
    auto iter = outdated_dl_records.begin();

    while (iter != outdated_dl_records.end()) {
      if (iter->release_time < release_time) {
        to_free_dls.emplace_back(iter->record);
      } else {
        purge_dl_records.push_back(iter->record);
      }
      iter++;
    }
    pending_clean_records.pending_purge_dls.emplace_front(
        std::move(to_free_dls), release_time);
  }
}

double KVEngine::cleanOutDated(PendingCleanRecords& pending_clean_records,
                               size_t start_slot_idx, size_t slot_block_size) {
  size_t total_num = 0;
  size_t need_purge_num = 0;
  version_controller_.UpdateLocalOldestSnapshot();

  std::vector<StringRecord*> purge_string_records;
  std::vector<DLRecord*> purge_dl_records;

  fetchCachedOutdatedVersion(pending_clean_records, purge_string_records,
                             purge_dl_records);
  double outdated_collection_ratio = cleaner_.SearchOutdatedCollections();
  cleaner_.FetchOutdatedCollections(pending_clean_records);

  // Iterate hash table
  size_t end_slot_idx = start_slot_idx + slot_block_size;
  if (end_slot_idx > hash_table_->GetSlotsNum()) {
    end_slot_idx = hash_table_->GetSlotsNum();
  }
  auto hashtable_iter = hash_table_->GetIterator(start_slot_idx, end_slot_idx);
  while (hashtable_iter.Valid() && !closing_) {
    {  // Slot lock section
      auto min_snapshot_ts = version_controller_.GlobalOldestSnapshotTs();
      auto now = TimeUtils::millisecond_time();

      auto slot_lock(hashtable_iter.AcquireSlotLock());
      auto slot_iter = hashtable_iter.Slot();
      while (slot_iter.Valid()) {
        if (!slot_iter->Empty()) {
          switch (slot_iter->GetIndexType()) {
            case PointerType::StringRecord: {
              total_num++;
              auto string_record = slot_iter->GetIndex().string_record;
              auto old_record = removeOutDatedVersion<StringRecord>(
                  string_record, min_snapshot_ts);
              if (old_record) {
                purge_string_records.emplace_back(old_record);
                need_purge_num++;
              }
              if ((string_record->GetRecordStatus() == RecordStatus::Outdated ||
                   string_record->GetExpireTime() <= now) &&
                  string_record->GetTimestamp() < min_snapshot_ts) {
                hash_table_->Erase(&(*slot_iter));
                purge_string_records.emplace_back(string_record);
                need_purge_num++;
              }
              break;
            }
            case PointerType::SkiplistNode: {
              total_num++;
              auto node = slot_iter->GetIndex().skiplist_node;
              auto dl_record = node->record;
              auto old_record =
                  removeOutDatedVersion<DLRecord>(dl_record, min_snapshot_ts);
              if (old_record) {
                purge_dl_records.emplace_back(old_record);
                need_purge_num++;
              }
              if (slot_iter->GetRecordStatus() == RecordStatus::Outdated &&
                  dl_record->GetTimestamp() < min_snapshot_ts) {
                bool success =
                    Skiplist::Remove(dl_record, node, pmem_allocator_.get(),
                                     dllist_locks_.get());
                kvdk_assert(success, "");
                hash_table_->Erase(&(*slot_iter));
                purge_dl_records.emplace_back(dl_record);
                need_purge_num++;
              }
              break;
            }
            case PointerType::DLRecord: {
              total_num++;
              auto dl_record = slot_iter->GetIndex().dl_record;
              auto old_record =
                  removeOutDatedVersion<DLRecord>(dl_record, min_snapshot_ts);
              if (old_record) {
                purge_dl_records.emplace_back(old_record);
                need_purge_num++;
              }
              if (slot_iter->GetRecordStatus() == RecordStatus::Outdated &&
                  dl_record->GetTimestamp() < min_snapshot_ts) {
                bool success = DLList::Remove(dl_record, pmem_allocator_.get(),
                                              dllist_locks_.get());
                kvdk_assert(success, "");
                hash_table_->Erase(&(*slot_iter));
                purge_dl_records.emplace_back(dl_record);
                need_purge_num++;
              }
              break;
            }
            case PointerType::Skiplist: {
              Skiplist* skiplist = slot_iter->GetIndex().skiplist;
              total_num++;
              if (!skiplist->IndexWithHashtable() &&
                  slot_iter->GetRecordStatus() != RecordStatus::Outdated &&
                  !skiplist->HasExpired()) {
                need_purge_num++;
                pending_clean_records.no_hash_skiplists.emplace_back(skiplist);
              }
              skiplist->CleanObsoletedNodes();
              break;
            }
            case PointerType::List: {
              List* list = slot_iter->GetIndex().list;
              total_num++;
              if (slot_iter->GetRecordStatus() != RecordStatus::Outdated &&
                  !list->HasExpired()) {
                need_purge_num++;
                pending_clean_records.valid_lists.emplace_back(list);
              }
              break;
            }
            default:
              break;
          }
        }
        slot_iter++;
      }
      hashtable_iter.Next();
    }  // Finish a slot.

    if (!pending_clean_records.no_hash_skiplists.empty()) {
      for (auto& skiplist : pending_clean_records.no_hash_skiplists) {
        if (skiplist && skiplist->TryCleaningLock()) {
          cleanNoHashIndexedSkiplist(skiplist, purge_dl_records);
          skiplist->ReleaseCleaningLock();
        }
      }
      pending_clean_records.no_hash_skiplists.clear();
    }

    if (!pending_clean_records.valid_lists.empty()) {
      for (auto& list : pending_clean_records.valid_lists) {
        if (list && list->TryCleaningLock()) {
          cleanList(list, purge_dl_records);
          list->ReleaseCleaningLock();
        }
      }
      pending_clean_records.valid_lists.clear();
    }

    auto new_ts = version_controller_.GetCurrentTimestamp();
    if (purge_string_records.size() > kMaxCachedOldRecords) {
      pending_clean_records.pending_purge_strings.emplace_back(
          std::move(purge_string_records), new_ts);
      purge_string_records = std::vector<StringRecord*>();
    }

    if (purge_dl_records.size() > kMaxCachedOldRecords) {
      pending_clean_records.pending_purge_dls.emplace_back(
          std::move(purge_dl_records), new_ts);
      purge_dl_records = std::vector<DLRecord*>();
    }

    purgeAndFree(pending_clean_records);

  }  // Finsh iterating hash table

  // Push the remaining need purged records to global pool.
  auto new_ts = version_controller_.GetCurrentTimestamp();
  if (!purge_string_records.empty()) {
    pending_clean_records.pending_purge_strings.emplace_back(
        std::move(purge_string_records), new_ts);
    purge_string_records = std::vector<StringRecord*>();
  }

  if (!purge_dl_records.empty()) {
    pending_clean_records.pending_purge_dls.emplace_back(
        std::move(purge_dl_records), new_ts);
    purge_dl_records = std::vector<DLRecord*>();
  }
  return total_num == 0
             ? 0.0f + outdated_collection_ratio
             : (need_purge_num / (double)total_num) + outdated_collection_ratio;
}

void KVEngine::TestCleanOutDated(size_t start_slot_idx, size_t end_slot_idx) {
  PendingCleanRecords pending_clean_records;
  while (!bg_work_signals_.terminating) {
    auto cur_slot_idx = start_slot_idx;
    while (cur_slot_idx < end_slot_idx && !bg_work_signals_.terminating) {
      cleanOutDated(pending_clean_records, cur_slot_idx, 1024);
      cur_slot_idx += 1024;
    }
  }
}
// Space Cleaner

Cleaner::OutDatedCollections::~OutDatedCollections() {}

void Cleaner::cleanWork() {
  PendingCleanRecords pending_clean_records;
  std::int64_t start_pos = start_slot_.fetch_add(kSlotBlockUnit) %
                           (kv_engine_->hash_table_->GetSlotsNum());
  kv_engine_->cleanOutDated(pending_clean_records, start_pos, kSlotBlockUnit);

  while (pending_clean_records.Size() != 0 && !close_.load()) {
    kv_engine_->version_controller_.UpdateLocalOldestSnapshot();
    kv_engine_->purgeAndFree(pending_clean_records);
  }
}

void Cleaner::AdjustCleanWorkers(size_t advice_wokers_num) {
  kvdk_assert(advice_wokers_num <= clean_workers_.size(), "");
  auto active_workers_num = active_clean_workers_.load();
  if (active_workers_num < advice_wokers_num) {
    for (size_t i = active_workers_num; i < advice_wokers_num; ++i) {
      clean_workers_[i].Run();
    }
  } else if (active_workers_num > advice_wokers_num) {
    for (size_t i = advice_wokers_num; i < active_workers_num; ++i) {
      clean_workers_[i].Stop();
    }
  }
  active_clean_workers_ = advice_wokers_num;
}

double Cleaner::SearchOutdatedCollections() {
  size_t limited_fetch_num = 0;
  std::unique_lock<SpinMutex> queue_lock(outdated_collections_.queue_mtx);
  int64_t before_queue_size =
      static_cast<int64_t>(outdated_collections_.hashlists.size() +
                           outdated_collections_.lists.size() +
                           outdated_collections_.skiplists.size());
  {
    std::unique_lock<std::mutex> skiplist_lock(kv_engine_->skiplists_mu_);
    auto iter = kv_engine_->expirable_skiplists_.begin();
    while (!kv_engine_->expirable_skiplists_.empty() && (*iter)->HasExpired() &&
           limited_fetch_num < max_thread_num_) {
      auto outdated_skiplist = *iter;
      iter = kv_engine_->expirable_skiplists_.erase(iter);
      outdated_collections_.skiplists.emplace(
          outdated_skiplist,
          kv_engine_->version_controller_.GetCurrentTimestamp());
      limited_fetch_num++;
    }
  }

  {
    std::unique_lock<std::mutex> list_lock(kv_engine_->lists_mu_);
    auto iter = kv_engine_->expirable_lists_.begin();
    while (!kv_engine_->expirable_lists_.empty() && (*iter)->HasExpired() &&
           limited_fetch_num < max_thread_num_) {
      auto outdated_list = *iter;
      iter = kv_engine_->expirable_lists_.erase(iter);
      outdated_collections_.lists.emplace(
          outdated_list, kv_engine_->version_controller_.GetCurrentTimestamp());
      limited_fetch_num++;
    }
  }

  {
    std::unique_lock<std::mutex> hlist_lock(kv_engine_->hlists_mu_);
    auto iter = kv_engine_->expirable_hlists_.begin();
    while (!kv_engine_->expirable_hlists_.empty() && (*iter)->HasExpired() &&
           limited_fetch_num < max_thread_num_) {
      auto expired_hash_list = *iter;
      iter = kv_engine_->expirable_hlists_.erase(iter);
      outdated_collections_.hashlists.emplace(
          expired_hash_list,
          kv_engine_->version_controller_.GetCurrentTimestamp());
      limited_fetch_num++;
    }
  }

  int64_t after_queue_size =
      static_cast<int64_t>(outdated_collections_.hashlists.size() +
                           outdated_collections_.lists.size() +
                           outdated_collections_.skiplists.size());

  double diff_size_ratio =
      (after_queue_size - before_queue_size) / max_thread_num_;
  if (diff_size_ratio > 0) {
    outdated_collections_.increase_ratio =
        outdated_collections_.increase_ratio == 0
            ? diff_size_ratio
            : diff_size_ratio / outdated_collections_.increase_ratio;
  }
  return outdated_collections_.increase_ratio;
}

void Cleaner::FetchOutdatedCollections(
    PendingCleanRecords& pending_clean_records) {
  Collection* outdated_collection = nullptr;
  RecordType record_type;
  auto min_snapshot_ts =
      kv_engine_->version_controller_.GlobalOldestSnapshotTs();
  {
    std::unique_lock<SpinMutex> queue_lock(outdated_collections_.queue_mtx);
    if (!outdated_collection && !outdated_collections_.skiplists.empty()) {
      auto skiplist_iter = outdated_collections_.skiplists.begin();
      if (skiplist_iter->second < min_snapshot_ts) {
        outdated_collection = skiplist_iter->first;
        record_type = RecordType::SortedRecord;
        outdated_collections_.skiplists.erase(skiplist_iter);
      }
    }

    if (!outdated_collection && !outdated_collections_.lists.empty()) {
      auto list_iter = outdated_collections_.lists.begin();
      if (list_iter->second < min_snapshot_ts) {
        outdated_collection = list_iter->first;
        record_type = RecordType::ListRecord;
        outdated_collections_.lists.erase(list_iter);
      }
    }

    if (!outdated_collection && !outdated_collections_.hashlists.empty()) {
      auto hash_list_iter = outdated_collections_.hashlists.begin();
      if (hash_list_iter->second < min_snapshot_ts) {
        outdated_collection = hash_list_iter->first;
        record_type = RecordType::HashRecord;
        outdated_collections_.hashlists.erase(hash_list_iter);
      }
    }
  }

  if (outdated_collection) {
    auto guard =
        kv_engine_->hash_table_->AcquireLock(outdated_collection->Name());
    auto lookup_result =
        kv_engine_->lookupKey<false>(outdated_collection->Name(), record_type);
    switch (lookup_result.entry_ptr->GetIndexType()) {
      case PointerType::HashList: {
        auto outdated_hlist = static_cast<HashList*>(outdated_collection);
        if (lookup_result.entry_ptr->GetIndex().hlist->ID() ==
            outdated_collection->ID()) {
          kv_engine_->hash_table_->Erase(lookup_result.entry_ptr);
        }
        kv_engine_->removeOutdatedCollection<HashList>(
            lookup_result.entry_ptr->GetIndex().hlist);
        pending_clean_records.outdated_hlists.emplace_back(std::make_pair(
            kv_engine_->version_controller_.GetCurrentTimestamp(),
            outdated_hlist));
        break;
      }
      case PointerType::List: {
        auto outdated_list = static_cast<List*>(outdated_collection);
        if (lookup_result.entry_ptr->GetIndex().list->ID() ==
            outdated_collection->ID()) {
          kv_engine_->hash_table_->Erase(lookup_result.entry_ptr);
        }
        kv_engine_->removeOutdatedCollection<List>(
            lookup_result.entry_ptr->GetIndex().list);
        pending_clean_records.outdated_lists.emplace_back(std::make_pair(
            kv_engine_->version_controller_.GetCurrentTimestamp(),
            outdated_list));
        break;
      }
      case PointerType::Skiplist: {
        auto outdated_skiplist = static_cast<Skiplist*>(outdated_collection);
        if (lookup_result.entry_ptr->GetIndex().skiplist->ID() ==
            outdated_collection->ID()) {
          kv_engine_->hash_table_->Erase(lookup_result.entry_ptr);
        }
        kv_engine_->removeOutdatedCollection<Skiplist>(
            lookup_result.entry_ptr->GetIndex().skiplist);
        pending_clean_records.outdated_skiplists.emplace_back(std::make_pair(
            kv_engine_->version_controller_.GetCurrentTimestamp(),
            outdated_skiplist));
        break;
      }
      default:
        break;
    }
  }
}

void Cleaner::mainWork() {
  PendingCleanRecords pending_clean_records;
  std::int64_t start_pos = start_slot_.fetch_add(kSlotBlockUnit) %
                           (kv_engine_->hash_table_->GetSlotsNum());

  double outdated_ratio = kv_engine_->cleanOutDated(pending_clean_records,
                                                    start_pos, kSlotBlockUnit);

  size_t advice_thread_num = min_thread_num_;
  if (outdated_ratio >= kWakeUpThreshold) {
    advice_thread_num = std::ceil(outdated_ratio * max_thread_num_);
    advice_thread_num =
        std::min(std::max(min_thread_num_, advice_thread_num), max_thread_num_);
  }
  TEST_SYNC_POINT_CALLBACK("KVEngine::Cleaner::AdjustCleanWorkers",
                           &advice_thread_num);
  size_t advice_clean_workers =
      advice_thread_num > 0 ? advice_thread_num - 1 : 0;

  AdjustCleanWorkers(advice_clean_workers);
  if (outdated_ratio < kWakeUpThreshold) {
    sleep(1);
  }

  while (pending_clean_records.Size() != 0 && !close_.load()) {
    kv_engine_->version_controller_.UpdateLocalOldestSnapshot();
    kv_engine_->purgeAndFree(pending_clean_records);
  }
}
}  // namespace KVDK_NAMESPACE
