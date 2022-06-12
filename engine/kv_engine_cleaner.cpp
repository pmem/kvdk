/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#include "kv_engine.hpp"
#include "utils/sync_point.hpp"

namespace KVDK_NAMESPACE {

template <typename T>
T* KVEngine::removeListOutDatedVersion(T* list, TimeStampType min_snapshot_ts) {
  static_assert(
      std::is_same<T, List>::value || std::is_same<T, HashList>::value,
      "Invalid collection type, should be list or hashlist.");
  T* old_list = list;
  while (old_list && old_list->GetTimeStamp() > min_snapshot_ts) {
    old_list = old_list->OldVersion();
  }

  // the snapshot should access the old record, so we need to purge and free the
  // older version of the old record
  if (old_list && old_list->OldVersion()) {
    auto older_list = old_list->OldVersion();
    old_list->RemoveOldVersion();
    return older_list;
  }
  return nullptr;
}

void KVEngine::purgeAndFreeStringRecords(
    const std::vector<StringRecord*>& old_records) {
  std::vector<SpaceEntry> entries;
  for (auto old_record : old_records) {
    while (old_record) {
      switch (old_record->GetRecordType()) {
        case StringDataRecord:
          old_record->entry.Destroy();
          entries.emplace_back(pmem_allocator_->addr2offset(old_record),
                               old_record->entry.header.record_size);
          break;
        case StringDeleteRecord:
          entries.emplace_back(pmem_allocator_->addr2offset(old_record),
                               old_record->entry.header.record_size);
          break;
        default:
          std::abort();
      }
      old_record = static_cast<StringRecord*>(
          pmem_allocator_->offset2addr(old_record->old_version));
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
      switch (pmem_record->GetRecordType()) {
        case RecordType::SortedElem:
        case RecordType::SortedHeader:
        case RecordType::SortedElemDelete: {
          entries.emplace_back(pmem_allocator_->addr2offset(pmem_record),
                               pmem_record->entry.header.record_size);
          pmem_record->Destroy();
          break;
        }
        case RecordType::SortedHeaderDelete: {
          auto skiplist_id = Skiplist::SkiplistID(pmem_record);
          // For the skiplist header, we should disconnect the old version list
          // of sorted header delete record. In order that `DestroyAll` function
          // could easily deal with destroying a sorted collection, instead of
          // may recusively destroy sorted collection, example case:
          // sortedHeaderDelete->sortedHeader->sortedHeaderDelete.
          skiplists_[skiplist_id]->HeaderRecord()->PersistOldVersion(
              kNullPMemOffset);
          skiplists_[skiplist_id]->DestroyAll();
          removeSkiplist(skiplist_id);
          break;
        }
        default:
          std::abort();
      }
      pmem_record = next_record;
    }
  }
  pmem_allocator_->BatchFree(entries);
}

void KVEngine::cleanNoHashIndexedSkiplist(
    Skiplist* skiplist, std::vector<DLRecord*>& purge_dl_records) {
  auto header = skiplist->HeaderRecord();
  auto prev_node = skiplist->HeaderNode();
  auto cur_record =
      pmem_allocator_->offset2addr_checked<DLRecord>(header->next);
  while ((cur_record->GetRecordType() & SortedHeaderType) == 0) {
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
        kvdk_assert((cur_node->record->GetRecordType() & SortedHeaderType) == 0,
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

    DLRecord* next_record =
        pmem_allocator_->offset2addr<DLRecord>(cur_record->next);
    switch (cur_record->GetRecordType()) {
      case SortedElemDelete: {
        if (cur_record->GetTimestamp() < min_snapshot_ts) {
          TEST_SYNC_POINT(
              "KVEngine::BackgroundCleaner::IterSkiplist::UnlinkDeleteRecord");
          /* Notice: a user thread firstly update this key, its old version
           * record is delete record(cur_record). So the cur_record is not in
           * this skiplist, `Remove` function returns false. Nothing to do for
           * this cur_record which will be purged and freed in the next
           * iteration.
           */
          if (Skiplist::Remove(cur_record, dram_node, pmem_allocator_.get(),
                               skiplist_locks_.get())) {
            purge_dl_records.emplace_back(cur_record);
          }
        }
        break;
        case SortedHeader:
        case SortedHeaderDelete:
        case SortedElem:
          break;
        default:
          std::abort();
      }
    }
    cur_record = next_record;
  }
}

struct PendingPurgeStrRecords {
  std::vector<StringRecord*> records;
  TimeStampType release_time;
};

struct PendingPurgeDLRecords {
  std::vector<DLRecord*> records;
  TimeStampType release_time;
};

void KVEngine::CleanOutDated(size_t start_slot_idx, size_t end_slot_idx) {
  using ListPtr = std::unique_ptr<List>;
  using HashListPtr = std::unique_ptr<HashList>;
  TEST_SYNC_POINT_CALLBACK("KVEngine::backgroundCleaner::Start",
                           &bg_work_signals_.terminating);
  constexpr uint64_t kMaxCachedOldRecords = 1024;
  constexpr size_t kSlotSegment = 1024;
  constexpr double kWakeUpThreshold = 0.1;

  std::deque<std::pair<TimeStampType, ListPtr>> outdated_lists;
  std::deque<std::pair<TimeStampType, HashListPtr>> outdated_hash_lists;
  std::deque<std::pair<TimeStampType, Skiplist*>> outdated_skip_lists;
  std::deque<PendingPurgeStrRecords> pending_purge_strings;
  std::deque<PendingPurgeDLRecords> pending_purge_dls;

  while (!bg_work_signals_.terminating) {
    size_t total_num = 0;
    size_t need_purge_num = 0;
    size_t slot_num = 0;
    version_controller_.UpdatedOldestSnapshot();

    std::vector<StringRecord*> purge_string_records;
    std::vector<DLRecord*> purge_dl_records;

    // Iterate hash table
    auto hashtable_iter =
        hash_table_->GetIterator(start_slot_idx, end_slot_idx);
    while (hashtable_iter.Valid()) {
      /* 1. If having few outdated and old records per slot segment, the thread
       * is wake up every seconds.
       * 2. Update min snapshot timestamp per slot segment to avoid snapshot
       * lock conflict.
       */
      if (slot_num++ % kSlotSegment == 0) {
        std::vector<StringRecord*> tmp_string_records;
        std::vector<DLRecord*> tmp_dl_records;
        {  // Deal with old records from forground
          round_robin_id_ = (round_robin_id_ + 1) % configs_.max_access_threads;
          auto& tc = cleaner_thread_cache_[round_robin_id_];
          std::unique_lock<SpinMutex> lock(tc.mtx);
          if (!tc.old_str_records.empty()) {
            tmp_string_records.swap(tc.old_str_records);
            need_purge_num += tmp_string_records.size();
          }
          if (!tc.old_dl_records.empty()) {
            tmp_dl_records.swap(tc.old_dl_records);
            need_purge_num += tmp_dl_records.size();
          }
        }
        if (!tmp_string_records.empty()) {
          purge_string_records.insert(purge_string_records.end(),
                                      tmp_string_records.begin(),
                                      tmp_string_records.end());
        }
        if (!tmp_dl_records.empty()) {
          purge_dl_records.insert(purge_dl_records.end(),
                                  tmp_dl_records.begin(), tmp_dl_records.end());
        }

        // total_num + kWakeUpThreshold to avoid division by zero.
        if (need_purge_num / (double)(total_num + kWakeUpThreshold) <
            kWakeUpThreshold) {
          sleep(1);
        }
        if (bg_work_signals_.terminating) break;
        total_num = 0;
        need_purge_num = 0;
        version_controller_.UpdatedOldestSnapshot();
      }

      std::vector<Skiplist*> no_index_skiplists;

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
                if ((string_record->GetRecordType() ==
                         RecordType::StringDeleteRecord ||
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
                if (slot_iter->GetRecordType() ==
                        RecordType::SortedElemDelete &&
                    dl_record->entry.meta.timestamp < min_snapshot_ts) {
                  bool success =
                      Skiplist::Remove(dl_record, node, pmem_allocator_.get(),
                                       skiplist_locks_.get());
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
                if (slot_iter->GetRecordType() ==
                        RecordType::SortedElemDelete &&
                    dl_record->entry.meta.timestamp < min_snapshot_ts) {
                  bool success = Skiplist::Remove(dl_record, nullptr,
                                                  pmem_allocator_.get(),
                                                  skiplist_locks_.get());
                  kvdk_assert(success, "");
                  hash_table_->Erase(&(*slot_iter));
                  purge_dl_records.emplace_back(dl_record);
                  need_purge_num++;
                }
                break;
              }
              case PointerType::Skiplist: {
                Skiplist* skiplist = slot_iter->GetIndex().skiplist;
                total_num += skiplist->Size();
                auto head_record = skiplist->HeaderRecord();
                auto old_record = removeOutDatedVersion<DLRecord>(
                    head_record, min_snapshot_ts);
                if (old_record) {
                  purge_dl_records.emplace_back(old_record);
                  need_purge_num++;
                }
                if ((slot_iter->GetRecordType() ==
                         RecordType::SortedHeaderDelete ||
                     head_record->GetExpireTime() <= now) &&
                    head_record->entry.meta.timestamp < min_snapshot_ts) {
                  hash_table_->Erase(&(*slot_iter));
                  outdated_skip_lists.emplace_back(std::make_pair(
                      version_controller_.GetCurrentTimestamp(), skiplist));
                  need_purge_num += skiplist->Size();
                } else if (!skiplist->IndexWithHashtable()) {
                  no_index_skiplists.emplace_back(skiplist);
                }
                break;
              }
              case PointerType::List: {
                List* list = slot_iter->GetIndex().list;
                total_num += list->Size();
                auto current_ts = version_controller_.GetCurrentTimestamp();
                auto old_list =
                    removeListOutDatedVersion(list, min_snapshot_ts);
                if (old_list) {
                  outdated_lists.emplace_back(
                      std::make_pair(current_ts, old_list));
                }
                if (list->GetExpireTime() <= now &&
                    list->GetTimeStamp() < min_snapshot_ts) {
                  hash_table_->Erase(&(*slot_iter));
                  outdated_lists.emplace_back(std::make_pair(current_ts, list));
                  need_purge_num += list->Size();
                  std::unique_lock<std::mutex> guard{lists_mu_};
                  lists_.erase(list);
                }
                break;
              }
              case PointerType::HashList: {
                HashList* hlist = slot_iter->GetIndex().hlist;
                total_num += hlist->Size();
                auto current_ts = version_controller_.GetCurrentTimestamp();
                auto old_list =
                    removeListOutDatedVersion(hlist, min_snapshot_ts);
                if (old_list) {
                  outdated_hash_lists.emplace_back(
                      std::make_pair(current_ts, old_list));
                }
                if (hlist->GetExpireTime() <= now &&
                    hlist->GetTimeStamp() < min_snapshot_ts) {
                  outdated_hash_lists.emplace_back(std::make_pair(
                      version_controller_.GetCurrentTimestamp(), hlist));
                  hash_table_->Erase(&(*slot_iter));
                  need_purge_num += hlist->Size();
                  std::unique_lock<std::mutex> guard{hlists_mu_};
                  hash_lists_.erase(hlist);
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

      auto new_ts = version_controller_.GetCurrentTimestamp();

      if (purge_string_records.size() > kMaxCachedOldRecords) {
        pending_purge_strings.emplace_back(
            PendingPurgeStrRecords{purge_string_records, new_ts});
        purge_string_records.clear();
      }

      if (purge_dl_records.size() > kMaxCachedOldRecords) {
        pending_purge_dls.emplace_back(
            PendingPurgeDLRecords{purge_dl_records, new_ts});
        purge_dl_records.clear();
      }

      {  // purge and free pending string records
        while (!pending_purge_strings.empty()) {
          auto& pending_strings = pending_purge_strings.front();
          if (pending_strings.release_time <
              version_controller_.LocalOldestSnapshotTS()) {
            purgeAndFreeStringRecords(pending_strings.records);
            pending_purge_strings.pop_front();
          } else {
            break;
          }
        }
      }

      {  // purge and free pending old dl records
        while (!pending_purge_dls.empty()) {
          auto& pending_dls = pending_purge_dls.front();
          if (pending_dls.release_time <
              version_controller_.LocalOldestSnapshotTS()) {
            purgeAndFreeDLRecords(pending_dls.records);
            pending_purge_dls.pop_front();
          } else {
            break;
          }
        }
      }

      {  // Deal with skiplist with hash index: remove outdated records.
        if (!no_index_skiplists.empty()) {
          for (auto& skiplist : no_index_skiplists) {
            cleanNoHashIndexedSkiplist(skiplist, purge_dl_records);
          }
        }
      }

      {  // Destroy skiplist
        while (!outdated_skip_lists.empty()) {
          auto& ts_skiplist = outdated_skip_lists.front();
          if (ts_skiplist.first < version_controller_.LocalOldestSnapshotTS()) {
            ts_skiplist.second->DestroyAll();
            removeSkiplist(ts_skiplist.second->ID());
            outdated_skip_lists.pop_front();
          } else {
            break;
          }
        }
      }

      {  // Destroy list
        while (!outdated_lists.empty()) {
          auto& ts_list = outdated_lists.front();
          if (ts_list.first < version_controller_.LocalOldestSnapshotTS()) {
            listDestroy(ts_list.second.release());
            outdated_lists.pop_front();
          } else {
            break;
          }
        }
      }

      {  // Destroy hash
        while (!outdated_hash_lists.empty()) {
          auto& ts_hlist = outdated_hash_lists.front();
          if (ts_hlist.first < version_controller_.LocalOldestSnapshotTS()) {
            hashListDestroy(ts_hlist.second.release());
            outdated_hash_lists.pop_front();
          } else {
            break;
          }
        }
      }
    }  // Finsh iterating hash table

    // Push the remaining need purged records to global pool.
    auto new_ts = version_controller_.GetCurrentTimestamp();
    if (!purge_string_records.empty()) {
      pending_purge_strings.emplace_back(
          PendingPurgeStrRecords{purge_string_records, new_ts});
      purge_string_records.clear();
    }

    if (!purge_dl_records.empty()) {
      pending_purge_dls.emplace_back(
          PendingPurgeDLRecords{purge_dl_records, new_ts});
      pending_purge_dls.clear();
    }

    TEST_SYNC_POINT_CALLBACK("KVEngine::backgroundCleaner::ExecuteNTime",
                             &bg_work_signals_.terminating);
  }  // Terminate background thread.
}

}  // namespace KVDK_NAMESPACE