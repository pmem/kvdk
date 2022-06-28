/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#include <algorithm>

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
      switch (old_record->GetRecordStatus()) {
        case RecordStatus::Normal:
          old_record->entry.Destroy();
          entries.emplace_back(pmem_allocator_->addr2offset(old_record),
                               old_record->entry.header.record_size);
          break;
        case RecordStatus::Outdated:
        case RecordStatus::Dirty:
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
      RecordType type = pmem_record->GetRecordType();
      RecordStatus record_status = pmem_record->GetRecordStatus();
      switch (type) {
        case RecordType::SortedElem: {
          entries.emplace_back(pmem_allocator_->addr2offset(pmem_record),
                               pmem_record->entry.header.record_size);
          if (record_status == RecordStatus::Normal) {
            pmem_record->Destroy();
          }
          break;
        }
        case RecordType::SortedHeader: {
          if (record_status == RecordStatus::Normal) {
            entries.emplace_back(pmem_allocator_->addr2offset(pmem_record),
                                 pmem_record->entry.header.record_size);
            pmem_record->Destroy();
          } else {
            auto skiplist_id = Skiplist::SkiplistID(pmem_record);
            // For the skiplist header, we should disconnect the old version
            // list of sorted header delete record. In order that `DestroyAll`
            // function could easily deal with destroying a sorted collection,
            // instead of may recusively destroy sorted collection, example
            // case: sortedHeaderDelete->sortedHeader->sortedHeaderDelete.
            skiplists_[skiplist_id]->HeaderRecord()->PersistOldVersion(
                kNullPMemOffset);
            skiplists_[skiplist_id]->DestroyAll();
            removeSkiplist(skiplist_id);
          }
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
  while (cur_record->GetRecordType() == RecordType::SortedElem) {
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

    DLRecord* next_record =
        pmem_allocator_->offset2addr<DLRecord>(cur_record->next);
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
                           skiplist_locks_.get())) {
        purge_dl_records.emplace_back(cur_record);
      }
    }
    cur_record = next_record;
  }
}

void KVEngine::prugeAndFreeAllType(
    PendingPrugeFreeRecords& pending_clean_records) {
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

  {  // Destroy skiplist
    while (!pending_clean_records.outdated_skip_lists.empty()) {
      auto& ts_skiplist = pending_clean_records.outdated_skip_lists.front();
      if (ts_skiplist.first < version_controller_.LocalOldestSnapshotTS()) {
        ts_skiplist.second->DestroyAll();
        removeSkiplist(ts_skiplist.second->ID());
        pending_clean_records.outdated_skip_lists.pop_front();
      } else {
        break;
      }
    }
  }

  {  // Destroy list
    while (!pending_clean_records.outdated_lists.empty()) {
      auto& ts_list = pending_clean_records.outdated_lists.front();
      if (ts_list.first < version_controller_.LocalOldestSnapshotTS()) {
        listDestroy(ts_list.second.release());
        pending_clean_records.outdated_lists.pop_front();
      } else {
        break;
      }
    }
  }

  {  // Destroy hash
    while (!pending_clean_records.outdated_hash_lists.empty()) {
      auto& ts_hlist = pending_clean_records.outdated_hash_lists.front();
      if (ts_hlist.first < version_controller_.LocalOldestSnapshotTS()) {
        hashListDestroy(ts_hlist.second.release());
        pending_clean_records.outdated_hash_lists.pop_front();
      } else {
        break;
      }
    }
  }
}

static inline int64_t PushToOutdatedPool(
    StringRecord* old_record, std::vector<StringRecord*> purge_string_records) {
  if (old_record) {
    purge_string_records.emplace_back(old_record);
    return 1;
  }
  return 0;
}

static inline int64_t PushToOutdatedPool(
    DLRecord* old_record, std::vector<DLRecord*> purge_dl_records) {
  if (old_record) {
    purge_dl_records.emplace_back(old_record);
    return 1;
  }
  return 0;
}

double KVEngine::cleanSlotBlockOutDated(
    PendingPrugeFreeRecords& pending_clean_records, size_t start_slot_idx,
    size_t slot_block_size) {
  constexpr uint64_t kMaxCachedOldRecords = 1024;
  size_t total_num = 0;
  size_t need_purge_num = 0;
  version_controller_.UpdatedOldestSnapshot();

  std::vector<StringRecord*> purge_string_records;
  std::vector<DLRecord*> purge_dl_records;

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
    purge_dl_records.insert(purge_dl_records.end(), tmp_dl_records.begin(),
                            tmp_dl_records.end());
  }

  // Iterate hash table
  size_t end_slot_idx = start_slot_idx + slot_block_size;
  if (end_slot_idx > hash_table_->GetSlotsNum()) {
    end_slot_idx = hash_table_->GetSlotsNum();
  }
  auto hashtable_iter = hash_table_->GetIterator(start_slot_idx, end_slot_idx);
  while (hashtable_iter.Valid()) {
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
              need_purge_num +=
                  PushToOutdatedPool(old_record, purge_string_records);
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
              need_purge_num +=
                  PushToOutdatedPool(old_record, purge_dl_records);
              if (slot_iter->GetRecordStatus() == RecordStatus::Outdated &&
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
              need_purge_num +=
                  PushToOutdatedPool(old_record, purge_dl_records);
              if (slot_iter->GetRecordStatus() == RecordStatus::Outdated &&
                  dl_record->entry.meta.timestamp < min_snapshot_ts) {
                bool success =
                    Skiplist::Remove(dl_record, nullptr, pmem_allocator_.get(),
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
              auto old_record =
                  removeOutDatedVersion<DLRecord>(head_record, min_snapshot_ts);
              need_purge_num +=
                  PushToOutdatedPool(old_record, purge_dl_records);
              if ((slot_iter->GetRecordStatus() == RecordStatus::Outdated ||
                   head_record->GetExpireTime() <= now) &&
                  head_record->entry.meta.timestamp < min_snapshot_ts) {
                hash_table_->Erase(&(*slot_iter));
                pending_clean_records.outdated_skip_lists.emplace_back(
                    std::make_pair(version_controller_.GetCurrentTimestamp(),
                                   skiplist));
                need_purge_num += skiplist->Size();
              } else if (!skiplist->IndexWithHashtable()) {
                pending_clean_records.no_index_skiplists.emplace_back(skiplist);
              }
              break;
            }
            case PointerType::List: {
              List* list = slot_iter->GetIndex().list;
              total_num += list->Size();
              auto current_ts = version_controller_.GetCurrentTimestamp();
              auto old_list = removeListOutDatedVersion(list, min_snapshot_ts);
              if (old_list) {
                pending_clean_records.outdated_lists.emplace_back(
                    std::make_pair(current_ts, old_list));
              }
              if (list->GetExpireTime() <= now &&
                  list->GetTimeStamp() < min_snapshot_ts) {
                hash_table_->Erase(&(*slot_iter));
                pending_clean_records.outdated_lists.emplace_back(
                    std::make_pair(current_ts, list));
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
              auto old_list = removeListOutDatedVersion(hlist, min_snapshot_ts);
              if (old_list) {
                pending_clean_records.outdated_hash_lists.emplace_back(
                    std::make_pair(current_ts, old_list));
              }
              if (hlist->GetExpireTime() <= now &&
                  hlist->GetTimeStamp() < min_snapshot_ts) {
                pending_clean_records.outdated_hash_lists.emplace_back(
                    std::make_pair(version_controller_.GetCurrentTimestamp(),
                                   hlist));
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

    if (!pending_clean_records.no_index_skiplists.empty()) {
      for (auto& skiplist : pending_clean_records.no_index_skiplists) {
        cleanNoHashIndexedSkiplist(skiplist, purge_dl_records);
      }
    }

    if (purge_string_records.size() > kMaxCachedOldRecords) {
      pending_clean_records.pending_purge_strings.emplace_back(
          PendingPurgeStrRecords{std::move(purge_string_records), new_ts});
      purge_string_records.clear();
    }

    if (purge_dl_records.size() > kMaxCachedOldRecords) {
      pending_clean_records.pending_purge_dls.emplace_back(
          PendingPurgeDLRecords{std::move(purge_dl_records), new_ts});
      purge_dl_records.clear();
    }

    prugeAndFreeAllType(pending_clean_records);

  }  // Finsh iterating hash table

  // Push the remaining need purged records to global pool.
  auto new_ts = version_controller_.GetCurrentTimestamp();
  if (!purge_string_records.empty()) {
    pending_clean_records.pending_purge_strings.emplace_back(
        PendingPurgeStrRecords{purge_string_records, new_ts});
    purge_string_records.clear();
  }

  if (!purge_dl_records.empty()) {
    pending_clean_records.pending_purge_dls.emplace_back(
        PendingPurgeDLRecords{purge_dl_records, new_ts});
    pending_clean_records.pending_purge_dls.clear();
  }
  return total_num == 0 ? 0.0f : need_purge_num / (double)total_num;
}

void KVEngine::TestCleanOutDated(int execute_time, size_t start_slot_idx,
                                 size_t end_slot_idx) {
  PendingPrugeFreeRecords pending_clean_records;
  while (!bg_work_signals_.terminating && (execute_time--)) {
    auto cur_slot_idx = start_slot_idx;
    while (cur_slot_idx < end_slot_idx && !bg_work_signals_.terminating) {
      cleanSlotBlockOutDated(pending_clean_records, cur_slot_idx, 1024);
      cur_slot_idx += 1024;
    }
  }
}

size_t KVEngine::ReclaimerThreadNum() {
  return space_reclaimer_.ReclaimerThreadNum();
}

// Space Reclaimer
void SpaceReclaimer::addNewWorker(size_t thread_id) {
  PendingPrugeFreeRecords pending_clean_records;
  while (true) {
    if (close_) return;
    cur_slot_idx_ = (cur_slot_idx_ + kSlotBlockUnit) %
                    kv_engine_->hash_table_->GetSlotsNum();
    kv_engine_->cleanSlotBlockOutDated(pending_clean_records,
                                       cur_slot_idx_.load(), kSlotBlockUnit);

    bool recycle = false;
    {
      std::unique_lock<std::mutex> worker_lock(workers_[thread_id].mtx);
      recycle = workers_[thread_id].recycle;
    }
    if (recycle) {
      while (pending_clean_records.Size() != 0 && !close_.load()) {
        kv_engine_->version_controller_.UpdatedOldestSnapshot();
        kv_engine_->prugeAndFreeAllType(pending_clean_records);
      }
      return;
    }
  }
}

void SpaceReclaimer::AdjustThread(size_t advice_thread_num) {
  auto active_thread_num = max_thread_num_ - idled_workers_.size();
  if (active_thread_num < advice_thread_num) {
    for (size_t i = active_thread_num; i < advice_thread_num; ++i) {
      size_t thread_id = idled_workers_.front();
      idled_workers_.pop_front();
      std::thread worker(&SpaceReclaimer::addNewWorker, this, thread_id);
      workers_[thread_id].recycle = false;
      workers_[thread_id].worker = std::move(worker);
      actived_workers_.push_back(thread_id);
      live_thread_num_++;
    }
  } else if (active_thread_num > advice_thread_num) {
    for (size_t i = advice_thread_num; i < active_thread_num; ++i) {
      auto thread_id = actived_workers_.front();
      actived_workers_.pop_front();
      {
        std::unique_lock<std::mutex> worker_lock(workers_[thread_id].mtx);
        workers_[thread_id].recycle = true;
      }
      workers_[thread_id].worker.detach();
      idled_workers_.push_back(thread_id);
      --live_thread_num_;
    }
  }
}

void SpaceReclaimer::mainWorker() {
  PendingPrugeFreeRecords pending_clean_records;
  while (true) {
    if (close_) return;
    cur_slot_idx_ = (cur_slot_idx_ + kSlotBlockUnit) %
                    kv_engine_->hash_table_->GetSlotsNum();
    auto outdated_ratio = kv_engine_->cleanSlotBlockOutDated(
        pending_clean_records, cur_slot_idx_.load(), kSlotBlockUnit);

    size_t advice_thread_num = std::ceil(outdated_ratio * max_thread_num_);
    advice_thread_num =
        std::min(std::max(advice_thread_num, min_thread_num_), max_thread_num_);
    AdjustThread(advice_thread_num);
  }
}

void SpaceReclaimer::StartReclaim() {
  TEST_SYNC_POINT_CALLBACK("KVEngine::backgroundCleaner::NothingToDo", &close_);
  if (!close_) {
    auto thread_id = idled_workers_.front();
    idled_workers_.pop_front();
    std::thread worker(&SpaceReclaimer::mainWorker, this);
    workers_[thread_id].recycle = false;
    workers_[thread_id].worker.swap(worker);
    live_thread_num_++;
  }
}

}  // namespace KVDK_NAMESPACE