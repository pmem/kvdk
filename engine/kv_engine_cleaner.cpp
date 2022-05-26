/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#include "kv_engine.hpp"
#include "utils/sync_point.hpp"

namespace KVDK_NAMESPACE {

template <typename T>
T* KVEngine::removeOutDatedVersion(T* record) {
  static_assert(
      std::is_same<T, StringRecord>::value || std::is_same<T, DLRecord>::value,
      "Invalid record type, should be StringRecord or DLRecord.");
  // auto old_record =
  //     static_cast<T*>(pmem_allocator_->offset2addr(record->old_version));
  auto old_record = record;
  while (old_record && old_record->entry.meta.timestamp >
                           version_controller_.OldestSnapshotTS()) {
    old_record =
        static_cast<T*>(pmem_allocator_->offset2addr(old_record->old_version));
  }

  // the snapshot should access the old record, so we need to purge and free the
  // older version of the old record
  if (old_record && old_record->old_version != kNullPMemOffset) {
    auto old_offset = old_record->old_version;
    old_record->PersistOldVersion(kNullPMemOffset);
    return static_cast<T*>(pmem_allocator_->offset2addr(old_offset));
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
  for (auto pmem_record : old_records) {
    while (pmem_record) {
      switch (pmem_record->GetRecordType()) {
        case RecordType::SortedElem:
        case RecordType::SortedHeader:
        case RecordType::SortedElemDelete: {
          pmem_record->entry.Destroy();
          entries.emplace_back(pmem_allocator_->addr2offset(pmem_record),
                               pmem_record->entry.header.record_size);
          break;
        }
        case RecordType::SortedHeaderDelete: {
          auto skiplist_id = Skiplist::SkiplistID(pmem_record);
          skiplists_[skiplist_id]->DestroyAll();
          removeSkiplist(skiplist_id);
          break;
        }
        default:
          std::abort();
      }
      pmem_record = static_cast<DLRecord*>(
          pmem_allocator_->offset2addr(pmem_record->old_version));
    }
  }
  pmem_allocator_->BatchFree(entries);
}

struct PendingPurgeStrRecords {
  std::vector<StringRecord*> records;
  TimeStampType release_time;
};

struct PendingPurgeDLRecords {
  std::vector<DLRecord*> records;
  TimeStampType release_time;
};

void KVEngine::CleanOutDated(size_t start_slot_idxx, size_t end_slot_idxx) {
  constexpr uint64_t kMaxCachedOldRecords = 1024;
  constexpr size_t kSlotSegment = 1024;
  constexpr double kWakeUpThreshold = 0.3;

  std::deque<std::pair<TimeStampType, List*>> outdated_lists;
  std::deque<std::pair<TimeStampType, HashList*>> outdated_hash_lists;
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
        hash_table_->GetIterator(start_slot_idxx, end_slot_idxx);
    while (hashtable_iter.Valid()) {
      /* 1. If having few outdated and old records per slot segment, the thread
       * is wake up every seconds.
       * 2. Update min snapshot timestamp per slot segment to avoid snapshot
       * lock conflict.
       */
      if (slot_num++ % kSlotSegment == 0) {
        if ((double)need_purge_num / total_num < kWakeUpThreshold) {
          sleep(1);
        }
        if (bg_work_signals_.terminating) break;
        total_num = 0;
        need_purge_num = 0;
        version_controller_.UpdatedOldestSnapshot();
      }
      auto min_snapshot_ts = version_controller_.OldestSnapshotTS();
      auto now = TimeUtils::millisecond_time();

      {  // Slot lock section
        auto slot_lock(hashtable_iter.AcquireSlotLock());
        auto slot_iter = hashtable_iter.Slot();
        while (slot_iter.Valid()) {
          if (!slot_iter->Empty()) {
            switch (slot_iter->GetIndexType()) {
              case PointerType::StringRecord: {
                total_num++;
                auto string_record = slot_iter->GetIndex().string_record;
                auto old_record =
                    removeOutDatedVersion<StringRecord>(string_record);
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
                auto dl_record = slot_iter->GetIndex().skiplist_node->record;
                auto old_record = removeOutDatedVersion<DLRecord>(dl_record);
                if (old_record) {
                  purge_dl_records.emplace_back(old_record);
                  need_purge_num++;
                }
                if (slot_iter->GetRecordType() ==
                        RecordType::SortedElemDelete &&
                    dl_record->entry.meta.timestamp < min_snapshot_ts) {
                  hash_table_->Erase(&(*slot_iter));
                  Skiplist::Remove(static_cast<DLRecord*>(dl_record),
                                   slot_iter->GetIndex().skiplist_node,
                                   pmem_allocator_.get(),
                                   skiplist_locks_.get());
                  purge_dl_records.emplace_back(dl_record);
                  need_purge_num++;
                }
                break;
              }
              case PointerType::DLRecord: {
                total_num++;
                auto dl_record = slot_iter->GetIndex().dl_record;
                auto old_record = removeOutDatedVersion<DLRecord>(dl_record);
                if (old_record) {
                  purge_dl_records.emplace_back(old_record);
                  need_purge_num++;
                }
                if (slot_iter->GetRecordType() ==
                        RecordType::SortedElemDelete &&
                    dl_record->entry.meta.timestamp < min_snapshot_ts) {
                  Skiplist::Remove(static_cast<DLRecord*>(dl_record), nullptr,
                                   pmem_allocator_.get(),
                                   skiplist_locks_.get());
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
                auto old_record = removeOutDatedVersion<DLRecord>(head_record);
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
                }
                break;
              }
              case PointerType::List: {
                List* list = slot_iter->GetIndex().list;
                total_num += list->Size();
                if (list->GetExpireTime() <= now &&
                    list->GetTimeStamp() < min_snapshot_ts) {
                  hash_table_->Erase(&(*slot_iter));
                  outdated_lists.emplace_back(std::make_pair(
                      version_controller_.GetCurrentTimestamp(), list));
                  need_purge_num += list->Size();
                }
                break;
              }
              case PointerType::HashList: {
                HashList* hlist = slot_iter->GetIndex().hlist;
                total_num += hlist->Size();
                if (hlist->GetExpireTime() <= now &&
                    hlist->GetTimeStamp() < min_snapshot_ts) {
                  outdated_hash_lists.emplace_back(std::make_pair(
                      version_controller_.GetCurrentTimestamp(), hlist));
                  hash_table_->Erase(&(*slot_iter));
                  need_purge_num += hlist->Size();
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

      {  // purge pending string records
        auto iter = pending_purge_strings.begin();
        while (iter != pending_purge_strings.end()) {
          if (iter->release_time < version_controller_.OldestSnapshotTS()) {
            purgeAndFreeStringRecords(iter->records);
            iter++;
          } else {
            break;
          }
        }
        pending_purge_strings.erase(pending_purge_strings.begin(), iter);
      }

      {  // purge pending old dl records
        auto iter = pending_purge_dls.begin();
        while (iter != pending_purge_dls.end()) {
          if (iter->release_time < version_controller_.OldestSnapshotTS()) {
            purgeAndFreeDLRecords(iter->records);
            iter++;
          } else {
            break;
          }
        }
        pending_purge_dls.erase(pending_purge_dls.begin(), iter);
      }

      // Destroy skiplist
      if (!outdated_skip_lists.empty()) {
        auto& ts_skiplist = outdated_skip_lists.front();
        if (ts_skiplist.first < min_snapshot_ts) {
          ts_skiplist.second->DestroyAll();
          removeSkiplist(ts_skiplist.second->ID());
          outdated_skip_lists.pop_front();
        }
      }

      // Destroy list
      if (!outdated_lists.empty()) {
        auto& ts_list = outdated_lists.front();
        if (ts_list.first < min_snapshot_ts) {
          std::unique_lock<std::mutex> guard{lists_mu_};
          lists_.erase(ts_list.second);
          guard.unlock();
          listDestroy(ts_list.second);
          outdated_lists.pop_front();
        }
      }

      // Destroy hash
      if (!outdated_hash_lists.empty()) {
        auto& ts_hlist = outdated_hash_lists.front();
        if (ts_hlist.first < min_snapshot_ts) {
          std::unique_lock<std::mutex> guard{hlists_mu_};
          hash_lists_.erase(ts_hlist.second);
          guard.unlock();
          hashListDestroy(ts_hlist.second);
          outdated_hash_lists.pop_front();
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

    TEST_SYNC_POINT_CALLBACK("KVEngine::backgroundCleaner::RunOnceTime",
                             &bg_work_signals_.terminating);
  }  // Terminate background thread.
}

}  // namespace KVDK_NAMESPACE