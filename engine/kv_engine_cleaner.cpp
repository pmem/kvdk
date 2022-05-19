/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#include "kv_engine.hpp"
#include "utils/sync_point.hpp"

namespace KVDK_NAMESPACE {

template <typename T>
PMemOffsetType KVEngine::updateVersionList(T* record) {
  auto old_record =
      static_cast<T*>(pmem_allocator_->offset2addr(record->old_version));
  while (old_record && old_record->entry.meta.timestamp >
                           version_controller_.OldestSnapshotTS()) {
    old_record =
        static_cast<T*>(pmem_allocator_->offset2addr(old_record->old_version));
  }

  // the snapshot should access the old record, so we need to purge and free the
  // older version of the old record
  if (old_record && old_record->old_version != kNullPMemOffset) {
    PMemOffsetType old_offset = old_record->old_version;
    assert(pmem_allocator_->addr2offset(record) != old_offset);
    old_record->PersistOldVersion(kNullPMemOffset);
    return old_offset;
  }
  return kNullPMemOffset;
}

void KVEngine::destroyOldStringRecords(PMemOffsetType old_offset,
                                       std::vector<SpaceEntry>* entries) {
  auto old_record =
      static_cast<StringRecord*>(pmem_allocator_->offset2addr(old_offset));
  while (old_record) {
    switch (old_record->GetRecordType()) {
      case StringDataRecord:
        old_record->entry.Destroy();
        entries->emplace_back(pmem_allocator_->addr2offset(old_record),
                              old_record->entry.header.record_size);
        break;
      case StringDeleteRecord:
        entries->emplace_back(pmem_allocator_->addr2offset(old_record),
                              old_record->entry.header.record_size);
        break;
      default:
        std::abort();
    }
    old_record = static_cast<StringRecord*>(
        pmem_allocator_->offset2addr(old_record->old_version));
  }
}

void KVEngine::destroyOldDLRecords(PMemOffsetType old_offset,
                                   std::vector<SpaceEntry>* entries) {
  auto old_record =
      static_cast<DLRecord*>(pmem_allocator_->offset2addr(old_offset));
  while (old_record) {
    switch (old_record->GetRecordType()) {
      case RecordType::SortedElem: {
        old_record->entry.Destroy();
        entries->emplace_back(pmem_allocator_->addr2offset(old_record),
                              old_record->entry.header.record_size);
        break;
      }
      case RecordType::SortedHeader: {
        old_record->entry.Destroy();
        entries->emplace_back(pmem_allocator_->addr2offset(old_record),
                              old_record->entry.header.record_size);
        break;
      }
      case RecordType::SortedElemDelete: {
        entries->emplace_back(purgeSortedRecord(nullptr, old_record));
        break;
      }
      case RecordType::SortedHeaderDelete: {
        auto skiplist_id = Skiplist::SkiplistID(old_record);
        skiplists_[skiplist_id]->Destroy();
        removeSkiplist(skiplist_id);
        break;
      }
      default:
        std::abort();
    }
    old_record = static_cast<DLRecord*>(
        pmem_allocator_->offset2addr(old_record->old_version));
  }
}

SpaceEntry KVEngine::purgeSortedRecord(SkiplistNode* dram_node,
                                       DLRecord* pmem_record) {
  // We check linkage to determine if the delete record already been
  // unlinked by updates. We only check the next linkage, as the record is
  // already been locked, its next record will not be changed.
  kvdk_assert(dram_node == nullptr || dram_node->record == pmem_record,
              "On-list old delete record of skiplist no pointed by its "
              "dram node");

  skiplist_locks_->Lock(Skiplist::RecordHash(pmem_record));
  bool record_on_list = Skiplist::CheckReocrdNextLinkage(
      static_cast<DLRecord*>(pmem_record), pmem_allocator_.get());

  if (record_on_list) {
    skiplist_locks_->Unlock(Skiplist::RecordHash(pmem_record));
    Skiplist::Purge(static_cast<DLRecord*>(pmem_record), dram_node,
                    pmem_allocator_.get(), skiplist_locks_.get());
  }
  return SpaceEntry(pmem_allocator_->addr2offset_checked(pmem_record),
                    pmem_record->entry.header.record_size);
}

void KVEngine::purgeOutDatedRecords(
    const std::vector<std::pair<void*, PointerType>>& outdated_records,
    std::vector<SpaceEntry>* pending_free_entries) {
  for (auto& record_pair : outdated_records) {
    switch (record_pair.second) {
      case PointerType::StringRecord: {
        StringRecord* record = static_cast<StringRecord*>(record_pair.first);
        if (record->GetRecordType() == StringDataRecord) {
          record->entry.Destroy();
        }
        pending_free_entries->emplace_back(
            SpaceEntry(pmem_allocator_->addr2offset(record),
                       record->entry.header.record_size));
        break;
      }
      case PointerType::SkiplistNode: {
        SkiplistNode* skiplist_node =
            static_cast<SkiplistNode*>(record_pair.first);
        kvdk_assert(skiplist_node->record->GetRecordType() == SortedElemDelete,
                    "Should be sorted elem delete type");
        pending_free_entries->emplace_back(
            purgeSortedRecord(skiplist_node, skiplist_node->record));
        break;
      }
      case PointerType::DLRecord: {
        DLRecord* dl_record = static_cast<DLRecord*>(record_pair.first);
        kvdk_assert(dl_record->GetRecordType() == SortedElemDelete,
                    "Should be sorted elem delete type");
        pending_free_entries->emplace_back(
            purgeSortedRecord(nullptr, dl_record));
        break;
      }
      default:
        std::abort();
    }
  }
}

struct PendingPurgeOldRecords {
  std::vector<PMemOffsetType> records;
  TimeStampType release_time;
};

struct PendingPurgeOutDatedRecords {
  std::vector<std::pair<void*, PointerType>> outdated_records;
  TimeStampType release_time;
};

void KVEngine::CleanOutDated(size_t start_slot_idx, size_t end_slot_idx) {
  std::deque<PendingFreeSpaceEntries> pending_free_spaces;
  std::deque<std::pair<TimeStampType, List*>> outdated_lists;
  std::deque<std::pair<TimeStampType, HashList*>> outdated_hash_lists;
  std::deque<std::pair<TimeStampType, Skiplist*>> outdated_skip_lists;
  std::deque<PendingPurgeOldRecords> pending_purge_strings;
  std::deque<PendingPurgeOldRecords> pending_purge_dls;
  std::deque<PendingPurgeOutDatedRecords> pending_purge_outdated;

  while (!bg_work_signals_.terminating) {
    size_t clean_num = 0;
    size_t total_num = 0;
    size_t total_scan_num = 0;
    // size_t purge0_num = 0;
    size_t purge1_num = 0;
    size_t slot_num = 0;

    // auto start_ts = std::chrono::system_clock::now();

    std::vector<PMemOffsetType> old_string_records;
    std::vector<PMemOffsetType> old_dl_records;
    std::vector<std::pair<void*, PointerType>> outdated_records;

    // Iterate hash table
    auto hashtable_iter =
        hash_table_->GetIterator(start_slot_idx, end_slot_idx);

    while (hashtable_iter.Valid()) {
      auto now = TimeUtils::millisecond_time();

      if (slot_num++ % 8 == 0) {
        version_controller_.UpdatedOldestSnapshot();
      }
      auto min_snapshot_ts = version_controller_.OldestSnapshotTS();
      {  // Slot lock section
        auto slot_lock(hashtable_iter.AcquireSlotLock());
        auto slot_iter = hashtable_iter.Slot();
        while (slot_iter.Valid()) {
          total_scan_num++;
          if (!slot_iter->Empty()) {
            total_num++;
            switch (slot_iter->GetIndexType()) {
              case PointerType::StringRecord: {
                auto string_record = slot_iter->GetIndex().string_record;
                if ((string_record->GetRecordType() ==
                         RecordType::StringDeleteRecord ||
                     string_record->GetExpireTime() <= now) &&
                    string_record->GetTimestamp() < min_snapshot_ts) {
                  outdated_records.emplace_back(
                      std::make_pair(string_record, slot_iter->GetIndexType()));
                  hash_table_->Erase(&(*slot_iter));
                  old_string_records.emplace_back(string_record->old_version);
                } else {
                  old_string_records.emplace_back(
                      updateVersionList<StringRecord>(string_record));
                }
                break;
              }
              case PointerType::SkiplistNode: {
                auto dl_record = slot_iter->GetIndex().skiplist_node->record;
                if (slot_iter->GetRecordType() ==
                        RecordType::SortedElemDelete &&
                    dl_record->entry.meta.timestamp < min_snapshot_ts) {
                  outdated_records.emplace_back(std::make_pair(
                      slot_iter->GetIndex().ptr, slot_iter->GetIndexType()));
                  hash_table_->Erase(&(*slot_iter));
                  old_dl_records.emplace_back(dl_record->old_version);
                } else {
                  old_dl_records.emplace_back(
                      updateVersionList<DLRecord>(dl_record));
                }
                break;
              }
              case PointerType::DLRecord: {
                auto dl_record = slot_iter->GetIndex().dl_record;
                if (slot_iter->GetRecordType() ==
                        RecordType::SortedElemDelete &&
                    dl_record->entry.meta.timestamp < min_snapshot_ts) {
                  outdated_records.emplace_back(std::make_pair(
                      slot_iter->GetIndex().ptr, slot_iter->GetIndexType()));
                  hash_table_->Erase(&(*slot_iter));
                  old_dl_records.emplace_back(dl_record->old_version);
                } else {
                  old_dl_records.emplace_back(
                      updateVersionList<DLRecord>(dl_record));
                }
                break;
              }
              case PointerType::Skiplist: {
                Skiplist* skiplist = slot_iter->GetIndex().skiplist;
                auto head_record = skiplist->HeaderRecord();
                if ((slot_iter->GetRecordType() ==
                         RecordType::SortedHeaderDelete ||
                     head_record->GetExpireTime() <= now) &&
                    head_record->entry.meta.timestamp < min_snapshot_ts) {
                  hash_table_->Erase(&(*slot_iter));
                  removeSkiplist(skiplist->ID());
                  outdated_skip_lists.emplace_back(std::make_pair(
                      version_controller_.GetCurrentTimestamp(), skiplist));
                  old_dl_records.emplace_back(head_record->old_version);
                } else {
                  old_dl_records.emplace_back(
                      updateVersionList<DLRecord>(head_record));
                }
                break;
              }
              case PointerType::List: {
                List* list = slot_iter->GetIndex().list;
                if (list->GetExpireTime() <= now &&
                    list->GetTimeStamp() < min_snapshot_ts) {
                  hash_table_->Erase(&(*slot_iter));
                  outdated_lists.emplace_back(std::make_pair(
                      version_controller_.GetCurrentTimestamp(), list));
                }
                break;
              }
              case PointerType::HashList: {
                HashList* hlist = slot_iter->GetIndex().hlist;
                if (hlist->GetExpireTime() <= now &&
                    hlist->GetTimeStamp() < min_snapshot_ts) {
                  outdated_hash_lists.emplace_back(std::make_pair(
                      version_controller_.GetCurrentTimestamp(), hlist));
                  hash_table_->Erase(&(*slot_iter));
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
      }

      auto new_ts = version_controller_.GetCurrentTimestamp();

      if (old_string_records.size() > kMaxCachedOldRecords) {
        pending_purge_strings.emplace_back(
            PendingPurgeOldRecords{old_string_records, new_ts});
        old_string_records.clear();
      }

      if (old_dl_records.size() > kMaxCachedOldRecords) {
        pending_purge_dls.emplace_back(
            PendingPurgeOldRecords{old_dl_records, new_ts});
        old_dl_records.clear();
      }

      if (outdated_records.size() > kMaxCachedOldRecords) {
        pending_purge_outdated.emplace_back(
            PendingPurgeOutDatedRecords{outdated_records, new_ts});
        outdated_records.clear();
      }

      {
        // purge pending old string records
        auto iter = pending_purge_strings.begin();
        while (iter != pending_purge_strings.end()) {
          if (iter->release_time < version_controller_.OldestSnapshotTS()) {
            std::vector<SpaceEntry> old_entries;
            for (auto old_record_offset : iter->records) {
              destroyOldStringRecords(old_record_offset, &old_entries);
            }
            pmem_allocator_->BatchFree(old_entries);
            iter++;
          } else {
            break;
          }
        }
        pending_purge_strings.erase(pending_purge_strings.begin(), iter);
      }

      {
        // purge pending old dl records
        auto iter = pending_purge_dls.begin();
        while (iter != pending_purge_dls.end()) {
          if (iter->release_time < version_controller_.OldestSnapshotTS()) {
            std::vector<SpaceEntry> old_entries;
            for (auto old_record_offset : iter->records) {
              destroyOldDLRecords(old_record_offset, &old_entries);
            }
            pmem_allocator_->BatchFree(old_entries);
            iter++;
          } else {
            break;
          }
        }
        pending_purge_dls.erase(pending_purge_dls.begin(), iter);
      }

      {
        // purge pending old outdated records
        auto outdated_iter = pending_purge_outdated.begin();
        while (outdated_iter != pending_purge_outdated.end()) {
          std::vector<SpaceEntry> outdated_entries;
          if (outdated_iter->release_time <
              version_controller_.OldestSnapshotTS()) {
            purgeOutDatedRecords(outdated_iter->outdated_records,
                                 &outdated_entries);
            pending_free_spaces.emplace_back(
                PendingFreeSpaceEntries{outdated_entries, new_ts});
            outdated_iter++;
          } else {
            break;
          }
        }
        pending_purge_outdated.erase(pending_purge_outdated.begin(),
                                     outdated_iter);
      }

      // Free pending space entries
      auto iter = pending_free_spaces.begin();
      while (iter != pending_free_spaces.end()) {
        if (iter->release_time < version_controller_.OldestSnapshotTS()) {
          clean_num += iter->entries.size();
          pmem_allocator_->BatchFree(iter->entries);
          iter++;
        } else {
          break;
        }
      }
      pending_free_spaces.erase(pending_free_spaces.begin(), iter);

      if (!outdated_skip_lists.empty()) {
        auto& ts_skiplist = outdated_skip_lists.front();
        if (ts_skiplist.first < min_snapshot_ts) {
          purge1_num += ts_skiplist.second->Size();
          ts_skiplist.second->Destroy();
          outdated_skip_lists.pop_front();
        }
      }

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

      // auto duration = std::chrono::duration_cast<std::chrono::seconds>(
      //     std::chrono::system_clock::now() - start_ts);
      // if (duration.count() > 20) {
      //   GlobalLogger.Info(
      //       "Thread: %ld,"
      //       "Cost Time: %d s,"
      //       "Total scan num: %ld,"
      //       " total handlem ops: % ld, "
      //       "total purge old ops: % ld, "
      //       "total purge outdated ops: % ld,"
      //       "Total clean ops: % ld\n ",
      //       std::this_thread::get_id(), duration.count(), total_scan_num,
      //       total_num / duration.count(), purge0_num / duration.count(),
      //       purge1_num / duration.count(), clean_num / duration.count());
      //   start_ts = std::chrono::system_clock::now();
      //   clean_num = 0;
      //   total_num = 0;
      //   purge0_num = 0;
      //   purge1_num = 0;
      // }
    }

    auto new_ts = version_controller_.GetCurrentTimestamp();
    if (!old_string_records.empty()) {
      pending_purge_strings.emplace_back(
          PendingPurgeOldRecords{old_string_records, new_ts});
      old_string_records.clear();
    }

    if (!old_dl_records.empty()) {
      pending_purge_dls.emplace_back(
          PendingPurgeOldRecords{old_dl_records, new_ts});
      old_dl_records.clear();
    }

    if (!outdated_records.empty()) {
      pending_purge_outdated.emplace_back(
          PendingPurgeOutDatedRecords{outdated_records, new_ts});
      outdated_records.clear();
    }

    TEST_SYNC_POINT_CALLBACK("KVEngine::backgroundCleaner::RunOnceTime",
                             &bg_work_signals_.terminating);
  }
}

}  // namespace KVDK_NAMESPACE