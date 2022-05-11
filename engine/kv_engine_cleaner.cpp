/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#include "kv_engine.hpp"
#include "utils/sync_point.hpp"

namespace KVDK_NAMESPACE {

template <typename T>
T* KVEngine::updateVersionList(T* record) {
  auto old_record =
      static_cast<T*>(pmem_allocator_->offset2addr(record->old_version));
  while (old_record && old_record->entry.meta.timestamp >=
                           version_controller_.OldestSnapshotTS()) {
    old_record =
        static_cast<T*>(pmem_allocator_->offset2addr(old_record->old_version));
  }
  if (old_record && old_record->old_version != kNullPMemOffset) {
    old_record->PersistOldVersion(kNullPMemOffset);
    return static_cast<T*>(pmem_allocator_->offset2addr(record->old_version));
  }
  return nullptr;
}

SpaceEntry KVEngine::purgeOldDataRecord(void* record) {
  DataEntry* data_entry = static_cast<DataEntry*>(record);
  switch (data_entry->meta.type) {
    case SortedHeader:
    case StringDataRecord:
    case SortedElem: {
      data_entry->Destroy();
      return SpaceEntry(pmem_allocator_->addr2offset(data_entry),
                        data_entry->header.record_size);
    }
    default:
      std::abort();
  }
}

SpaceEntry KVEngine::purgeSortedRecord(SkiplistNode* dram_node,
                                       DLRecord* pmem_record) {
  auto hint = hash_table_->GetHint(static_cast<DLRecord*>(pmem_record)->Key());
  std::unique_lock<SpinMutex> ul(*hint.spin);
  // We check linkage to determine if the delete record already been
  // unlinked by updates. We only check the next linkage, as the record is
  // already been locked, its next record will not be changed.
  kvdk_assert(dram_node == nullptr || dram_node->record == pmem_record,
              "On-list old delete record of skiplist no pointed by its "
              "dram node");
  bool record_on_list = Skiplist::CheckReocrdNextLinkage(
      static_cast<DLRecord*>(pmem_record), pmem_allocator_.get());
  if (record_on_list) {
    Skiplist::Purge(static_cast<DLRecord*>(pmem_record), dram_node,
                    pmem_allocator_.get(), skiplist_locks_.get());
  }
  return SpaceEntry(pmem_allocator_->addr2offset_checked(pmem_record),
                    pmem_record->entry.header.record_size);
}

std::vector<SpaceEntry> KVEngine::purgeOutDatedRecords(
    const std::vector<std::pair<void*, PointerType>>& outdated_records) {
  std::vector<SpaceEntry> pending_free_entries;
  for (auto& record_pair : outdated_records) {
    switch (record_pair.second) {
      case PointerType::StringRecord: {
        StringRecord* record = static_cast<StringRecord*>(record_pair.first);
        auto old_record = static_cast<StringRecord*>(
            pmem_allocator_->offset2addr(record->old_version));
        while (old_record) {
          purgeOldDataRecord(old_record);
          auto old_offset = old_record->old_version;
          // pmem_allocator_->BatchFree({purgeOldDataRecord(old_record)});
          old_record = static_cast<StringRecord*>(
              pmem_allocator_->offset2addr(old_offset));
        }
        if (record->GetRecordType() == StringDataRecord) record->Destroy();
        pending_free_entries.emplace_back(
            SpaceEntry(pmem_allocator_->addr2offset(record),
                       record->entry.header.record_size));
        break;
      }
      case PointerType::SkiplistNode: {
        SkiplistNode* skiplist_node =
            static_cast<SkiplistNode*>(record_pair.first);
        DLRecord* dl_record = skiplist_node->record;
        kvdk_assert(dl_record->entry.meta.type == RecordType::SortedElemDelete,
                    "should be sorted delete record type");
        auto old_record = static_cast<DLRecord*>(
            pmem_allocator_->offset2addr(dl_record->old_version));
        while (old_record) {
          pmem_allocator_->BatchFree({purgeOldDataRecord(old_record)});
          old_record = static_cast<DLRecord*>(
              pmem_allocator_->offset2addr(old_record->old_version));
        }
        pending_free_entries.emplace_back(
            purgeSortedRecord(skiplist_node, dl_record));
        break;
      }
      case PointerType::DLRecord: {
        DLRecord* dl_record = static_cast<DLRecord*>(record_pair.first);
        auto old_record = static_cast<DLRecord*>(
            pmem_allocator_->offset2addr(dl_record->old_version));
        while (old_record) {
          pmem_allocator_->BatchFree({purgeOldDataRecord(old_record)});
          old_record = static_cast<DLRecord*>(
              pmem_allocator_->offset2addr(old_record->old_version));
        }
        pending_free_entries.emplace_back(
            purgeSortedRecord(nullptr, dl_record));
        break;
      }
      default:
        std::abort();
    }
  }
  return pending_free_entries;
}

void KVEngine::CleanOutDated() {
  auto start_ts = std::chrono::system_clock::now();
  // Iterate hash table
  auto slot_iter = hash_table_->GetSlotIterator();

  std::deque<PendingFreeSpaceEntries> pending_free_spaces;
  std::deque<std::pair<TimeStampType, List*>> outdated_lists;
  std::deque<std::pair<TimeStampType, HashList*>> outdated_hash_lists;
  std::deque<std::pair<TimeStampType, Skiplist*>> outdated_skip_lists;

  while (slot_iter.Valid()) {
    auto now = TimeUtils::millisecond_time();
    version_controller_.UpdatedOldestSnapshot();
    auto min_snapshot_ts = version_controller_.OldestSnapshotTS();

    std::vector<std::pair<void*, PointerType>> outdated_records;
    std::vector<void*> old_records;
    PendingFreeSpaceEntries space_entries;

    {  // Slot lock section
      auto slot_lock(slot_iter.AcquireSlotLock());
      auto bucket_iter = slot_iter.Begin();
      auto end_bucket_iter = slot_iter.End();
      while (bucket_iter != end_bucket_iter) {
        if (!bucket_iter->Empty()) {
          switch (bucket_iter->GetIndexType()) {
            case PointerType::StringRecord: {
              auto string_record = bucket_iter->GetIndex().string_record;
              if ((string_record->GetExpireTime() <= now ||
                   string_record->GetRecordType() ==
                       RecordType::StringDeleteRecord) &&
                  string_record->GetTimestamp() < min_snapshot_ts) {
                outdated_records.emplace_back(std::make_pair(
                    bucket_iter->GetIndex().ptr, bucket_iter->GetIndexType()));
                hash_table_->Erase(&(*bucket_iter));
              } else {
                auto outdated_record =
                    updateVersionList<StringRecord>(string_record);
                if (outdated_record) {
                  old_records.emplace_back(outdated_record);
                }
              }
              break;
            }
            case PointerType::SkiplistNode: {
              auto dl_record = bucket_iter->GetIndex().skiplist_node->record;
              if (bucket_iter->GetRecordType() ==
                      RecordType::SortedElemDelete &&
                  dl_record->entry.meta.timestamp < min_snapshot_ts) {
                outdated_records.emplace_back(std::make_pair(
                    bucket_iter->GetIndex().ptr, bucket_iter->GetIndexType()));
                hash_table_->Erase(&(*bucket_iter));
              } else {
                auto outdated_record = updateVersionList<DLRecord>(dl_record);
                if (outdated_record) {
                  old_records.emplace_back(outdated_record);
                }
              }
              break;
            }
            case PointerType::DLRecord: {
              auto dl_record = bucket_iter->GetIndex().dl_record;
              if (bucket_iter->GetRecordType() ==
                      RecordType::SortedElemDelete &&
                  dl_record->entry.meta.timestamp < min_snapshot_ts) {
                outdated_records.emplace_back(std::make_pair(
                    bucket_iter->GetIndex().ptr, bucket_iter->GetIndexType()));
                hash_table_->Erase(&(*bucket_iter));
              } else {
                auto outdated_record = updateVersionList<DLRecord>(dl_record);
                if (outdated_record) {
                  old_records.emplace_back(outdated_record);
                }
              }
              break;
            }
            case PointerType::Skiplist: {
              auto head_record =
                  bucket_iter->GetIndex().skiplist->HeaderRecord();
              if ((bucket_iter->GetRecordType() ==
                       RecordType::SortedHeaderDelete ||
                   head_record->GetExpireTime() <= now) &&
                  head_record->entry.meta.timestamp < min_snapshot_ts) {
                hash_table_->Erase(&(*bucket_iter));
                outdated_skip_lists.emplace_back(
                    std::make_pair(version_controller_.GetCurrentTimestamp(),
                                   bucket_iter->GetIndex().skiplist));
                auto outdated_record = static_cast<DLRecord*>(
                    pmem_allocator_->offset2addr(head_record->old_version));
                if (outdated_record) {
                  old_records.emplace_back(outdated_record);
                }
              } else {
                auto outdated_record = updateVersionList<DLRecord>(head_record);
                if (outdated_record) {
                  old_records.emplace_back(outdated_record);
                }
              }
              break;
            }
            case PointerType::List: {
              List* list = bucket_iter->GetIndex().list;
              if (list->GetExpireTime() <= now &&
                  list->GetTimeStamp() < min_snapshot_ts) {
                hash_table_->Erase(&(*bucket_iter));
                outdated_lists.emplace_back(std::make_pair(
                    version_controller_.GetCurrentTimestamp(), list));
              }
              break;
            }
            case PointerType::HashList: {
              HashList* hlist = bucket_iter->GetIndex().hlist;
              if (hlist->GetExpireTime() <= now &&
                  hlist->GetTimeStamp() < min_snapshot_ts) {
                outdated_hash_lists.emplace_back(std::make_pair(
                    version_controller_.GetCurrentTimestamp(), hlist));
                hash_table_->Erase(&(*bucket_iter));
              }
              break;
            }
            default:
              break;
          }
        }
        bucket_iter++;
      }
      slot_iter.Next();
    }

    if (!outdated_records.empty()) {
      auto free_entries = purgeOutDatedRecords(outdated_records);
      space_entries.entries.emplace_back();
      space_entries.entries.swap(free_entries);
    }

    if (!old_records.empty()) {
      for (auto& record : old_records) {
        space_entries.entries.emplace_back(purgeOldDataRecord(record));
      }
    }

    if (!space_entries.entries.empty()) {
      space_entries.release_time = version_controller_.GetCurrentTimestamp();
      pending_free_spaces.emplace_back(std::move(space_entries));
    }

    // Free pending space entries
    auto iter = pending_free_spaces.begin();
    while (iter != pending_free_spaces.end()) {
      if (iter->release_time < version_controller_.OldestSnapshotTS()) {
        pmem_allocator_->BatchFree(iter->entries);
        iter++;
      } else {
        pending_free_spaces.erase(pending_free_spaces.begin(), iter);
        break;
      }
    }

    if (!outdated_skip_lists.empty()) {
      auto& ts_skiplist = outdated_skip_lists.front();
      if (ts_skiplist.first < min_snapshot_ts) {
        ts_skiplist.second->Destroy();
        removeSkiplist(ts_skiplist.second->ID());
        outdated_skip_lists.pop_front();
      }
    }

    if (!outdated_lists.empty()) {
      auto& ts_list = outdated_lists.front();
      if (ts_list.first < min_snapshot_ts) {
        listDestroy(ts_list.second);
        outdated_lists.pop_front();
      }
    }

    if (!outdated_hash_lists.empty()) {
      auto& ts_hlist = outdated_hash_lists.front();
      if (ts_hlist.first < min_snapshot_ts) {
        hashListDestroy(ts_hlist.second);
        outdated_hash_lists.pop_front();
      }
    }
  }

  auto duration = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now() - start_ts);
}

}  // namespace KVDK_NAMESPACE