
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kv_engine.hpp"
#include "utils/sync_point.hpp"

namespace KVDK_NAMESPACE {

// Producer: Similar with `Producer-Cosumer` thread model.
void KVEngine::CleanOutDated() {
  auto start_ts = std::chrono::system_clock::now();
  // Iterate hash table
  auto slot_iter = hash_table_->GetSlotIterator();
  int64_t total_num = 0;
  int64_t clean_num = 0;
  int64_t collection_num = 0;

  std::vector<std::pair<void*, PointerType>> outdated_records;
  std::deque<OutdatedCollection> outdated_collections;

  while (slot_iter.Valid()) {
    auto now = TimeUtils::millisecond_time();
    auto release_time = version_controller_.GetCurrentTimestamp();

    version_controller_.UpdatedOldestSnapshot();

    {  // Slot lock section
      auto slot_lock(slot_iter.AcquireSlotLock());
      auto bucket_iter = slot_iter.Begin();
      auto end_bucket_iter = slot_iter.End();
      while (bucket_iter != end_bucket_iter) {
        if (!bucket_iter->Empty()) {
          auto is_expired = bucket_iter->IsExpiredStatus();
          total_num++;
          switch (bucket_iter->GetIndexType()) {
            case PointerType::StringRecord: {
              if (!is_expired &&
                  bucket_iter->GetIndex().string_record->GetExpireTime() <=
                      now) {
                hash_table_->UpdateEntryStatus(&(*bucket_iter),
                                               KeyStatus::Expired);
              }
              if (bucket_iter->IsExpiredStatus() &&
                  bucket_iter->GetIndex().string_record->GetTimestamp() <
                      version_controller_.OldestSnapshotTS()) {
                outdated_records.emplace_back(std::make_pair(
                    bucket_iter->GetIndex().ptr, PointerType::StringRecord));
                clean_num++;
                hash_table_->Erase(&(*bucket_iter));
              }
              break;
            }
            case PointerType::SkiplistNode: {
              if (bucket_iter->GetRecordType() ==
                      RecordType::SortedElemDelete &&
                  bucket_iter->GetIndex().skiplist_node->GetTimestamp() <
                      version_controller_.OldestSnapshotTS()) {
                printf("111\n");
                outdated_records.emplace_back(std::make_pair(
                    bucket_iter->GetIndex().ptr, PointerType::SkiplistNode));
                clean_num++;
                hash_table_->Erase(&(*bucket_iter));
              }
              break;
            }
            case PointerType::DLRecord: {
              if (bucket_iter->GetRecordType() ==
                      RecordType::SortedElemDelete &&
                  bucket_iter->GetIndex().dl_record->GetTimestamp() <
                      version_controller_.OldestSnapshotTS()) {
                printf("222\n");
                clean_num++;
                outdated_records.emplace_back(std::make_pair(
                    bucket_iter->GetIndex().ptr, PointerType::DLRecord));
                hash_table_->Erase(&(*bucket_iter));
              }
              break;
            }
            case PointerType::Skiplist: {
              if (!is_expired &&
                  bucket_iter->GetIndex().skiplist->HasExpired()) {
                hash_table_->UpdateEntryStatus(&(*bucket_iter),
                                               KeyStatus::Expired);
              }
              if (bucket_iter->IsExpiredStatus() &&
                  bucket_iter->GetIndex().skiplist->GetTimestamp() <
                      version_controller_.OldestSnapshotTS()) {
                printf("333\n");
                outdated_collections.emplace_back(
                    (Collection*)bucket_iter->GetIndex().ptr,
                    PointerType::Skiplist, release_time);
                collection_num++;
                hash_table_->Erase(&(*bucket_iter));
              }
              break;
            }
            case PointerType::List: {
              if (!is_expired &&
                  bucket_iter->GetIndex().list->GetExpireTime() <= now) {
                hash_table_->UpdateEntryStatus(&(*bucket_iter),
                                               KeyStatus::Expired);
              }
              if (bucket_iter->IsExpiredStatus() &&
                  bucket_iter->GetIndex().list->GetTimestamp() <
                      version_controller_.OldestSnapshotTS()) {
                outdated_collections.emplace_back(
                    (Collection*)bucket_iter->GetIndex().ptr, PointerType::List,
                    release_time);
                collection_num++;
                hash_table_->Erase(&(*bucket_iter));
              }
              break;
            }
            case PointerType::HashList: {
              if (!is_expired &&
                  bucket_iter->GetIndex().hlist->GetExpireTime() <= now) {
                hash_table_->UpdateEntryStatus(&(*bucket_iter),
                                               KeyStatus::Expired);
              }
              if (bucket_iter->IsExpiredStatus() &&
                  bucket_iter->GetIndex().hlist->GetTimestamp() <
                      version_controller_.OldestSnapshotTS()) {
                outdated_collections.emplace_back(
                    (Collection*)bucket_iter->GetIndex().ptr,
                    PointerType::HashList, release_time);
                collection_num++;
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

    if (outdated_records.size() >= kMaxCachedOldRecords) {
      printf("444444444\n");
      std::vector<std::pair<void*, PointerType>> tmp_records;
      outdated_records.swap(tmp_records);
      // Could purge for expired records.
      old_records_cleaner_.PushToGlobal(tmp_records);
    }

    if (!outdated_collections.empty()) {
      printf("5555555555\n");
      std::deque<OutdatedCollection> tmp_collections;
      outdated_collections.swap(tmp_collections);
      old_records_cleaner_.PushToGlobal(std::move(tmp_collections));
    }
  }

  if (!outdated_records.empty()) {
    std::vector<std::pair<void*, PointerType>> tmp_records;
    outdated_records.swap(tmp_records);
    // Could purge for expired records.
    old_records_cleaner_.PushToGlobal(tmp_records);
  }

  auto duration = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now() - start_ts);

  std::cout << "total num: " << total_num << " clean num: " << clean_num
            << "collection num: " << collection_num
            << " cost time: " << duration.count() << "s\n";
}

}  // namespace KVDK_NAMESPACE