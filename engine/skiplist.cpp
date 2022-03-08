/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "skiplist.hpp"

#include <libpmem.h>

#include <algorithm>
#include <future>

#include "hash_table.hpp"
#include "kv_engine.hpp"
#include "utils/coding.hpp"
#include "utils/sync_point.hpp"

namespace KVDK_NAMESPACE {

StringView SkiplistNode::UserKey() { return Skiplist::UserKey(this); }

uint64_t SkiplistNode::SkiplistID() { return Skiplist::SkiplistID(this); }

void Skiplist::SeekNode(const StringView& key, SkiplistNode* start_node,
                        uint8_t start_height, uint8_t end_height,
                        Splice* result_splice) {
  std::vector<SkiplistNode*> to_delete;
  assert(start_node->height >= start_height && end_height >= 1);
  SkiplistNode* prev = start_node;
  PointerWithTag<SkiplistNode> next;
  for (uint8_t i = start_height; i >= end_height; i--) {
    uint64_t round = 0;
    while (1) {
      next = prev->Next(i);
      // prev is logically deleted, roll back to prev height.
      if (next.GetTag()) {
        if (i < start_height) {
          i++;
          prev = result_splice->prevs[i];
        } else if (prev != start_node) {
          // re-seek from this node for start height
          i = start_height;
          prev = start_node;
        } else {
          // this node has been deleted, so seek from header
          kvdk_assert(result_splice->seeking_list != nullptr,
                      "skiplist must be set for seek operation!");
          return SeekNode(key, result_splice->seeking_list->header(),
                          kMaxHeight, end_height, result_splice);
        }
        continue;
      }

      if (next.Null()) {
        result_splice->nexts[i] = nullptr;
        result_splice->prevs[i] = prev;
        break;
      }

      // Physically remove deleted "next" nodes from skiplist
      auto next_next = next->Next(i);
      if (next_next.GetTag()) {
        if (prev->CASNext(i, next, next_next.RawPointer())) {
          if (--next->valid_links == 0) {
            to_delete.push_back(next.RawPointer());
          }
        }
        // if prev is marked deleted before cas, cas will be failed, and prev
        // will be roll back in next round
        continue;
      }

      DLRecord* next_pmem_record = next->record;
      int cmp = compare(key, next->UserKey());
      // pmem record maybe updated before comparing string, then the compare
      // result will be invalid, so we need to do double check
      if (next->record != next_pmem_record) {
        continue;
      }

      if (cmp > 0) {
        prev = next.RawPointer();
      } else {
        result_splice->nexts[i] = next.RawPointer();
        result_splice->prevs[i] = prev;
        break;
      }
    }
  }
  if (to_delete.size() > 0) {
    result_splice->seeking_list->obsoleteNodes(to_delete);
  }
}

void Skiplist::LinkDLRecord(DLRecord* prev, DLRecord* next, DLRecord* linking,
                            PMEMAllocator* pmem_allocator) {
  uint64_t inserting_record_offset = pmem_allocator->addr2offset(linking);
  prev->next = inserting_record_offset;
  pmem_persist(&prev->next, 8);
  TEST_SYNC_POINT("KVEngine::Skiplist::InsertDLRecord::UpdatePrev");
  next->prev = inserting_record_offset;
  pmem_persist(&next->prev, 8);
}

void Skiplist::Seek(const StringView& key, Splice* result_splice) {
  result_splice->seeking_list = this;
  SeekNode(key, header_, header_->Height(), 1, result_splice);
  assert(result_splice->prevs[1] != nullptr);
  DLRecord* prev_record = result_splice->prevs[1]->record;
  DLRecord* next_record = nullptr;
  while (1) {
    next_record = pmem_allocator_->offset2addr<DLRecord>(prev_record->next);
    if (next_record == header()->record) {
      break;
    }

    if (next_record == nullptr) {
      return Seek(key, result_splice);
    }
    int cmp = compare(key, UserKey(next_record));
    // pmem record maybe updated before comparing string, then the comparing
    // result will be invalid, so we need to do double check
    if (!validateDLRecord(next_record)) {
      return Seek(key, result_splice);
    }

    if (cmp > 0) {
      prev_record = next_record;
    } else {
      break;
    }
  }
  result_splice->next_pmem_record = next_record;
  result_splice->prev_pmem_record = prev_record;
}

Status Skiplist::CheckConnection(int height) {
  SkiplistNode* cur_node = header_;
  DLRecord* cur_record = cur_node->record;
  while (true) {
    SkiplistNode* next_node = cur_node->Next(height).RawPointer();
    uint64_t next_offset = cur_record->next;
    DLRecord* next_record = pmem_allocator_->offset2addr<DLRecord>(next_offset);
    assert(next_record != nullptr);
    if (next_record == header()->record) {
      if (next_node != nullptr) {
        GlobalLogger.Error(
            "when next pmem data record is skiplist header, the "
            "next node should be nullptr\n");
        return Status::Abort;
      }
      break;
    }
    HashEntry hash_entry;
    DataEntry data_entry;
    HashEntry* entry_ptr = nullptr;
    StringView key = next_record->Key();
    Status s = hash_table_->SearchForRead(hash_table_->GetHint(key), key,
                                          next_record->entry.meta.type,
                                          &entry_ptr, &hash_entry, &data_entry);
    assert(s == Status::Ok && "search node fail!");

    if (hash_entry.GetIndexType() == HashIndexType::SkiplistNode) {
      SkiplistNode* dram_node = hash_entry.GetIndex().skiplist_node;
      if (next_node == nullptr) {
        if (dram_node->Height() >= height) {
          GlobalLogger.Error(
              "when next_node is nullptr, the dram data entry should be "
              "DLRecord or dram node's height < cur_node's height\n");
          return Status::Abort;
        }
      } else {
        if (dram_node->Height() >= height) {
          if (!(dram_node->Height() == next_node->Height() &&
                dram_node->SkiplistID() == next_node->SkiplistID() &&
                dram_node->UserKey() == next_node->UserKey())) {
            GlobalLogger.Error("incorret skiplist node info\n");
            return Status::Abort;
          }
          cur_node = next_node;
        }
      }
    }
    cur_record = next_record;
  }
  return Status::Ok;
}

bool Skiplist::lockRecordPosition(const DLRecord* record,
                                  const SpinMutex* record_key_lock,
                                  std::unique_lock<SpinMutex>* prev_record_lock,
                                  PMEMAllocator* pmem_allocator,
                                  HashTable* hash_table) {
  while (1) {
    DLRecord* prev =
        pmem_allocator->offset2addr_checked<DLRecord>(record->prev);
    DLRecord* next = pmem_allocator->offset2addr<DLRecord>(record->next);
    PMemOffsetType prev_offset = pmem_allocator->addr2offset_checked(prev);
    PMemOffsetType next_offset = pmem_allocator->addr2offset_checked(next);

    SpinMutex* prev_lock = hash_table->GetHint(prev->Key()).spin;
    if (prev_lock != record_key_lock) {
      if (!prev_lock->try_lock()) {
        return false;
      }
      *prev_record_lock =
          std::unique_lock<SpinMutex>(*prev_lock, std::adopt_lock);
    }

    // Check if the list has changed before we successfully acquire lock.
    // As the record is already locked, so we don't need to check its next
    if (record->prev != prev_offset ||
        prev->next != pmem_allocator->addr2offset(record)) {
      continue;
    }

    assert(record->prev == prev_offset);
    assert(record->next == next_offset);
    assert(next->prev == pmem_allocator->addr2offset(record));

    return true;
  }
}

bool Skiplist::lockInsertPosition(
    const StringView& inserting_key, DLRecord* prev_record,
    const SpinMutex* inserting_key_lock,
    std::unique_lock<SpinMutex>* prev_record_lock) {
  PMemOffsetType prev_offset =
      pmem_allocator_->addr2offset_checked(prev_record);
  auto prev_hint = hash_table_->GetHint(prev_record->Key());
  if (prev_hint.spin != inserting_key_lock) {
    if (!prev_hint.spin->try_lock()) {
      return false;
    }
    *prev_record_lock =
        std::unique_lock<SpinMutex>(*prev_hint.spin, std::adopt_lock);
  }

  PMemOffsetType next_offset = prev_record->next;
  DLRecord* next_record =
      pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
  // Check if the linkage has changed before we successfully acquire lock.
  auto check_linkage = [&]() { return next_record->prev == prev_offset; };
  // Check id and order as prev and next may be both freed, then inserted
  // to another position while keep linkage, before we lock them
  // For example:
  // Before lock:
  // this skip list: record1 -> "prev" -> "next" -> record2
  // After lock:
  // this skip list: "new record reuse prev" -> "new record reuse next" ->
  // record1 -> record2
  // or:
  // this skip list: record1 -> record2
  // another skip list:"new record reuse prev" -> "new record reuse next" ->
  // In this case, inserting record will be mis-inserted between "new record
  // reuse prev" and "new record reuse next"
  auto check_id = [&]() {
    return SkiplistID(next_record) == ID() && SkiplistID(prev_record) == ID();
  };

  auto check_order = [&]() {
    bool res =
        /*check next*/ (next_record == header_->record ||
                        compare(inserting_key, UserKey(next_record)) <= 0) &&
        /*check prev*/ (prev_record == header_->record ||
                        compare(inserting_key, UserKey(prev_record)) > 0);
    return res;
  };
  if (!check_linkage() || !check_id() || !check_order()) {
    prev_record_lock->unlock();
    return false;
  }
  assert(prev_record->next == next_offset);
  assert(next_record->prev == prev_offset);

  assert(prev_record == header_->record ||
         compare(Skiplist::UserKey(prev_record), inserting_key) < 0);
  assert(next_record == header_->record ||
         compare(Skiplist::UserKey(next_record), inserting_key) >= 0);

  return true;
}

Skiplist::WriteResult Skiplist::Delete(const StringView& key,
                                       const HashTable::KeyHashHint& hash_hint,
                                       TimeStampType timestamp) {
  if (IndexedByHashtable()) {
    return deleteImplWithHash(key, hash_hint, timestamp);
  } else {
    return deleteImplNoHash(key, hash_hint.spin, timestamp);
  }
}

Skiplist::WriteResult Skiplist::Set(const StringView& key,
                                    const StringView& value,
                                    const HashTable::KeyHashHint& hash_hint,
                                    TimeStampType timestamp) {
  if (IndexedByHashtable()) {
    return setImplWithHash(key, value, hash_hint, timestamp);
  } else {
    return setImplNoHash(key, value, hash_hint.spin, timestamp);
  }
}

bool Skiplist::Replace(DLRecord* old_record, DLRecord* new_record,
                       const SpinMutex* old_record_lock,
                       SkiplistNode* dram_node, PMEMAllocator* pmem_allocator,
                       HashTable* hash_table) {
  std::unique_lock<SpinMutex> prev_record_lock;
  if (!lockRecordPosition(old_record, old_record_lock, &prev_record_lock,
                          pmem_allocator, hash_table)) {
    return false;
  }
  PMemOffsetType prev_offset = old_record->prev;
  PMemOffsetType next_offset = old_record->next;
  DLRecord* prev = pmem_allocator->offset2addr_checked<DLRecord>(prev_offset);
  DLRecord* next = pmem_allocator->offset2addr_checked<DLRecord>(next_offset);

  kvdk_assert(prev->next == pmem_allocator->addr2offset(old_record),
              "Bad prev linkage in Skiplist::Replace");
  kvdk_assert(next->prev == pmem_allocator->addr2offset(old_record),
              "Bad next linkage in Skiplist::Replace");
  new_record->prev = prev_offset;
  pmem_persist(&new_record->prev, sizeof(PMemOffsetType));
  new_record->next = next_offset;
  pmem_persist(&new_record->next, sizeof(PMemOffsetType));
  Skiplist::LinkDLRecord(prev, next, new_record, pmem_allocator);
  if (dram_node != nullptr) {
    kvdk_assert(dram_node->record == old_record,
                "Dram node not belong to old record in Skiplist::Replace");
    dram_node->record = new_record;
  }
  return true;
}

bool Skiplist::Purge(DLRecord* purging_record,
                     const SpinMutex* purging_record_lock,
                     SkiplistNode* dram_node, PMEMAllocator* pmem_allocator,
                     HashTable* hash_table) {
  std::unique_lock<SpinMutex> prev_record_lock;
  if (!lockRecordPosition(purging_record, purging_record_lock,
                          &prev_record_lock, pmem_allocator, hash_table)) {
    return false;
  }

  // Modify linkage to drop deleted record
  PMemOffsetType purging_offset = pmem_allocator->addr2offset(purging_record);
  PMemOffsetType prev_offset = purging_record->prev;
  PMemOffsetType next_offset = purging_record->next;
  DLRecord* prev = pmem_allocator->offset2addr_checked<DLRecord>(prev_offset);
  DLRecord* next = pmem_allocator->offset2addr_checked<DLRecord>(next_offset);
  assert(prev->next == purging_offset);
  assert(next->prev == purging_offset);
  // For repair in recovery due to crashes during pointers changing, we should
  // first unlink deleting entry from next's prev.(It is the reverse process
  // of insertion)
  next->prev = prev_offset;
  pmem_persist(&next->prev, 8);
  TEST_SYNC_POINT("KVEngine::Skiplist::Delete::PersistNext'sPrev::After");
  prev->next = next_offset;
  pmem_persist(&prev->next, 8);
  purging_record->Destroy();

  if (dram_node) {
    dram_node->MarkAsRemoved();
  }
  return true;
}

void SortedIterator::Seek(const std::string& key) {
  assert(skiplist_);
  Splice splice(skiplist_);
  skiplist_->Seek(key, &splice);
  current_ = splice.next_pmem_record;
  while (Valid()) {
    DLRecord* valid_version_record = findValidVersion(current_);
    if (valid_version_record == nullptr ||
        valid_version_record->entry.meta.type == SortedDeleteRecord) {
      current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->next);
    } else {
      current_ = valid_version_record;
      break;
    }
  }
  while (current_->entry.meta.type == SortedDeleteRecord) {
    current_ = pmem_allocator_->offset2addr<DLRecord>(current_->next);
  }
}

void SortedIterator::SeekToFirst() {
  uint64_t first = skiplist_->header()->record->next;
  current_ = pmem_allocator_->offset2addr<DLRecord>(first);
  while (Valid()) {
    DLRecord* valid_version_record = findValidVersion(current_);
    if (valid_version_record == nullptr ||
        valid_version_record->entry.meta.type == SortedDeleteRecord) {
      current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->next);
    } else {
      current_ = valid_version_record;
      break;
    }
  }
}

void SortedIterator::SeekToLast() {
  uint64_t last = skiplist_->header()->record->prev;
  current_ = pmem_allocator_->offset2addr<DLRecord>(last);
  while (Valid()) {
    DLRecord* valid_version_record = findValidVersion(current_);
    if (valid_version_record == nullptr ||
        valid_version_record->entry.meta.type == SortedDeleteRecord) {
      current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->prev);
    } else {
      current_ = valid_version_record;
      break;
    }
  }
}

void SortedIterator::Next() {
  if (!Valid()) {
    return;
  }
  current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->next);
  while (Valid()) {
    DLRecord* valid_version_record = findValidVersion(current_);
    if (valid_version_record == nullptr ||
        valid_version_record->entry.meta.type == SortedDeleteRecord) {
      current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->next);
    } else {
      current_ = valid_version_record;
      break;
    }
  }
}

void SortedIterator::Prev() {
  if (!Valid()) {
    return;
  }
  current_ = (pmem_allocator_->offset2addr<DLRecord>(current_->prev));
  while (Valid()) {
    DLRecord* valid_version_record = findValidVersion(current_);
    if (valid_version_record == nullptr ||
        valid_version_record->entry.meta.type == SortedDeleteRecord) {
      current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->prev);
    } else {
      current_ = valid_version_record;
      break;
    }
  }
}

std::string SortedIterator::Key() {
  if (!Valid()) return "";
  return string_view_2_string(Skiplist::UserKey(current_));
}

std::string SortedIterator::Value() {
  if (!Valid()) return "";
  return string_view_2_string(current_->Value());
}

DLRecord* SortedIterator::findValidVersion(DLRecord* pmem_record) {
  DLRecord* curr = pmem_record;
  TimeStampType ts = snapshot_->GetTimestamp();
  while (curr != nullptr && curr->entry.meta.timestamp > ts) {
    curr = pmem_allocator_->offset2addr<DLRecord>(curr->older_version_offset);
    kvdk_assert(curr == nullptr || curr->Validate(),
                "Broken checkpoint: invalid older version sorted record");
    kvdk_assert(
        curr == nullptr || equal_string_view(curr->Key(), pmem_record->Key()),
        "Broken checkpoint: key of older version sorted data is "
        "not same as new "
        "version");
  }
  return curr;
}

DLRecord* SortedCollectionRebuilder::findValidVersion(
    DLRecord* pmem_record, std::vector<DLRecord*>* invalid_version_records) {
  if (!checkpoint_.Valid()) {
    return pmem_record;
  }
  DLRecord* curr = pmem_record;
  while (curr != nullptr &&
         curr->entry.meta.timestamp > checkpoint_.CheckpointTS()) {
    if (invalid_version_records) {
      invalid_version_records->push_back(curr);
    }
    curr = pmem_allocator_->offset2addr<DLRecord>(curr->older_version_offset);
    kvdk_assert(curr == nullptr || curr->Validate(),
                "Broken checkpoint: invalid older version sorted record");
    kvdk_assert(
        curr == nullptr || equal_string_view(curr->Key(), pmem_record->Key()),
        "Broken checkpoint: key of older version sorted data is "
        "not same as new "
        "version");
  }
  return curr;
}

Status SortedCollectionRebuilder::segmentBasedIndexRebuild() {
  thread_end_points_.resize(num_rebuild_threads_);
  Status s = buildStartPoints();
  if (s != Status::Ok) {
    return s;
  }
  std::vector<std::future<Status>> fs;

  auto repair_linkage_at_height_n = [&](uint32_t thread_num,
                                        uint8_t height) -> Status {
    Status s = this->thread_manager_->MaybeInitThread(access_thread);
    if (s != Status::Ok) {
      return s;
    }
    defer(this->thread_manager_->Release(access_thread));
    while (true) {
      StartPoint* start_point = getStartPoint(height);
      if (!start_point) {
        break;
      }
      if (height == 1) {
        Status s =
            rebuildIndex(start_point->node, start_point->build_hash_index);
        if (s != Status::Ok) {
          return s;
        }
      } else {
        rebuildLinkage(start_point->node, height);
      }
    }
    linkEndPoints(height);
    return Status::Ok;
  };

  for (uint8_t height = 1; height <= kMaxHeight; ++height) {
    for (uint32_t thread_num = 0; thread_num < num_rebuild_threads_;
         ++thread_num) {
      fs.push_back(std::async(repair_linkage_at_height_n, thread_num, height));
    }
    for (auto& f : fs) {
      Status s = f.get();
      if (s != Status::Ok) {
        return s;
      }
    }
    fs.clear();
    for (auto& kv : start_points_) {
      kv.second.visited = false;
    }
  }
  return Status::Ok;
}

Status SortedCollectionRebuilder::rebuildSkiplistIndex(Skiplist* skiplist) {
  Status s = thread_manager_->MaybeInitThread(access_thread);
  if (s != Status::Ok) {
    GlobalLogger.Error("too many threads repair skiplist linkage\n");
    return s;
  }
  defer(thread_manager_->Release(access_thread));
  Splice splice(skiplist);
  HashEntry hash_entry;
  for (uint8_t i = 1; i <= kMaxHeight; i++) {
    splice.prevs[i] = skiplist->header();
    splice.prev_pmem_record = skiplist->header()->record;
  }

  while (true) {
    uint64_t next_offset = splice.prev_pmem_record->next;
    DLRecord* next_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
    if (next_record == skiplist->header()->record) {
      break;
    }

    StringView internal_key = next_record->Key();
    auto hash_hint = hash_table_->GetHint(internal_key);
    while (true) {
      std::lock_guard<SpinMutex> lg(*hash_hint.spin);
      DLRecord* valid_version_record = findValidVersion(next_record, nullptr);
      if (valid_version_record == nullptr) {
        // purge invalid version record from list
        if (!Skiplist::Purge(next_record, hash_hint.spin, nullptr,
                             pmem_allocator_, hash_table_)) {
          asm volatile("pause");
          continue;
        }
      } else {
        RemoveInvalidRecords(valid_version_record);
        if (valid_version_record != next_record) {
          // repair linkage of checkpoint version
          if (!Skiplist::Replace(next_record, valid_version_record,
                                 hash_hint.spin, nullptr, pmem_allocator_,
                                 hash_table_)) {
            continue;
          }
        }

        // Rebuild dram node
        assert(valid_version_record != nullptr);
        SkiplistNode* dram_node = Skiplist::NewNodeBuild(valid_version_record);

        if (dram_node != nullptr) {
          auto height = dram_node->Height();
          for (uint8_t i = 1; i <= height; i++) {
            splice.prevs[i]->RelaxedSetNext(i, dram_node);
            dram_node->RelaxedSetNext(i, nullptr);
            splice.prevs[i] = dram_node;
          }
        }

        // Rebuild hash index
        if (skiplist->IndexedByHashtable()) {
          HashEntry* entry_ptr = nullptr;
          Status s = hash_table_->SearchForWrite(
              hash_hint, internal_key, SortedDataRecord | SortedDeleteRecord,
              &entry_ptr, &hash_entry, nullptr);
          if (s == Status::MemoryOverflow) {
            return s;
          }
          if (s == Status::Ok) {
            GlobalLogger.Error(
                "Rebuild skiplist error, hash entry of sorted data/delete "
                "records should not be inserted before repair linkage\n");
            return Status::Abort;
          } else if (s == Status::NotFound) {
            if (dram_node) {
              hash_table_->Insert(hash_hint, entry_ptr,
                                  valid_version_record->entry.meta.type,
                                  dram_node, HashIndexType::SkiplistNode);
            } else {
              hash_table_->Insert(
                  hash_hint, entry_ptr, valid_version_record->entry.meta.type,
                  valid_version_record, HashIndexType::DLRecord);
            }
          } else {
            return s;
          }
        }

        splice.prev_pmem_record = valid_version_record;
      }
      break;
    }
  }
  return Status::Ok;
}

Status SortedCollectionRebuilder::listBasedIndexRebuild() {
  std::vector<std::future<Status>> fs;
  int i = 0;
  for (auto skiplist : *skiplists_) {
    i++;
    fs.push_back(std::async(&SortedCollectionRebuilder::rebuildSkiplistIndex,
                            this, skiplist.second.get()));
    if (i % num_rebuild_threads_ == 0 || i == skiplists_->size()) {
      for (auto& f : fs) {
        Status s = f.get();
        if (s != Status::Ok) {
          return s;
        }
      }
      fs.clear();
    }
  }
}

Status SortedCollectionRebuilder::RebuildIndex() {
  defer(this->cleanInvalidRecords());
  Status s = Status::Ok;
  if (skiplists_->size() == 0) {
    return s;
  }

  if (segment_based_rebuild_) {
    s = segmentBasedIndexRebuild();
  } else {
    listBasedIndexRebuild();
  }
#ifdef DEBUG_CHECK
  for (uint8_t h = 1; h <= kMaxHeight; h++) {
    for (auto skiplist : *skiplists_) {
      if (skiplist.second->IndexedByHashtable()) {
        Status s = skiplist.second->CheckConnection(h);
        if (s != Status::Ok) {
          GlobalLogger.Info("Check skiplist connecton at height %u error\n", h);
          return s;
        }
      }
    }
  }
#endif
  return s;
}

void SortedCollectionRebuilder::linkEndPoints(int height) {
  for (SkiplistNode* node : thread_end_points_[access_thread.id]) {
    if (node->Height() < height) {
      continue;
    }
    assert(node->RelaxedNext(height).RawPointer() == nullptr);
    if (height == 1) {
      PMemOffsetType next_offset = node->record->next;
      DLRecord* next_record =
          pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
      while (next_record->entry.meta.type != SortedHeaderRecord) {
        auto iter = start_points_.find(next_offset);
        if (iter != start_points_.end()) {
          assert(iter->second.node->Height() >= height);
          node->RelaxedSetNext(height, iter->second.node);
          break;
        } else {
          next_offset = next_record->next;
          next_record =
              pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
        }
      }
    } else {
      SkiplistNode* next_node = node->RelaxedNext(height - 1).RawPointer();
      while (next_node != nullptr) {
        if (next_node->Height() >= height) {
          node->RelaxedSetNext(height, next_node);
          break;
        } else {
          next_node = next_node->RelaxedNext(height - 1).RawPointer();
        }
      }
    }
  }
  thread_end_points_[access_thread.id].clear();
}

SortedCollectionRebuilder::StartPoint* SortedCollectionRebuilder::getStartPoint(
    int height) {
  std::lock_guard<SpinMutex> kv_mux(mu_);
  for (auto& kv : start_points_) {
    if (!kv.second.visited && kv.second.node->Height() >= height - 1) {
      kv.second.visited = true;
      return &kv.second;
    }
  }
  return nullptr;
}

Status SortedCollectionRebuilder::rebuildIndex(SkiplistNode* start_node,
                                               bool build_hash_index) {
  SkiplistNode* cur_node = start_node;
  DLRecord* cur_record = cur_node->record;
  while (true) {
    PMemOffsetType next_offset = cur_record->next;
    DLRecord* next_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
    if (next_record->entry.meta.type == SortedHeaderRecord) {
      cur_node->RelaxedSetNext(1, nullptr);
      break;
    }

    if (start_points_.find(next_offset) == start_points_.end()) {
      HashEntry hash_entry;
      DataEntry data_entry;
      HashEntry* entry_ptr = nullptr;
      StringView internal_key = next_record->Key();

      auto hash_hint = hash_table_->GetHint(internal_key);
      while (true) {
        std::lock_guard<SpinMutex> lg(*hash_hint.spin);
        DLRecord* valid_version_record = findValidVersion(next_record, nullptr);
        if (valid_version_record == nullptr) {
          if (!Skiplist::Purge(next_record, hash_hint.spin, nullptr,
                               pmem_allocator_, hash_table_)) {
            continue;
          }
        } else {
          RemoveInvalidRecords(valid_version_record);
          if (valid_version_record != next_record) {
            if (!Skiplist::Replace(next_record, valid_version_record,
                                   hash_hint.spin, nullptr, pmem_allocator_,
                                   hash_table_)) {
              continue;
            }
          }

          assert(valid_version_record != nullptr);
          SkiplistNode* dram_node =
              Skiplist::NewNodeBuild(valid_version_record);
          if (dram_node != nullptr) {
            cur_node->RelaxedSetNext(1, dram_node);
            dram_node->RelaxedSetNext(1, nullptr);
            cur_node = dram_node;
          }

          if (build_hash_index) {
            Status s = hash_table_->SearchForWrite(hash_hint, internal_key,
                                                   SortedRecordType, &entry_ptr,
                                                   &hash_entry, &data_entry);
            if (s == Status::Ok) {
              GlobalLogger.Error(
                  "Rebuild skiplist error, hash entry of sorted data/delete "
                  "records should not be inserted before repair linkage\n");
              return Status::Abort;
            } else if (s == Status::NotFound) {
              if (dram_node) {
                hash_table_->Insert(hash_hint, entry_ptr,
                                    valid_version_record->entry.meta.type,
                                    dram_node, HashIndexType::SkiplistNode);
              } else {
                hash_table_->Insert(
                    hash_hint, entry_ptr, valid_version_record->entry.meta.type,
                    valid_version_record, HashIndexType::DLRecord);
              }
            } else {
              return s;
            }
          }

          cur_record = valid_version_record;
        }
        break;
      }
    } else {
      cur_node->RelaxedSetNext(1, nullptr);
      thread_end_points_[access_thread.id].insert(cur_node);
      break;
    }
  }
  return Status::Ok;
}

void SortedCollectionRebuilder::rebuildLinkage(SkiplistNode* start_node,
                                               int height) {
  while (start_node->Height() < height) {
    start_node = start_node->RelaxedNext(height - 1).RawPointer();
    if (start_node == nullptr ||
        start_points_.find(pmem_allocator_->addr2offset_checked(
            start_node->record)) != start_points_.end()) {
      return;
    }
  }
  SkiplistNode* cur_node = start_node;
  SkiplistNode* next_node = cur_node->RelaxedNext(height - 1).RawPointer();
  assert(start_node && start_node->Height() >= height);
  while (true) {
    if (next_node == nullptr ||
        start_points_.find(pmem_allocator_->addr2offset_checked(
            next_node->record)) != start_points_.end()) {
      cur_node->RelaxedSetNext(height, nullptr);
      thread_end_points_[access_thread.id].insert(cur_node);
      break;
    }

    if (next_node->Height() >= height) {
      cur_node->RelaxedSetNext(height, next_node);
      next_node->RelaxedSetNext(height, nullptr);
      cur_node = next_node;
    }
    next_node = next_node->RelaxedNext(height - 1).RawPointer();
  }
}

Status SortedCollectionRebuilder::buildStartPoints() {
  std::unordered_map<uint64_t, StartPoint> new_kvs;
  std::unordered_map<uint64_t, StartPoint>::iterator it = start_points_.begin();
  while (it != start_points_.end()) {
    DataEntry data_entry;
    HashEntry* entry_ptr = nullptr;
    HashEntry hash_entry;
    SkiplistNode* node = nullptr;
    DLRecord* cur_record = pmem_allocator_->offset2addr<DLRecord>(it->first);
    StringView internal_key = cur_record->Key();
    auto hash_hint = hash_table_->GetHint(internal_key);
    while (true) {
      std::lock_guard<SpinMutex> lg(*hash_hint.spin);
      Status s =
          hash_table_->SearchForWrite(hash_hint, internal_key, SortedRecordType,
                                      &entry_ptr, &hash_entry, &data_entry);
      if (s == Status::Ok) {
        if (hash_entry.GetIndexType() != HashIndexType::Skiplist) {
          GlobalLogger.Error(
              "Rebuild skiplist error, hash entry of sorted data/delete "
              "records should not be inserted before repair linkage\n");
          return Status::Abort;
        }
        node = hash_entry.GetIndex().skiplist->header();
        it->second.node = node;
        it->second.build_hash_index =
            hash_entry.GetIndex().skiplist->IndexedByHashtable();
        it++;
      } else if (s == Status::NotFound) {
        it = start_points_.erase(it);
        DLRecord* valid_version_record = findValidVersion(cur_record, nullptr);
        if (valid_version_record == nullptr) {
          // purge invalid version record from list
          if (!Skiplist::Purge(cur_record, hash_hint.spin, nullptr,
                               pmem_allocator_, hash_table_)) {
            continue;
          }
        } else {
          RemoveInvalidRecords(valid_version_record);
          // repair linkage of checkpoint version
          if (valid_version_record != cur_record) {
            if (!Skiplist::Replace(cur_record, valid_version_record,
                                   hash_hint.spin, nullptr, pmem_allocator_,
                                   hash_table_)) {
              continue;
            }
          }
          assert(valid_version_record != nullptr);
          node = Skiplist::NewNodeBuild(valid_version_record);

          if (node != nullptr) {
            auto iter = skiplists_->find(node->SkiplistID());
            assert(iter != skiplists_->end());
            bool index_with_hashtable = iter->second->IndexedByHashtable();
            if (index_with_hashtable) {
              hash_table_->Insert(hash_hint, entry_ptr,
                                  valid_version_record->entry.meta.type, node,
                                  HashIndexType::SkiplistNode);
            }
            new_kvs.insert(
                {pmem_allocator_->addr2offset_checked(valid_version_record),
                 {false, index_with_hashtable, node}});
          }
          // TODO: comment why not insert dlrecord hash index here
        }
      } else {
        assert(s == Status::MemoryOverflow);
        return s;
      }
      break;
    }
  }
  start_points_.insert(new_kvs.begin(), new_kvs.end());
  return Status::Ok;
}

SkiplistNode* Skiplist::NewNodeBuild(DLRecord* pmem_record) {
  SkiplistNode* dram_node = nullptr;
  auto height = Skiplist::RandomHeight();
  if (height > 0) {
    StringView user_key = UserKey(pmem_record);
    dram_node = SkiplistNode::NewNode(user_key, pmem_record, height);
    if (dram_node == nullptr) {
      GlobalLogger.Error("Memory overflow in Skiplist::NewNodeBuild\n");
    }
  }
  return dram_node;
}

std::string Skiplist::EncodeSortedCollectionValue(
    CollectionIDType id, const SortedCollectionConfigs& s_configs) {
  const size_t num_config_fields = 1;
  std::string value_str;

  AppendInt64(&value_str, id);
  AppendFixedString(&value_str, s_configs.comparator_name);
  AppendInt32(&value_str, s_configs.index_with_hashtable);

  return value_str;
}

Status Skiplist::DecodeSortedCollectionValue(
    StringView value_str, CollectionIDType& id,
    SortedCollectionConfigs& s_configs) {
  if (!FetchInt64(&value_str, &id)) {
    return Status::Abort;
  }
  if (!FetchFixedString(&value_str, &s_configs.comparator_name)) {
    return Status::Abort;
  }
  if (!FetchInt32(&value_str, (uint32_t*)&s_configs.index_with_hashtable)) {
    return Status::Abort;
  }

  return Status::Ok;
}

Status Skiplist::Get(const StringView& key, std::string* value) {
  if (!IndexedByHashtable()) {
    Splice splice(this);
    Seek(key, &splice);
    if (equal_string_view(key, UserKey(splice.next_pmem_record)) &&
        splice.next_pmem_record->entry.meta.type == SortedDataRecord) {
      value->assign(splice.next_pmem_record->Value().data(),
                    splice.next_pmem_record->Value().size());
      return Status::Ok;
    } else {
      return Status::NotFound;
    }
  } else {
    std::string internal_key = InternalKey(key);
    HashEntry hash_entry;
    HashEntry* entry_ptr = nullptr;
    DataEntry data_entry;
    bool is_found = hash_table_->SearchForRead(
                        hash_table_->GetHint(internal_key), internal_key,
                        SortedDataRecord | SortedDeleteRecord, &entry_ptr,
                        &hash_entry, &data_entry) == Status::Ok;
    if (!is_found || (hash_entry.GetRecordType() & DeleteRecordType)) {
      return Status::NotFound;
    }

    DLRecord* pmem_record;
    switch (hash_entry.GetIndexType()) {
      case HashIndexType::SkiplistNode: {
        pmem_record = hash_entry.GetIndex().skiplist_node->record;
        break;
      }
      case HashIndexType::DLRecord: {
        pmem_record = hash_entry.GetIndex().dl_record;
        break;
      }
      default: {
        GlobalLogger.Error(
            "Wrong hash index type while search sorted data in hash table\n");
        return Status::Abort;
      }
    }

    value->assign(pmem_record->Value().data(), pmem_record->Value().size());
    return Status::Ok;
  }
}

Skiplist::WriteResult Skiplist::deleteImplNoHash(
    const StringView& key, const SpinMutex* locked_key_lock,
    TimeStampType timestamp) {
  WriteResult ret;
  std::string internal_key(InternalKey(key));
  Splice splice(this);
  Seek(key, &splice);
  bool found = (splice.next_pmem_record->entry.meta.type &
                (SortedDataRecord | SortedDeleteRecord)) &&
               equal_string_view(splice.next_pmem_record->Key(), internal_key);

  if (!found) {
    return ret;
  }

  if (splice.next_pmem_record->entry.meta.type == SortedDeleteRecord) {
    return ret;
  }

  // try to write delete record
  ret.existing_record = splice.next_pmem_record;
  std::unique_lock<SpinMutex> prev_record_lock;
  if (!lockRecordPosition(ret.existing_record, locked_key_lock,
                          &prev_record_lock)) {
    ret.s = Status::Abort;
    return ret;
  }

  auto request_size = internal_key.size() + sizeof(DLRecord);
  auto space_to_write = pmem_allocator_->Allocate(request_size);
  if (space_to_write.size == 0) {
    ret.s = Status::PmemOverflow;
    return ret;
  }

  assert(ret.existing_record->entry.meta.timestamp < timestamp);
  PMemOffsetType existing_offset =
      pmem_allocator_->addr2offset_checked(ret.existing_record);
  PMemOffsetType prev_offset = ret.existing_record->prev;
  DLRecord* prev_record =
      pmem_allocator_->offset2addr_checked<DLRecord>(prev_offset);
  PMemOffsetType next_offset = ret.existing_record->next;
  DLRecord* next_record =
      pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
  DLRecord* delete_record = DLRecord::PersistDLRecord(
      pmem_allocator_->offset2addr(space_to_write.offset), space_to_write.size,
      timestamp, SortedDeleteRecord, existing_offset, prev_offset, next_offset,
      internal_key, "");
  ret.write_record = delete_record;

  assert(splice.prev_pmem_record->next == existing_offset);

  linkDLRecord(prev_record, next_record, delete_record);

  if (splice.nexts[1] && splice.nexts[1]->record == ret.existing_record) {
    splice.nexts[1]->record = delete_record;
  }
  return ret;
}

Skiplist::WriteResult Skiplist::deleteImplWithHash(
    const StringView& key, const HashTable::KeyHashHint& locked_hash_hint,
    TimeStampType timestamp) {
  WriteResult ret;
  assert(IndexedByHashtable());
  std::string internal_key(InternalKey(key));
  SpinMutex* deleting_key_lock = locked_hash_hint.spin;
  HashEntry* entry_ptr = nullptr;
  HashEntry hash_entry;
  std::unique_lock<SpinMutex> prev_record_lock;
  ret.s = hash_table_->SearchForRead(locked_hash_hint, internal_key,
                                     SortedDataRecord | SortedDeleteRecord,
                                     &entry_ptr, &hash_entry, nullptr);

  if (ret.s == Status::NotFound) {
    ret.s = Status::Ok;
    return ret;
  } else if (ret.s == Status::Ok) {
    if (hash_entry.GetRecordType() == SortedDeleteRecord) {
      return ret;
    }

    if (hash_entry.GetIndexType() == HashIndexType::SkiplistNode) {
      ret.dram_node = hash_entry.GetIndex().skiplist_node;
      ret.existing_record = ret.dram_node->record;
    } else {
      ret.dram_node = nullptr;
      assert(hash_entry.GetIndexType() == HashIndexType::DLRecord);
      ret.existing_record = hash_entry.GetIndex().dl_record;
    }
    assert(timestamp > ret.existing_record->entry.meta.timestamp);

    // Try to write delete record
    if (!lockRecordPosition(ret.existing_record, deleting_key_lock,
                            &prev_record_lock)) {
      ret.s = Status::Abort;
      return ret;
    }

    auto space_to_write =
        pmem_allocator_->Allocate(internal_key.size() + sizeof(DLRecord));
    if (space_to_write.size == 0) {
      ret.s = Status::PmemOverflow;
      return ret;
    }

    PMemOffsetType prev_offset = ret.existing_record->prev;
    DLRecord* prev_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(prev_offset);
    PMemOffsetType next_offset = ret.existing_record->next;
    DLRecord* next_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
    PMemOffsetType existing_offset =
        pmem_allocator_->addr2offset_checked(ret.existing_record);
    DLRecord* delete_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr(space_to_write.offset),
        space_to_write.size, timestamp, SortedDeleteRecord, existing_offset,
        prev_offset, next_offset, internal_key, "");
    linkDLRecord(prev_record, next_record, delete_record);
    ret.write_record = delete_record;
  } else {
    std::abort();  // never reach
  }

  // until here, new record is already inserted to list
  assert(ret.write_record != nullptr);
  if (ret.dram_node == nullptr) {
    hash_table_->Insert(locked_hash_hint, entry_ptr, SortedDeleteRecord,
                        ret.write_record, HashIndexType::DLRecord);
  } else {
    ret.dram_node->record = ret.write_record;
    hash_table_->Insert(locked_hash_hint, entry_ptr, SortedDeleteRecord,
                        ret.dram_node, HashIndexType::SkiplistNode);
  }

  return ret;
}

Skiplist::WriteResult Skiplist::setImplWithHash(
    const StringView& key, const StringView& value,
    const HashTable::KeyHashHint& locked_hash_hint, TimeStampType timestamp) {
  WriteResult ret;
  assert(IndexedByHashtable());
  std::string internal_key(InternalKey(key));
  SpinMutex* inserting_key_lock = locked_hash_hint.spin;
  HashEntry* entry_ptr = nullptr;
  HashEntry hash_entry;
  std::unique_lock<SpinMutex> prev_record_lock;
  ret.s = hash_table_->SearchForWrite(locked_hash_hint, internal_key,
                                      SortedDataRecord | SortedDeleteRecord,
                                      &entry_ptr, &hash_entry, nullptr);

  if (ret.s == Status::MemoryOverflow) {
    return ret;
  } else if (ret.s == Status::NotFound) {
    ret = setImplNoHash(key, value, locked_hash_hint.spin, timestamp);
    if (ret.s != Status::Ok) {
      return ret;
    }
  } else if (ret.s == Status::Ok) {
    if (hash_entry.GetIndexType() == HashIndexType::SkiplistNode) {
      ret.dram_node = hash_entry.GetIndex().skiplist_node;
      ret.existing_record = ret.dram_node->record;
    } else {
      ret.dram_node = nullptr;
      assert(hash_entry.GetIndexType() == HashIndexType::DLRecord);
      ret.existing_record = hash_entry.GetIndex().dl_record;
    }
    assert(timestamp > ret.existing_record->entry.meta.timestamp);

    // Try to write delete record
    if (!lockRecordPosition(ret.existing_record, locked_hash_hint.spin,
                            &prev_record_lock)) {
      ret.s = Status::Abort;
      return ret;
    }

    auto request_size = internal_key.size() + value.size() + sizeof(DLRecord);
    auto space_to_write = pmem_allocator_->Allocate(request_size);
    if (space_to_write.size == 0) {
      ret.s = Status::PmemOverflow;
      return ret;
    }

    PMemOffsetType prev_offset = ret.existing_record->prev;
    DLRecord* prev_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(prev_offset);
    PMemOffsetType next_offset = ret.existing_record->next;
    DLRecord* next_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
    PMemOffsetType existing_offset =
        pmem_allocator_->addr2offset_checked(ret.existing_record);
    DLRecord* new_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr(space_to_write.offset),
        space_to_write.size, timestamp, SortedDataRecord, existing_offset,
        prev_offset, next_offset, internal_key, value);
    ret.write_record = new_record;
    linkDLRecord(prev_record, next_record, new_record);
  } else {
    std::abort();  // never should reach
  }

  // until here, new record is already inserted to list
  assert(ret.write_record != nullptr);
  if (ret.dram_node == nullptr) {
    hash_table_->Insert(locked_hash_hint, entry_ptr, SortedDataRecord,
                        ret.write_record, HashIndexType::DLRecord);
  } else {
    ret.dram_node->record = ret.write_record;
    hash_table_->Insert(locked_hash_hint, entry_ptr, SortedDataRecord,
                        ret.dram_node, HashIndexType::SkiplistNode);
  }

  return ret;
}

Skiplist::WriteResult Skiplist::setImplNoHash(
    const StringView& key, const StringView& value,
    const SpinMutex* inserting_key_lock, TimeStampType timestamp) {
  WriteResult ret;
  std::string internal_key(InternalKey(key));
  Splice splice(this);
  Seek(key, &splice);

  bool exist = !IndexedByHashtable() /* a hash indexed skiplist call this
                                        function only if key not exist */
               && (splice.next_pmem_record->entry.meta.type &
                   (SortedDataRecord | SortedDeleteRecord)) &&
               equal_string_view(splice.next_pmem_record->Key(), internal_key);

  DLRecord* prev_record;
  DLRecord* next_record;
  std::unique_lock<SpinMutex> prev_record_lock;
  if (exist) {
    ret.existing_record = splice.next_pmem_record;
    if (splice.nexts[1] && splice.nexts[1]->record == ret.existing_record) {
      ret.dram_node = splice.nexts[1];
    }
    if (!lockRecordPosition(ret.existing_record, inserting_key_lock,
                            &prev_record_lock)) {
      ret.s = Status::Abort;
      return ret;
    }
    prev_record = pmem_allocator_->offset2addr_checked<DLRecord>(
        ret.existing_record->prev);
    next_record = pmem_allocator_->offset2addr_checked<DLRecord>(
        ret.existing_record->next);
  } else {
    ret.existing_record = nullptr;
    if (!lockInsertPosition(key, splice.prev_pmem_record, inserting_key_lock,
                            &prev_record_lock)) {
      ret.s = Status::Abort;
      return ret;
    }
    next_record = splice.next_pmem_record;
    prev_record = splice.prev_pmem_record;
  }

  auto request_size = internal_key.size() + value.size() + sizeof(DLRecord);
  auto space_to_write = pmem_allocator_->Allocate(request_size);
  if (space_to_write.size == 0) {
    ret.s = Status::PmemOverflow;
    return ret;
  }

  uint64_t prev_offset = pmem_allocator_->addr2offset_checked(prev_record);
  uint64_t next_offset = pmem_allocator_->addr2offset_checked(next_record);
  DLRecord* new_record = DLRecord::PersistDLRecord(
      pmem_allocator_->offset2addr(space_to_write.offset), space_to_write.size,
      timestamp, SortedDataRecord,
      pmem_allocator_->addr2offset(ret.existing_record), prev_offset,
      next_offset, internal_key, value);
  ret.write_record = new_record;
  // link new record to PMem
  linkDLRecord(prev_record, next_record, new_record);
  if (!exist) {
    // create dram node for new record
    ret.dram_node = Skiplist::NewNodeBuild(new_record);
    if (ret.dram_node != nullptr) {
      auto height = ret.dram_node->Height();
      for (int i = 1; i <= height; i++) {
        while (1) {
          auto now_next = splice.prevs[i]->Next(i);
          // if next has been changed or been deleted, re-compute
          if (now_next.RawPointer() == splice.nexts[i] &&
              now_next.GetTag() == 0) {
            ret.dram_node->RelaxedSetNext(i, splice.nexts[i]);
            if (splice.prevs[i]->CASNext(i, splice.nexts[i], ret.dram_node)) {
              break;
            }
          } else {
            splice.Recompute(key, i);
          }
        }
      }
    }
  } else if (ret.dram_node) {
    ret.dram_node->record = new_record;
  }
  return ret;
}

}  // namespace KVDK_NAMESPACE