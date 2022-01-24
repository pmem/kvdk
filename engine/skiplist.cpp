/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <algorithm>
#include <future>

#include <libpmem.h>

#include "hash_table.hpp"
#include "kv_engine.hpp"
#include "skiplist.hpp"

namespace KVDK_NAMESPACE {

StringView SkiplistNode::UserKey() { return Skiplist::UserKey(this); }

uint64_t SkiplistNode::SkiplistID() { return Skiplist::SkiplistID(this); }

void Skiplist::SeekNode(const StringView &key, SkiplistNode *start_node,
                        uint8_t start_height, uint8_t end_height,
                        Splice *result_splice) {
  std::vector<SkiplistNode *> to_delete;
  assert(start_node->height >= start_height && end_height >= 1);
  SkiplistNode *prev = start_node;
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

      DLRecord *next_pmem_record = next->record;
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
    result_splice->seeking_list->ObsoleteNodes(to_delete);
  }
}

void Skiplist::LinkDLRecord(DLRecord *prev, DLRecord *next, DLRecord *linking,
                            PMEMAllocator *pmem_allocator) {
  uint64_t inserting_record_offset = pmem_allocator->addr2offset(linking);
  prev->next = inserting_record_offset;
  pmem_persist(&prev->next, 8);
  next->prev = inserting_record_offset;
  pmem_persist(&next->prev, 8);
}

void Skiplist::Seek(const StringView &key, Splice *result_splice) {
  result_splice->seeking_list = this;
  SeekNode(key, header_, header_->Height(), 1, result_splice);
  assert(result_splice->prevs[1] != nullptr);
  DLRecord *prev_record = result_splice->prevs[1]->record;
  DLRecord *next_record = nullptr;
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
    if (!ValidateDLRecord(next_record)) {
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
  SkiplistNode *cur_node = header_;
  DLRecord *cur_record = cur_node->record;
  while (true) {
    SkiplistNode *next_node = cur_node->Next(height).RawPointer();
    uint64_t next_offset = cur_record->next;
    DLRecord *next_record = pmem_allocator_->offset2addr<DLRecord>(next_offset);
    assert(next_record != nullptr);
    if (next_record == header()->record) {
      if (next_node != nullptr) {
        GlobalLogger.Error("when next pmem data record is skiplist header, the "
                           "next node should be nullptr\n");
        return Status::Abort;
      }
      break;
    }
    HashEntry hash_entry;
    DataEntry data_entry;
    HashEntry *entry_ptr = nullptr;
    StringView key = next_record->Key();
    Status s = hash_table_->SearchForRead(hash_table_->GetHint(key), key,
                                          next_record->entry.meta.type,
                                          &entry_ptr, &hash_entry, &data_entry);
    assert(s == Status::Ok && "search node fail!");

    if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
      SkiplistNode *dram_node = hash_entry.index.skiplist_node;
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

bool Skiplist::SearchAndLockRecordPos(
    Splice *splice, const DLRecord *searching_record,
    const SpinMutex *record_lock, std::unique_lock<SpinMutex> *prev_record_lock,
    PMEMAllocator *pmem_allocator, HashTable *hash_table, bool check_linkage) {
  while (1) {
    StringView user_key = UserKey(searching_record);
    DLRecord *prev =
        pmem_allocator->offset2addr<DLRecord>(searching_record->prev);
    DLRecord *next =
        pmem_allocator->offset2addr<DLRecord>(searching_record->next);
    assert(prev != nullptr);
    assert(next != nullptr);
    splice->prev_pmem_record = prev;
    splice->next_pmem_record = next;
    uint64_t prev_offset = pmem_allocator->addr2offset(prev);
    uint64_t next_offset = pmem_allocator->addr2offset(next);

    auto prev_hint = hash_table->GetHint(prev->Key());
    if (prev_hint.spin != record_lock) {
      if (!prev_hint.spin->try_lock()) {
        return false;
      }
      *prev_record_lock =
          std::unique_lock<SpinMutex>(*prev_hint.spin, std::adopt_lock);
    }

    if (check_linkage) {
      // Check if the list has changed before we successfully acquire lock.
      // As updating searching_record is already locked, so we don't need to
      // check its next
      if (searching_record->prev != prev_offset ||
          prev->next != pmem_allocator->addr2offset(searching_record)) {
        continue;
      }

      assert(searching_record->prev == prev_offset);
      assert(searching_record->next == next_offset);
      assert(next->prev == pmem_allocator->addr2offset(searching_record));
    }

    return true;
  }
}

bool Skiplist::searchAndLockInsertPos(
    Splice *splice, const StringView &inserting_key,
    const SpinMutex *inserting_key_lock,
    std::unique_lock<SpinMutex> *prev_record_lock) {
  while (1) {
    Seek(inserting_key, splice);
    DLRecord *prev = splice->prev_pmem_record;

    assert(prev != nullptr);
    uint64_t prev_offset = pmem_allocator_->addr2offset(prev);

    auto prev_hint = hash_table_->GetHint(prev->Key());
    if (prev_hint.spin != inserting_key_lock) {
      if (!prev_hint.spin->try_lock()) {
        return false;
      }
      *prev_record_lock =
          std::unique_lock<SpinMutex>(*prev_hint.spin, std::adopt_lock);
    }
    uint64_t next_offset = prev->next;
    DLRecord *next = pmem_allocator_->offset2addr<DLRecord>(next_offset);
    assert(next != nullptr);
    // Check if the linkage has changed before we successfully acquire lock.
    auto check_linkage = [&]() {
      return next == splice->next_pmem_record && next->prev == prev_offset;
    };
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
      return SkiplistID(next) == ID() && SkiplistID(prev) == ID();
    };

    auto check_order = [&]() {
      bool res =
          /*check next*/ (next == header_->record ||
                          compare(inserting_key, UserKey(next)) < 0) &&
          /*check prev*/ (prev == header_->record ||
                          compare(inserting_key, UserKey(prev)) > 0);
      return res;
    };
    if (!check_linkage() || !check_id() || !check_order()) {
      prev_record_lock->unlock();
      continue;
    }
    assert(prev->next == next_offset);
    assert(next->prev == prev_offset);

    assert(prev == header_->record ||
           compare(Skiplist::UserKey(prev), inserting_key) < 0);
    assert(next == header_->record ||
           compare(Skiplist::UserKey(next), inserting_key) > 0);

    return true;
  }
}

bool Skiplist::Insert(const StringView &key, const StringView &value,
                      const SpinMutex *inserting_key_lock,
                      TimeStampType timestamp, SkiplistNode **dram_node,
                      const SpaceEntry &space_to_write) {
  Splice splice(this);
  std::unique_lock<SpinMutex> prev_record_lock;
  if (!searchAndLockInsertPos(&splice, key, inserting_key_lock,
                              &prev_record_lock)) {
    return false;
  }

  std::string internal_key(InternalKey(key));
  uint64_t prev_offset = pmem_allocator_->addr2offset(splice.prev_pmem_record);
  uint64_t next_offset = pmem_allocator_->addr2offset(splice.next_pmem_record);
  DLRecord *new_record = DLRecord::PersistDLRecord(
      pmem_allocator_->offset2addr(space_to_write.offset), space_to_write.size,
      timestamp, SortedDataRecord, kNullPMemOffset, prev_offset, next_offset,
      internal_key, value);

  // link new record to PMem
  LinkDLRecord(splice.prev_pmem_record, splice.next_pmem_record, new_record);

  // create dram node for new record
  auto height = Skiplist::RandomHeight();
  if (height > 0) {
    *dram_node = SkiplistNode::NewNode(key, new_record, height);
    for (int i = 1; i <= height; i++) {
      while (1) {
        auto now_next = splice.prevs[i]->Next(i);
        // if next has been changed or been deleted, re-compute
        if (now_next.RawPointer() == splice.nexts[i] &&
            now_next.GetTag() == 0) {
          (*dram_node)->RelaxedSetNext(i, splice.nexts[i]);
          if (splice.prevs[i]->CASNext(i, splice.nexts[i], *(dram_node))) {
            break;
          }
        } else {
          splice.Recompute(key, i);
        }
      }
    }
  } else {
    *dram_node = nullptr;
  }
  return true;
}

bool Skiplist::Update(const StringView &key, const StringView &value,
                      const DLRecord *updating_record,
                      const SpinMutex *updating_record_lock,
                      TimeStampType timestamp, SkiplistNode *dram_node,
                      const SpaceEntry &space_to_write) {
  Splice splice(this);
  std::unique_lock<SpinMutex> prev_record_lock;
  if (!searchAndLockUpdatePos(&splice, updating_record, updating_record_lock,
                              &prev_record_lock)) {
    return false;
  }

  std::string internal_key(InternalKey(key));
  PMemOffsetType updated_offset =
      pmem_allocator_->addr2offset_checked(updating_record);
  PMemOffsetType prev_offset =
      pmem_allocator_->addr2offset_checked(splice.prev_pmem_record);
  PMemOffsetType next_offset =
      pmem_allocator_->addr2offset_checked(splice.next_pmem_record);
  DLRecord *new_record = DLRecord::PersistDLRecord(
      pmem_allocator_->offset2addr(space_to_write.offset), space_to_write.size,
      timestamp, SortedDataRecord, updated_offset, prev_offset, next_offset,
      internal_key, value);

  // link new record
  LinkDLRecord(splice.prev_pmem_record, splice.next_pmem_record, new_record);
  if (dram_node != nullptr) {
    dram_node->record = new_record;
  }
  return true;
}

bool Skiplist::Delete(const StringView &key, DLRecord *deleting_record,
                      const SpinMutex *deleting_record_lock,
                      TimeStampType timestamp, SkiplistNode *dram_node,
                      const SpaceEntry &space_to_write) {
  Splice splice(this);
  std::unique_lock<SpinMutex> prev_record_lock;
  if (!searchAndLockDeletePos(&splice, deleting_record, deleting_record_lock,
                              &prev_record_lock)) {
    return false;
  }

  std::string internal_key(InternalKey(key));
  PMemOffsetType prev_offset =
      pmem_allocator_->addr2offset_checked(splice.prev_pmem_record);
  PMemOffsetType next_offset =
      pmem_allocator_->addr2offset_checked(splice.next_pmem_record);
  PMemOffsetType deleted_offset =
      pmem_allocator_->addr2offset_checked(deleting_record);
  DLRecord *delete_record = DLRecord::PersistDLRecord(
      pmem_allocator_->offset2addr(space_to_write.offset), space_to_write.size,
      timestamp, SortedDeleteRecord, deleted_offset, prev_offset, next_offset,
      internal_key, "");

  assert(splice.prev_pmem_record->next == deleted_offset);
  assert(splice.next_pmem_record->prev == deleted_offset);

  // link delete record
  LinkDLRecord(splice.prev_pmem_record, splice.next_pmem_record, delete_record);
  if (dram_node != nullptr) {
    dram_node->record = delete_record;
  }
  return true;
}

bool Skiplist::Purge(DLRecord *purging_record,
                     const SpinMutex *purging_record_lock,
                     SkiplistNode *dram_node, PMEMAllocator *pmem_allocator,
                     HashTable *hash_table) {
  Splice splice(nullptr);
  std::unique_lock<SpinMutex> prev_record_lock;
  if (!SearchAndLockRecordPos(&splice, purging_record, purging_record_lock,
                              &prev_record_lock, pmem_allocator, hash_table)) {
    return false;
  }

  // Modify linkage to drop deleted record
  uint64_t purging_offset = pmem_allocator->addr2offset(purging_record);
  DLRecord *prev = splice.prev_pmem_record;
  DLRecord *next = splice.next_pmem_record;
  assert(prev->next == purging_offset);
  assert(next->prev == purging_offset);
  // For repair in recovery due to crashes during pointers changing, we should
  // first unlink deleting entry from prev's next
  prev->next = pmem_allocator->addr2offset(next);
  pmem_persist(&prev->next, 8);
  next->prev = pmem_allocator->addr2offset(prev);
  pmem_persist(&next->prev, 8);
  purging_record->Destroy();

  if (dram_node) {
    dram_node->MarkAsRemoved();
  }
  return true;
}

void SortedIterator::Seek(const std::string &key) {
  assert(skiplist_);
  Splice splice(skiplist_);
  skiplist_->Seek(key, &splice);
  current = splice.next_pmem_record;
  while (current->entry.meta.type == SortedDeleteRecord) {
    current = pmem_allocator_->offset2addr<DLRecord>(current->next);
  }
}

void SortedIterator::SeekToFirst() {
  uint64_t first = skiplist_->header()->record->next;
  current = pmem_allocator_->offset2addr<DLRecord>(first);
  while (current->entry.meta.type == SortedDeleteRecord) {
    current = pmem_allocator_->offset2addr<DLRecord>(current->next);
  }
}

void SortedIterator::SeekToLast() {
  uint64_t last = skiplist_->header()->record->prev;
  current = pmem_allocator_->offset2addr<DLRecord>(last);
  while (current->entry.meta.type == SortedDeleteRecord) {
    current = pmem_allocator_->offset2addr<DLRecord>(current->prev);
  }
}

void SortedIterator::Next() {
  if (!Valid()) {
    return;
  }
  do {
    current = pmem_allocator_->offset2addr<DLRecord>(current->next);
  } while (current->entry.meta.type == SortedDeleteRecord);
}

void SortedIterator::Prev() {
  if (!Valid()) {
    return;
  }
  do {
    current = (pmem_allocator_->offset2addr<DLRecord>(current->prev));
  } while (current->entry.meta.type == SortedDeleteRecord);
}

std::string SortedIterator::Key() {
  if (!Valid())
    return "";
  return string_view_2_string(Skiplist::UserKey(current));
}

std::string SortedIterator::Value() {
  if (!Valid())
    return "";
  return string_view_2_string(current->Value());
}

DLRecord *SortedCollectionRebuilder::findValidVersion(
    DLRecord *pmem_record, std::vector<DLRecord *> *invalid_version_records) {
  if (invalid_version_records) {
    invalid_version_records->clear();
  }
  if (!checkpoint_.Valid()) {
    return pmem_record;
  }
  DLRecord *curr = pmem_record;
  while (curr != nullptr &&
         curr->entry.meta.timestamp > checkpoint_.CheckpointTS()) {
    if (invalid_version_records) {
      invalid_version_records->push_back(curr);
    }
    curr = pmem_allocator_->offset2addr<DLRecord>(curr->older_version_offset);
    kvdk_assert(curr == nullptr || curr->Validate(),
                "Broken checkpoint: invalid older version sorted record");
    kvdk_assert(curr == nullptr ||
                    equal_string_view(curr->Key(), pmem_record->Key()),
                "Broken checkpoint: key of older version sorted data is "
                "not same as new "
                "version");
  }
  return curr;
}

Status SortedCollectionRebuilder::parallelRepairSkiplistLinkage() {
  thread_cache_node_.resize(num_rebuild_threads_);
  Status s = updateEntriesOffset();
  if (s != Status::Ok) {
    return s;
  }
  std::vector<std::future<Status>> fs;

  for (uint8_t h = 1; h <= kMaxHeight; ++h) {
    for (uint32_t j = 0; j < num_rebuild_threads_; ++j) {
      fs.push_back(std::async(std::launch::async, [j, h, this]() -> Status {
        while (true) {
          SkiplistNode *cur_node = getSortedOffset(h);
          if (!cur_node) {
            break;
          }
          if (h == 1) {
            Status s = dealWithFirstHeight(j, cur_node);
            if (s != Status::Ok) {
              return s;
            }
          } else {
            dealWithOtherHeight(j, cur_node, h);
          }
        }
        linkedNode(j, h);
        return Status::Ok;
      }));
    }
    for (auto &f : fs) {
      Status s = f.get();
      if (s != Status::Ok) {
        return s;
      }
    }
    fs.clear();
    for (auto &kv : record_offsets_) {
      kv.second.visited = false;
    }
  }
  return Status::Ok;
}

Status SortedCollectionRebuilder::repairSkiplistLinkage(Skiplist *skiplist) {
  Splice splice(skiplist);
  HashEntry hash_entry;
  for (uint8_t i = 1; i <= kMaxHeight; i++) {
    splice.prevs[i] = skiplist->header();
    splice.prev_pmem_record = skiplist->header()->record;
  }
  std::vector<DLRecord *> invalid_records;

  while (1) {
    HashEntry *entry_ptr = nullptr;
    uint64_t next_offset = splice.prev_pmem_record->next;
    DLRecord *next_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
    if (next_record == skiplist->header()->record) {
      break;
    }

    StringView internal_key = next_record->Key();
    StringView user_key = CollectionUtils::ExtractUserKey(internal_key);
    auto hash_hint = hash_table_->GetHint(internal_key);

    std::lock_guard<SpinMutex> lg(*hash_hint.spin);
    Status s = hash_table_->SearchForRead(hash_hint, internal_key,
                                          SortedDataRecord | SortedDeleteRecord,
                                          &entry_ptr, &hash_entry, nullptr);
    if (s != Status::Ok) {
      GlobalLogger.Error("Rebuild skiplist error, hash entry should be "
                         "insert first before repair linkage");
      return Status::Abort;
    }
    DLRecord *valid_version_record =
        findValidVersion(next_record, &invalid_records);
    if (valid_version_record == nullptr) {
      // purge invalid version record from list
      while (1) {
        if (!Skiplist::Purge(next_record, hash_hint.spin, nullptr,
                             pmem_allocator_, hash_table_)) {
          asm volatile("pause");
          continue;
        }
        break;
      }
      entry_ptr->Clear();
      batchPurgeAndFree(invalid_records);
      continue;
    } else if (valid_version_record != next_record) {
      // repair linkage of checkpoint version
      while (1) {
        Splice tmp_splice(nullptr);
        std::unique_lock<SpinMutex> prev_record_lock;
        if (!Skiplist::SearchAndLockRecordPos(
                &tmp_splice, next_record, hash_hint.spin, &prev_record_lock,
                pmem_allocator_, hash_table_, false)) {
          continue;
        }
        kvdk_assert(tmp_splice.prev_pmem_record == splice.prev_pmem_record,
                    "Broken checkpoint: skiplist rebuild order corrupted");
        valid_version_record->prev =
            pmem_allocator_->addr2offset(tmp_splice.prev_pmem_record);
        pmem_persist(&valid_version_record->prev, sizeof(PMemOffsetType));
        valid_version_record->next =
            pmem_allocator_->addr2offset(tmp_splice.next_pmem_record);
        pmem_persist(&valid_version_record->next, sizeof(PMemOffsetType));
        Skiplist::LinkDLRecord(tmp_splice.prev_pmem_record,
                               tmp_splice.next_pmem_record,
                               valid_version_record, pmem_allocator_);
        break;
      }
      batchPurgeAndFree(invalid_records);
    }

    assert(valid_version_record != nullptr);
    kvdk_assert(hash_entry.header.offset_type == HashOffsetType::DLRecord,
                "wrong hash offset type in repair skiplist linkage");
    entry_ptr->header.data_type = valid_version_record->entry.meta.type;
    auto height = Skiplist::RandomHeight();
    if (height > 0) {
      SkiplistNode *dram_node =
          SkiplistNode::NewNode(user_key, valid_version_record, height);
      if (dram_node == nullptr) {
        GlobalLogger.Error("Memory overflow in repair skiplist linkage\n");
        return Status::MemoryOverflow;
      }
      entry_ptr->index.skiplist_node = dram_node;
      entry_ptr->header.offset_type = HashOffsetType::SkiplistNode;
      for (uint8_t i = 1; i <= height; i++) {
        splice.prevs[i]->RelaxedSetNext(i, dram_node);
        dram_node->RelaxedSetNext(i, nullptr);
        splice.prevs[i] = dram_node;
      }
    } else {
      entry_ptr->index.dl_record = valid_version_record;
    }
    splice.prev_pmem_record = valid_version_record;
  }
  return Status::Ok;
}

Status SortedCollectionRebuilder::RebuildLinkage(
    const std::vector<std::shared_ptr<Skiplist>> &skiplists) {
  Status s = Status::Ok;
  if (skiplists.size() == 0) {
    return s;
  }

  if (opt_parallel_rebuild_) {
    s = parallelRepairSkiplistLinkage();
  } else {
    std::vector<std::future<Status>> fs;
    for (auto s : skiplists) {
      fs.push_back(std::async(&SortedCollectionRebuilder::repairSkiplistLinkage,
                              this, s.get()));
    }
    for (auto &f : fs) {
      s = f.get();
      if (s != Status::Ok) {
        return s;
      }
    }
  }
#ifdef DEBUG_CHECK
  for (uint8_t h = 1; h <= kMaxHeight; h++) {
    for (auto skiplist : skiplists) {
      Status s = skiplist->CheckConnection(h);
      if (s != Status::Ok) {
        GlobalLogger.Info("Check skiplist connecton at height %u error\n", h);
        return s;
      }
    }
  }
#endif
  return s;
}

void SortedCollectionRebuilder::linkedNode(uint64_t thread_id, int height) {
  for (auto v : thread_cache_node_[thread_id]) {
    if (v->Height() < height) {
      continue;
    }
    uint64_t next_offset = v->record->next;
    DLRecord *next_record = pmem_allocator_->offset2addr<DLRecord>(next_offset);
    assert(next_record != nullptr);
    // TODO jiayu: maybe cache data entry
    if (next_record->entry.meta.type != SortedHeaderRecord &&
        v->Next(height) == nullptr) {
      SkiplistNode *next_node = nullptr;
      // if height == 1, need to scan pmem data_entry
      if (height == 1) {
        while (next_record->entry.meta.type != SortedHeaderRecord) {
          HashEntry hash_entry;
          DataEntry data_entry;
          HashEntry *entry_ptr = nullptr;
          StringView key = next_record->Key();
          Status s = hash_table_->SearchForRead(hash_table_->GetHint(key), key,
                                                SortedRecordType, &entry_ptr,
                                                &hash_entry, &data_entry);
          assert(s == Status::Ok &&
                 "It should be in hash_table when reseting entries_offset map");
          next_offset = next_record->next;
          next_record = pmem_allocator_->offset2addr<DLRecord>(next_offset);

          if (hash_entry.header.offset_type == HashOffsetType::Skiplist) {
            next_node = hash_entry.index.skiplist->header();
          } else if (hash_entry.header.offset_type ==
                     HashOffsetType::SkiplistNode) {
            next_node = hash_entry.index.skiplist_node;
          } else {
            continue;
          }
          if (next_node->Height() >= height) {
            break;
          }
        }
      } else {
        SkiplistNode *pnode = v->Next(height - 1).RawPointer();
        while (pnode) {
          if (pnode->Height() >= height) {
            next_node = pnode;
            break;
          }
          pnode = pnode->Next(height - 1).RawPointer();
        }
      }
      if (next_node) {
        v->RelaxedSetNext(height, next_node);
      }
    }
  }
  thread_cache_node_[thread_id].clear();
}

SkiplistNode *SortedCollectionRebuilder::getSortedOffset(int height) {
  std::lock_guard<SpinMutex> kv_mux(map_mu_);
  for (auto &kv : record_offsets_) {
    if (!kv.second.visited && kv.second.node->Height() >= height - 1) {
      kv.second.visited = true;
      return kv.second.node;
    }
  }
  return nullptr;
}

Status SortedCollectionRebuilder::dealWithFirstHeight(uint64_t thread_id,
                                                      SkiplistNode *cur_node) {
  DLRecord *visiting_record = cur_node->record;
  std::vector<DLRecord *> invalid_records;
  while (true) {
    uint64_t next_offset = visiting_record->next;
    DLRecord *next_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
    if (next_record->entry.meta.type == SortedHeaderRecord) {
      cur_node->RelaxedSetNext(1, nullptr);
      break;
    }
    // continue to build connention
    if (record_offsets_.find(next_offset) == record_offsets_.end()) {
      HashEntry hash_entry;
      DataEntry data_entry;
      HashEntry *entry_ptr = nullptr;
      StringView internal_key = next_record->Key();
      StringView user_key = CollectionUtils::ExtractUserKey(internal_key);

      auto hash_hint = hash_table_->GetHint(internal_key);

      std::lock_guard<SpinMutex> lg(*hash_hint.spin);
      Status s =
          hash_table_->SearchForRead(hash_hint, internal_key, SortedRecordType,
                                     &entry_ptr, &hash_entry, &data_entry);
      if (s != Status::Ok) {
        GlobalLogger.Error(
            "Parallel rebuild skiplist error, hash entry should be "
            "insert first before repair linkage");
        return Status::Abort;
      }
      assert(entry_ptr->header.offset_type == HashOffsetType::DLRecord);
      // TODO continue
      DLRecord *valid_version_record =
          findValidVersion(next_record, &invalid_records);
      if (valid_version_record == nullptr) {
        // purge invalid version record from list
        while (1) {
          if (!Skiplist::Purge(next_record, hash_hint.spin, nullptr,
                               pmem_allocator_, hash_table_)) {
            asm volatile("pause");
            continue;
          }
          break;
        }
        entry_ptr->Clear();
        batchPurgeAndFree(invalid_records);
        continue;
      } else if (valid_version_record != next_record) {
        // repair linkage of checkpoint version
        while (1) {
          Splice tmp_splice(nullptr);
          std::unique_lock<SpinMutex> prev_record_lock;
          if (!Skiplist::SearchAndLockRecordPos(
                  &tmp_splice, next_record, hash_hint.spin, &prev_record_lock,
                  pmem_allocator_, hash_table_, false)) {
            continue;
          }
          kvdk_assert(tmp_splice.prev_pmem_record == visiting_record,
                      "Broken checkpoint: skiplist rebuild order corrupted");
          valid_version_record->prev =
              pmem_allocator_->addr2offset(tmp_splice.prev_pmem_record);
          pmem_persist(&valid_version_record->prev, sizeof(PMemOffsetType));
          valid_version_record->next =
              pmem_allocator_->addr2offset(tmp_splice.next_pmem_record);
          pmem_persist(&valid_version_record->next, sizeof(PMemOffsetType));
          Skiplist::LinkDLRecord(tmp_splice.prev_pmem_record,
                                 tmp_splice.next_pmem_record,
                                 valid_version_record, pmem_allocator_);
          break;
        }
        batchPurgeAndFree(invalid_records);
      }

      assert(valid_version_record != nullptr);
      kvdk_assert(hash_entry.header.offset_type == HashOffsetType::DLRecord,
                  "wrong hash offset type in repair skiplist linkage");
      entry_ptr->header.data_type = valid_version_record->entry.meta.type;
      auto height = Skiplist::RandomHeight();
      if (height > 0) {
        SkiplistNode *dram_node =
            SkiplistNode::NewNode(user_key, valid_version_record, height);
        if (dram_node == nullptr) {
          GlobalLogger.Error("Memory overflow in repair skiplist linkage\n");
          return Status::MemoryOverflow;
        }
        entry_ptr->index.skiplist_node = dram_node;
        entry_ptr->header.offset_type = HashOffsetType::SkiplistNode;
        cur_node->RelaxedSetNext(1, dram_node);
        dram_node->RelaxedSetNext(1, nullptr);
        cur_node = dram_node;
      } else {
        entry_ptr->index.dl_record = valid_version_record;
      }
      visiting_record = valid_version_record;
    } else {
      cur_node->RelaxedSetNext(1, nullptr);
      thread_cache_node_[thread_id].insert(cur_node);
      break;
    }
  }
  return Status::Ok;
}

void SortedCollectionRebuilder::dealWithOtherHeight(uint64_t thread_id,
                                                    SkiplistNode *cur_node,
                                                    int height) {
  SkiplistNode *visited_node = cur_node;
  bool first_visited = true;
  while (true) {
    if (visited_node->Height() >= height) {
      if (first_visited) {
        cur_node = visited_node;
        first_visited = false;
      }
      if (cur_node != visited_node) {
        cur_node->RelaxedSetNext(height, visited_node);
        visited_node->RelaxedSetNext(height, nullptr);
        cur_node = visited_node;
      }
    }

    SkiplistNode *next_node =
        visited_node->Height() >= height
            ? visited_node->Next(height - 1).RawPointer()
            : visited_node->Next(visited_node->Height()).RawPointer();
    if (next_node == nullptr) {
      if (cur_node->Height() >= height) {
        cur_node->RelaxedSetNext(height, nullptr);
        thread_cache_node_[thread_id].insert(cur_node);
      }
      break;
    }
    // continue to find next
    uint64_t next_offset = pmem_allocator_->addr2offset(next_node->record);
    if (record_offsets_.find(next_offset) == record_offsets_.end()) {
      visited_node = next_node;
    } else {
      if (cur_node->Height() >= height) {
        cur_node->RelaxedSetNext(height, nullptr);
        thread_cache_node_[thread_id].insert(cur_node);
      }
      break;
    }
  }
}

// TODO: jiayu modify it offset
Status SortedCollectionRebuilder::updateEntriesOffset() {
  std::unordered_map<uint64_t, SkiplistNodeInfo> new_kvs;
  std::unordered_map<uint64_t, SkiplistNodeInfo>::iterator it =
      record_offsets_.begin();
  while (it != record_offsets_.end()) {
    DataEntry data_entry;
    HashEntry *entry_ptr = nullptr;
    HashEntry hash_entry;
    SkiplistNode *node = nullptr;
    DLRecord *cur_record = pmem_allocator_->offset2addr<DLRecord>(it->first);
    StringView internal_key = cur_record->Key();
    auto hash_hint = hash_table_->GetHint(internal_key);
    std::lock_guard<SpinMutex> lg(*hash_hint.spin);
    Status s =
        hash_table_->SearchForRead(hash_hint, internal_key, SortedRecordType,
                                   &entry_ptr, &hash_entry, &data_entry);
    if (s == Status::NotFound) {
      it = record_offsets_.erase(it);
      continue;
    }

    if (s != Status::Ok) {
      return Status::Abort;
    }

    if (hash_entry.header.offset_type == HashOffsetType::Skiplist) {
      node = hash_entry.index.skiplist->header();
      it->second.node = node;
      it++;
    } else {
      kvdk_assert(hash_entry.header.offset_type == HashOffsetType::DLRecord,
                  "wrong hash offset type in repair skiplist linkage");
      it = record_offsets_.erase(it);
      cur_record = hash_entry.index.dl_record;
      std::vector<DLRecord *> invalid_version_records;
      DLRecord *valid_version_record = findValidVersion(
          hash_entry.index.dl_record, &invalid_version_records);
      if (valid_version_record == nullptr) {
        // purge invalid version record from list
        while (1) {
          if (!Skiplist::Purge(hash_entry.index.dl_record, hash_hint.spin,
                               nullptr, pmem_allocator_, hash_table_)) {
            asm volatile("pause");
            continue;
          }
          break;
        }
        entry_ptr->Clear();
        batchPurgeAndFree(invalid_version_records);
        continue;
      } else if (valid_version_record != hash_entry.index.dl_record) {
        // repair linkage of checkpoint version
        while (1) {
          Splice tmp_splice(nullptr);
          std::unique_lock<SpinMutex> prev_record_lock;
          if (!Skiplist::SearchAndLockRecordPos(
                  &tmp_splice, hash_entry.index.dl_record, hash_hint.spin,
                  &prev_record_lock, pmem_allocator_, hash_table_, false)) {
            continue;
          }
          valid_version_record->prev =
              pmem_allocator_->addr2offset(tmp_splice.prev_pmem_record);
          pmem_persist(&valid_version_record->prev, sizeof(PMemOffsetType));
          valid_version_record->next =
              pmem_allocator_->addr2offset(tmp_splice.next_pmem_record);
          pmem_persist(&valid_version_record->next, sizeof(PMemOffsetType));
          Skiplist::LinkDLRecord(tmp_splice.prev_pmem_record,
                                 tmp_splice.next_pmem_record,
                                 valid_version_record, pmem_allocator_);
          break;
        }
        batchPurgeAndFree(invalid_version_records);
      }

      assert(valid_version_record != nullptr);
      entry_ptr->header.data_type = valid_version_record->entry.meta.type;
      auto height = Skiplist::RandomHeight();
      if (height > 0) {
        StringView user_key = CollectionUtils::ExtractUserKey(internal_key);
        node = SkiplistNode::NewNode(user_key, valid_version_record, height);
        if (node == nullptr) {
          GlobalLogger.Error("Memory overflow in repair skiplist linkage\n");
          return Status::MemoryOverflow;
        }
        entry_ptr->index.skiplist_node = node;
        entry_ptr->header.offset_type = HashOffsetType::SkiplistNode;
        new_kvs.insert({pmem_allocator_->addr2offset(valid_version_record),
                        {false, node}});
      } else {
        entry_ptr->index.dl_record = valid_version_record;
        continue;
      }
    }
    assert(node && "should be not empty!");
  }
  record_offsets_.insert(new_kvs.begin(), new_kvs.end());
  return Status::Ok;
}

} // namespace KVDK_NAMESPACE