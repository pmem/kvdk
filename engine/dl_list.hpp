/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "collection.hpp"
#include "data_record.hpp"
#include "kvdk/types.hpp"
#include "lock_table.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "utils/sync_point.hpp"

namespace KVDK_NAMESPACE {
// Persistent doubly linked list
class DLList {
 public:
  DLList(DLRecord* header, PMEMAllocator* pmem_allocator, LockTable* lock_table)
      : header_(header),
        pmem_allocator_(pmem_allocator),
        lock_table_(lock_table) {}

  struct WriteArgs {
    WriteArgs(const StringView& _key, const StringView& _val, RecordType _type,
              RecordStatus _status, TimeStampType _ts, const SpaceEntry& _space)
        : key(_key),
          val(_val),
          type(_type),
          status(_status),
          ts(_ts),
          space(_space) {
      kvdk_assert(space.size >= DLRecord::RecordSize(_key, _val),
                  "space to write dl record too small");
    }

    WriteArgs(const WriteArgs&) = delete;

    StringView key;
    StringView val;
    RecordType type;
    RecordStatus status;
    TimeStampType ts;
    SpaceEntry space;
  };

  const DLRecord* Header() const { return header_; }

  DLRecord* Header() { return header_; }

  Status PushBack(const WriteArgs& args) {
    Status s;
    do {
      s = InsertBefore(args, header_);
    } while (s == Status::Fail);
    return s;
  }

  Status PushFront(const WriteArgs& args) {
    Status s;
    do {
      s = InsertAfter(args, header_);
    } while (s == Status::Fail);
    return s;
  }

  DLRecord* RemoveFront() {
    while (true) {
      DLRecord* front =
          pmem_allocator_->offset2addr_checked<DLRecord>(header_->next);
      if (front == header_) {
        return nullptr;
      }
      // Maybe removed by another thread
      bool success = Remove(front);
      if (success) {
        return front;
      }
    }
  }

  DLRecord* RemoveBack() {
    while (true) {
      DLRecord* back =
          pmem_allocator_->offset2addr_checked<DLRecord>(header_->prev);
      if (back == header_) {
        return nullptr;
      }
      // Maybe removed by another thread
      bool success = Remove(back);
      if (success) {
        return back;
      }
    }
  }

  Status InsertBetween(const WriteArgs& args, DLRecord* prev, DLRecord* next) {
    auto ul = acquireInsertLock(prev);
    PMemOffsetType next_offset = pmem_allocator_->addr2offset_checked(next);
    PMemOffsetType prev_offset = pmem_allocator_->addr2offset_checked(prev);
    // Check if the linkage has changed before we successfully acquire lock.
    bool check_linkage = prev->next == next_offset && next->prev == prev_offset;
    if (!check_linkage) {
      return Status::Fail;
    }

    DLRecord* new_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr_checked(args.space.offset),
        args.space.size, args.ts, args.type, args.status, kNullPMemOffset,
        prev_offset, next_offset, args.key, args.val);
    linkRecord(prev, next, new_record);

    return Status::Ok;
  }

  Status InsertAfter(const WriteArgs& args, DLRecord* prev) {
    return InsertBetween(
        args, prev, pmem_allocator_->offset2addr_checked<DLRecord>(prev->next));
  }

  Status InsertBefore(const WriteArgs& args, DLRecord* next) {
    return InsertBetween(
        args, pmem_allocator_->offset2addr_checked<DLRecord>(next->prev), next);
  }

  Status Update(const WriteArgs& args, DLRecord* current) {
    kvdk_assert(
        current != nullptr && equal_string_view(current->Key(), args.key), "");
    auto guard = acquireRecordLock(current);
    PMemOffsetType current_offset =
        pmem_allocator_->addr2offset_checked(current);
    PMemOffsetType prev_offset = current->prev;
    PMemOffsetType next_offset = current->next;
    DLRecord* prev =
        pmem_allocator_->offset2addr_checked<DLRecord>(prev_offset);
    DLRecord* next =
        pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
    if (next->prev != current_offset || prev->next != current_offset) {
      return Status::Fail;
    }
    DLRecord* new_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr_checked(args.space.offset),
        args.space.size, args.ts, args.type, args.status, current_offset,
        prev_offset, next_offset, args.key, args.val);
    linkRecord(prev, next, new_record);
    return Status::Ok;
  }

  bool Replace(DLRecord* old_record, DLRecord* new_record) {
    bool ret = Replace(old_record, new_record, pmem_allocator_, lock_table_);
    if (ret && old_record == header_) {
      header_ = new_record;
    }
    return ret;
  }

  bool Remove(DLRecord* removing_record) {
    return Remove(removing_record, pmem_allocator_, lock_table_);
  }

  static bool Replace(DLRecord* old_record, DLRecord* new_record,
                      PMEMAllocator* pmem_allocator, LockTable* lock_table) {
    auto guard = acquireRecordLock(old_record, pmem_allocator, lock_table);
    PMemOffsetType prev_offset = old_record->prev;
    PMemOffsetType next_offset = old_record->next;
    auto old_record_offset = pmem_allocator->addr2offset(old_record);
    DLRecord* prev = pmem_allocator->offset2addr_checked<DLRecord>(prev_offset);
    DLRecord* next = pmem_allocator->offset2addr_checked<DLRecord>(next_offset);
    bool on_list =
        prev != nullptr && next != nullptr && prev->next == old_record_offset;
    if (on_list) {
      if (prev_offset == old_record_offset &&
          next_offset == old_record_offset) {
        // old record is the only record (the header) in the list, so we
        // make
        // new record point to itself and break linkage of the old one for
        // recovery
        kvdk_assert((new_record->GetRecordType() & HeaderType) &&
                        (old_record->GetRecordType() & HeaderType),
                    "Non-header record shouldn't be the only record in a list");
        linkRecord(new_record, new_record, new_record, pmem_allocator);
        auto new_record_offset = pmem_allocator->addr2offset(new_record);
        old_record->PersistPrevNT(new_record_offset);
        // kvdk_assert(
        // !Skiplist::CheckRecordPrevLinkage(old_record, pmem_allocator) &&
        // !Skiplist::CheckReocrdNextLinkage(old_record, pmem_allocator),
        // "");
      } else {
        new_record->prev = prev_offset;
        pmem_persist(&new_record->prev, sizeof(PMemOffsetType));
        new_record->next = next_offset;
        pmem_persist(&new_record->next, sizeof(PMemOffsetType));
        linkRecord(prev, next, new_record, pmem_allocator);
      }
    }
    return on_list;
  }

  static bool Remove(DLRecord* removing_record, PMEMAllocator* pmem_allocator,
                     LockTable* lock_table) {
    auto guard = acquireRecordLock(removing_record, pmem_allocator, lock_table);
    PMemOffsetType removing_offset =
        pmem_allocator->addr2offset(removing_record);
    PMemOffsetType prev_offset = removing_record->prev;
    PMemOffsetType next_offset = removing_record->next;
    DLRecord* prev = pmem_allocator->offset2addr_checked<DLRecord>(prev_offset);
    DLRecord* next = pmem_allocator->offset2addr_checked<DLRecord>(next_offset);
    bool on_list =
        prev != nullptr && next != nullptr && prev->next == removing_offset;
    if (on_list) {
      // For repair in recovery due to crashes during pointers changing, we
      // should
      // first unlink deleting entry from next's prev.(It is the reverse process
      // of insertion)
      next->prev = prev_offset;
      pmem_persist(&next->prev, 8);
      TEST_SYNC_POINT("KVEngine::DLList::Remove::PersistNext'sPrev::After");
      prev->next = next_offset;
      pmem_persist(&prev->next, 8);
    }
    return on_list;
  }

  static bool CheckNextLinkage(DLRecord* record,
                               PMEMAllocator* pmem_allocator) {
    uint64_t offset = pmem_allocator->addr2offset_checked(record);
    DLRecord* next =
        pmem_allocator->offset2addr_checked<DLRecord>(record->next);

    auto check_linkage = [&]() { return next->prev == offset; };

    auto check_type = [&]() {
      return next->GetRecordType() == record->GetRecordType() ||
             (next->GetRecordType() & HeaderType) ||
             (record->GetRecordType() & HeaderType);
    };

    auto check_id = [&]() {
      auto next_id = ExtractID(next);
      auto record_id = ExtractID(record);
      return record_id == next_id;
    };

    return check_linkage() && check_type() && check_id();
  }

  static bool CheckPrevLinkage(DLRecord* record,
                               PMEMAllocator* pmem_allocator) {
    uint64_t offset = pmem_allocator->addr2offset_checked(record);
    DLRecord* prev =
        pmem_allocator->offset2addr_checked<DLRecord>(record->prev);

    auto check_linkage = [&]() { return prev->next == offset; };

    auto check_type = [&]() {
      return prev->GetRecordType() == record->GetRecordType() ||
             (prev->GetRecordType() & HeaderType) ||
             (record->GetRecordType() & HeaderType);
    };

    auto check_id = [&]() {
      auto prev_id = ExtractID(prev);
      auto record_id = ExtractID(record);
      return record_id == prev_id;
    };

    return check_linkage() && check_type() && check_id();
  }

  static bool CheckLinkage(DLRecord* record, PMEMAllocator* pmem_allocator) {
    return CheckPrevLinkage(record, pmem_allocator) &&
           CheckNextLinkage(record, pmem_allocator);
  }

  static bool ExtractID(DLRecord* record) {
    auto type = record->GetRecordType();
    if (type & HeaderType) {
      return Collection::DecodeID(record->Value());
    } else {
      return Collection::ExtractID(record->Key());
    }
  }

 private:
  // lock position to insert a new record by locking the prev DLRecord
  LockTable::ULockType acquireInsertLock(DLRecord* prev) {
    return lock_table_->AcquireLock(recordHash(prev));
  }

  // lock position of "record" to replace or unlink it by locking its prev
  // DLRecord and itself
  LockTable::GuardType acquireRecordLock(DLRecord* record) {
    return acquireRecordLock(record, pmem_allocator_, lock_table_);
  }

  // lock position of "record" to replace or unlink it by locking its prev
  // DLRecord and itself
  static LockTable::GuardType acquireRecordLock(DLRecord* record,
                                                PMEMAllocator* pmem_allocator,
                                                LockTable* lock_table) {
    while (1) {
      PMemOffsetType prev_offset = record->prev;
      PMemOffsetType next_offset = record->next;
      DLRecord* prev =
          pmem_allocator->offset2addr_checked<DLRecord>(prev_offset);
      auto guard =
          lock_table->MultiGuard({recordHash(prev), recordHash(record)});
      // Check if the linkage has changed before we successfully acquire lock.
      if (record->prev != prev_offset || record->next != next_offset) {
        continue;
      }

      return guard;
    }
  }

  void linkRecord(DLRecord* prev, DLRecord* next, DLRecord* linking_record) {
    linkRecord(prev, next, linking_record, pmem_allocator_);
  }

  static LockTable::HashType recordHash(const DLRecord* record) {
    kvdk_assert(record != nullptr, "");
    return XXH3_64bits(record, sizeof(const DLRecord*));
  }

  static void linkRecord(DLRecord* prev, DLRecord* next,
                         DLRecord* linking_record,
                         PMEMAllocator* pmem_allocator) {
    auto linking_record_offset =
        pmem_allocator->addr2offset_checked(linking_record);
    prev->PersistNextNT(linking_record_offset);
    TEST_SYNC_POINT("KVEngine::DLList::LinkDLRecord::HalfLink");
    next->PersistPrevNT(linking_record_offset);
  }

  DLRecord* header_;
  PMEMAllocator* pmem_allocator_;
  LockTable* lock_table_;
};

// Iter all valid data in a dl list
class DLListAccessIterator {
 public:
  DLListAccessIterator(DLList* dl_list, const PMEMAllocator* pmem_allocator,
                       const SnapshotImpl* snapshot, bool own_snapshot)
      : dl_list_(dl_list),
        pmem_allocator_(pmem_allocator),
        current_(nullptr),
        snapshot_(snapshot),
        own_snapshot_(own_snapshot) {}

  void Locate(DLRecord* record, bool forward) {
    kvdk_assert(record != nullptr, "");
    current_ = record;
    skipInvalidRecords(forward);
  }

  void SeekToFirst() {
    auto first = dl_list_->Header()->next;
    current_ = pmem_allocator_->offset2addr_checked<DLRecord>(first);
    skipInvalidRecords(true);
  }

  void SeekToLast() {
    auto last = dl_list_->Header()->prev;
    current_ = pmem_allocator_->offset2addr<DLRecord>(last);
    skipInvalidRecords(false);
  }

  bool Valid() const {
    return current_ != nullptr && (current_->GetRecordType() & ElemType);
  }

  virtual void Next() {
    if (!Valid()) {
      return;
    }
    current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->next);
    skipInvalidRecords(true);
  }

  void Prev() {
    if (!Valid()) {
      return;
    }
    current_ = (pmem_allocator_->offset2addr<DLRecord>(current_->prev));
    skipInvalidRecords(false);
  }

  StringView Key() const {
    if (!Valid()) return "";
    return current_->Key();
  }

  StringView Value() const {
    if (!Valid()) return "";
    return current_->Value();
  }

 private:
  DLRecord* findValidVersion(DLRecord* pmem_record) {
    DLRecord* curr = pmem_record;
    TimeStampType ts = snapshot_->GetTimestamp();
    while (curr != nullptr && curr->GetTimestamp() > ts) {
      curr = pmem_allocator_->offset2addr<DLRecord>(curr->old_version);
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

  // Move current_ to next/prev valid version data record
  void skipInvalidRecords(bool forward) {
    while (Valid()) {
      DLRecord* valid_version_record = findValidVersion(current_);
      if (valid_version_record == nullptr ||
          valid_version_record->GetRecordStatus() == RecordStatus::Outdated) {
        current_ =
            forward
                ? pmem_allocator_->offset2addr_checked<DLRecord>(current_->next)
                : pmem_allocator_->offset2addr_checked<DLRecord>(
                      current_->prev);
      } else {
        current_ = valid_version_record;
        break;
      }
    }
  }

  DLList* dl_list_;
  const PMEMAllocator* pmem_allocator_;
  DLRecord* current_;
  const SnapshotImpl* snapshot_;
  bool own_snapshot_;
};

// Iter all records in a dl list
class DLListRecordIterator {
 public:
  DLListRecordIterator(DLList* dl_list, PMEMAllocator* pmem_allocator)
      : dl_list_(dl_list),
        header_(dl_list->Header()),
        current_(header_),
        pmem_allocator_(pmem_allocator) {}

  void Next() {
    if (Valid()) {
      current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->next);
    }
  }

  void Prev() {
    if (Valid()) {
      current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->prev);
    }
  }

  bool Valid() { return current_ != header_; }

  void SeekToFirst() {
    kvdk_assert(header_ != nullptr, "");
    current_ = pmem_allocator_->offset2addr_checked<DLRecord>(header_->next);
  }

  void SeekToLast() {
    kvdk_assert(header_ != nullptr, "");
    current_ = pmem_allocator_->offset2addr_checked<DLRecord>(header_->prev);
  }

  DLRecord* Record() { return Valid() ? current_ : nullptr; }

 private:
  DLList* dl_list_;
  DLRecord* header_;
  DLRecord* current_;
  const PMEMAllocator* pmem_allocator_;
};

class DLListRebuilderHelper {
 public:
  static bool CheckAndRepairLinkage(DLRecord* record,
                                    PMEMAllocator* pmem_allocator) {
    // The next linkage is correct. If the prev linkage is correct too, the
    // record linkage is ok. If the prev linkage is not correct, it will be
    // repaired by the correct prodecessor soon, so directly return true here.
    if (DLList::CheckNextLinkage(record, pmem_allocator)) {
      return true;
    }
    // If only prev linkage is correct, then repair the next linkage
    if (DLList::CheckPrevLinkage(record, pmem_allocator)) {
      DLRecord* next =
          pmem_allocator->offset2addr_checked<DLRecord>(record->next);
      next->PersistPrevNT(pmem_allocator->addr2offset_checked(record));
      return true;
    }

    return false;
  }
};
}  // namespace KVDK_NAMESPACE