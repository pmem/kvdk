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
class DLListRecordIterator;

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

    WriteArgs(WriteArgs&& args) = default;

    StringView key;
    StringView val;
    RecordType type;
    RecordStatus status;
    TimeStampType ts;
    SpaceEntry space;
  };

  const DLRecord* Header() const { return header_; }

  DLRecord* Header() { return header_; }

  Status PushBack(const WriteArgs& args);

  Status PushFront(const WriteArgs& args);

  DLRecord* RemoveFront();

  DLRecord* RemoveBack();

  Status InsertBetween(const WriteArgs& args, DLRecord* prev, DLRecord* next);

  Status InsertAfter(const WriteArgs& args, DLRecord* prev);

  Status InsertBefore(const WriteArgs& args, DLRecord* next);

  Status Update(const WriteArgs& args, DLRecord* current);

  bool Replace(DLRecord* old_record, DLRecord* new_record);

  bool Remove(DLRecord* removing_record);

  std::unique_ptr<DLListRecordIterator> GetRecordIterator();

  static bool Replace(DLRecord* old_record, DLRecord* new_record,
                      PMEMAllocator* pmem_allocator, LockTable* lock_table);

  static bool Remove(DLRecord* removing_record, PMEMAllocator* pmem_allocator,
                     LockTable* lock_table);

  static bool CheckNextLinkage(DLRecord* record, PMEMAllocator* pmem_allocator);

  static bool CheckPrevLinkage(DLRecord* record, PMEMAllocator* pmem_allocator);

  static bool CheckLinkage(DLRecord* record, PMEMAllocator* pmem_allocator) {
    return CheckPrevLinkage(record, pmem_allocator) &&
           CheckNextLinkage(record, pmem_allocator);
  }

  static bool ExtractID(DLRecord* record) {
    auto type = record->GetRecordType();
    if (type & CollectionType) {
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

// Iter valid data under a snapshot in a dl list
class DLListDataIterator {
 public:
  DLListDataIterator(DLList* dl_list, const PMEMAllocator* pmem_allocator,
                     const SnapshotImpl* snapshot)
      : dl_list_(dl_list),
        pmem_allocator_(pmem_allocator),
        current_(nullptr),
        snapshot_(snapshot) {}

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
};

// Iter all records in a dl list
class DLListRecordIterator {
 public:
  DLListRecordIterator(DLList* dl_list, PMEMAllocator* pmem_allocator)
      : dl_list_(dl_list),
        header_(dl_list->Header()),
        current_(header_),
        pmem_allocator_(pmem_allocator) {}

  void Locate(DLRecord* record) { current_ = record; }

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

  bool Valid() { return current_ && (current_->GetRecordType() & ElemType); }

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