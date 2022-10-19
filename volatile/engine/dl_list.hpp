/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#pragma once

#include "allocator.hpp"
#include "collection.hpp"
#include "data_record.hpp"
#include "kvdk/volatile/types.hpp"
#include "lock_table.hpp"
#include "utils/sync_point.hpp"
#include "version/version_controller.hpp"

namespace KVDK_NAMESPACE {
class DLListRecordIterator;

// Persistent doubly linked list
class DLList {
 public:
  DLList(DLRecord* header, Allocator* kv_allocator, LockTable* lock_table)
      : header_(header), kv_allocator_(kv_allocator), lock_table_(lock_table) {}

  struct WriteArgs {
    WriteArgs(const StringView& _key, const StringView& _val, RecordType _type,
              RecordStatus _status, TimestampType _ts, const SpaceEntry& _space)
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

    WriteArgs(WriteArgs&& args) = delete;

    StringView key;
    StringView val;
    RecordType type;
    RecordStatus status;
    TimestampType ts;
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
                      Allocator* kv_allocator, LockTable* lock_table);

  static bool Remove(DLRecord* removing_record, Allocator* kv_allocator,
                     LockTable* lock_table);

 private:
  // lock position to insert a new record by locking the prev DLRecord
  LockTable::ULockType acquireInsertLock(DLRecord* prev) {
    return lock_table_->AcquireLock(recordHash(prev));
  }

  // lock position of "record" to replace or unlink it by locking its prev
  // DLRecord and itself
  LockTable::MultiGuardType acquireRecordLock(DLRecord* record) {
    return acquireRecordLock(record, kv_allocator_, lock_table_);
  }

  // lock position of "record" to replace or unlink it by locking its prev
  // DLRecord and itself
  static LockTable::MultiGuardType acquireRecordLock(DLRecord* record,
                                                     Allocator* kv_allocator,
                                                     LockTable* lock_table) {
    while (1) {
      MemoryOffsetType prev_offset = record->prev;
      MemoryOffsetType next_offset = record->next;
      DLRecord* prev = kv_allocator->offset2addr_checked<DLRecord>(prev_offset);
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
    linkRecord(prev, next, linking_record, kv_allocator_);
  }

  static LockTable::HashValueType recordHash(const DLRecord* record) {
    kvdk_assert(record != nullptr, "");
    return XXH3_64bits(record, sizeof(const DLRecord*));
  }

  static void linkRecord(DLRecord* prev, DLRecord* next,
                         DLRecord* linking_record, Allocator* kv_allocator) {
    auto linking_record_offset =
        kv_allocator->addr2offset_checked(linking_record);
    prev->PersistNextNT(linking_record_offset);
    TEST_SYNC_POINT("KVEngine::DLList::LinkDLRecord::HalfLink");
    next->PersistPrevNT(linking_record_offset);
  }

  DLRecord* header_;
  Allocator* kv_allocator_;
  LockTable* lock_table_;
};

// Iter valid data under a snapshot in a dl list
class DLListDataIterator {
 public:
  DLListDataIterator(DLList* dl_list, const Allocator* kv_allocator,
                     const SnapshotImpl* snapshot)
      : dl_list_(dl_list),
        kv_allocator_(kv_allocator),
        current_(nullptr),
        snapshot_(snapshot) {}

  void Locate(DLRecord* record, bool forward) {
    kvdk_assert(record != nullptr, "");
    current_ = record;
    skipInvalidRecords(forward);
  }

  void SeekToFirst() {
    auto first = dl_list_->Header()->next;
    current_ = kv_allocator_->offset2addr_checked<DLRecord>(first);
    skipInvalidRecords(true);
  }

  void SeekToLast() {
    auto last = dl_list_->Header()->prev;
    current_ = kv_allocator_->offset2addr<DLRecord>(last);
    skipInvalidRecords(false);
  }

  bool Valid() const {
    return current_ != nullptr && (current_->GetRecordType() & ElemType);
  }

  virtual void Next() {
    if (!Valid()) {
      return;
    }
    current_ = kv_allocator_->offset2addr_checked<DLRecord>(current_->next);
    skipInvalidRecords(true);
  }

  void Prev() {
    if (!Valid()) {
      return;
    }
    current_ = (kv_allocator_->offset2addr<DLRecord>(current_->prev));
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
  DLRecord* findValidVersion(DLRecord* data_record) {
    DLRecord* curr = data_record;
    TimestampType ts = snapshot_->GetTimestamp();
    while (curr != nullptr && curr->GetTimestamp() > ts) {
      curr = kv_allocator_->offset2addr<DLRecord>(curr->old_version);
      kvdk_assert(curr == nullptr || curr->Validate(),
                  "Broken checkpoint: invalid older version sorted record");
      kvdk_assert(
          curr == nullptr || equal_string_view(curr->Key(), data_record->Key()),
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
                ? kv_allocator_->offset2addr_checked<DLRecord>(current_->next)
                : kv_allocator_->offset2addr_checked<DLRecord>(current_->prev);
      } else {
        current_ = valid_version_record;
        break;
      }
    }
  }

  DLList* dl_list_;
  const Allocator* kv_allocator_;
  DLRecord* current_;
  const SnapshotImpl* snapshot_;
};

// Iter all records in a dl list
class DLListRecordIterator {
 public:
  DLListRecordIterator(DLList* dl_list, Allocator* kv_allocator)
      : dl_list_(dl_list),
        header_(dl_list->Header()),
        current_(header_),
        kv_allocator_(kv_allocator) {}

  void Locate(DLRecord* record) { current_ = record; }

  void Next() {
    if (Valid()) {
      current_ = kv_allocator_->offset2addr_checked<DLRecord>(current_->next);
    }
  }

  void Prev() {
    if (Valid()) {
      current_ = kv_allocator_->offset2addr_checked<DLRecord>(current_->prev);
    }
  }

  bool Valid() { return current_ && (current_->GetRecordType() & ElemType); }

  void SeekToFirst() {
    kvdk_assert(header_ != nullptr, "");
    current_ = kv_allocator_->offset2addr_checked<DLRecord>(header_->next);
  }

  void SeekToLast() {
    kvdk_assert(header_ != nullptr, "");
    current_ = kv_allocator_->offset2addr_checked<DLRecord>(header_->prev);
  }

  DLRecord* Record() { return Valid() ? current_ : nullptr; }

 private:
  DLList* dl_list_;
  DLRecord* header_;
  DLRecord* current_;
  const Allocator* kv_allocator_;
};

// Used in recovery of dl list based collections
template <typename CType>
class DLListRecoveryUtils {
 public:
  DLListRecoveryUtils(const Allocator* kv_allocator)
      : kv_allocator_(kv_allocator) {}

  bool CheckAndRepairLinkage(DLRecord* record) {
    // The next linkage is correct. If the prev linkage is correct too, the
    // record linkage is ok. If the prev linkage is not correct, it will be
    // repaired by the correct prodecessor soon, so directly return true here.
    if (CheckNextLinkage(record)) {
      return true;
    }
    // If only prev linkage is correct, then repair the next linkage
    if (CheckPrevLinkage(record)) {
      DLRecord* next =
          kv_allocator_->offset2addr_checked<DLRecord>(record->next);
      next->PersistPrevNT(kv_allocator_->addr2offset_checked(record));
      return true;
    }

    return false;
  }

  bool CheckNextLinkage(DLRecord* record) {
    uint64_t offset = kv_allocator_->addr2offset_checked(record);
    DLRecord* next = kv_allocator_->offset2addr_checked<DLRecord>(record->next);

    auto check_linkage = [&]() { return next->prev == offset; };

    auto check_type = [&]() { return CType::MatchType(record); };

    auto check_id = [&]() {
      auto next_id = CType::FetchID(next);
      auto record_id = CType::FetchID(record);
      return record_id == next_id;
    };

    return check_linkage() && check_type() && check_id();
  }

  bool CheckPrevLinkage(DLRecord* record) {
    uint64_t offset = kv_allocator_->addr2offset_checked(record);
    DLRecord* prev = kv_allocator_->offset2addr_checked<DLRecord>(record->prev);

    auto check_linkage = [&]() { return prev->next == offset; };

    auto check_type = [&]() { return CType::MatchType(record); };

    auto check_id = [&]() {
      auto prev_id = CType::FetchID(prev);
      auto record_id = CType::FetchID(record);
      return record_id == prev_id;
    };

    return check_linkage() && check_type() && check_id();
  }

  bool CheckLinkage(DLRecord* record) {
    return CheckPrevLinkage(record) && CheckNextLinkage(record);
  }

 private:
  const Allocator* kv_allocator_;
};
}  // namespace KVDK_NAMESPACE