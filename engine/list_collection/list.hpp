/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "../alias.hpp"
#include "../collection.hpp"
#include "../dl_list.hpp"
#include "../hash_table.hpp"
#include "../lock_table.hpp"
#include "../structures.hpp"
#include "../utils/utils.hpp"
#include "../write_batch_impl.hpp"
#include "kvdk/engine.hpp"

namespace KVDK_NAMESPACE {
class ListIteratorImpl;
class List : public Collection {
 public:
  List(DLRecord* header, const StringView& name, CollectionIDType id,
       PMEMAllocator* pmem_allocator, LockTable* lock_table)
      : Collection(name, id),
        list_lock_(),
        dl_list_(header, pmem_allocator, lock_table),
        pmem_allocator_(pmem_allocator),
        size_(0) {}

  struct WriteResult {
    Status s = Status::Ok;
    DLRecord* write_record = nullptr;
    DLRecord* existing_record = nullptr;
  };

  const DLRecord* HeaderRecord() const { return dl_list_.Header(); }

  DLRecord* HeaderRecord() { return dl_list_.Header(); }

  ExpireTimeType GetExpireTime() const final {
    return HeaderRecord()->GetExpireTime();
  }

  TimeStampType GetTimeStamp() const { return HeaderRecord()->GetTimestamp(); }

  bool HasExpired() const final { return dl_list_.Header()->HasExpired(); }

  Status SetExpireTime(ExpireTimeType) final { return Status::Ok; }

  WriteResult SetExpireTime(ExpireTimeType expired_time,
                            TimeStampType timestamp) {
    WriteResult ret;
    DLRecord* header = HeaderRecord();
    SpaceEntry space = pmem_allocator_->Allocate(
        DLRecord::RecordSize(header->Key(), header->Value()));
    if (space.size == 0) {
      ret.s = Status::PmemOverflow;
      return ret;
    }
    DLRecord* pmem_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr_checked(space.offset), space.size,
        timestamp, RecordType::ListRecord, RecordStatus::Normal,
        pmem_allocator_->addr2offset_checked(header), header->prev,
        header->next, header->Key(), header->Value(), expired_time);
    bool success = dl_list_.Replace(header, pmem_record);
    kvdk_assert(success, "existing header should be linked on its list");
    ret.existing_record = header;
    ret.write_record = pmem_record;
    return ret;
  }

  WriteResult PushFront(const StringView& key, const StringView& value,
                        TimeStampType ts) {
    WriteResult ret;
    std::string internal_key(InternalKey(key));
    SpaceEntry space =
        pmem_allocator_->Allocate(DLRecord::RecordSize(internal_key, value));
    if (space.size == 0) {
      ret.s = Status::PmemOverflow;
      return ret;
    }

    DLList::WriteArgs args(internal_key, value, RecordType::ListElem,
                           RecordStatus::Normal, ts, space);
    ret.s = dl_list_.PushFront(args);
    kvdk_assert(ret.s == Status::Ok, "Push front should alwasy success");
    UpdateSize(1);
    ret.write_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
    return ret;
  }

  WriteResult PushBack(const StringView& key, const StringView& value,
                       TimeStampType ts) {
    WriteResult ret;
    std::string internal_key(InternalKey(key));
    SpaceEntry space =
        pmem_allocator_->Allocate(DLRecord::RecordSize(internal_key, value));
    if (space.size == 0) {
      ret.s = Status::PmemOverflow;
      return ret;
    }

    DLList::WriteArgs args(internal_key, value, RecordType::ListElem,
                           RecordStatus::Normal, ts, space);
    ret.s = dl_list_.PushBack(args);
    kvdk_assert(ret.s == Status::Ok, "Push front should alwasy success");
    UpdateSize(1);
    ret.write_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
    return ret;
  }

  WriteResult PopFront() {
    WriteResult ret;
    ret.existing_record = dl_list_.PopFront();
    ret.s = ret.existing_record ? Status::Ok : Status::NotFound;
    return ret;
  };

  WriteResult PopBack() {
    WriteResult ret;
    ret.existing_record = dl_list_.PopBack();
    ret.s = ret.existing_record ? Status::Ok : Status::NotFound;
    return ret;
  }

  WriteResult InsertBefore(const StringView& key, const StringView& pos,
                           TimeStampType ts) {
    WriteResult ret;
    DLListRecordIterator iter(&dl_list_, pmem_allocator_);
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
      DLRecord* record = iter.Record();
      if (record->GetRecordStatus() == RecordStatus::Normal &&
          equal_string_view(record->Key(), pos)) {
        SpaceEntry space =
            pmem_allocator_->Allocate(DLRecord::RecordSize(key, ""));
        if (space.size == 0) {
          ret.s = Status::PmemOverflow;
          return ret;
        }
        DLList::WriteArgs args(key, "", RecordType::ListElem,
                               RecordStatus::Normal, ts, space);
        ret.s = dl_list_.InsertBefore(args, record);
        kvdk_assert(
            ret.s == Status::Ok,
            "the whole list is locked, so the insertion must be success");
        if (ret.s == Status::Ok) {
          ret.write_record =
              pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
        }
        return ret;
      }
    }
    ret.s = Status::NotFound;
    return ret;
  }

  WriteResult InsertAfter(const StringView& key, const StringView& pos,
                          TimeStampType ts) {
    WriteResult ret;
    DLListRecordIterator iter(&dl_list_, pmem_allocator_);
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
      DLRecord* record = iter.Record();
      if (record->GetRecordStatus() == RecordStatus::Normal &&
          equal_string_view(record->Key(), pos)) {
        SpaceEntry space =
            pmem_allocator_->Allocate(DLRecord::RecordSize(key, ""));
        if (space.size == 0) {
          ret.s = Status::PmemOverflow;
          return ret;
        }
        DLList::WriteArgs args(key, "", RecordType::ListElem,
                               RecordStatus::Normal, ts, space);
        ret.s = dl_list_.InsertAfter(args, record);
        kvdk_assert(
            ret.s == Status::Ok,
            "the whole list is locked, so the insertion must be success");
        if (ret.s == Status::Ok) {
          ret.write_record =
              pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
        }
        return ret;
      }
    }
    ret.s = Status::NotFound;
    return ret;
  }

  WriteResult InsertAt(const StringView& key, uint64_t pos, TimeStampType ts) {
    WriteResult ret;
    DLListRecordIterator iter(&dl_list_, pmem_allocator_);
    uint64_t cur = 0;
    DLRecord* prev = dl_list_.Header();
    for (iter.SeekToFirst(); iter.Valid() && cur < pos; iter.Next()) {
      DLRecord* record = iter.Record();
      if (record->GetRecordStatus() == RecordStatus::Outdated) {
        continue;
      }
      cur++;
      prev = record;
    }

    if (cur < pos) {
      ret.s = Status::NotFound;
      return ret;
    }

    SpaceEntry space = pmem_allocator_->Allocate(DLRecord::RecordSize("", key));
    if (space.size == 0) {
      ret.s = Status::PmemOverflow;
      return ret;
    }
    DLList::WriteArgs args("", key, RecordType::ListElem, RecordStatus::Normal,
                           ts, space);
    ret.s = dl_list_.InsertAfter(args, prev);
    kvdk_assert(ret.s == Status::Ok,
                "the whole list is locked, so the insertion must be success");
    ret.write_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
    return ret;
  }

  WriteResult Erase(uint64_t pos) {
    WriteResult ret;
    if (pos >= Size()) {
      ret.s = Status::NotFound;
      return ret;
    }
    DLListRecordIterator iter(&dl_list_, pmem_allocator_);
    uint64_t cur = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
      DLRecord* record = iter.Record();
      if (record->GetRecordStatus() == RecordStatus::Outdated) {
        continue;
      }
      if (cur == pos) {
        bool success = dl_list_.Remove(record);
        ret.existing_record = record;
        kvdk_assert(success,
                    "the whole list is locked, so the remove must be success");
        break;
      }
      cur++;
    }
    kvdk_assert(cur == pos, "size already checked");
    return ret;
  }

  bool Replace(DLRecord* old_record, DLRecord* new_record) {
    return dl_list_.Replace(old_record, new_record);
  }

  WriteResult Replace(uint64_t pos, const StringView& elem, TimeStampType ts) {
    WriteResult ret;
    if (pos >= Size()) {
      ret.s = Status::NotFound;
      return ret;
    }
    SpaceEntry space =
        pmem_allocator_->Allocate(DLRecord::RecordSize("", elem));
    if (space.size == 0) {
      ret.s = Status::PmemOverflow;
      return ret;
    }
    DLList::WriteArgs args("", elem, RecordType::ListElem, RecordStatus::Normal,
                           ts, space);
    DLListRecordIterator iter(&dl_list_, pmem_allocator_);
    uint64_t cur = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
      DLRecord* record = iter.Record();
      if (record->GetRecordStatus() == RecordStatus::Outdated) {
        continue;
      }
      if (cur == pos) {
        bool success = dl_list_.Update(args, record);
        ret.existing_record = record;
        ret.write_record =
            pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
        kvdk_assert(success,
                    "the whole list is locked, so the remove must be success");
        break;
      }
      cur++;
    }
    kvdk_assert(cur == pos, "size already checked");
    return ret;
  }

  void UpdateSize(int64_t delta) {
    kvdk_assert(delta >= 0 || size_.load() >= static_cast<size_t>(-delta),
                "Update hash list size to negative");
    size_.fetch_add(delta, std::memory_order_relaxed);
  }

  Status Move() { return Status::Ok; }

  size_t Size() { return size_.load(); }

  std::unique_lock<std::recursive_mutex> AcquireLock() {
    return std::unique_lock<std::recursive_mutex>(list_lock_);
  }

  void DestroyAll() {}

 private:
  friend ListIteratorImpl;
  std::recursive_mutex list_lock_;
  DLList dl_list_;
  PMEMAllocator* pmem_allocator_;
  std::atomic<size_t> size_;
};

class ListIteratorImpl final : public ListIterator {
 public:
  ListIteratorImpl(Engine* engine, List* list, const SnapshotImpl* snapshot,
                   bool own_snapshot)
      : engine_(engine),
        list_(list),
        snapshot_(snapshot),
        own_snapshot_(own_snapshot),
        dl_iter_(&list->dl_list_, list->pmem_allocator_, snapshot,
                 own_snapshot) {}

  void Seek(long index) final {
    if (pos < 0) {
      SeekToLast();
      long cur = -1;
      while (cur-- > pos && Valid()) {
        Prev();
      }
    } else {
      SeekToFirst();
      long cur = 0;
      while (cur++ < pos && Valid()) {
        Next();
      }
    }
  }

  void SeekToFirst() final { dl_iter_.SeekToFirst(); }

  void SeekToLast() final { dl_iter_.SeekToLast(); }

  void SeekToFirst(StringView elem) final {
    SeekToFirst();
    Next(elem);
  }

  void SeekToLast(StringView elem) final {
    SeekToLast();
    Prev(elem);
  }

  bool Valid() const final { return dl_iter_.Valid(); }

  void Next() final { dl_iter_.Next(); }

  void Prev() final { dl_iter_.Prev(); }

  void Next(StringView elem) final {
    while (Valid()) {
      Next();
      if (!Valid() || equal_string_view(elem, dl_iter_.Value())) {
        break;
      }
    }
  }

  void Prev(StringView elem) final {
    while (Valid()) {
      Prev();
      if (!Valid() || equal_string_view(elem, dl_iter_.Value())) {
        break;
      }
    }
  }

  std::string Value() const final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid ListIterator!");
      return std::string{};
    }
    auto sw = dl_iter_.Value();
    return std::string{sw.data(), sw.size()};
  }

  ~ListIteratorImpl() final {
    if (own_snapshot_ && snapshot_) {
      engine_->ReleaseSnapshot(snapshot_);
    }
  }

 private:
  Engine* engine_;
  List* list_;
  const SnapshotImpl* snapshot_;
  bool own_snapshot_;
  DLListAccessIterator dl_iter_;
};
}  // namespace KVDK_NAMESPACE