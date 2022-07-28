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

  struct PopNArgs {
   public:
    Status s{Status::Ok};
    std::vector<DLList::WriteArgs> write_args{};

   private:
    friend List;
    std::vector<DLRecord*> to_pop{};
  };

  struct PushNArgs {
   public:
    Status s{Status::Ok};
    std::vector<DLList::WriteArgs> write_args{};
    int pos{0};
  };

  const DLRecord* HeaderRecord() const { return dl_list_.Header(); }

  DLRecord* HeaderRecord() { return dl_list_.Header(); }

  ExpireTimeType GetExpireTime() const final {
    return HeaderRecord()->GetExpireTime();
  }

  TimeStampType GetTimeStamp() const { return HeaderRecord()->GetTimestamp(); }

  bool HasExpired() const final { return dl_list_.Header()->HasExpired(); }

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

  Status BatchPopBack(size_t n, std::vector<std::string>* elems,
                      TimeStampType batch_ts, char* batch_log) {
    return batchPopImpl(n, -1, elems, batch_ts, batch_log);
  }

  Status BatchPopFront(size_t n, std::vector<std::string>* elems,
                       TimeStampType batch_ts, char* batch_log) {
    return batchPopImpl(n, 0, elems, batch_ts, batch_log);
  }

  WriteResult PushFront(const StringView& elem, TimeStampType ts,
                        const SpaceEntry space) {
    WriteResult ret;
    std::string internal_key(InternalKey(""));

    DLList::WriteArgs args(internal_key, elem, RecordType::ListElem,
                           RecordStatus::Normal, ts, space);
    ret.s = dl_list_.PushFront(args);
    kvdk_assert(ret.s == Status::Ok, "Push front should alwasy success");
    UpdateSize(1);
    ret.write_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
    return ret;
  }

  WriteResult PushFront(const StringView& elem, TimeStampType ts) {
    WriteResult ret;
    std::string internal_key(InternalKey(""));
    SpaceEntry space =
        pmem_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
    if (space.size == 0) {
      ret.s = Status::PmemOverflow;
      return ret;
    }

    DLList::WriteArgs args(internal_key, elem, RecordType::ListElem,
                           RecordStatus::Normal, ts, space);
    ret.s = dl_list_.PushFront(args);
    kvdk_assert(ret.s == Status::Ok, "Push front should alwasy success");
    UpdateSize(1);
    ret.write_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
    return ret;
  }

  WriteResult PushBack(const StringView& elem, TimeStampType ts) {
    WriteResult ret;
    std::string internal_key(InternalKey(""));
    SpaceEntry space =
        pmem_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
    if (space.size == 0) {
      ret.s = Status::PmemOverflow;
      return ret;
    }

    DLList::WriteArgs args(internal_key, elem, RecordType::ListElem,
                           RecordStatus::Normal, ts, space);
    ret.s = dl_list_.PushBack(args);
    kvdk_assert(ret.s == Status::Ok, "Push front should alwasy success");
    UpdateSize(1);
    ret.write_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
    return ret;
  }

  WriteResult PushBack(const StringView& elem, TimeStampType ts,
                       const SpaceEntry& space) {
    WriteResult ret;
    std::string internal_key(InternalKey(""));

    DLList::WriteArgs args(internal_key, elem, RecordType::ListElem,
                           RecordStatus::Normal, ts, space);
    ret.s = dl_list_.PushBack(args);
    kvdk_assert(ret.s == Status::Ok, "Push back should alwasy success");
    UpdateSize(1);
    ret.write_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
    return ret;
  }

  WriteResult PopFront(TimeStampType ts) {
    WriteResult ret;
    DLListRecordIterator iter(&dl_list_, pmem_allocator_);
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
      DLRecord* record = iter.Record();
      if (record->GetRecordStatus() == RecordStatus::Normal) {
        SpaceEntry space =
            pmem_allocator_->Allocate(DLRecord::RecordSize(record->Key(), ""));
        if (space.size == 0) {
          ret.s = Status::PmemOverflow;
          return ret;
        }
        DLList::WriteArgs args(record->Key(), "", RecordType::ListElem,
                               RecordStatus::Outdated, ts, space);
        ret.s = dl_list_.Update(args, record);
        kvdk_assert(ret.s == Status::Ok,
                    "the whole list is locked so the update must be success");
        ret.write_record =
            pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
        ret.existing_record = record;
        UpdateSize(-1);
        return ret;
      }
    }
    ret.s = Status::NotFound;
    return ret;
  };

  WriteResult PopBack(TimeStampType ts) {
    // TODO cache back to avoid iter
    WriteResult ret;
    DLListRecordIterator iter(&dl_list_, pmem_allocator_);
    for (iter.SeekToLast(); iter.Valid(); iter.Prev()) {
      DLRecord* record = iter.Record();
      if (record->GetRecordStatus() == RecordStatus::Normal) {
        SpaceEntry space =
            pmem_allocator_->Allocate(DLRecord::RecordSize(record->Key(), ""));
        if (space.size == 0) {
          ret.s = Status::PmemOverflow;
          return ret;
        }
        DLList::WriteArgs args(record->Key(), "", RecordType::ListElem,
                               RecordStatus::Outdated, ts, space);
        ret.s = dl_list_.Update(args, record);
        kvdk_assert(ret.s == Status::Ok,
                    "the whole list is locked so the update must be success");
        ret.write_record =
            pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
        ret.existing_record = record;
        UpdateSize(-1);
        return ret;
      }
    }
    ret.s = Status::NotFound;
    return ret;
  }

  WriteResult InsertBefore(const StringView& elem,
                           const StringView& existing_elem, TimeStampType ts) {
    WriteResult ret;
    std::string internal_key(InternalKey(""));
    DLListRecordIterator iter(&dl_list_, pmem_allocator_);
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
      DLRecord* record = iter.Record();
      if (record->GetRecordStatus() == RecordStatus::Normal &&
          equal_string_view(record->Value(), existing_elem)) {
        SpaceEntry space =
            pmem_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
        if (space.size == 0) {
          ret.s = Status::PmemOverflow;
          return ret;
        }
        DLList::WriteArgs args(internal_key, elem, RecordType::ListElem,
                               RecordStatus::Normal, ts, space);
        ret.s = dl_list_.InsertBefore(args, record);
        kvdk_assert(
            ret.s == Status::Ok,
            "the whole list is locked, so the insertion must be success");
        if (ret.s == Status::Ok) {
          ret.write_record =
              pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
          UpdateSize(1);
        }
        return ret;
      }
    }
    ret.s = Status::NotFound;
    return ret;
  }

  WriteResult InsertAfter(const StringView& elem,
                          const StringView& existing_elem, TimeStampType ts) {
    WriteResult ret;
    std::string internal_key(InternalKey(""));
    DLListRecordIterator iter(&dl_list_, pmem_allocator_);
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
      DLRecord* record = iter.Record();
      if (record->GetRecordStatus() == RecordStatus::Normal &&
          equal_string_view(record->Value(), existing_elem)) {
        SpaceEntry space =
            pmem_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
        if (space.size == 0) {
          ret.s = Status::PmemOverflow;
          return ret;
        }
        DLList::WriteArgs args(internal_key, elem, RecordType::ListElem,
                               RecordStatus::Normal, ts, space);
        ret.s = dl_list_.InsertAfter(args, record);
        kvdk_assert(
            ret.s == Status::Ok,
            "the whole list is locked, so the insertion must be success");
        if (ret.s == Status::Ok) {
          ret.write_record =
              pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
          UpdateSize(1);
        }
        return ret;
      }
    }
    ret.s = Status::NotFound;
    return ret;
  }

  WriteResult InsertAt(const StringView& elem, uint64_t pos, TimeStampType ts) {
    WriteResult ret;
    std::string internal_key(InternalKey(""));
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

    SpaceEntry space =
        pmem_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
    if (space.size == 0) {
      ret.s = Status::PmemOverflow;
      return ret;
    }
    DLList::WriteArgs args(internal_key, elem, RecordType::ListElem,
                           RecordStatus::Normal, ts, space);
    ret.s = dl_list_.InsertAfter(args, prev);
    kvdk_assert(ret.s == Status::Ok,
                "the whole list is locked, so the insertion must be success");
    ret.write_record =
        pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
    UpdateSize(1);
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
    UpdateSize(-1);
    return ret;
  }

  Status Front(std::string* elem) {
    if (Size() == 0) {
      return Status::NotFound;
    }
    DLListRecordIterator iter(&dl_list_, pmem_allocator_);
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
      DLRecord* record = iter.Record();
      if (record->GetRecordStatus() == RecordStatus::Normal) {
        StringView sw = record->Value();
        elem->assign(sw.data(), sw.size());
        return Status::Ok;
      }
    }

    return Status::NotFound;
  }

  Status Back(std::string* elem) {
    if (Size() == 0) {
      return Status::NotFound;
    }
    DLListRecordIterator iter(&dl_list_, pmem_allocator_);
    for (iter.SeekToLast(); iter.Valid(); iter.Prev()) {
      DLRecord* record = iter.Record();
      if (record->GetRecordStatus() == RecordStatus::Normal) {
        StringView sw = record->Value();
        elem->assign(sw.data(), sw.size());
        return Status::Ok;
      }
    }

    return Status::NotFound;
  }

  bool Replace(DLRecord* old_record, DLRecord* new_record) {
    return dl_list_.Replace(old_record, new_record);
  }

  WriteResult Replace(uint64_t pos, const StringView& elem, TimeStampType ts) {
    WriteResult ret;
    std::string internal_key(InternalKey(""));
    if (pos >= Size()) {
      ret.s = Status::NotFound;
      return ret;
    }
    SpaceEntry space =
        pmem_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
    if (space.size == 0) {
      ret.s = Status::PmemOverflow;
      return ret;
    }
    DLList::WriteArgs args(internal_key, elem, RecordType::ListElem,
                           RecordStatus::Normal, ts, space);
    DLListRecordIterator iter(&dl_list_, pmem_allocator_);
    uint64_t cur = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
      DLRecord* record = iter.Record();
      if (record->GetRecordStatus() == RecordStatus::Outdated) {
        continue;
      }
      if (cur == pos) {
        ret.s = dl_list_.Update(args, record);
        ret.existing_record = record;
        ret.write_record =
            pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
        kvdk_assert(ret.s == Status::Ok,
                    "the whole list is locked, so the update must be success");
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

  DLList* GetDLList() { return &dl_list_; }

  void DestroyAll() {}

  void Destroy() {}

  PushNArgs PreparePushN(int pos, const std::vector<StringView>& elems,
                         TimeStampType ts) {
    PushNArgs args;
    args.pos = pos;
    if (elems.size() > 0) {
      std::string internal_key(InternalKey(""));
      for (auto& elem : elems) {
        SpaceEntry space =
            pmem_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
        if (space.size == 0) {
          GlobalLogger.Error("Try allocate %lu error\n",
                             DLRecord::RecordSize(internal_key, elem));
          for (auto& wa : args.write_args) {
            pmem_allocator_->Free(wa.space);
          }
          args.s = Status::PmemOverflow;
          break;
        }
        args.write_args.emplace_back(internal_key, elem, RecordType::ListElem,
                                     RecordStatus::Normal, ts, space);
      }
    }
    return args;
  }

  PopNArgs PreparePopN(size_t n, int pos, TimeStampType ts,
                       std::vector<std::string>* elems) {
    size_t nn = n;
    PopNArgs args;
    DLListRecordIterator iter(&dl_list_, pmem_allocator_);
    for (pos == 0 ? iter.SeekToFirst() : iter.SeekToLast();
         iter.Valid() && nn > 0; pos == 0 ? iter.Next() : iter.Prev()) {
      DLRecord* record = iter.Record();
      if (record->GetRecordStatus() == RecordStatus::Normal) {
        SpaceEntry space =
            pmem_allocator_->Allocate(DLRecord::RecordSize(record->Key(), ""));
        if (space.size == 0) {
          for (auto& wa : args.write_args) {
            pmem_allocator_->Free(wa.space);
          }
          args.s = Status::PmemOverflow;
          return args;
        }
        if (elems) {
          StringView sw = record->Value();
          elems->emplace_back(sw.data(), sw.size());
        }
        args.write_args.emplace_back(record->Key(), "", RecordType::ListElem,
                                     RecordStatus::Outdated, ts, space);
        args.to_pop.emplace_back(record);
        nn--;
      }
    }
    return args;
  }

  Status PushN(const PushNArgs& args) {
    if (args.s != Status::Ok) {
      return args.s;
    }
    for (auto& wa : args.write_args) {
      Status s = args.pos == 0 ? dl_list_.PushFront(wa) : dl_list_.PushBack(wa);
      kvdk_assert(s == Status::Ok, "Push back/front should always success");
      TEST_CRASH_POINT("List::PushN", "");
    }
    UpdateSize(args.write_args.size());
    return Status::Ok;
  }

  Status PopN(const PopNArgs& args) {
    if (args.s != Status::Ok) {
      return args.s;
    }
    kvdk_assert(args.write_args.size() == args.to_pop.size(), "");
    for (size_t i = 0; i < args.write_args.size(); i++) {
      Status s = dl_list_.Update(args.write_args[i], args.to_pop[i]);
      kvdk_assert(
          s == Status::Ok,
          "the whole list should be locked, so the update must be success");
      TEST_CRASH_POINT("List::PopN", "");
    }
    UpdateSize(-args.to_pop.size());
    return Status::Ok;
  }

  static CollectionIDType ListID(DLRecord* record) {
    assert(record != nullptr);
    switch (record->GetRecordType()) {
      case RecordType::ListElem:
        return ExtractID(record->Key());
      case RecordType::ListRecord:
        return DecodeID(record->Value());
      default:
        GlobalLogger.Error("Wrong record type %u in ListID",
                           record->GetRecordType());
        kvdk_assert(false, "Wrong type in ListID");
        return 0;
    }
  }

 private:
  Status batchPushImpl(int pos, const std::vector<StringView>& elems,
                       TimeStampType batch_ts, char* batch_log) {
    BatchWriteLog log;
    log.SetTimestamp(batch_ts);
    std::vector<DLList::WriteArgs> write_args;
    std::string internal_key(InternalKey(""));
    for (auto& elem : elems) {
      SpaceEntry space =
          pmem_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
      if (space.size == 0) {
        return Status::PmemOverflow;
      }
      write_args.emplace_back(internal_key, elem, RecordType::ListElem,
                              RecordStatus::Normal, batch_ts, space);
      log.ListEmplace(space.offset);
    }
    log.EncodeTo(batch_log);
    BatchWriteLog::MarkProcessing(batch_log);
    for (auto& wa : write_args) {
      Status s = pos == 0 ? dl_list_.PushFront(wa) : dl_list_.PushBack(wa);
      kvdk_assert(s == Status::Ok, "Push back should always success");
    }
    UpdateSize(elems.size());
    BatchWriteLog::MarkCommitted(batch_log);
    return Status::Ok;
  }

  Status batchPopImpl(size_t n, int pos, std::vector<std::string>* elems,
                      TimeStampType batch_ts, char* batch_log) {
    kvdk_assert(batch_log != nullptr, "");
    BatchWriteLog log;
    log.SetTimestamp(batch_ts);
    if (elems) {
      elems->clear();
    }
    size_t nn = n;
    std::vector<DLRecord*> to_pop;
    std::vector<DLList::WriteArgs> write_args;
    DLListRecordIterator iter(&dl_list_, pmem_allocator_);
    for (pos == 0 ? iter.SeekToFirst() : iter.SeekToLast();
         iter.Valid() && nn-- > 0; pos == 0 ? iter.Next() : iter.Prev()) {
      DLRecord* record = iter.Record();
      if (record->GetRecordStatus() == RecordStatus::Normal) {
        SpaceEntry space =
            pmem_allocator_->Allocate(DLRecord::RecordSize(record->Key(), ""));
        if (space.size == 0) {
          return Status::PmemOverflow;
        }
        to_pop.emplace_back(record);
        write_args.emplace_back(record->Key(), "", RecordType::ListElem,
                                RecordStatus::Outdated, batch_ts, space);
        log.ListDelete(space.offset);
      }
    }
    log.EncodeTo(batch_log);
    BatchWriteLog::MarkProcessing(batch_log);
    for (size_t i = 0; i < to_pop.size(); i++) {
      if (elems) {
        StringView sw = to_pop[i]->Value();
        elems->emplace_back(sw.data(), sw.size());
      }
      Status s = dl_list_.Update(write_args[i], to_pop[i]);
      kvdk_assert(
          s == Status::Ok,
          "the whole list should be locked, so the update must be success");
    }
    UpdateSize(-to_pop.size());
    BatchWriteLog::MarkCommitted(batch_log);
    return Status::Ok;
  }

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
    if (index < 0) {
      SeekToLast();
      long cur = -1;
      while (cur-- > index && Valid()) {
        Prev();
      }
    } else {
      SeekToFirst();
      long cur = 0;
      while (cur++ < index && Valid()) {
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

  std::string Elem() const final {
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