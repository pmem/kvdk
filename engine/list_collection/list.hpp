/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "../dl_list.hpp"
#include "kvdk/types.hpp"

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
        live_records_() {}

  struct WriteResult {
    Status s = Status::Ok;
    DLRecord* write_record = nullptr;
    DLRecord* existing_record = nullptr;
  };

  struct PopNArgs {
   public:
    Status s{Status::InvalidArgument};
    std::vector<SpaceEntry> spaces{};

   private:
    friend List;
    std::vector<std::deque<DLRecord*>::iterator> to_pop{};
    TimeStampType ts;
  };

  struct PushNArgs {
   public:
    Status s{Status::InvalidArgument};
    std::vector<SpaceEntry> spaces;
    std::vector<StringView> elems;
    ListPos pos;
    TimeStampType ts;
  };

  const DLRecord* HeaderRecord() const { return dl_list_.Header(); }

  DLRecord* HeaderRecord() { return dl_list_.Header(); }

  ExpireTimeType GetExpireTime() const final {
    return HeaderRecord()->GetExpireTime();
  }

  TimeStampType GetTimeStamp() const { return HeaderRecord()->GetTimestamp(); }

  bool HasExpired() const final { return HeaderRecord()->HasExpired(); }

  WriteResult SetExpireTime(ExpireTimeType expired_time,
                            TimeStampType timestamp);

  WriteResult PushFront(const StringView& elem, TimeStampType ts);

  WriteResult PushBack(const StringView& elem, TimeStampType ts);

  WriteResult PopFront(TimeStampType ts);

  WriteResult PopBack(TimeStampType ts);

  WriteResult InsertBefore(const StringView& elem,
                           const StringView& existing_elem, TimeStampType ts);

  WriteResult InsertAfter(const StringView& elem,
                          const StringView& existing_elem, TimeStampType ts);

  WriteResult InsertAt(const StringView& elem, long index, TimeStampType ts);

  WriteResult Erase(long index, TimeStampType ts);

  Status Front(std::string* elem);

  Status Back(std::string* elem);

  bool Replace(DLRecord* old_record, DLRecord* new_record) {
    return dl_list_.Replace(old_record, new_record);
  }

  WriteResult Update(long index, const StringView& elem, TimeStampType ts);

  void AddLiveRecord(DLRecord* elem, ListPos pos) {
    if (pos == ListPos::Front) {
      live_records_.push_front(elem);
    } else {
      live_records_.push_back(elem);
    }
  }

  size_t Size() { return live_records_.size(); }

  std::unique_lock<std::recursive_mutex> AcquireLock() {
    return std::unique_lock<std::recursive_mutex>(list_lock_);
  }

  DLList* GetDLList() { return &dl_list_; }

  void DestroyAll();

  void Destroy();

  PushNArgs PreparePushN(ListPos pos, const std::vector<StringView>& elems,
                         TimeStampType ts);

  PopNArgs PreparePopN(ListPos pos, size_t n, TimeStampType ts,
                       std::vector<std::string>* elems);

  Status PushN(const PushNArgs& args);

  Status PopN(const PopNArgs& args);

  bool TryCleaningLock() { return cleaning_lock_.try_lock(); }

  void ReleaseCleaningLock() { cleaning_lock_.unlock(); }

  static CollectionIDType FetchID(DLRecord* record) {
    assert(record != nullptr);
    switch (record->GetRecordType()) {
      case RecordType::ListElem:
        return ExtractID(record->Key());
      case RecordType::ListHeader:
        return DecodeID(record->Value());
      default:
        GlobalLogger.Error("Wrong record type %u in ListID",
                           record->GetRecordType());
        kvdk_assert(false, "Wrong type in ListID");
        return 0;
    }
  }

  static bool MatchType(const DLRecord* record) {
    RecordType type = record->GetRecordType();
    return type == RecordType::ListElem || type == RecordType::ListHeader;
  }

 private:
  // find the first live record of elem
  std::deque<DLRecord*>::iterator findLiveRecord(StringView elem) {
    auto iter = live_records_.begin();
    while (iter != live_records_.end()) {
      if (equal_string_view((*iter)->Value(), elem)) {
        return iter;
      }
      ++iter;
    }
    return live_records_.end();
  }

  friend ListIteratorImpl;
  std::recursive_mutex list_lock_;
  DLList dl_list_;
  PMEMAllocator* pmem_allocator_;
  std::atomic<size_t> size_;
  // to avoid illegal access caused by cleaning skiplist by multi-thread
  SpinMutex cleaning_lock_;
  // we keep outdated records on list to support mvcc, so we track live records
  // in a deque to support fast write operations
  std::deque<DLRecord*> live_records_;
};
}  // namespace KVDK_NAMESPACE