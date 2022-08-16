/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "../dl_list.hpp"
#include "kvdk/types.hpp"

namespace KVDK_NAMESPACE {
class ListIteratorImpl;

enum class ListPos : int {
  Front = 0,
  Back = -1,
};

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
    Status s{Status::InvalidArgument};
    std::vector<SpaceEntry> spaces{};

   private:
    friend List;
    std::vector<DLRecord*> to_pop{};
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

  void UpdateSize(int64_t delta) {
    kvdk_assert(delta >= 0 || size_.load() >= static_cast<size_t>(-delta),
                "Update hash list size to negative");
    size_.fetch_add(delta, std::memory_order_relaxed);
  }

  size_t Size() { return size_.load(); }

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
  friend ListIteratorImpl;
  std::recursive_mutex list_lock_;
  DLList dl_list_;
  PMEMAllocator* pmem_allocator_;
  std::atomic<size_t> size_;
  // to avoid illegal access caused by cleaning skiplist by multi-thread
  SpinMutex cleaning_lock_;
};
}  // namespace KVDK_NAMESPACE