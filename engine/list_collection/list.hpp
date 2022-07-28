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

  bool HasExpired() const final { return dl_list_.Header()->HasExpired(); }

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

  WriteResult InsertAt(const StringView& elem, uint64_t pos, TimeStampType ts);

  WriteResult Erase(uint64_t pos);

  Status Front(std::string* elem);

  Status Back(std::string* elem);

  bool Replace(DLRecord* old_record, DLRecord* new_record) {
    return dl_list_.Replace(old_record, new_record);
  }

  WriteResult Update(uint64_t pos, const StringView& elem, TimeStampType ts);

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

  void DestroyAll() {}

  void Destroy() {}

  PushNArgs PreparePushN(ListPos pos, const std::vector<StringView>& elems,
                         TimeStampType ts);

  PopNArgs PreparePopN(ListPos pos, size_t n, TimeStampType ts,
                       std::vector<std::string>* elems);

  Status PushN(const PushNArgs& args);

  Status PopN(const PopNArgs& args);

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
        dl_iter_(&list->dl_list_, list->pmem_allocator_, snapshot) {}

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
  DLListDataIterator dl_iter_;
};
}  // namespace KVDK_NAMESPACE