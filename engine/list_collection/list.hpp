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
class List : public Collection {
 public:
  struct WriteResult {
    Status s = Status::Ok;
    DLRecord* write_record = nullptr;
    DLRecord* existing_record = nullptr;
  };

  WriteResult PushFront(const StringView& key, const StringView& value,
                        TimeStampType ts) {
    WriteResult ret;
    std::string internal_key(InternalKey(key);
    SpaceEntry space = pmem_allocator_->Allocate(DLRecord::RecordSize(internal_key, value));
    if(space.size == 0){
      ret.s = Status::PmemOverflow;
      return ret;
    }

    DLList::WriteArgs args(internal_key, value, RecordType::ListElem, RecordStatus::Normal, ts, space);
    ret.s = dl_list_.PushFront(args);
    kvdk_assert(ret.s == Status::Ok, "Push front should alwasy success");
    UpdateSize(1);
    ret.write_record = pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
    return ret;
  }

  WriteResult PushBack(const StringView& key, const StringView& value,
                       TimeStampType ts) {
    WriteResult ret;
    std::string internal_key(InternalKey(key);
    SpaceEntry space = pmem_allocator_->Allocate(DLRecord::RecordSize(internal_key, value));
    if(space.size == 0){
      ret.s = Status::PmemOverflow;
      return ret;
    }

    DLList::WriteArgs args(internal_key, value, RecordType::ListElem, RecordStatus::Normal, ts, space);
    ret.s = dl_list_.PushBack(args);
    kvdk_assert(ret.s == Status::Ok, "Push front should alwasy success");
    UpdateSize(1);
    ret.write_record = pmem_allocator_->offset2addr_checked<DLRecord>(space.offset);
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
    std::lock_guard<std::mutex> lg(list_lock_);
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
    std::lock_guard<std::mutex> lg(list_lock_);
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
    std::lock_guard<std::mutex> lg(list_lock_);
    uint64_t curr = 0;
    DLRecord* prev = dl_list_.Header();
    while (curr < pos) {
      prev = pmem_allocator_->offset2addr_checked<DLRecord>(prev->next);
      if (prev == dl_list_.Header()) {
        ret.s = Status::InvalidArgument;
        return ret;
      }
      curr++;
    }
    SpaceEntry space = pmem_allocator_->Allocate(DLRecord::RecordSize(key));
    if (space.size == 0) {
      ret.s = Status::PmemOverflow;
      return ret;
    }

    DLList::WriteArgs args(key, "", RecordType::ListElem, RecordStatus::Normal,
                           ts, space);
    ret.s = dl_list_.InsertAfter(args, prev);
    kvdk_assert(ret.s == Status::Ok, "");
    return ret;
  }

  WriteResult InsertAfter();

  WriteResult Erase();

  WriteResult Replace();

  void UpdateSize(int64_t delta);

  Status Move();

 private:
  DLList dl_list_;
  PMEMAllocator* pmem_allocator_;
  std::mutex list_lock_;
};
}  // namespace KVDK_NAMESPACE