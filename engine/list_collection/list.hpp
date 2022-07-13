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

  WriteResult InsertBefore();

  WriteResult InsertAfter();

  WriteResult Erase();

  WriteResult Replace();

  void UpdateSize(int64_t delta);

  Status Move();

 private:
  DLList dl_list_;
  PMEMAllocator* pmem_allocator_;
};
}  // namespace KVDK_NAMESPACE