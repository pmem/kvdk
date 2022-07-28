/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "list.hpp"

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
List::WriteResult List::SetExpireTime(ExpireTimeType expired_time,
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
      pmem_allocator_->offset2addr_checked(space.offset), space.size, timestamp,
      RecordType::ListRecord, RecordStatus::Normal,
      pmem_allocator_->addr2offset_checked(header), header->prev, header->next,
      header->Key(), header->Value(), expired_time);
  bool success = dl_list_.Replace(header, pmem_record);
  kvdk_assert(success, "existing header should be linked on its list");
  ret.existing_record = header;
  ret.write_record = pmem_record;
  return ret;
}

List::WriteResult List::PushFront(const StringView& elem, TimeStampType ts) {
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

List::WriteResult List::PushBack(const StringView& elem, TimeStampType ts) {
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

List::WriteResult List::PopFront(TimeStampType ts) {
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
}

List::WriteResult List::PopBack(TimeStampType ts) {
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

List::WriteResult List::InsertBefore(const StringView& elem,
                                     const StringView& existing_elem,
                                     TimeStampType ts) {
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
      kvdk_assert(ret.s == Status::Ok,
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

List::WriteResult List::InsertAfter(const StringView& elem,
                                    const StringView& existing_elem,
                                    TimeStampType ts) {
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
      kvdk_assert(ret.s == Status::Ok,
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

List::WriteResult List::InsertAt(const StringView& elem, uint64_t pos,
                                 TimeStampType ts) {
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

List::WriteResult List::Erase(uint64_t pos) {
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

Status List::Front(std::string* elem) {
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

Status List::Back(std::string* elem) {
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

List::WriteResult List::Update(uint64_t pos, const StringView& elem,
                               TimeStampType ts) {
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

List::PushNArgs List::PreparePushN(ListPos pos,
                                   const std::vector<StringView>& elems,
                                   TimeStampType ts) {
  PushNArgs args;
  args.pos = pos;
  args.ts = ts;
  if (elems.size() > 0) {
    std::string internal_key(InternalKey(""));
    for (auto& elem : elems) {
      SpaceEntry space =
          pmem_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
      if (space.size == 0) {
        GlobalLogger.Error("Try allocate %lu error\n",
                           DLRecord::RecordSize(internal_key, elem));
        for (auto& sp : args.spaces) {
          pmem_allocator_->Free(sp);
        }
        args.s = Status::PmemOverflow;
        break;
      }
      args.spaces.emplace_back(space);
    }
    args.elems = elems;
  }
  args.s = Status::Ok;
  return args;
}

List::PopNArgs List::PreparePopN(ListPos pos, size_t n, TimeStampType ts,
                                 std::vector<std::string>* elems) {
  size_t nn = n;
  PopNArgs args;
  args.ts = ts;
  DLListRecordIterator iter(&dl_list_, pmem_allocator_);
  for (pos == ListPos::Front ? iter.SeekToFirst() : iter.SeekToLast();
       iter.Valid() && nn > 0;
       pos == ListPos::Front ? iter.Next() : iter.Prev()) {
    DLRecord* record = iter.Record();
    if (record->GetRecordStatus() == RecordStatus::Normal) {
      SpaceEntry space =
          pmem_allocator_->Allocate(DLRecord::RecordSize(record->Key(), ""));
      if (space.size == 0) {
        for (auto& sp : args.spaces) {
          pmem_allocator_->Free(sp);
        }
        args.s = Status::PmemOverflow;
        return args;
      }
      if (elems) {
        StringView sw = record->Value();
        elems->emplace_back(sw.data(), sw.size());
      }
      args.spaces.emplace_back(space);
      args.to_pop.emplace_back(record);
      nn--;
    }
  }
  args.s = Status::Ok;
  return args;
}

Status List::PushN(const List::PushNArgs& args) {
  if (args.s != Status::Ok) {
    return args.s;
  }
  std::string internal_key(InternalKey(""));
  kvdk_assert(args.elems.size() == args.spaces.size(), "");
  for (size_t i = 0; i < args.elems.size(); i++) {
    DLList::WriteArgs wa(internal_key, args.elems[i], RecordType::ListElem,
                         RecordStatus::Normal, args.ts, args.spaces[i]);
    Status s = args.pos == ListPos::Front ? dl_list_.PushFront(wa)
                                          : dl_list_.PushBack(wa);
    kvdk_assert(s == Status::Ok, "Push back/front should always success");
    TEST_CRASH_POINT("List::PushN", "");
  }
  UpdateSize(args.elems.size());
  return Status::Ok;
}

Status List::PopN(const List::PopNArgs& args) {
  if (args.s != Status::Ok) {
    return args.s;
  }
  std::string internal_key(InternalKey(""));
  kvdk_assert(args.spaces.size() == args.to_pop.size(), "");
  for (size_t i = 0; i < args.to_pop.size(); i++) {
    DLList::WriteArgs wa(internal_key, "", RecordType::ListElem,
                         RecordStatus::Outdated, args.ts, args.spaces[i]);
    Status s = dl_list_.Update(wa, args.to_pop[i]);
    kvdk_assert(
        s == Status::Ok,
        "the whole list should be locked, so the update must be success");
    TEST_CRASH_POINT("List::PopN", "");
  }
  UpdateSize(-args.to_pop.size());
  return Status::Ok;
}
}  // namespace KVDK_NAMESPACE