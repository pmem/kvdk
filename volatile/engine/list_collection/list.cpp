/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "list.hpp"

namespace KVDK_NAMESPACE {
List::WriteResult List::SetExpireTime(ExpireTimeType expired_time,
                                      TimestampType timestamp) {
  WriteResult ret;
  DLRecord* header = HeaderRecord();
  SpaceEntry space = kv_allocator_->Allocate(
      DLRecord::RecordSize(header->Key(), header->Value()));
  if (space.size == 0) {
    ret.s = Status::MemoryOverflow;
    return ret;
  }
  DLRecord* data_record = DLRecord::ConstructDLRecord(
      kv_allocator_->offset2addr_checked(space.offset), space.size, timestamp,
      RecordType::ListHeader, RecordStatus::Normal,
      kv_allocator_->addr2offset_checked(header), header->prev, header->next,
      header->Key(), header->Value(), expired_time);
  bool success = dl_list_.Replace(header, data_record);
  kvdk_assert(success, "existing header should be linked on its list");
  ret.existing_record = header;
  ret.write_record = data_record;
  return ret;
}

List::WriteResult List::PushFront(const StringView& elem, TimestampType ts) {
  WriteResult ret;
  std::string internal_key(InternalKey(""));
  SpaceEntry space =
      kv_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
  if (space.size == 0) {
    ret.s = Status::MemoryOverflow;
    return ret;
  }

  DLList::WriteArgs args(internal_key, elem, RecordType::ListElem,
                         RecordStatus::Normal, ts, space);
  ret.s = dl_list_.PushFront(args);
  kvdk_assert(ret.s == Status::Ok, "Push front should alwasy success");
  ret.write_record = kv_allocator_->offset2addr_checked<DLRecord>(space.offset);
  live_records_.push_front(ret.write_record);
  return ret;
}

List::WriteResult List::PushBack(const StringView& elem, TimestampType ts) {
  WriteResult ret;
  std::string internal_key(InternalKey(""));
  SpaceEntry space =
      kv_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
  if (space.size == 0) {
    ret.s = Status::MemoryOverflow;
    return ret;
  }

  DLList::WriteArgs args(internal_key, elem, RecordType::ListElem,
                         RecordStatus::Normal, ts, space);
  ret.s = dl_list_.PushBack(args);
  kvdk_assert(ret.s == Status::Ok, "Push front should alwasy success");
  ret.write_record = kv_allocator_->offset2addr_checked<DLRecord>(space.offset);
  live_records_.push_back(ret.write_record);
  return ret;
}

List::WriteResult List::PopFront(TimestampType ts) {
  WriteResult ret;
  if (Size() == 0) {
    ret.s = Status::NotFound;
  } else {
    DLRecord* record = live_records_.front();
    kvdk_assert(record->GetRecordStatus() == RecordStatus::Normal, "");
    SpaceEntry space =
        kv_allocator_->Allocate(DLRecord::RecordSize(record->Key(), ""));
    if (space.size == 0) {
      ret.s = Status::MemoryOverflow;
      return ret;
    }
    DLList::WriteArgs args(record->Key(), "", RecordType::ListElem,
                           RecordStatus::Outdated, ts, space);
    while ((ret.s = dl_list_.Update(args, record)) != Status::Ok) {
      kvdk_assert(ret.s == Status::Fail, "");
    };
    ret.write_record =
        kv_allocator_->offset2addr_checked<DLRecord>(space.offset);
    ret.existing_record = record;
    live_records_.pop_front();
  }
  return ret;
}

List::WriteResult List::PopBack(TimestampType ts) {
  WriteResult ret;
  if (Size() == 0) {
    ret.s = Status::NotFound;
  } else {
    DLRecord* record = live_records_.back();
    kvdk_assert(record->GetRecordStatus() == RecordStatus::Normal, "");
    SpaceEntry space =
        kv_allocator_->Allocate(DLRecord::RecordSize(record->Key(), ""));
    if (space.size == 0) {
      ret.s = Status::MemoryOverflow;
      return ret;
    }
    DLList::WriteArgs args(record->Key(), "", RecordType::ListElem,
                           RecordStatus::Outdated, ts, space);
    while ((ret.s = dl_list_.Update(args, record)) != Status::Ok) {
      kvdk_assert(ret.s == Status::Fail, "");
    };
    ret.write_record =
        kv_allocator_->offset2addr_checked<DLRecord>(space.offset);
    ret.existing_record = record;
    live_records_.pop_back();
  }
  return ret;
}

List::WriteResult List::InsertBefore(const StringView& elem,
                                     const StringView& existing_elem,
                                     TimestampType ts) {
  WriteResult ret;
  auto iter = findLiveRecord(existing_elem);
  if (iter == live_records_.end()) {
    ret.s = Status::NotFound;
  } else {
    std::string internal_key(InternalKey(""));
    SpaceEntry space =
        kv_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
    if (space.size == 0) {
      ret.s = Status::MemoryOverflow;
      return ret;
    }
    DLList::WriteArgs args(internal_key, elem, RecordType::ListElem,
                           RecordStatus::Normal, ts, space);
    ret.s = dl_list_.InsertBefore(args, *iter);
    kvdk_assert(ret.s == Status::Ok,
                "the whole list is locked, so the insertion must be success");
    ret.write_record =
        kv_allocator_->offset2addr_checked<DLRecord>(space.offset);
    live_records_.insert(iter, ret.write_record);
  }
  return ret;
}

List::WriteResult List::InsertAfter(const StringView& elem,
                                    const StringView& existing_elem,
                                    TimestampType ts) {
  WriteResult ret;
  auto iter = findLiveRecord(existing_elem);
  if (iter == live_records_.end()) {
    ret.s = Status::NotFound;
  } else {
    std::string internal_key(InternalKey(""));
    SpaceEntry space =
        kv_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
    if (space.size == 0) {
      ret.s = Status::MemoryOverflow;
      return ret;
    }
    DLList::WriteArgs args(internal_key, elem, RecordType::ListElem,
                           RecordStatus::Normal, ts, space);
    ret.s = dl_list_.InsertAfter(args, *iter);
    kvdk_assert(ret.s == Status::Ok,
                "the whole list is locked, so the insertion must be success");
    ret.write_record =
        kv_allocator_->offset2addr_checked<DLRecord>(space.offset);
    live_records_.insert(iter + 1, ret.write_record);
  }
  return ret;
}

List::WriteResult List::InsertAt(const StringView& elem, long index,
                                 TimestampType ts) {
  WriteResult ret;
  size_t required_size = index < 0 ? std::abs(index) - 1 : index;
  if (required_size > Size()) {
    ret.s = Status::NotFound;
    return ret;
  }
  if (index < 0) {
    index = live_records_.size() + index;
  }
  auto iter = live_records_.begin() + index;
  DLRecord* next = *iter;
  std::string internal_key(InternalKey(""));
  kvdk_assert(next->GetRecordType() == RecordType::ListElem &&
                  next->GetRecordStatus() == RecordStatus::Normal,
              "");

  SpaceEntry space =
      kv_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
  if (space.size == 0) {
    ret.s = Status::MemoryOverflow;
    return ret;
  }
  DLList::WriteArgs args(internal_key, elem, RecordType::ListElem,
                         RecordStatus::Normal, ts, space);
  ret.s = dl_list_.InsertBefore(args, next);
  kvdk_assert(ret.s == Status::Ok,
              "the whole list is locked, so the insertion must be success");
  ret.write_record = kv_allocator_->offset2addr_checked<DLRecord>(space.offset);
  live_records_.insert(iter, ret.write_record);
  return ret;
}

List::WriteResult List::Erase(long index, TimestampType ts) {
  WriteResult ret;
  size_t required_size = index < 0 ? std::abs(index) - 1 : index;
  if (required_size > Size()) {
    ret.s = Status::NotFound;
    return ret;
  }
  if (index < 0) {
    index = live_records_.size() + index;
  }
  auto iter = live_records_.begin() + index;
  DLRecord* record = *iter;
  kvdk_assert(record->GetRecordType() == RecordType::ListElem &&
                  record->GetRecordStatus() == RecordStatus::Normal,
              "");

  SpaceEntry space =
      kv_allocator_->Allocate(DLRecord::RecordSize(record->Key(), ""));
  if (space.size == 0) {
    ret.s = Status::MemoryOverflow;
    return ret;
  }
  DLList::WriteArgs args(record->Key(), "", RecordType::ListElem,
                         RecordStatus::Outdated, ts, space);
  while ((ret.s = dl_list_.Update(args, record)) != Status::Ok) {
    kvdk_assert(ret.s == Status::Fail, "");
  };
  ret.existing_record = record;
  ret.write_record = kv_allocator_->offset2addr_checked<DLRecord>(space.offset);
  live_records_.erase(iter);
  return ret;
}

Status List::Front(std::string* elem) {
  if (Size() == 0) {
    return Status::NotFound;
  }
  DLRecord* record = live_records_.front();
  StringView sw = record->Value();
  elem->assign(sw.data(), sw.size());
  return Status::Ok;
}

Status List::Back(std::string* elem) {
  if (Size() == 0) {
    return Status::NotFound;
  }
  DLRecord* record = live_records_.back();
  StringView sw = record->Value();
  elem->assign(sw.data(), sw.size());
  return Status::Ok;
}

List::WriteResult List::Update(long index, const StringView& elem,
                               TimestampType ts) {
  WriteResult ret;
  size_t required_size = index < 0 ? std::abs(index) - 1 : index;
  if (required_size > Size()) {
    ret.s = Status::NotFound;
    return ret;
  }
  if (index < 0) {
    index = live_records_.size() + index;
  }
  auto iter = live_records_.begin() + index;
  DLRecord* record = *iter;
  kvdk_assert(record->GetRecordType() == RecordType::ListElem &&
                  record->GetRecordStatus() == RecordStatus::Normal,
              "");
  std::string internal_key(InternalKey(""));
  SpaceEntry space =
      kv_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
  if (space.size == 0) {
    ret.s = Status::MemoryOverflow;
    return ret;
  }
  DLList::WriteArgs args(internal_key, elem, RecordType::ListElem,
                         RecordStatus::Normal, ts, space);

  while ((ret.s = dl_list_.Update(args, record)) != Status::Ok) {
    kvdk_assert(ret.s == Status::Fail, "");
  }
  ret.existing_record = record;
  ret.write_record = kv_allocator_->offset2addr_checked<DLRecord>(space.offset);
  *iter = ret.write_record;
  return ret;
}

List::PushNArgs List::PreparePushN(ListPos pos,
                                   const std::vector<StringView>& elems,
                                   TimestampType ts) {
  PushNArgs args;
  args.pos = pos;
  args.ts = ts;
  if (elems.size() > 0) {
    std::string internal_key(InternalKey(""));
    for (auto& elem : elems) {
      SpaceEntry space =
          kv_allocator_->Allocate(DLRecord::RecordSize(internal_key, elem));
      if (space.size == 0) {
        GlobalLogger.Error("Try allocate %lu error\n",
                           DLRecord::RecordSize(internal_key, elem));
        for (auto& sp : args.spaces) {
          kv_allocator_->Free(sp);
        }
        args.s = Status::MemoryOverflow;
        break;
      }
      args.spaces.emplace_back(space);
    }
    args.elems = elems;
  }
  args.s = Status::Ok;
  return args;
}

List::PopNArgs List::PreparePopN(ListPos pos, size_t n, TimestampType ts,
                                 std::vector<std::string>* elems) {
  size_t nn = n;
  PopNArgs args;
  args.timestamp_ = ts;
  if (Size() > 0) {
    auto iter =
        pos == ListPos::Front ? live_records_.begin() : live_records_.end() - 1;
    while (nn > 0) {
      DLRecord* record = *iter;
      SpaceEntry space =
          kv_allocator_->Allocate(DLRecord::RecordSize(record->Key(), ""));
      if (space.size == 0) {
        for (auto& sp : args.spaces) {
          kv_allocator_->Free(sp);
        }
        args.s = Status::MemoryOverflow;
        return args;
      }

      if (elems) {
        StringView sw = record->Value();
        elems->emplace_back(sw.data(), sw.size());
      }
      args.spaces.emplace_back(space);
      args.to_pop_.emplace_back(iter);
      nn--;
      if (pos == ListPos::Front) {
        iter++;
        if (iter == live_records_.end()) {
          break;
        }
      } else {
        if (iter == live_records_.begin()) {
          break;
        }
        iter--;
      }
    }
    args.s = Status::Ok;
  }
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
    Status s;
    if (args.pos == ListPos::Front) {
      s = dl_list_.PushFront(wa);
      live_records_.push_front(
          kv_allocator_->offset2addr_checked<DLRecord>(wa.space.offset));
    } else {
      s = dl_list_.PushBack(wa);
      live_records_.push_back(
          kv_allocator_->offset2addr_checked<DLRecord>(wa.space.offset));
    }
    kvdk_assert(s == Status::Ok, "Push back/front should always success");
    TEST_CRASH_POINT("List::PushN", "");
  }
  return Status::Ok;
}

Status List::PopN(const List::PopNArgs& args) {
  if (args.s != Status::Ok) {
    return args.s;
  }
  std::string internal_key(InternalKey(""));
  kvdk_assert(args.spaces.size() == args.to_pop_.size(), "");
  for (size_t i = 0; i < args.to_pop_.size(); i++) {
    DLList::WriteArgs wa(internal_key, "", RecordType::ListElem,
                         RecordStatus::Outdated, args.timestamp_,
                         args.spaces[i]);
    Status s;
    while ((s = dl_list_.Update(wa, *args.to_pop_[i])) != Status::Ok) {
      kvdk_assert(s == Status::Fail, "");
    }
    live_records_.erase(args.to_pop_[i]);
    TEST_CRASH_POINT("List::PopN", "");
  }
  return Status::Ok;
}

void List::Destroy() {
  std::vector<SpaceEntry> to_free;
  DLRecord* header = HeaderRecord();
  if (header) {
    DLRecord* to_destroy = nullptr;
    do {
      to_destroy = kv_allocator_->offset2addr_checked<DLRecord>(header->next);
      if (dl_list_.Remove(to_destroy)) {
        to_destroy->Destroy();
        to_free.emplace_back(kv_allocator_->addr2offset_checked(to_destroy),
                             to_destroy->GetRecordSize());
        if (to_free.size() > kMaxCachedOldRecords) {
          kv_allocator_->BatchFree(to_free);
          to_free.clear();
        }
      }
    } while (to_destroy != header);
  }
  kv_allocator_->BatchFree(to_free);
}

void List::DestroyAll() {
  std::vector<SpaceEntry> to_free;
  DLRecord* header = HeaderRecord();
  if (header) {
    DLRecord* to_destroy = nullptr;
    do {
      to_destroy = kv_allocator_->offset2addr_checked<DLRecord>(header->next);
      if (dl_list_.Remove(to_destroy)) {
        auto old_record =
            kv_allocator_->offset2addr<DLRecord>(to_destroy->old_version);
        while (old_record) {
          auto old_version = old_record->old_version;
          old_record->Destroy();
          to_free.emplace_back(kv_allocator_->addr2offset_checked(old_record),
                               old_record->GetRecordSize());
          old_record = kv_allocator_->offset2addr<DLRecord>(old_version);
        }

        to_destroy->Destroy();
        to_free.emplace_back(kv_allocator_->addr2offset_checked(to_destroy),
                             to_destroy->GetRecordSize());
        if (to_free.size() > kMaxCachedOldRecords) {
          kv_allocator_->BatchFree(to_free);
          to_free.clear();
        }
      }
    } while (to_destroy != header);
  }
  kv_allocator_->BatchFree(to_free);
}
}  // namespace KVDK_NAMESPACE