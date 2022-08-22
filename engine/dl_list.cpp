#include "dl_list.hpp"

namespace KVDK_NAMESPACE {
std::unique_ptr<DLListRecordIterator> DLList::GetRecordIterator() {
  return std::unique_ptr<DLListRecordIterator>(
      new DLListRecordIterator(this, pmem_allocator_));
}

Status DLList::PushBack(const DLList::WriteArgs& args) {
  kvdk_assert(header_ != nullptr, "");
  Status s;
  do {
    s = InsertBefore(args, header_);
  } while (s == Status::Fail);
  return s;
}

Status DLList::PushFront(const DLList::WriteArgs& args) {
  kvdk_assert(header_ != nullptr, "");
  Status s;
  do {
    s = InsertAfter(args, header_);
  } while (s == Status::Fail);
  return s;
}

DLRecord* DLList::RemoveFront() {
  kvdk_assert(header_ != nullptr, "");
  while (true) {
    DLRecord* front =
        pmem_allocator_->offset2addr_checked<DLRecord>(header_->next);
    if (front == header_) {
      return nullptr;
    }
    // Maybe removed by another thread
    bool success = Remove(front);
    if (success) {
      return front;
    }
  }
}

DLRecord* DLList::RemoveBack() {
  kvdk_assert(header_ != nullptr, "");
  while (true) {
    DLRecord* back =
        pmem_allocator_->offset2addr_checked<DLRecord>(header_->prev);
    if (back == header_) {
      return nullptr;
    }
    // Maybe removed by another thread
    bool success = Remove(back);
    if (success) {
      return back;
    }
  }
}

Status DLList::InsertBetween(const DLList::WriteArgs& args, DLRecord* prev,
                             DLRecord* next) {
  auto ul = acquireInsertLock(prev);
  PMemOffsetType next_offset = pmem_allocator_->addr2offset_checked(next);
  PMemOffsetType prev_offset = pmem_allocator_->addr2offset_checked(prev);
  // Check if the linkage has changed before we successfully acquire lock.
  bool check_linkage = prev->next == next_offset && next->prev == prev_offset;
  if (!check_linkage) {
    return Status::Fail;
  }

  DLRecord* new_record = DLRecord::PersistDLRecord(
      pmem_allocator_->offset2addr_checked(args.space.offset), args.space.size,
      args.ts, args.type, args.status, kNullPMemOffset, prev_offset,
      next_offset, args.key, args.val);
  linkRecord(prev, next, new_record);

  return Status::Ok;
}

Status DLList::InsertAfter(const DLList::WriteArgs& args, DLRecord* prev) {
  return InsertBetween(
      args, prev, pmem_allocator_->offset2addr_checked<DLRecord>(prev->next));
}

Status DLList::InsertBefore(const DLList::WriteArgs& args, DLRecord* next) {
  return InsertBetween(
      args, pmem_allocator_->offset2addr_checked<DLRecord>(next->prev), next);
}

Status DLList::Update(const DLList::WriteArgs& args, DLRecord* current) {
  kvdk_assert(current != nullptr && equal_string_view(current->Key(), args.key),
              "");
  auto guard = acquireRecordLock(current);
  PMemOffsetType current_offset = pmem_allocator_->addr2offset_checked(current);
  PMemOffsetType prev_offset = current->prev;
  PMemOffsetType next_offset = current->next;
  DLRecord* prev = pmem_allocator_->offset2addr_checked<DLRecord>(prev_offset);
  DLRecord* next = pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
  if (next->prev != current_offset || prev->next != current_offset) {
    return Status::Fail;
  }
  DLRecord* new_record = DLRecord::PersistDLRecord(
      pmem_allocator_->offset2addr_checked(args.space.offset), args.space.size,
      args.ts, args.type, args.status, current_offset, prev_offset, next_offset,
      args.key, args.val);
  linkRecord(prev, next, new_record);
  return Status::Ok;
}

bool DLList::Replace(DLRecord* old_record, DLRecord* new_record) {
  bool ret = Replace(old_record, new_record, pmem_allocator_, lock_table_);
  if (ret && old_record == header_) {
    header_ = new_record;
  }
  return ret;
}

bool DLList::Remove(DLRecord* removing_record) {
  bool ret = Remove(removing_record, pmem_allocator_, lock_table_);
  return ret;
}

bool DLList::Replace(DLRecord* old_record, DLRecord* new_record,
                     PMEMAllocator* pmem_allocator, LockTable* lock_table) {
  auto guard = acquireRecordLock(old_record, pmem_allocator, lock_table);
  PMemOffsetType prev_offset = old_record->prev;
  PMemOffsetType next_offset = old_record->next;
  auto old_record_offset = pmem_allocator->addr2offset(old_record);
  DLRecord* prev = pmem_allocator->offset2addr_checked<DLRecord>(prev_offset);
  DLRecord* next = pmem_allocator->offset2addr_checked<DLRecord>(next_offset);
  bool on_list =
      prev != nullptr && next != nullptr && prev->next == old_record_offset;
  if (on_list) {
    if (prev_offset == old_record_offset && next_offset == old_record_offset) {
      // old record is the only record (the header) in the list, so we
      // make
      // new record point to itself and break linkage of the old one for
      // recovery
      kvdk_assert((new_record->GetRecordType() & CollectionType) &&
                      (old_record->GetRecordType() & CollectionType),
                  "Non-header record shouldn't be the only record in a list");
      linkRecord(new_record, new_record, new_record, pmem_allocator);
      auto new_record_offset = pmem_allocator->addr2offset(new_record);
      old_record->PersistPrevNT(new_record_offset);
    } else {
      new_record->prev = prev_offset;
      pmem_persist(&new_record->prev, sizeof(PMemOffsetType));
      new_record->next = next_offset;
      pmem_persist(&new_record->next, sizeof(PMemOffsetType));
      linkRecord(prev, next, new_record, pmem_allocator);
    }
  }
  return on_list;
}

bool DLList::Remove(DLRecord* removing_record, PMEMAllocator* pmem_allocator,
                    LockTable* lock_table) {
  auto guard = acquireRecordLock(removing_record, pmem_allocator, lock_table);
  PMemOffsetType removing_offset = pmem_allocator->addr2offset(removing_record);
  PMemOffsetType prev_offset = removing_record->prev;
  PMemOffsetType next_offset = removing_record->next;
  DLRecord* prev = pmem_allocator->offset2addr_checked<DLRecord>(prev_offset);
  DLRecord* next = pmem_allocator->offset2addr_checked<DLRecord>(next_offset);
  bool on_list =
      prev != nullptr && next != nullptr && prev->next == removing_offset;
  if (on_list) {
    // For repair in recovery due to crashes during pointers changing, we
    // should
    // first unlink deleting entry from next's prev.(It is the reverse process
    // of insertion)
    next->prev = prev_offset;
    pmem_persist(&next->prev, 8);
    TEST_SYNC_POINT("KVEngine::DLList::Remove::PersistNext'sPrev::After");
    prev->next = next_offset;
    pmem_persist(&prev->next, 8);
  }
  return on_list;
}
}  // namespace KVDK_NAMESPACE