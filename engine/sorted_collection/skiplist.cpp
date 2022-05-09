/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "skiplist.hpp"

#include <libpmem.h>

#include <algorithm>
#include <future>

#include "../kv_engine.hpp"
#include "../utils/codec.hpp"
#include "../utils/sync_point.hpp"

namespace KVDK_NAMESPACE {

StringView SkiplistNode::UserKey() { return Skiplist::UserKey(this); }

uint64_t SkiplistNode::SkiplistID() { return Skiplist::SkiplistID(this); }

Skiplist::~Skiplist() {
  destroyNodes();
  std::lock_guard<SpinMutex> lg_a(pending_delete_nodes_spin_);
  for (SkiplistNode* node : pending_deletion_nodes_) {
    SkiplistNode::DeleteNode(node);
  }
  pending_deletion_nodes_.clear();
  std::lock_guard<SpinMutex> lg_b(obsolete_nodes_spin_);
  for (SkiplistNode* node : obsolete_nodes_) {
    SkiplistNode::DeleteNode(node);
  }
  obsolete_nodes_.clear();
}

Skiplist::Skiplist(DLRecord* h, const std::string& name, CollectionIDType id,
                   Comparator comparator, PMEMAllocator* pmem_allocator,
                   HashTable* hash_table, LockTable* lock_table,
                   bool index_with_hashtable)
    : Collection(name, id),
      size_(0),
      comparator_(comparator),
      pmem_allocator_(pmem_allocator),
      hash_table_(hash_table),
      record_locks_(lock_table),
      index_with_hashtable_(index_with_hashtable),
      deleted_(false) {
  header_ = SkiplistNode::NewNode(name, h, kMaxHeight);
  for (uint8_t i = 1; i <= kMaxHeight; i++) {
    header_->RelaxedSetNext(i, nullptr);
  }
};

Status Skiplist::SetExpireTime(ExpireTimeType expired_time) {
  header_->record->expired_time = expired_time;
  pmem_persist(&header_->record->expired_time, sizeof(ExpireTimeType));
  return Status::Ok;
}

Skiplist::WriteResult Skiplist::SetExpireTime(ExpireTimeType expired_time,
                                              TimeStampType timestamp) {
  WriteResult ret;
  DLRecord* header = HeaderRecord();
  auto request_size =
      sizeof(DLRecord) + header->Key().size() + header->Value().size();
  SpaceEntry space_entry = pmem_allocator_->Allocate(request_size);
  if (space_entry.size == 0) {
    ret.s = Status::PmemOverflow;
    return ret;
  }
  DLRecord* pmem_record = DLRecord::PersistDLRecord(
      pmem_allocator_->offset2addr_checked(space_entry.offset),
      space_entry.size, timestamp, SortedHeader,
      pmem_allocator_->addr2offset_checked(header), header->prev, header->next,
      header->Key(), header->Value(), expired_time);
  Skiplist::Replace(header, pmem_record, HeaderNode(), pmem_allocator_,
                    record_locks_);
  ret.existing_record = header;
  ret.dram_node = HeaderNode();
  ret.write_record = pmem_record;
  return ret;
}

Status Skiplist::MarkAsDeleted() {
  deleted_ = false;
  header_->record->entry.meta.type = RecordType::SortedHeaderDelete;
  pmem_persist(&header_->record->entry.meta.type, sizeof(RecordType));
  return Status::Ok;
}

void Skiplist::SeekNode(const StringView& key, SkiplistNode* start_node,
                        uint8_t start_height, uint8_t end_height,
                        Splice* result_splice) {
  std::vector<SkiplistNode*> to_delete;
  assert(start_node->height >= start_height && end_height >= 1);
  SkiplistNode* prev = start_node;
  PointerWithTag<SkiplistNode, SkiplistNode::NodeStatus> next;
  for (uint8_t i = start_height; i >= end_height; i--) {
    while (1) {
      next = prev->Next(i);
      // prev is logically deleted, roll back to prev height.
      if (next.GetTag() == SkiplistNode::NodeStatus::Deleted) {
        if (i < start_height) {
          i++;
          prev = result_splice->prevs[i];
        } else if (prev != start_node) {
          // re-seek from this node for start height
          i = start_height;
          prev = start_node;
        } else {
          // this node has been deleted, so seek from header
          kvdk_assert(result_splice->seeking_list != nullptr,
                      "skiplist must be set for seek operation!");
          return SeekNode(key, result_splice->seeking_list->HeaderNode(),
                          kMaxHeight, end_height, result_splice);
        }
        continue;
      }

      if (next.Null()) {
        result_splice->nexts[i] = nullptr;
        result_splice->prevs[i] = prev;
        break;
      }

      // Physically remove deleted "next" nodes from skiplist
      auto next_next = next->Next(i);
      if (next_next.GetTag() == SkiplistNode::NodeStatus::Deleted) {
        if (prev->CASNext(i, next, next_next.RawPointer())) {
          if (--next->valid_links == 0) {
            to_delete.push_back(next.RawPointer());
          }
        }
        // if prev is marked deleted before cas, cas will be failed, and prev
        // will be roll back in next round
        continue;
      }

      DLRecord* next_pmem_record = next->record;
      int cmp = compare(key, next->UserKey());
      // pmem record maybe updated before comparing string, then the compare
      // result will be invalid, so we need to do double check
      if (next->record != next_pmem_record) {
        continue;
      }

      if (cmp > 0) {
        prev = next.RawPointer();
      } else {
        result_splice->nexts[i] = next.RawPointer();
        result_splice->prevs[i] = prev;
        break;
      }
    }
  }
  if (to_delete.size() > 0) {
    result_splice->seeking_list->obsoleteNodes(to_delete);
  }
}

void Skiplist::linkDLRecord(DLRecord* prev, DLRecord* next, DLRecord* linking,
                            PMEMAllocator* pmem_allocator) {
  uint64_t inserting_record_offset = pmem_allocator->addr2offset(linking);
  prev->next = inserting_record_offset;
  pmem_persist(&prev->next, 8);
  TEST_SYNC_POINT("KVEngine::Skiplist::InsertDLRecord::UpdatePrev");
  next->prev = inserting_record_offset;
  pmem_persist(&next->prev, 8);
}

void Skiplist::Seek(const StringView& key, Splice* result_splice) {
  result_splice->seeking_list = this;
  SeekNode(key, header_, header_->Height(), 1, result_splice);
  assert(result_splice->prevs[1] != nullptr);
  DLRecord* prev_record = result_splice->prevs[1]->record;
  DLRecord* next_record = nullptr;
  while (1) {
    next_record = pmem_allocator_->offset2addr<DLRecord>(prev_record->next);
    if (next_record == HeaderRecord()) {
      break;
    }

    if (next_record == nullptr) {
      return Seek(key, result_splice);
    }
    int cmp = compare(key, UserKey(next_record));
    // pmem record maybe updated before comparing string, then the comparing
    // result will be invalid, so we need to do double check
    //
    // Notice: In current implementation with the guard of snapshot mechanism,
    // the record won't be freed during this operation, so this should not be
    // happen
    // if (!validateDLRecord(next_record)) {
    // return Seek(key, result_splice);
    // }

    if (cmp > 0) {
      prev_record = next_record;
    } else {
      break;
    }
  }
  result_splice->next_pmem_record = next_record;
  result_splice->prev_pmem_record = prev_record;
}

Status Skiplist::CheckIndex() {
  Splice splice(this);
  splice.prev_pmem_record = header_->record;
  for (uint8_t i = 1; i <= kMaxHeight; i++) {
    splice.prevs[i] = header_;
  }

  while (true) {
    DLRecord* next_record = pmem_allocator_->offset2addr_checked<DLRecord>(
        splice.prev_pmem_record->next);
    if (next_record == header_->record) {
      break;
    }
    SkiplistNode* next_node = splice.prevs[1]->RelaxedNext(1).RawPointer();
    if (IndexWithHashtable()) {
      StringView key = next_record->Key();
      auto ret = hash_table_->Lookup<false>(key, next_record->entry.meta.type);
      if (ret.s != Status::Ok) {
        GlobalLogger.Error(
            "Check skiplist index error: record not exist in hash table\n");
        return Status::Abort;
      }

      if (ret.entry.GetIndexType() == PointerType::SkiplistNode) {
        if (ret.entry.GetIndex().skiplist_node != next_node) {
          GlobalLogger.Error(
              "Check skiplist index error: Dram node miss-match with hash "
              "table\n");
          return Status::Abort;
        }
      } else {
        if (ret.entry.GetIndex().dl_record != next_record) {
          GlobalLogger.Error(
              "Check skiplist index error: Dlrecord miss-match with hash "
              "table\n");
          return Status::Abort;
        }
      }
    }

    // Check dram linkage
    if (next_node && next_node->record == next_record) {
      for (uint8_t i = 1; i <= next_node->Height(); i++) {
        if (splice.prevs[i]->RelaxedNext(i).RawPointer() != next_node) {
          GlobalLogger.Error(
              "Check skiplist index error: node linkage error\n");
          return Status::Abort;
        }
        splice.prevs[i] = next_node;
      }
    }
    splice.prev_pmem_record = next_record;
  }

  return Status::Ok;
}

LockTable::GuardType Skiplist::lockRecordPosition(const DLRecord* record,
                                                  PMEMAllocator* pmem_allocator,
                                                  LockTable* lock_table) {
  while (1) {
    PMemOffsetType record_offset = pmem_allocator->addr2offset_checked(record);
    PMemOffsetType prev_offset = record->prev;
    PMemOffsetType next_offset = record->next;
    DLRecord* prev = pmem_allocator->offset2addr_checked<DLRecord>(prev_offset);
    DLRecord* next = pmem_allocator->offset2addr<DLRecord>(next_offset);

    auto guard = lock_table->MultiGuard({recordHash(prev), recordHash(record)});

    // Check if the list has changed before we successfully acquire lock.
    if (record->prev != prev_offset || prev->next != record_offset ||
        record->next != next_offset || next->prev != record_offset) {
      continue;
    }

    kvdk_assert(record->prev == prev_offset, "");
    kvdk_assert(record->next == next_offset, "");
    kvdk_assert(next->prev == record_offset, "");
    kvdk_assert(prev->next == record_offset, "");

    return guard;
  }
}

bool Skiplist::lockInsertPosition(const StringView& inserting_key,
                                  DLRecord* prev_record, DLRecord* next_record,
                                  LockTable::ULockType* prev_record_lock) {
  PMemOffsetType prev_offset =
      pmem_allocator_->addr2offset_checked(prev_record);
  PMemOffsetType next_offset =
      pmem_allocator_->addr2offset_checked(next_record);
  *prev_record_lock = record_locks_->AcquireLock(recordHash(prev_record));

  // Check if the linkage has changed before we successfully acquire lock.
  auto check_linkage = [&]() {
    return prev_record->next == next_offset && next_record->prev == prev_offset;
  };
  // Check id and order as prev and next may be both freed, then inserted
  // to another position while keep linkage, before we lock them
  // For example:
  // Before lock:
  // this skip list: record1 -> "prev" -> "next" -> record2
  // After lock:
  // this skip list: "new record reuse prev" -> "new record reuse next" ->
  // record1 -> record2
  // or:
  // this skip list: record1 -> record2
  // another skip list:"new record reuse prev" -> "new record reuse next" ->
  // In this case, inserting record will be mis-inserted between "new record
  // reuse prev" and "new record reuse next"
  //
  // Notice: In current implementation with the guard of snapshot mechanism, the
  // prev and next won't be freed during this operation, so id and order won't
  // be changed anymore. We only check id and order in debug mode
  auto check_id = [&]() {
    return SkiplistID(next_record) == ID() && SkiplistID(prev_record) == ID();
  };

  auto check_order = [&]() {
    bool res =
        /*check next*/ (next_record == header_->record ||
                        compare(inserting_key, UserKey(next_record)) <= 0) &&
        /*check prev*/ (prev_record == header_->record ||
                        compare(inserting_key, UserKey(prev_record)) > 0);
    return res;
  };
  if (!check_linkage()) {
    *prev_record_lock = std::unique_lock<SpinMutex>();
    return false;
  }

  kvdk_assert(check_id(),
              "Check id of prev and next failed during skiplist insert\n");
  kvdk_assert(check_order(),
              "Check key order of prev and next failed during skiplist "
              "insert\n");

  assert(prev_record->next == next_offset);
  assert(next_record->prev == prev_offset);

  return true;
}

Skiplist::WriteResult Skiplist::Delete(const StringView& key,
                                       TimeStampType timestamp) {
  Skiplist::WriteResult ret;
  if (IndexWithHashtable()) {
    ret = deleteImplWithHash(key, timestamp);
  } else {
    ret = deleteImplNoHash(key, timestamp);
  }
  if (ret.existing_record != nullptr) {
    UpdateSize(-1);
  }
  return ret;
}

Skiplist::WriteResult Skiplist::Set(const StringView& key,
                                    const StringView& value,
                                    TimeStampType timestamp) {
  Skiplist::WriteResult ret;
  if (IndexWithHashtable()) {
    ret = setImplWithHash(key, value, timestamp);
  } else {
    ret = setImplNoHash(key, value, timestamp);
  }
  if (ret.existing_record == nullptr) {
    UpdateSize(1);
  }
  return ret;
}

bool Skiplist::Replace(DLRecord* old_record, DLRecord* new_record,
                       SkiplistNode* dram_node, PMEMAllocator* pmem_allocator,
                       LockTable* lock_table) {
  auto guard = lockRecordPosition(old_record, pmem_allocator, lock_table);
  PMemOffsetType prev_offset = old_record->prev;
  PMemOffsetType next_offset = old_record->next;
  DLRecord* prev = pmem_allocator->offset2addr_checked<DLRecord>(prev_offset);
  DLRecord* next = pmem_allocator->offset2addr_checked<DLRecord>(next_offset);

  kvdk_assert(prev->next == pmem_allocator->addr2offset(old_record),
              "Bad prev linkage in Skiplist::Replace");
  kvdk_assert(next->prev == pmem_allocator->addr2offset(old_record),
              "Bad next linkage in Skiplist::Replace");
  new_record->prev = prev_offset;
  pmem_persist(&new_record->prev, sizeof(PMemOffsetType));
  new_record->next = next_offset;
  pmem_persist(&new_record->next, sizeof(PMemOffsetType));
  Skiplist::linkDLRecord(prev, next, new_record, pmem_allocator);
  if (dram_node != nullptr) {
    kvdk_assert(dram_node->record == old_record,
                "Dram node not belong to old record in Skiplist::Replace");
    dram_node->record = new_record;
  }
  return true;
}

bool Skiplist::Purge(DLRecord* purging_record, SkiplistNode* dram_node,
                     PMEMAllocator* pmem_allocator, LockTable* lock_table) {
  auto guard = lockRecordPosition(purging_record, pmem_allocator, lock_table);

  // Modify linkage to drop deleted record
  PMemOffsetType purging_offset = pmem_allocator->addr2offset(purging_record);
  PMemOffsetType prev_offset = purging_record->prev;
  PMemOffsetType next_offset = purging_record->next;
  DLRecord* prev = pmem_allocator->offset2addr_checked<DLRecord>(prev_offset);
  DLRecord* next = pmem_allocator->offset2addr_checked<DLRecord>(next_offset);
  kvdk_assert(prev->next == purging_offset, "");
  kvdk_assert(next->prev == purging_offset, "");
  // For repair in recovery due to crashes during pointers changing, we should
  // first unlink deleting entry from next's prev.(It is the reverse process
  // of insertion)
  next->prev = prev_offset;
  pmem_persist(&next->prev, 8);
  TEST_SYNC_POINT("KVEngine::Skiplist::Delete::PersistNext'sPrev::After");
  prev->next = next_offset;
  pmem_persist(&prev->next, 8);
  purging_record->Destroy();

  if (dram_node) {
    dram_node->MarkAsRemoved();
  }
  return true;
}

SkiplistNode* Skiplist::NewNodeBuild(DLRecord* pmem_record) {
  SkiplistNode* dram_node = nullptr;
  auto height = Skiplist::randomHeight();
  if (height > 0) {
    StringView user_key = UserKey(pmem_record);
    dram_node = SkiplistNode::NewNode(user_key, pmem_record, height);
    if (dram_node == nullptr) {
      GlobalLogger.Error("Memory overflow in Skiplist::NewNodeBuild\n");
    }
  }
  return dram_node;
}

std::string Skiplist::EncodeSortedCollectionValue(
    CollectionIDType id, const SortedCollectionConfigs& s_configs) {
  std::string value_str;

  AppendUint64(&value_str, id);
  AppendFixedString(&value_str, s_configs.comparator_name);
  AppendUint32(&value_str, s_configs.index_with_hashtable);

  return value_str;
}

Status Skiplist::DecodeSortedCollectionValue(
    StringView value_str, CollectionIDType& id,
    SortedCollectionConfigs& s_configs) {
  if (!FetchUint64(&value_str, &id)) {
    return Status::Abort;
  }
  if (!FetchFixedString(&value_str, &s_configs.comparator_name)) {
    return Status::Abort;
  }
  if (!FetchUint32(&value_str, (uint32_t*)&s_configs.index_with_hashtable)) {
    return Status::Abort;
  }

  return Status::Ok;
}

Status Skiplist::Get(const StringView& key, std::string* value) {
  if (!IndexWithHashtable()) {
    Splice splice(this);
    Seek(key, &splice);
    if (equal_string_view(key, UserKey(splice.next_pmem_record)) &&
        splice.next_pmem_record->entry.meta.type == SortedElem) {
      value->assign(splice.next_pmem_record->Value().data(),
                    splice.next_pmem_record->Value().size());
      return Status::Ok;
    } else {
      return Status::NotFound;
    }
  } else {
    std::string internal_key = InternalKey(key);
    auto ret =
        hash_table_->Lookup<false>(internal_key, SortedElem | SortedElemDelete);
    if (ret.s != Status::Ok) {
      return Status::NotFound;
    }

    DLRecord* pmem_record;
    switch (ret.entry.GetIndexType()) {
      case PointerType::SkiplistNode: {
        pmem_record = ret.entry.GetIndex().skiplist_node->record;
        break;
      }
      case PointerType::DLRecord: {
        pmem_record = ret.entry.GetIndex().dl_record;
        break;
      }
      default: {
        GlobalLogger.Error(
            "Wrong hash index type while search sorted data in hash table\n");
        return Status::Abort;
      }
    }

    if (pmem_record->entry.meta.type == SortedElemDelete) {
      return Status::NotFound;
    } else {
      assert(pmem_record->entry.meta.type == SortedElem);
      value->assign(pmem_record->Value().data(), pmem_record->Value().size());
      return Status::Ok;
    }
  }
}

Skiplist::WriteResult Skiplist::deleteImplNoHash(const StringView& key,
                                                 TimeStampType timestamp) {
  WriteResult ret;
  std::string internal_key(InternalKey(key));
  Splice splice(this);
  Seek(key, &splice);
  bool found = (splice.next_pmem_record->entry.meta.type &
                (SortedElem | SortedElemDelete)) &&
               equal_string_view(splice.next_pmem_record->Key(), internal_key);

  if (!found) {
    return ret;
  }

  if (splice.next_pmem_record->entry.meta.type == SortedElemDelete) {
    return ret;
  }

  ret.existing_record = splice.next_pmem_record;
  if (splice.nexts[1] && splice.nexts[1]->record == ret.existing_record) {
    ret.dram_node = splice.nexts[1];
  }

  // try to write delete record
  auto guard = lockRecordPosition(ret.existing_record);

  auto request_size = internal_key.size() + sizeof(DLRecord);
  auto space_to_write = pmem_allocator_->Allocate(request_size);
  if (space_to_write.size == 0) {
    ret.s = Status::PmemOverflow;
    return ret;
  }

  assert(ret.existing_record->entry.meta.timestamp < timestamp);
  PMemOffsetType existing_offset =
      pmem_allocator_->addr2offset_checked(ret.existing_record);
  PMemOffsetType prev_offset = ret.existing_record->prev;
  DLRecord* prev_record =
      pmem_allocator_->offset2addr_checked<DLRecord>(prev_offset);
  PMemOffsetType next_offset = ret.existing_record->next;
  DLRecord* next_record =
      pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
  DLRecord* delete_record = DLRecord::PersistDLRecord(
      pmem_allocator_->offset2addr(space_to_write.offset), space_to_write.size,
      timestamp, SortedElemDelete, existing_offset, prev_offset, next_offset,
      internal_key, "");
  ret.write_record = delete_record;

  kvdk_assert(prev_record->next == existing_offset,
              "wrong linkage in skiplist delete after acquiring lock");
  kvdk_assert(next_record->prev == existing_offset,
              "wrong linkage in skiplist delete after acquiring lock");

  linkDLRecord(prev_record, next_record, delete_record);

  if (ret.dram_node) {
    ret.dram_node->record = delete_record;
  }
  return ret;
}

Skiplist::WriteResult Skiplist::deleteImplWithHash(const StringView& key,
                                                   TimeStampType timestamp) {
  WriteResult ret;
  assert(IndexWithHashtable());
  std::string internal_key(InternalKey(key));
  auto lookup_result =
      hash_table_->Lookup<false>(internal_key, SortedElem | SortedElemDelete);
  ret.s = lookup_result.s;

  switch (ret.s) {
    case Status::NotFound: {
      ret.s = Status::Ok;
      return ret;
    }
    case Status::Ok: {
      if (lookup_result.entry.GetRecordType() == SortedElemDelete) {
        return ret;
      }

      if (lookup_result.entry.GetIndexType() == PointerType::SkiplistNode) {
        ret.dram_node = lookup_result.entry.GetIndex().skiplist_node;
        ret.existing_record = ret.dram_node->record;
      } else {
        ret.dram_node = nullptr;
        assert(lookup_result.entry.GetIndexType() == PointerType::DLRecord);
        ret.existing_record = lookup_result.entry.GetIndex().dl_record;
      }
      assert(timestamp > ret.existing_record->entry.meta.timestamp);

      // Try to write delete record
      auto guard = lockRecordPosition(ret.existing_record);

      auto space_to_write =
          pmem_allocator_->Allocate(internal_key.size() + sizeof(DLRecord));
      if (space_to_write.size == 0) {
        ret.s = Status::PmemOverflow;
        return ret;
      }

      PMemOffsetType prev_offset = ret.existing_record->prev;
      DLRecord* prev_record =
          pmem_allocator_->offset2addr_checked<DLRecord>(prev_offset);
      PMemOffsetType next_offset = ret.existing_record->next;
      DLRecord* next_record =
          pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
      PMemOffsetType existing_offset =
          pmem_allocator_->addr2offset_checked(ret.existing_record);
      DLRecord* delete_record = DLRecord::PersistDLRecord(
          pmem_allocator_->offset2addr(space_to_write.offset),
          space_to_write.size, timestamp, SortedElemDelete, existing_offset,
          prev_offset, next_offset, internal_key, "");
      linkDLRecord(prev_record, next_record, delete_record);
      ret.write_record = delete_record;
      ret.hash_entry_ptr = lookup_result.entry_ptr;

      break;
    }
    default:
      std::abort();  // never reach
  }

  // until here, new record is already inserted to list
  assert(ret.write_record != nullptr);
  if (ret.dram_node == nullptr) {
    hash_table_->Insert(lookup_result.hint, lookup_result.entry_ptr,
                        SortedElemDelete, ret.write_record,
                        PointerType::DLRecord);
  } else {
    ret.dram_node->record = ret.write_record;
    hash_table_->Insert(lookup_result.hint, lookup_result.entry_ptr,
                        SortedElemDelete, ret.dram_node,
                        PointerType::SkiplistNode);
  }

  return ret;
}

Skiplist::WriteResult Skiplist::setImplWithHash(const StringView& key,
                                                const StringView& value,
                                                TimeStampType timestamp) {
  WriteResult ret;
  assert(IndexWithHashtable());
  std::string internal_key(InternalKey(key));
  auto hint = hash_table_->GetHint(internal_key);
  HashEntry* entry_ptr = nullptr;
  HashEntry hash_entry;
  ret.s = hash_table_->SearchForWrite(hint, internal_key,
                                      SortedElem | SortedElemDelete, &entry_ptr,
                                      &hash_entry, nullptr);

  switch (ret.s) {
    case Status::Ok: {
      if (hash_entry.GetIndexType() == PointerType::SkiplistNode) {
        ret.dram_node = hash_entry.GetIndex().skiplist_node;
        ret.existing_record = ret.dram_node->record;
      } else {
        ret.dram_node = nullptr;
        assert(hash_entry.GetIndexType() == PointerType::DLRecord);
        ret.existing_record = hash_entry.GetIndex().dl_record;
      }
      assert(timestamp > ret.existing_record->entry.meta.timestamp);

      // Try to write delete record
      auto guard = lockRecordPosition(ret.existing_record);

      auto request_size = internal_key.size() + value.size() + sizeof(DLRecord);
      auto space_to_write = pmem_allocator_->Allocate(request_size);
      if (space_to_write.size == 0) {
        ret.s = Status::PmemOverflow;
        return ret;
      }

      PMemOffsetType prev_offset = ret.existing_record->prev;
      DLRecord* prev_record =
          pmem_allocator_->offset2addr_checked<DLRecord>(prev_offset);
      PMemOffsetType next_offset = ret.existing_record->next;
      DLRecord* next_record =
          pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
      PMemOffsetType existing_offset =
          pmem_allocator_->addr2offset_checked(ret.existing_record);
      DLRecord* new_record = DLRecord::PersistDLRecord(
          pmem_allocator_->offset2addr(space_to_write.offset),
          space_to_write.size, timestamp, SortedElem, existing_offset,
          prev_offset, next_offset, internal_key, value);
      ret.write_record = new_record;
      ret.hash_entry_ptr = entry_ptr;
      linkDLRecord(prev_record, next_record, new_record);

      break;
    }
    case Status::NotFound: {
      ret = setImplNoHash(key, value, timestamp);
      if (ret.s != Status::Ok) {
        return ret;
      }

      break;
    }
    case Status::MemoryOverflow: {
      return ret;
    }
    default:
      std::abort();  // never should reach
  }

  // until here, new record is already inserted to list
  assert(ret.write_record != nullptr);
  if (ret.dram_node == nullptr) {
    hash_table_->Insert(hint, entry_ptr, SortedElem, ret.write_record,
                        PointerType::DLRecord);
  } else {
    ret.dram_node->record = ret.write_record;
    hash_table_->Insert(hint, entry_ptr, SortedElem, ret.dram_node,
                        PointerType::SkiplistNode);
  }

  return ret;
}

Skiplist::WriteResult Skiplist::setImplNoHash(const StringView& key,
                                              const StringView& value,
                                              TimeStampType timestamp) {
  WriteResult ret;
  std::string internal_key(InternalKey(key));
  DLRecord* prev_record;
  DLRecord* next_record;
  bool key_exist;
  LockTable::ULockType insert_guard;
  LockTable::GuardType update_guard;

seek_write_position:
  Splice splice(this);
  Seek(key, &splice);

  key_exist = !IndexWithHashtable() /* a hash indexed skiplist call this
                                        function only if key not exist */
              && (splice.next_pmem_record->entry.meta.type &
                  (SortedElem | SortedElemDelete)) &&
              equal_string_view(splice.next_pmem_record->Key(), internal_key);

  if (key_exist) {
    ret.existing_record = splice.next_pmem_record;
    if (splice.nexts[1] && splice.nexts[1]->record == ret.existing_record) {
      ret.dram_node = splice.nexts[1];
    }

    update_guard = lockRecordPosition(ret.existing_record);
    prev_record = pmem_allocator_->offset2addr_checked<DLRecord>(
        ret.existing_record->prev);
    next_record = pmem_allocator_->offset2addr_checked<DLRecord>(
        ret.existing_record->next);
  } else {
    ret.existing_record = nullptr;
    if (!lockInsertPosition(key, splice.prev_pmem_record,
                            splice.next_pmem_record, &insert_guard)) {
      goto seek_write_position;
    }
    next_record = splice.next_pmem_record;
    prev_record = splice.prev_pmem_record;
  }

  auto request_size = internal_key.size() + value.size() + sizeof(DLRecord);
  auto space_to_write = pmem_allocator_->Allocate(request_size);
  if (space_to_write.size == 0) {
    ret.s = Status::PmemOverflow;
    return ret;
  }

  uint64_t prev_offset = pmem_allocator_->addr2offset_checked(prev_record);
  uint64_t next_offset = pmem_allocator_->addr2offset_checked(next_record);
  DLRecord* new_record = DLRecord::PersistDLRecord(
      pmem_allocator_->offset2addr(space_to_write.offset), space_to_write.size,
      timestamp, SortedElem, pmem_allocator_->addr2offset(ret.existing_record),
      prev_offset, next_offset, internal_key, value);
  ret.write_record = new_record;
  // link new record to PMem
  linkDLRecord(prev_record, next_record, new_record);
  if (!key_exist) {
    // create dram node for new record
    ret.dram_node = Skiplist::NewNodeBuild(new_record);
    if (ret.dram_node != nullptr) {
      auto height = ret.dram_node->Height();
      for (int i = 1; i <= height; i++) {
        while (1) {
          auto now_next = splice.prevs[i]->Next(i);
          // if next has been changed or been deleted, re-compute
          if (now_next.RawPointer() == splice.nexts[i] &&
              now_next.GetTag() == SkiplistNode::NodeStatus::Normal) {
            ret.dram_node->RelaxedSetNext(i, splice.nexts[i]);
            if (splice.prevs[i]->CASNext(i, splice.nexts[i], ret.dram_node)) {
              break;
            }
          } else {
            splice.Recompute(key, i);
          }
        }
      }
    }
  } else if (ret.dram_node) {
    ret.dram_node->record = new_record;
  }
  return ret;
}

void Skiplist::CleanObsoletedNodes() {
  std::lock_guard<SpinMutex> lg_a(pending_delete_nodes_spin_);
  if (pending_deletion_nodes_.size() > 0) {
    for (SkiplistNode* node : pending_deletion_nodes_) {
      SkiplistNode::DeleteNode(node);
    }
    pending_deletion_nodes_.clear();
  }

  std::lock_guard<SpinMutex> lg_b(obsolete_nodes_spin_);
  obsolete_nodes_.swap(pending_deletion_nodes_);
}

void Skiplist::Destroy() {
  GlobalLogger.Debug("Destroy skiplist %s\n", Name().c_str());
  destroyRecords();
  destroyNodes();
}

void Skiplist::destroyNodes() {
  if (header_) {
    SkiplistNode* to_delete = header_;
    while (to_delete) {
      SkiplistNode* next = to_delete->Next(1).RawPointer();
      SkiplistNode::DeleteNode(to_delete);
      to_delete = next;
    }
    header_ = nullptr;
  }
}

void Skiplist::destroyRecords() {
  std::vector<SpaceEntry> to_free;
  if (header_) {
    DLRecord* header_record = header_->record;
    DLRecord* next_destroy =
        pmem_allocator_->offset2addr_checked<DLRecord>(header_record->next);
    DLRecord* to_destroy = nullptr;
    do {
      to_destroy = next_destroy;
      next_destroy =
          pmem_allocator_->offset2addr_checked<DLRecord>(to_destroy->next);
      StringView key = to_destroy->Key();
      auto hash_hint = hash_table_->GetHint(key);
      std::lock_guard<SpinMutex> lg(*hash_hint.spin);
      // We need to purge destroyed records one by one in case engine crashed
      // during destroy
      Skiplist::Purge(to_destroy, nullptr, pmem_allocator_, record_locks_);

      if (IndexWithHashtable()) {
        auto lookup_result =
            hash_table_->Lookup<false>(key, to_destroy->entry.meta.type);

        if (lookup_result.s == Status::Ok) {
          DLRecord* hash_indexed_record;
          auto hash_index = lookup_result.entry.GetIndex();
          switch (lookup_result.entry.GetIndexType()) {
            case PointerType::Skiplist:
              hash_indexed_record = hash_index.skiplist->HeaderRecord();
              break;
            case PointerType::SkiplistNode:
              hash_indexed_record = hash_index.skiplist_node->record;
              break;
            case PointerType::DLRecord:
              hash_indexed_record = hash_index.dl_record;
              break;
            default:
              kvdk_assert(false, "Wrong hash index type of sorted record");
          }
          if (hash_indexed_record == to_destroy) {
            hash_table_->Erase(lookup_result.entry_ptr);
          }
        }
      }

      to_free.emplace_back(pmem_allocator_->addr2offset_checked(to_destroy),
                           to_destroy->entry.header.record_size);

    } while (to_destroy !=
             header_record /* header record should be the last detroyed one */);
  }

  pmem_allocator_->BatchFree(to_free);
}

size_t Skiplist::Size() { return size_.load(std::memory_order_relaxed); }

void Skiplist::UpdateSize(int64_t delta) {
  kvdk_assert(delta >= 0 || size_.load() >= static_cast<size_t>(-delta),
              "Update skiplist size to negative");
  size_.fetch_add(delta, std::memory_order_relaxed);
}
}  // namespace KVDK_NAMESPACE