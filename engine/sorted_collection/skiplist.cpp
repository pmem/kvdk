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
                   Comparator comparator,
                   std::shared_ptr<PMEMAllocator> pmem_allocator,
                   std::shared_ptr<HashTable> hash_table,
                   bool index_with_hashtable)
    : Collection(name, id),
      comparator_(comparator),
      pmem_allocator_(pmem_allocator),
      hash_table_(hash_table),
      index_with_hashtable_(index_with_hashtable) {
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

void Skiplist::SeekNode(const StringView& key, SkiplistNode* start_node,
                        uint8_t start_height, uint8_t end_height,
                        Splice* result_splice) {
  std::vector<SkiplistNode*> to_delete;
  assert(start_node->height >= start_height && end_height >= 1);
  SkiplistNode* prev = start_node;
  PointerWithTag<SkiplistNode, SkiplistNode::NodeStatus> next;
  for (uint8_t i = start_height; i >= end_height; i--) {
    uint64_t round = 0;
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
          return SeekNode(key, result_splice->seeking_list->Header(),
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
    if (next_record == Header()->record) {
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
      HashEntry hash_entry;
      HashEntry* entry_ptr = nullptr;
      StringView key = next_record->Key();
      Status s = hash_table_->SearchForRead(hash_table_->GetHint(key), key,
                                            next_record->entry.meta.type,
                                            &entry_ptr, &hash_entry, nullptr);
      if (s != Status::Ok) {
        return Status::Abort;
      }

      if (hash_entry.GetIndexType() == PointerType::SkiplistNode) {
        if (hash_entry.GetIndex().skiplist_node != next_node) {
          return Status::Abort;
        }
      } else {
        if (hash_entry.GetIndex().dl_record != next_record) {
          return Status::Abort;
        }
      }
    }

    // Check dram linkage
    if (next_node && next_node->record == next_record) {
      for (uint8_t i = 1; i <= next_node->Height(); i++) {
        if (splice.prevs[i]->RelaxedNext(i).RawPointer() != next_node) {
          return Status::Abort;
        }
        splice.prevs[i] = next_node;
      }
    }
    splice.prev_pmem_record = next_record;
  }

  return Status::Ok;
}

bool Skiplist::lockRecordPosition(const DLRecord* record,
                                  const SpinMutex* record_key_lock,
                                  std::unique_lock<SpinMutex>* prev_record_lock,
                                  PMEMAllocator* pmem_allocator,
                                  HashTable* hash_table) {
  while (1) {
    DLRecord* prev =
        pmem_allocator->offset2addr_checked<DLRecord>(record->prev);
    DLRecord* next = pmem_allocator->offset2addr<DLRecord>(record->next);
    PMemOffsetType prev_offset = pmem_allocator->addr2offset_checked(prev);
    PMemOffsetType next_offset = pmem_allocator->addr2offset_checked(next);

    SpinMutex* prev_lock = hash_table->GetHint(prev->Key()).spin;
    if (prev_lock != record_key_lock) {
      if (!prev_lock->try_lock()) {
        return false;
      }
      *prev_record_lock =
          std::unique_lock<SpinMutex>(*prev_lock, std::adopt_lock);
    }

    // Check if the list has changed before we successfully acquire lock.
    // As the record is already locked, so we don't need to check its next
    if (record->prev != prev_offset ||
        prev->next != pmem_allocator->addr2offset(record)) {
      continue;
    }

    assert(record->prev == prev_offset);
    assert(record->next == next_offset);
    assert(next->prev == pmem_allocator->addr2offset(record));

    return true;
  }
}

bool Skiplist::lockInsertPosition(
    const StringView& inserting_key, DLRecord* prev_record,
    DLRecord* next_record, const SpinMutex* inserting_key_lock,
    std::unique_lock<SpinMutex>* prev_record_lock) {
  PMemOffsetType prev_offset =
      pmem_allocator_->addr2offset_checked(prev_record);
  auto prev_hint = hash_table_->GetHint(prev_record->Key());
  if (prev_hint.spin != inserting_key_lock) {
    if (!prev_hint.spin->try_lock()) {
      return false;
    }
    *prev_record_lock =
        std::unique_lock<SpinMutex>(*prev_hint.spin, std::adopt_lock);
  }

  PMemOffsetType next_offset =
      pmem_allocator_->addr2offset_checked(next_record);
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
    prev_record_lock->unlock();
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
                                       const HashTable::KeyHashHint& hash_hint,
                                       TimeStampType timestamp) {
  if (IndexWithHashtable()) {
    return deleteImplWithHash(key, hash_hint, timestamp);
  } else {
    return deleteImplNoHash(key, hash_hint.spin, timestamp);
  }
}

Skiplist::WriteResult Skiplist::Set(const StringView& key,
                                    const StringView& value,
                                    const HashTable::KeyHashHint& hash_hint,
                                    TimeStampType timestamp) {
  if (IndexWithHashtable()) {
    return setImplWithHash(key, value, hash_hint, timestamp);
  } else {
    return setImplNoHash(key, value, hash_hint.spin, timestamp);
  }
}

bool Skiplist::Replace(DLRecord* old_record, DLRecord* new_record,
                       const SpinMutex* old_record_lock,
                       SkiplistNode* dram_node, PMEMAllocator* pmem_allocator,
                       HashTable* hash_table) {
  std::unique_lock<SpinMutex> prev_record_lock;
  if (!lockRecordPosition(old_record, old_record_lock, &prev_record_lock,
                          pmem_allocator, hash_table)) {
    return false;
  }
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

bool Skiplist::Purge(DLRecord* purging_record,
                     const SpinMutex* purging_record_lock,
                     SkiplistNode* dram_node, PMEMAllocator* pmem_allocator,
                     HashTable* hash_table) {
  std::unique_lock<SpinMutex> prev_record_lock;
  if (!lockRecordPosition(purging_record, purging_record_lock,
                          &prev_record_lock, pmem_allocator, hash_table)) {
    return false;
  }

  // Modify linkage to drop deleted record
  PMemOffsetType purging_offset = pmem_allocator->addr2offset(purging_record);
  PMemOffsetType prev_offset = purging_record->prev;
  PMemOffsetType next_offset = purging_record->next;
  DLRecord* prev = pmem_allocator->offset2addr_checked<DLRecord>(prev_offset);
  DLRecord* next = pmem_allocator->offset2addr_checked<DLRecord>(next_offset);
  assert(prev->next == purging_offset);
  assert(next->prev == purging_offset);
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
  const size_t num_config_fields = 1;
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
        splice.next_pmem_record->entry.meta.type == SortedDataRecord) {
      value->assign(splice.next_pmem_record->Value().data(),
                    splice.next_pmem_record->Value().size());
      return Status::Ok;
    } else {
      return Status::NotFound;
    }
  } else {
    std::string internal_key = InternalKey(key);
    HashEntry hash_entry;
    HashEntry* entry_ptr = nullptr;
    bool is_found = hash_table_->SearchForRead(
                        hash_table_->GetHint(internal_key), internal_key,
                        SortedDataRecord | SortedDeleteRecord, &entry_ptr,
                        &hash_entry, nullptr) == Status::Ok;
    if (!is_found) {
      return Status::NotFound;
    }

    DLRecord* pmem_record;
    switch (hash_entry.GetIndexType()) {
      case PointerType::SkiplistNode: {
        pmem_record = hash_entry.GetIndex().skiplist_node->record;
        break;
      }
      case PointerType::DLRecord: {
        pmem_record = hash_entry.GetIndex().dl_record;
        break;
      }
      default: {
        GlobalLogger.Error(
            "Wrong hash index type while search sorted data in hash table\n");
        return Status::Abort;
      }
    }

    if (pmem_record->entry.meta.type == SortedDeleteRecord) {
      return Status::NotFound;
    } else {
      assert(pmem_record->entry.meta.type == SortedDataRecord);
      value->assign(pmem_record->Value().data(), pmem_record->Value().size());
      return Status::Ok;
    }
  }
}

Skiplist::WriteResult Skiplist::deleteImplNoHash(
    const StringView& key, const SpinMutex* locked_key_lock,
    TimeStampType timestamp) {
  WriteResult ret;
  std::string internal_key(InternalKey(key));
  Splice splice(this);
  Seek(key, &splice);
  bool found = (splice.next_pmem_record->entry.meta.type &
                (SortedDataRecord | SortedDeleteRecord)) &&
               equal_string_view(splice.next_pmem_record->Key(), internal_key);

  if (!found) {
    return ret;
  }

  if (splice.next_pmem_record->entry.meta.type == SortedDeleteRecord) {
    return ret;
  }

  // try to write delete record
  ret.existing_record = splice.next_pmem_record;
  std::unique_lock<SpinMutex> prev_record_lock;
  if (!lockRecordPosition(ret.existing_record, locked_key_lock,
                          &prev_record_lock)) {
    ret.s = Status::Fail;
    return ret;
  }

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
      timestamp, SortedDeleteRecord, existing_offset, prev_offset, next_offset,
      internal_key, "");
  ret.write_record = delete_record;

  assert(splice.prev_pmem_record->next == existing_offset);

  linkDLRecord(prev_record, next_record, delete_record);

  if (splice.nexts[1] && splice.nexts[1]->record == ret.existing_record) {
    splice.nexts[1]->record = delete_record;
  }
  return ret;
}

Skiplist::WriteResult Skiplist::deleteImplWithHash(
    const StringView& key, const HashTable::KeyHashHint& locked_hash_hint,
    TimeStampType timestamp) {
  WriteResult ret;
  assert(IndexWithHashtable());
  std::string internal_key(InternalKey(key));
  SpinMutex* deleting_key_lock = locked_hash_hint.spin;
  HashEntry* entry_ptr = nullptr;
  HashEntry hash_entry;
  std::unique_lock<SpinMutex> prev_record_lock;
  ret.s = hash_table_->SearchForRead(locked_hash_hint, internal_key,
                                     SortedDataRecord | SortedDeleteRecord,
                                     &entry_ptr, &hash_entry, nullptr);

  switch (ret.s) {
    case Status::NotFound: {
      ret.s = Status::Ok;
      return ret;
    }
    case Status::Ok: {
      if (hash_entry.GetRecordType() == SortedDeleteRecord) {
        return ret;
      }

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
      if (!lockRecordPosition(ret.existing_record, deleting_key_lock,
                              &prev_record_lock)) {
        ret.s = Status::Fail;
        return ret;
      }

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
          space_to_write.size, timestamp, SortedDeleteRecord, existing_offset,
          prev_offset, next_offset, internal_key, "");
      linkDLRecord(prev_record, next_record, delete_record);
      ret.write_record = delete_record;
      ret.hash_entry_ptr = entry_ptr;

      break;
    }
    default:
      std::abort();  // never reach
  }

  // until here, new record is already inserted to list
  assert(ret.write_record != nullptr);
  if (ret.dram_node == nullptr) {
    hash_table_->Insert(locked_hash_hint, entry_ptr, SortedDeleteRecord,
                        ret.write_record, PointerType::DLRecord);
  } else {
    ret.dram_node->record = ret.write_record;
    hash_table_->Insert(locked_hash_hint, entry_ptr, SortedDeleteRecord,
                        ret.dram_node, PointerType::SkiplistNode);
  }

  return ret;
}

Skiplist::WriteResult Skiplist::setImplWithHash(
    const StringView& key, const StringView& value,
    const HashTable::KeyHashHint& locked_hash_hint, TimeStampType timestamp) {
  WriteResult ret;
  assert(IndexWithHashtable());
  std::string internal_key(InternalKey(key));
  SpinMutex* inserting_key_lock = locked_hash_hint.spin;
  HashEntry* entry_ptr = nullptr;
  HashEntry hash_entry;
  std::unique_lock<SpinMutex> prev_record_lock;
  ret.s = hash_table_->SearchForWrite(locked_hash_hint, internal_key,
                                      SortedDataRecord | SortedDeleteRecord,
                                      &entry_ptr, &hash_entry, nullptr);

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
      if (!lockRecordPosition(ret.existing_record, locked_hash_hint.spin,
                              &prev_record_lock)) {
        ret.s = Status::Fail;
        return ret;
      }

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
          space_to_write.size, timestamp, SortedDataRecord, existing_offset,
          prev_offset, next_offset, internal_key, value);
      ret.write_record = new_record;
      ret.hash_entry_ptr = entry_ptr;
      linkDLRecord(prev_record, next_record, new_record);

      break;
    }
    case Status::NotFound: {
      ret = setImplNoHash(key, value, locked_hash_hint.spin, timestamp);
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
    hash_table_->Insert(locked_hash_hint, entry_ptr, SortedDataRecord,
                        ret.write_record, PointerType::DLRecord);
  } else {
    ret.dram_node->record = ret.write_record;
    hash_table_->Insert(locked_hash_hint, entry_ptr, SortedDataRecord,
                        ret.dram_node, PointerType::SkiplistNode);
  }

  return ret;
}

Skiplist::WriteResult Skiplist::setImplNoHash(
    const StringView& key, const StringView& value,
    const SpinMutex* inserting_key_lock, TimeStampType timestamp) {
  WriteResult ret;
  std::string internal_key(InternalKey(key));
  Splice splice(this);
  Seek(key, &splice);

  bool exist = !IndexWithHashtable() /* a hash indexed skiplist call this
                                        function only if key not exist */
               && (splice.next_pmem_record->entry.meta.type &
                   (SortedDataRecord | SortedDeleteRecord)) &&
               equal_string_view(splice.next_pmem_record->Key(), internal_key);

  DLRecord* prev_record;
  DLRecord* next_record;
  std::unique_lock<SpinMutex> prev_record_lock;
  if (exist) {
    ret.existing_record = splice.next_pmem_record;
    if (splice.nexts[1] && splice.nexts[1]->record == ret.existing_record) {
      ret.dram_node = splice.nexts[1];
    }
    if (!lockRecordPosition(ret.existing_record, inserting_key_lock,
                            &prev_record_lock)) {
      ret.s = Status::Fail;
      return ret;
    }
    prev_record = pmem_allocator_->offset2addr_checked<DLRecord>(
        ret.existing_record->prev);
    next_record = pmem_allocator_->offset2addr_checked<DLRecord>(
        ret.existing_record->next);
  } else {
    ret.existing_record = nullptr;
    if (!lockInsertPosition(key, splice.prev_pmem_record,
                            splice.next_pmem_record, inserting_key_lock,
                            &prev_record_lock)) {
      ret.s = Status::Fail;
      return ret;
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
      timestamp, SortedDataRecord,
      pmem_allocator_->addr2offset(ret.existing_record), prev_offset,
      next_offset, internal_key, value);
  ret.write_record = new_record;
  // link new record to PMem
  linkDLRecord(prev_record, next_record, new_record);
  if (!exist) {
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
      while (true) {
        std::lock_guard<SpinMutex> lg(*hash_hint.spin);
        // We need to purge destroyed records one by one in case engine crashed
        // during destroy
        if (!Skiplist::Purge(to_destroy, hash_hint.spin, nullptr,
                             pmem_allocator_.get(), hash_table_.get())) {
          GlobalLogger.Debug("Purge failed in destroy skiplist records\n");
          continue;
        }

        if (IndexWithHashtable()) {
          HashEntry* entry_ptr = nullptr;
          HashEntry hash_entry;
          auto s = hash_table_->SearchForRead(hash_hint, key,
                                              to_destroy->entry.meta.type,
                                              &entry_ptr, &hash_entry, nullptr);
          if (s == Status::Ok) {
            hash_table_->Erase(entry_ptr);
          }
        }

        to_free.emplace_back(pmem_allocator_->addr2offset_checked(to_destroy),
                             to_destroy->entry.header.record_size);

        break;
      }
    } while (to_destroy !=
             header_record /* header record should be the last detroyed one */);
  }

  pmem_allocator_->BatchFree(to_free);
}
}  // namespace KVDK_NAMESPACE