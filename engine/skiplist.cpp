/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "skiplist.hpp"

#include <libpmem.h>

#include <algorithm>
#include <future>

#include "hash_table.hpp"
#include "kv_engine.hpp"
#include "utils/codec.hpp"
#include "utils/sync_point.hpp"

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

void Skiplist::SeekNode(const StringView& key, SkiplistNode* start_node,
                        uint8_t start_height, uint8_t end_height,
                        Splice* result_splice) {
  std::vector<SkiplistNode*> to_delete;
  assert(start_node->height >= start_height && end_height >= 1);
  SkiplistNode* prev = start_node;
  PointerWithTag<SkiplistNode> next;
  for (uint8_t i = start_height; i >= end_height; i--) {
    uint64_t round = 0;
    while (1) {
      next = prev->Next(i);
      // prev is logically deleted, roll back to prev height.
      if (next.GetTag()) {
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
      if (next_next.GetTag()) {
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
    if (!validateDLRecord(next_record)) {
      return Seek(key, result_splice);
    }

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

      if (hash_entry.GetIndexType() == HashIndexType::SkiplistNode) {
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
  if (!check_linkage() || !check_id() || !check_order()) {
    prev_record_lock->unlock();
    return false;
  }
  assert(prev_record->next == next_offset);
  assert(next_record->prev == prev_offset);

  assert(prev_record == header_->record ||
         compare(Skiplist::UserKey(prev_record), inserting_key) < 0);
  assert(next_record == header_->record ||
         compare(Skiplist::UserKey(next_record), inserting_key) >= 0);

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
    DataEntry data_entry;
    bool is_found = hash_table_->SearchForRead(
                        hash_table_->GetHint(internal_key), internal_key,
                        SortedDataRecord | SortedDeleteRecord, &entry_ptr,
                        &hash_entry, &data_entry) == Status::Ok;
    if (!is_found || (hash_entry.GetRecordType() & DeleteRecordType)) {
      return Status::NotFound;
    }

    DLRecord* pmem_record;
    switch (hash_entry.GetIndexType()) {
      case HashIndexType::SkiplistNode: {
        pmem_record = hash_entry.GetIndex().skiplist_node->record;
        break;
      }
      case HashIndexType::DLRecord: {
        pmem_record = hash_entry.GetIndex().dl_record;
        break;
      }
      default: {
        GlobalLogger.Error(
            "Wrong hash index type while search sorted data in hash table\n");
        return Status::Abort;
      }
    }

    value->assign(pmem_record->Value().data(), pmem_record->Value().size());
    return Status::Ok;
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
    ret.s = Status::Abort;
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

      if (hash_entry.GetIndexType() == HashIndexType::SkiplistNode) {
        ret.dram_node = hash_entry.GetIndex().skiplist_node;
        ret.existing_record = ret.dram_node->record;
      } else {
        ret.dram_node = nullptr;
        assert(hash_entry.GetIndexType() == HashIndexType::DLRecord);
        ret.existing_record = hash_entry.GetIndex().dl_record;
      }
      assert(timestamp > ret.existing_record->entry.meta.timestamp);

      // Try to write delete record
      if (!lockRecordPosition(ret.existing_record, deleting_key_lock,
                              &prev_record_lock)) {
        ret.s = Status::Abort;
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

      break;
    }
    default:
      std::abort();  // never reach
  }

  // until here, new record is already inserted to list
  assert(ret.write_record != nullptr);
  if (ret.dram_node == nullptr) {
    hash_table_->Insert(locked_hash_hint, entry_ptr, SortedDeleteRecord,
                        ret.write_record, HashIndexType::DLRecord);
  } else {
    ret.dram_node->record = ret.write_record;
    hash_table_->Insert(locked_hash_hint, entry_ptr, SortedDeleteRecord,
                        ret.dram_node, HashIndexType::SkiplistNode);
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
      if (hash_entry.GetIndexType() == HashIndexType::SkiplistNode) {
        ret.dram_node = hash_entry.GetIndex().skiplist_node;
        ret.existing_record = ret.dram_node->record;
      } else {
        ret.dram_node = nullptr;
        assert(hash_entry.GetIndexType() == HashIndexType::DLRecord);
        ret.existing_record = hash_entry.GetIndex().dl_record;
      }
      assert(timestamp > ret.existing_record->entry.meta.timestamp);

      // Try to write delete record
      if (!lockRecordPosition(ret.existing_record, locked_hash_hint.spin,
                              &prev_record_lock)) {
        ret.s = Status::Abort;
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
                        ret.write_record, HashIndexType::DLRecord);
  } else {
    ret.dram_node->record = ret.write_record;
    hash_table_->Insert(locked_hash_hint, entry_ptr, SortedDataRecord,
                        ret.dram_node, HashIndexType::SkiplistNode);
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
      ret.s = Status::Abort;
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
      ret.s = Status::Abort;
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
              now_next.GetTag() == 0) {
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
  GlobalLogger.Debug("Destroy pmem records of skiplist %s finish\n",
                     Name().c_str());
  destroyNodes();
  GlobalLogger.Debug("Destroy skiplist nodes of skiplist %s finish\n",
                     Name().c_str());
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
          HashEntry* entry_ptr;
          HashEntry hash_entry;
          auto s = hash_table_->SearchForRead(hash_hint, key,
                                              to_destroy->entry.meta.type,
                                              &entry_ptr, &hash_entry, nullptr);
          if (s == Status::Ok) {
            entry_ptr->Clear();
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

void SortedIterator::Seek(const std::string& key) {
  assert(skiplist_);
  Splice splice(skiplist_);
  skiplist_->Seek(key, &splice);
  current_ = splice.next_pmem_record;
  while (Valid()) {
    DLRecord* valid_version_record = findValidVersion(current_);
    if (valid_version_record == nullptr ||
        valid_version_record->entry.meta.type == SortedDeleteRecord) {
      current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->next);
    } else {
      current_ = valid_version_record;
      break;
    }
  }
  while (current_->entry.meta.type == SortedDeleteRecord) {
    current_ = pmem_allocator_->offset2addr<DLRecord>(current_->next);
  }
}

void SortedIterator::SeekToFirst() {
  uint64_t first = skiplist_->Header()->record->next;
  current_ = pmem_allocator_->offset2addr<DLRecord>(first);
  while (Valid()) {
    DLRecord* valid_version_record = findValidVersion(current_);
    if (valid_version_record == nullptr ||
        valid_version_record->entry.meta.type == SortedDeleteRecord) {
      current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->next);
    } else {
      current_ = valid_version_record;
      break;
    }
  }
}

void SortedIterator::SeekToLast() {
  uint64_t last = skiplist_->Header()->record->prev;
  current_ = pmem_allocator_->offset2addr<DLRecord>(last);
  while (Valid()) {
    DLRecord* valid_version_record = findValidVersion(current_);
    if (valid_version_record == nullptr ||
        valid_version_record->entry.meta.type == SortedDeleteRecord) {
      current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->prev);
    } else {
      current_ = valid_version_record;
      break;
    }
  }
}

void SortedIterator::Next() {
  if (!Valid()) {
    return;
  }
  current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->next);
  while (Valid()) {
    DLRecord* valid_version_record = findValidVersion(current_);
    if (valid_version_record == nullptr ||
        valid_version_record->entry.meta.type == SortedDeleteRecord) {
      current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->next);
    } else {
      current_ = valid_version_record;
      break;
    }
  }
}

void SortedIterator::Prev() {
  if (!Valid()) {
    return;
  }
  current_ = (pmem_allocator_->offset2addr<DLRecord>(current_->prev));
  while (Valid()) {
    DLRecord* valid_version_record = findValidVersion(current_);
    if (valid_version_record == nullptr ||
        valid_version_record->entry.meta.type == SortedDeleteRecord) {
      current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->prev);
    } else {
      current_ = valid_version_record;
      break;
    }
  }
}

std::string SortedIterator::Key() {
  if (!Valid()) return "";
  return string_view_2_string(Skiplist::UserKey(current_));
}

std::string SortedIterator::Value() {
  if (!Valid()) return "";
  return string_view_2_string(current_->Value());
}

DLRecord* SortedIterator::findValidVersion(DLRecord* pmem_record) {
  DLRecord* curr = pmem_record;
  TimeStampType ts = snapshot_->GetTimestamp();
  while (curr != nullptr && curr->entry.meta.timestamp > ts) {
    curr = pmem_allocator_->offset2addr<DLRecord>(curr->older_version_offset);
    kvdk_assert(curr == nullptr || curr->Validate(),
                "Broken checkpoint: invalid older version sorted record");
    kvdk_assert(
        curr == nullptr || equal_string_view(curr->Key(), pmem_record->Key()),
        "Broken checkpoint: key of older version sorted data is "
        "not same as new "
        "version");
  }
  return curr;
}

SortedCollectionRebuilder::SortedCollectionRebuilder(
    KVEngine* kv_engine, bool segment_based_rebuild,
    uint64_t num_rebuild_threads, const CheckPoint& checkpoint)
    : kv_engine_(kv_engine),
      checkpoint_(checkpoint),
      segment_based_rebuild_(segment_based_rebuild),
      num_rebuild_threads_(std::min(num_rebuild_threads,
                                    kv_engine->configs_.max_access_threads)),
      recovery_segments_(),
      rebuild_skiplits_(),
      invalid_skiplists_() {
  rebuilder_thread_cache_.resize(num_rebuild_threads_);
}

DLRecord* SortedCollectionRebuilder::findValidVersion(
    DLRecord* pmem_record, std::vector<DLRecord*>* invalid_version_records) {
  if (!recoverToCheckpoint()) {
    return pmem_record;
  }
  DLRecord* curr = pmem_record;
  while (curr != nullptr &&
         curr->entry.meta.timestamp > checkpoint_.CheckpointTS()) {
    curr = kv_engine_->pmem_allocator_->offset2addr<DLRecord>(
        curr->older_version_offset);
    kvdk_assert(curr == nullptr || curr->Validate(),
                "Broken checkpoint: invalid older version sorted record");
    kvdk_assert(
        curr == nullptr || equal_string_view(curr->Key(), pmem_record->Key()),
        "Broken checkpoint: key of older version sorted data is "
        "not same as new "
        "version");
  }
  return curr;
}

Status SortedCollectionRebuilder::segmentBasedIndexRebuild() {
  GlobalLogger.Info("segment based rebuild start\n");
  std::vector<std::future<Status>> fs;

  auto rebuild_segments_index = [&]() -> Status {
    Status s = this->kv_engine_->MaybeInitAccessThread();
    if (s != Status::Ok) {
      return s;
    }
    defer(this->kv_engine_->ReleaseAccessThread());
    for (auto iter = this->recovery_segments_.begin();
         iter != this->recovery_segments_.end(); iter++) {
      if (!iter->second.visited) {
        std::lock_guard<SpinMutex> lg(this->lock_);
        if (!iter->second.visited) {
          iter->second.visited = true;
        } else {
          continue;
        }
      } else {
        continue;
      }

      auto rebuild_skiplist_iter = rebuild_skiplits_.find(
          Skiplist::SkiplistID(iter->second.start_node->record));
      if (rebuild_skiplist_iter == rebuild_skiplits_.end()) {
        // this start point belong to a invalid skiplist
        kvdk_assert(
            invalid_skiplists_.find(Skiplist::SkiplistID(
                iter->second.start_node->record)) != invalid_skiplists_.end(),
            "Start record of a recovery segment should belong to a skiplist");
      } else {
        bool build_hash_index =
            rebuild_skiplist_iter->second->IndexWithHashtable();

        Status s =
            rebuildSegmentIndex(iter->second.start_node, build_hash_index);
        if (s != Status::Ok) {
          return s;
        }
      }
    }
    return Status::Ok;
  };

  GlobalLogger.Info("build segment index\n");
  for (uint32_t thread_num = 0; thread_num < num_rebuild_threads_;
       ++thread_num) {
    fs.push_back(std::async(rebuild_segments_index));
  }
  for (auto& f : fs) {
    Status s = f.get();
    if (s != Status::Ok) {
      return s;
    }
  }
  fs.clear();
  GlobalLogger.Info("link dram nodes\n");

  int i = 0;
  for (auto& s : rebuild_skiplits_) {
    i++;
    fs.push_back(std::async(&SortedCollectionRebuilder::linkHighDramNodes, this,
                            s.second.get()));
    if (i % num_rebuild_threads_ == 0 || i == rebuild_skiplits_.size()) {
      for (auto& f : fs) {
        Status s = f.get();
        if (s != Status::Ok) {
          return s;
        }
      }
      fs.clear();
    }
  }

  recovery_segments_.clear();
  GlobalLogger.Info("segment based rebuild done\n");

  return Status::Ok;
}

Status SortedCollectionRebuilder::linkHighDramNodes(Skiplist* skiplist) {
  Splice splice(skiplist);
  for (uint8_t i = 1; i <= kMaxHeight; i++) {
    splice.prevs[i] = skiplist->Header();
  }

  SkiplistNode* next_node = splice.prevs[1]->RelaxedNext(1).RawPointer();
  while (next_node != nullptr) {
    assert(splice.prevs[1]->RelaxedNext(1).RawPointer() == next_node);
    splice.prevs[1] = next_node;
    if (next_node->Height() > 1) {
      for (uint8_t i = 2; i <= next_node->Height(); i++) {
        splice.prevs[i]->RelaxedSetNext(i, next_node);
        splice.prevs[i] = next_node;
      }
    }
    next_node = next_node->RelaxedNext(1).RawPointer();
  }
  for (uint8_t i = 1; i <= kMaxHeight; i++) {
    splice.prevs[i]->RelaxedSetNext(i, nullptr);
  }

  return Status::Ok;
}

Status SortedCollectionRebuilder::rebuildSkiplistIndex(Skiplist* skiplist) {
  Status s = kv_engine_->MaybeInitAccessThread();
  if (s != Status::Ok) {
    GlobalLogger.Error("too many threads repair skiplist linkage\n");
    return s;
  }
  defer(kv_engine_->ReleaseAccessThread());

  if (s != Status::Ok) {
    return s;
  }

  Splice splice(skiplist);
  HashEntry hash_entry;
  for (uint8_t i = 1; i <= kMaxHeight; i++) {
    splice.prevs[i] = skiplist->Header();
    splice.prev_pmem_record = skiplist->Header()->record;
  }

  while (true) {
    uint64_t next_offset = splice.prev_pmem_record->next;
    DLRecord* next_record =
        kv_engine_->pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
    if (next_record == skiplist->Header()->record) {
      break;
    }

    StringView internal_key = next_record->Key();
    auto hash_hint = kv_engine_->hash_table_->GetHint(internal_key);
    while (true) {
      std::lock_guard<SpinMutex> lg(*hash_hint.spin);
      DLRecord* valid_version_record = findValidVersion(next_record, nullptr);
      if (valid_version_record == nullptr) {
        // purge invalid version record from list
        if (!Skiplist::Purge(next_record, hash_hint.spin, nullptr,
                             kv_engine_->pmem_allocator_.get(),
                             kv_engine_->hash_table_.get())) {
          asm volatile("pause");
          continue;
        }
        addUnlinkedRecord(next_record);
      } else {
        if (valid_version_record != next_record) {
          // repair linkage of checkpoint version
          if (!Skiplist::Replace(next_record, valid_version_record,
                                 hash_hint.spin, nullptr,
                                 kv_engine_->pmem_allocator_.get(),
                                 kv_engine_->hash_table_.get())) {
            continue;
          }
          addUnlinkedRecord(next_record);
        }

        // Rebuild dram node
        assert(valid_version_record != nullptr);
        SkiplistNode* dram_node = Skiplist::NewNodeBuild(valid_version_record);

        if (dram_node != nullptr) {
          auto height = dram_node->Height();
          for (uint8_t i = 1; i <= height; i++) {
            splice.prevs[i]->RelaxedSetNext(i, dram_node);
            dram_node->RelaxedSetNext(i, nullptr);
            splice.prevs[i] = dram_node;
          }
        }

        // Rebuild hash index
        if (skiplist->IndexWithHashtable()) {
          Status s;
          if (dram_node) {
            s = insertHashIndex(internal_key, dram_node,
                                HashIndexType::SkiplistNode);
          } else {
            s = insertHashIndex(internal_key, valid_version_record,
                                HashIndexType::DLRecord);
          }

          if (s != Status::Ok) {
            return s;
          }
        }

        splice.prev_pmem_record = valid_version_record;
      }
      break;
    }
  }
  return Status::Ok;
}

Status SortedCollectionRebuilder::listBasedIndexRebuild() {
  std::vector<std::future<Status>> fs;
  int i = 0;
  for (auto skiplist : rebuild_skiplits_) {
    i++;
    fs.push_back(std::async(&SortedCollectionRebuilder::rebuildSkiplistIndex,
                            this, skiplist.second.get()));
    if (i % num_rebuild_threads_ == 0 || i == rebuild_skiplits_.size()) {
      for (auto& f : fs) {
        Status s = f.get();
        if (s != Status::Ok) {
          return s;
        }
      }
      fs.clear();
    }
  }

  return Status::Ok;
}

SortedCollectionRebuilder::RebuildResult
SortedCollectionRebuilder::RebuildIndex() {
  RebuildResult ret;
  if (rebuild_skiplits_.size() == 0) {
    return ret;
  }

  if (segment_based_rebuild_) {
    ret.s = segmentBasedIndexRebuild();
  } else {
    ret.s = listBasedIndexRebuild();
  }

  if (ret.s == Status::Ok) {
    ret.max_id = max_recovered_id_;
    ret.rebuild_skiplits.swap(rebuild_skiplits_);
  }
  cleanInvalidRecords();
  return ret;
}

Status SortedCollectionRebuilder::rebuildSegmentIndex(SkiplistNode* start_node,
                                                      bool build_hash_index) {
  Status s;
  // First insert hash index for the start node
  if (build_hash_index &&
      start_node->record->entry.meta.type != SortedHeaderRecord) {
    s = insertHashIndex(start_node->record->Key(), start_node,
                        HashIndexType::SkiplistNode);
    if (s != Status::Ok) {
      return s;
    }
  }

  SkiplistNode* cur_node = start_node;
  DLRecord* cur_record = cur_node->record;

  while (true) {
    DLRecord* next_record =
        kv_engine_->pmem_allocator_->offset2addr_checked<DLRecord>(
            cur_record->next);
    if (next_record->entry.meta.type == SortedHeaderRecord) {
      cur_node->RelaxedSetNext(1, nullptr);
      break;
    }

    auto iter = recovery_segments_.find(next_record);
    if (iter == recovery_segments_.end()) {
      HashEntry hash_entry;
      DataEntry data_entry;
      HashEntry* entry_ptr = nullptr;
      StringView internal_key = next_record->Key();

      auto hash_hint = kv_engine_->hash_table_->GetHint(internal_key);
      while (true) {
        std::lock_guard<SpinMutex> lg(*hash_hint.spin);
        DLRecord* valid_version_record = findValidVersion(next_record, nullptr);
        if (valid_version_record == nullptr) {
          if (!Skiplist::Purge(next_record, hash_hint.spin, nullptr,
                               kv_engine_->pmem_allocator_.get(),
                               kv_engine_->hash_table_.get())) {
            continue;
          }
          addUnlinkedRecord(next_record);
        } else {
          if (valid_version_record != next_record) {
            if (!Skiplist::Replace(next_record, valid_version_record,
                                   hash_hint.spin, nullptr,
                                   kv_engine_->pmem_allocator_.get(),
                                   kv_engine_->hash_table_.get())) {
              continue;
            }
            addUnlinkedRecord(next_record);
          }

          assert(valid_version_record != nullptr);
          SkiplistNode* dram_node =
              Skiplist::NewNodeBuild(valid_version_record);
          if (dram_node != nullptr) {
            cur_node->RelaxedSetNext(1, dram_node);
            dram_node->RelaxedSetNext(1, nullptr);
            cur_node = dram_node;
          }

          if (build_hash_index) {
            if (dram_node) {
              s = insertHashIndex(internal_key, dram_node,
                                  HashIndexType::SkiplistNode);
            } else {
              s = insertHashIndex(internal_key, valid_version_record,
                                  HashIndexType::DLRecord);
            }

            if (s != Status::Ok) {
              return s;
            }
          }

          cur_record = valid_version_record;
        }
        break;
      }
    } else {
      // link end node of this segment to adjacent segment
      if (iter->second.start_node->record->entry.meta.type !=
          SortedHeaderRecord) {
        cur_node->RelaxedSetNext(1, iter->second.start_node);
      } else {
        cur_node->RelaxedSetNext(1, nullptr);
      }
      break;
    }
  }
  return Status::Ok;
}

void SortedCollectionRebuilder::linkSegmentDramNodes(SkiplistNode* start_node,
                                                     int height) {
  assert(height > 1);
  while (start_node->Height() < height) {
    start_node = start_node->RelaxedNext(height - 1).RawPointer();
    if (start_node == nullptr || recovery_segments_.find(start_node->record) !=
                                     recovery_segments_.end()) {
      return;
    }
  }
  SkiplistNode* cur_node = start_node;
  SkiplistNode* next_node = cur_node->RelaxedNext(height - 1).RawPointer();
  assert(start_node && start_node->Height() >= height);
  bool finish = false;
  while (true) {
    if (next_node == nullptr) {
      cur_node->RelaxedSetNext(height, nullptr);
      break;
    }

    if (recovery_segments_.find(next_node->record) !=
        recovery_segments_.end()) {
      // link end point of this segment
      while (true) {
        if (next_node == nullptr || next_node->Height() >= height) {
          cur_node->RelaxedSetNext(height, next_node);
          break;
        } else {
          next_node = next_node->RelaxedNext(height - 1).RawPointer();
        }
      }
      break;
    }

    if (next_node->Height() >= height) {
      cur_node->RelaxedSetNext(height, next_node);
      next_node->RelaxedSetNext(height, nullptr);
      cur_node = next_node;
    }
    next_node = next_node->RelaxedNext(height - 1).RawPointer();
  }
}

void SortedCollectionRebuilder::cleanInvalidRecords() {
  std::vector<SpaceEntry> to_free;

  // clean unlinked records
  for (auto& thread_cache : rebuilder_thread_cache_) {
    for (DLRecord* pmem_record : thread_cache.unlinked_records) {
      if (!checkRecordLinkage(pmem_record)) {
        pmem_record->Destroy();
        to_free.emplace_back(
            kv_engine_->pmem_allocator_->addr2offset_checked(pmem_record),
            pmem_record->entry.header.record_size);
      }
    }
    kv_engine_->pmem_allocator_->BatchFree(to_free);
    to_free.clear();
    thread_cache.unlinked_records.clear();
  }

  // clean invalid skiplists
  for (auto& s : invalid_skiplists_) {
    s.second->Destroy();
  }
  invalid_skiplists_.clear();
}

Status SortedCollectionRebuilder::AddHeader(DLRecord* header_record) {
  assert(header_record->entry.meta.type == SortedHeaderRecord);

  std::string collection_name = string_view_2_string(header_record->Key());
  CollectionIDType id;
  SortedCollectionConfigs s_configs;
  Status s = Skiplist::DecodeSortedCollectionValue(header_record->Value(), id,
                                                   s_configs);

  if (s != Status::Ok) {
    GlobalLogger.Error("Decode id and configs of sorted collection %s error\n",
                       string_view_2_string(header_record->Key()).c_str());
    return s;
  }

  auto comparator =
      kv_engine_->comparators_.GetComparator(s_configs.comparator_name);
  if (comparator == nullptr) {
    GlobalLogger.Error(
        "Compare function %s of restoring sorted collection %s is not "
        "registered\n",
        s_configs.comparator_name.c_str(),
        string_view_2_string(header_record->Key()).c_str());
    return Status::Abort;
  }

  // Check if this skiplist has newer version than checkpoint
  bool valid_skiplist =
      !recoverToCheckpoint() ||
      header_record->entry.meta.timestamp <= checkpoint_.CheckpointTS();

  auto skiplist = std::
      make_shared<Skiplist>(header_record, collection_name, id, comparator,
                            kv_engine_->pmem_allocator_,
                            kv_engine_->hash_table_,
                            s_configs.index_with_hashtable && valid_skiplist /* we do not build hash index for a invalid skiplist as it will be destroyed soon */);

  if (!valid_skiplist) {
    GlobalLogger.Debug("add invalid skiplist %s\n", skiplist->Name().c_str());
    std::lock_guard<SpinMutex> lg(lock_);
    invalid_skiplists_.insert({id, skiplist});
    max_recovered_id_ = std::max(max_recovered_id_, id);
    s = Status::Ok;
  } else {
    GlobalLogger.Debug("add recovery skiplist %s\n", skiplist->Name().c_str());
    {
      // TODO: maybe return a skiplist map in rebuild finish, instead of access
      // engine directly
      std::lock_guard<SpinMutex> lg(lock_);
      rebuild_skiplits_.insert({id, skiplist});
      max_recovered_id_ = std::max(max_recovered_id_, id);
    }

    if (segment_based_rebuild_) {
      // Always use header as a recovery segment
      addRecoverySegment(skiplist->Header());
    }

    // Always index skiplist header with hash table
    s = insertHashIndex(skiplist->Name(), skiplist.get(),
                        HashIndexType::Skiplist);
  }
  return s;
}

Status SortedCollectionRebuilder::AddElement(DLRecord* record) {
  kvdk_assert(record->entry.meta.type == SortedDataRecord ||
                  record->entry.meta.type == SortedDeleteRecord,
              "wrong record type in RestoreSkiplistRecord");
  bool linked_record = checkAndRepairRecordLinkage(record);

  if (!linked_record) {
    if (!recoverToCheckpoint()) {
      kv_engine_->purgeAndFree(record);
    } else {
      // We do not know if this is a checkpoint version record, so we can't free
      // it here
      addUnlinkedRecord(record);
    }
  } else {
    if (segment_based_rebuild_ &&
        ++rebuilder_thread_cache_[access_thread.id]
                    .visited_skiplists[Skiplist::SkiplistID(record)] %
                kRestoreSkiplistStride ==
            0 &&
        findValidVersion(record, nullptr) == record) {
      SkiplistNode* start_node = nullptr;
      while (start_node == nullptr) {
        // Always build dram node for a recovery segment start record
        start_node = Skiplist::NewNodeBuild(record);
      }
      addRecoverySegment(start_node);
    }
  }
  return Status::Ok;
}

bool SortedCollectionRebuilder::checkRecordLinkage(DLRecord* record) {
  PMEMAllocator* pmem_allocator = kv_engine_->pmem_allocator_.get();
  uint64_t offset = pmem_allocator->addr2offset(record);
  DLRecord* prev = pmem_allocator->offset2addr<DLRecord>(record->prev);
  DLRecord* next = pmem_allocator->offset2addr<DLRecord>(record->next);
  return prev->next == offset && next->prev == offset;
}

bool SortedCollectionRebuilder::checkAndRepairRecordLinkage(DLRecord* record) {
  PMEMAllocator* pmem_allocator = kv_engine_->pmem_allocator_.get();
  uint64_t offset = pmem_allocator->addr2offset(record);
  DLRecord* prev = pmem_allocator->offset2addr<DLRecord>(record->prev);
  DLRecord* next = pmem_allocator->offset2addr<DLRecord>(record->next);
  if (prev->next != offset && next->prev != offset) {
    return false;
  }
  // Repair un-finished write
  if (next && next->prev != offset) {
    next->prev = offset;
    pmem_persist(&next->prev, 8);
  }
  return true;
}

void SortedCollectionRebuilder::addRecoverySegment(SkiplistNode* start_node) {
  if (segment_based_rebuild_) {
    std::lock_guard<SpinMutex> lg(lock_);
    recovery_segments_.insert({start_node->record, {false, start_node}});
  }
}

Status SortedCollectionRebuilder::insertHashIndex(const StringView& key,
                                                  void* index_ptr,
                                                  HashIndexType index_type) {
  uint16_t search_type_mask;
  RecordType record_type;
  if (index_type == HashIndexType::DLRecord) {
    search_type_mask = SortedDataRecord | SortedDeleteRecord;
    record_type = static_cast<DLRecord*>(index_ptr)->entry.meta.type;
  } else if (index_type == HashIndexType::SkiplistNode) {
    search_type_mask = SortedDataRecord | SortedDeleteRecord;
    record_type =
        static_cast<SkiplistNode*>(index_ptr)->record->entry.meta.type;
  } else if (index_type == HashIndexType::Skiplist) {
    search_type_mask = SortedHeaderRecord;
    record_type = SortedHeaderRecord;
  }

  HashEntry* entry_ptr = nullptr;
  HashEntry hash_entry;
  auto hash_hint = kv_engine_->hash_table_->GetHint(key);
  Status s = kv_engine_->hash_table_->SearchForWrite(
      hash_hint, key, search_type_mask, &entry_ptr, &hash_entry, nullptr);
  switch (s) {
    case Status::NotFound: {
      kv_engine_->hash_table_->Insert(hash_hint, entry_ptr, record_type,
                                      index_ptr, index_type);
      return Status::Ok;
    }
    case Status::Ok: {
      GlobalLogger.Error(
          "Rebuild skiplist error, hash entry of sorted records should not be "
          "inserted before rebuild\n");
      return Status::Abort;
    }

    case Status::MemoryOverflow: {
      return s;
    }

    default:
      std::abort();  // never reach
  }

  return Status::Ok;
}
}  // namespace KVDK_NAMESPACE