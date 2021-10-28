/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <algorithm>
#include <future>
#include <libpmem.h>

#include "hash_table.hpp"
#include "kv_engine.hpp"
#include "skiplist.hpp"

namespace KVDK_NAMESPACE {

pmem::obj::string_view SkiplistNode::UserKey() {
  if (cached_key_size > 0) {
    return pmem::obj::string_view(cached_key, cached_key_size);
  }
  return Skiplist::UserKey(record->Key());
}

void SkiplistNode::SeekNode(const pmem::obj::string_view &key,
                            uint8_t start_height, uint8_t end_height,
                            Splice *result_splice) {
  std::unique_ptr<std::vector<SkiplistNode *>> to_delete(nullptr);
  assert(height >= start_height && end_height >= 1);
  SkiplistNode *prev = this;
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
        } else if (prev == this) {
          // this node has been deleted, so seek from header
          assert(result_splice->seeking_list);
          prev = result_splice->seeking_list->header();
          i = kMaxHeight;
        } else {
          prev = this;
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
            if (to_delete == nullptr) {
              to_delete.reset(new std::vector<SkiplistNode *>());
            }
            to_delete->push_back(next.RawPointer());
          }
        }
        // if prev is marked deleted before cas, cas will be failed, and prev
        // will be roll back in next round
        continue;
      }
      int cmp = compare_string_view(key, next->UserKey());

      if (cmp > 0) {
        prev = next.RawPointer();
      } else {
        result_splice->nexts[i] = next.RawPointer();
        result_splice->prevs[i] = prev;
        break;
      }
    }
  }
  if (to_delete && to_delete->size() > 0) {
    result_splice->seeking_list->ObsoleteNodes(*to_delete);
  }
}

Status Skiplist::Rebuild() {
  Splice splice(this);
  HashEntry hash_entry;
  for (uint8_t i = 1; i <= kMaxHeight; i++) {
    splice.prevs[i] = header_;
    splice.prev_pmem_record = header_->record;
  }
  while (1) {
    HashEntry *entry_ptr = nullptr;
    uint64_t next_offset = splice.prev_pmem_record->next;
    DLRecord *next_record = pmem_allocator_->offset2addr<DLRecord>(next_offset);
    assert(next_record != nullptr);
    if (next_record == header()->record) {
      break;
    }

    pmem::obj::string_view key = next_record->Key();
    Status s = hash_table_->SearchForRead(hash_table_->GetHint(key), key,
                                          SortedDataRecord, &entry_ptr,
                                          &hash_entry, nullptr);
    // these nodes should be already created during data restoring
    if (s != Status::Ok) {
      GlobalLogger.Error("Rebuild skiplist error\n");
      return s;
    }
    if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
      SkiplistNode *dram_node = (SkiplistNode *)hash_entry.offset;
      int height = dram_node->Height();
      for (int i = 1; i <= height; i++) {
        splice.prevs[i]->RelaxedSetNext(i, dram_node);
        dram_node->RelaxedSetNext(i, nullptr);
        splice.prevs[i] = dram_node;
      }
    }
    splice.prev_pmem_record = next_record;
  }
  return Status::Ok;
}

void Skiplist::Seek(const pmem::obj::string_view &key, Splice *result_splice) {
  result_splice->seeking_list = this;
  header_->SeekNode(key, header_->Height(), 1, result_splice);
  assert(result_splice->prevs[1] != nullptr);
  DLRecord *prev_record = result_splice->prevs[1]->record;
  DLRecord *next_record = nullptr;
  while (1) {
    next_record = pmem_allocator_->offset2addr<DLRecord>(prev_record->next);
    assert(next_record != nullptr);
    if (next_record == header()->record) {
      break;
    }

    int cmp = compare_string_view(key, UserKey(next_record->Key()));
    if (cmp > 0) {
      prev_record = next_record;
    } else {
      break;
    }
  }
  result_splice->next_pmem_record = next_record;
  result_splice->prev_pmem_record = prev_record;
}

uint64_t SkiplistNode::GetSkipListId() {
  uint64_t id;
  memcpy_8(&id, record->Key().data());
  return id;
}

Status Skiplist::CheckConnection(int height) {
  SkiplistNode *cur_node = header_;
  DLRecord *cur_record = cur_node->record;
  while (true) {
    SkiplistNode *next_node = cur_node->Next(height).RawPointer();
    uint64_t next_offset = cur_record->next;
    DLRecord *next_record = pmem_allocator_->offset2addr<DLRecord>(next_offset);
    assert(next_record != nullptr);
    if (next_record == header()->record) {
      if (next_node != nullptr) {
        GlobalLogger.Error("when next pmem data record is skiplist header, the "
                           "next node should be nullptr\n");
        return Status::Abort;
      }
      break;
    }
    HashEntry hash_entry;
    DataEntry data_entry;
    HashEntry *entry_ptr = nullptr;
    pmem::obj::string_view key = next_record->Key();
    Status s = hash_table_->SearchForRead(hash_table_->GetHint(key), key,
                                          SortedDataRecord, &entry_ptr,
                                          &hash_entry, &data_entry);
    assert(s == Status::Ok && "search node fail!");

    if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
      SkiplistNode *dram_node = (SkiplistNode *)hash_entry.offset;
      if (next_node == nullptr) {
        if (dram_node->Height() >= height) {
          GlobalLogger.Error(
              "when next_node is nullptr, the dram data entry should be "
              "DLRecord or dram node's height < cur_node's height\n");
          return Status::Abort;
        }
      } else {
        if (dram_node->Height() >= height) {
          if (!(dram_node->Height() == next_node->Height() &&
                dram_node->GetSkipListId() == next_node->GetSkipListId() &&
                dram_node->UserKey() == next_node->UserKey())) {
            GlobalLogger.Error("incorret skiplist node info\n");
            return Status::Abort;
          }
          cur_node = next_node;
        }
      }
    }
    cur_record = next_record;
  }
  return Status::Ok;
}

bool Skiplist::FindUpdatePos(Splice *splice,
                             const pmem::obj::string_view &updated_key,
                             const HashTable::KeyHashHint &hint,
                             const DLRecord *updated_record,
                             std::unique_lock<SpinMutex> *prev_record_lock) {
  while (1) {
    DLRecord *prev =
        pmem_allocator_->offset2addr<DLRecord>(updated_record->prev);
    DLRecord *next =
        pmem_allocator_->offset2addr<DLRecord>(updated_record->next);
    assert(prev != nullptr);
    assert(next != nullptr);
    splice->prev_pmem_record = prev;
    splice->next_pmem_record = next;
    uint64_t prev_offset = pmem_allocator_->addr2offset(prev);
    uint64_t next_offset = pmem_allocator_->addr2offset(next);

    auto prev_hint = hash_table_->GetHint(prev->Key());
    if (prev_hint.spin != hint.spin) {
      if (!prev_hint.spin->try_lock()) {
        return false;
      }
      *prev_record_lock =
          std::unique_lock<SpinMutex>(*prev_hint.spin, std::adopt_lock);
    }

    // Check if the list has changed before we successfully acquire lock.
    // As updating record is already locked, so we don't need to
    // check its next
    if (updated_record->prev != prev_offset ||
        prev->next != pmem_allocator_->addr2offset((void *)updated_record)) {
      continue;
    }

    assert(updated_record->prev == prev_offset);
    assert(updated_record->next == next_offset);
    assert(next->prev == pmem_allocator_->addr2offset((void *)updated_record));
    assert(prev == header_->record ||
           compare_string_view(Skiplist::UserKey(prev->Key()), updated_key) <
               0);
    assert(next == header_->record ||
           compare_string_view(Skiplist::UserKey(next->Key()), updated_key) >
               0);

    return true;
  }
}

bool Skiplist::FindInsertPos(Splice *splice,
                             const pmem::obj::string_view &insert_key,
                             const HashTable::KeyHashHint &hint,
                             std::unique_lock<SpinMutex> *prev_record_lock) {
  while (1) {
    Seek(insert_key, splice);
    DLRecord *prev = splice->prev_pmem_record;

    assert(prev != nullptr);
    uint64_t prev_offset = pmem_allocator_->addr2offset(prev);

    auto prev_hint = hash_table_->GetHint(prev->Key());
    if (prev_hint.spin != hint.spin) {
      if (!prev_hint.spin->try_lock()) {
        return false;
      }
      *prev_record_lock =
          std::unique_lock<SpinMutex>(*prev_hint.spin, std::adopt_lock);
    }
    uint64_t next_offset = prev->next;
    DLRecord *next = pmem_allocator_->offset2addr<DLRecord>(next_offset);
    assert(next != nullptr);
    // Check if the linkage has changed before we successfully acquire lock.
    if (next != splice->next_pmem_record || next->prev != prev_offset) {
      prev_record_lock->unlock();
      continue;
    }
    assert(prev->next == next_offset);
    assert(next->prev == prev_offset);

    assert(prev == header_->record ||
           compare_string_view(Skiplist::UserKey(prev->Key()), insert_key) < 0);
    assert(next == header_->record ||
           compare_string_view(Skiplist::UserKey(next->Key()), insert_key) > 0);

    return true;
  }
}

void Skiplist::DeleteRecord(DLRecord *deleting_record, Splice *delete_splice,
                            SkiplistNode *dram_node) {
  uint64_t deleting_offset = pmem_allocator_->addr2offset(deleting_record);
  DLRecord *prev = delete_splice->prev_pmem_record;
  DLRecord *next = delete_splice->next_pmem_record;
  assert(prev->next == deleting_offset);
  // For repair in recovery due to crashes during pointers changing, we should
  // first unlink deleting entry from prev's next
  prev->next = pmem_allocator_->addr2offset(next);
  pmem_persist(&prev->next, 8);
  assert(next != nullptr);
  assert(next->prev == pmem_allocator_->addr2offset(deleting_record));
  next->prev = pmem_allocator_->addr2offset(prev);
  pmem_persist(&next->prev, 8);
  deleting_record->Destroy();

  if (dram_node) {
    dram_node->MarkAsRemoved();
  }
}

bool Skiplist::Insert(const pmem::obj::string_view &inserting_key,
                      const pmem::obj::string_view &value,
                      const SizedSpaceEntry &space_to_write, uint64_t timestamp,
                      SkiplistNode **dram_node, SpinMutex *inserting_key_lock) {
  Splice splice(this);
  std::unique_lock<SpinMutex> prev_record_lock;
  // if (!FindInsertPos(&splice, inserting_key, , &prev_record_lock)) {
  // return false;
  // }

  std::string internal_key(InternalKey(inserting_key));
  uint64_t prev_offset = pmem_allocator_->addr2offset(splice.prev_pmem_record);
  uint64_t next_offset = pmem_allocator_->addr2offset(splice.next_pmem_record);
  DLRecord *pmem_record = DLRecord::PersistDLRecord(
      pmem_allocator_->offset2addr(space_to_write.space_entry.offset),
      space_to_write.size, timestamp, SortedDataRecord, prev_offset,
      next_offset, internal_key, value);

  *dram_node =
      InsertRecord(&splice, pmem_record, inserting_key, *dram_node, false);
  return true;
}

bool Skiplist::Update(const pmem::obj::string_view &key,
                      const pmem::obj::string_view &value,
                      const DLRecord *updated_record,
                      const SizedSpaceEntry &space_to_write, uint64_t timestamp,
                      SkiplistNode *dram_node, SpinMutex *inserting_key_lock) {
  Splice splice(this);
  std::unique_lock<SpinMutex> prev_record_lock;
  // if (!FindUpdatePos(&splice, key, , updated_record, &prev_record_lock)) {
  // return false;
  // }

  std::string internal_key(InternalKey(key));
  uint64_t prev_offset = pmem_allocator_->addr2offset(splice.prev_pmem_record);
  uint64_t next_offset = pmem_allocator_->addr2offset(splice.next_pmem_record);
  DLRecord *pmem_record = DLRecord::PersistDLRecord(
      pmem_allocator_->offset2addr(space_to_write.space_entry.offset),
      space_to_write.size, timestamp, SortedDataRecord, prev_offset,
      next_offset, internal_key, value);
  UpdateRecord(&splice, pmem_record, dram_node);
  return true;
}

void Skiplist::UpdateRecord(Splice *update_splice, DLRecord *new_record,
                            SkiplistNode *dram_node) {
  uint64_t new_record_offset = pmem_allocator_->addr2offset(new_record);
  DLRecord *prev = update_splice->prev_pmem_record;
  DLRecord *next = update_splice->next_pmem_record;
  prev->next = new_record_offset;
  pmem_persist(&prev->next, 8);
  next->prev = new_record_offset;
  pmem_persist(&next->prev, 8);
  if (dram_node != nullptr) {
    dram_node->record = new_record;
  }
}

SkiplistNode *Skiplist::InsertRecord(Splice *insert_splice,
                                     DLRecord *new_record,
                                     const pmem::obj::string_view &key) {
  uint64_t new_record_offset = pmem_allocator_->addr2offset(new_record);
  DLRecord *prev = insert_splice->prev_pmem_record;
  DLRecord *next = insert_splice->next_pmem_record;
  prev->next = new_record_offset;
  pmem_persist(&prev->next, 8);
  next->prev = new_record_offset;
  pmem_persist(&next->prev, 8);

  auto height = Skiplist::RandomHeight();
  if (height > 0) {
    SkiplistNode *dram_node = SkiplistNode::NewNode(key, new_record, height);
    for (int i = 1; i <= height; i++) {
      while (1) {
        auto now_next = insert_splice->prevs[i]->Next(i);
        // if next has been changed or been deleted, re-compute
        if (now_next.RawPointer() == insert_splice->nexts[i] &&
            now_next.GetTag() == 0) {
          dram_node->RelaxedSetNext(i, insert_splice->nexts[i]);
          if (insert_splice->prevs[i]->CASNext(i, insert_splice->nexts[i],
                                               dram_node)) {
            break;
          }
        } else {
          insert_splice->Recompute(key, i);
        }
      }
    }
    return dram_node;
  } else {
    return nullptr;
  }
}

SkiplistNode *
Skiplist::InsertRecord(Splice *insert_splice, DLRecord *inserting_record,
                       const pmem::obj::string_view &inserting_key,
                       SkiplistNode *dram_node, bool is_update) {
  uint64_t inserting_record_offset =
      pmem_allocator_->addr2offset(inserting_record);
  DLRecord *prev = insert_splice->prev_pmem_record;
  DLRecord *next = insert_splice->next_pmem_record;
  assert(is_update || prev->next == pmem_allocator_->addr2offset(next));
  // For repair in recovery due to crashes during pointers changing, we should
  // first link inserting entry to prev's next
  prev->next = inserting_record_offset;
  pmem_persist(&prev->next, 8);
  assert(next != nullptr);
  assert(is_update || next->prev == pmem_allocator_->addr2offset(prev));
  next->prev = inserting_record_offset;
  pmem_persist(&next->prev, 8);

  // new dram node
  if (!is_update) {
    assert(dram_node == nullptr);
    auto height = Skiplist::RandomHeight();
    if (height > 0) {
      dram_node =
          SkiplistNode::NewNode(inserting_key, inserting_record, height);
      for (int i = 1; i <= height; i++) {
        while (1) {
          auto now_next = insert_splice->prevs[i]->Next(i);
          // if next has been changed or been deleted, re-compute
          if (now_next.RawPointer() == insert_splice->nexts[i] &&
              now_next.GetTag() == 0) {
            dram_node->RelaxedSetNext(i, insert_splice->nexts[i]);
            if (insert_splice->prevs[i]->CASNext(i, insert_splice->nexts[i],
                                                 dram_node)) {
              break;
            }
          } else {
            insert_splice->Recompute(inserting_key, i);
          }
        }
      }
    }
  } else {
    if (dram_node != nullptr) {
      dram_node->record = inserting_record;
    }
  }
  return dram_node;
}

void SortedIterator::Seek(const std::string &key) {
  assert(skiplist_);
  Splice splice(skiplist_);
  skiplist_->Seek(key, &splice);
  current = splice.next_pmem_record;
}

void SortedIterator::SeekToFirst() {
  uint64_t first = skiplist_->header()->record->next;
  current = pmem_allocator_->offset2addr<DLRecord>(first);
}

void SortedIterator::SeekToLast() {
  uint64_t last = skiplist_->header()->record->prev;
  current = pmem_allocator_->offset2addr<DLRecord>(last);
}

void SortedIterator::Next() {
  if (!Valid()) {
    return;
  }
  current = pmem_allocator_->offset2addr<DLRecord>(current->next);
}

void SortedIterator::Prev() {
  if (!Valid()) {
    return;
  }
  current = (pmem_allocator_->offset2addr<DLRecord>(current->prev));
}

std::string SortedIterator::Key() {
  if (!Valid())
    return "";
  pmem::obj::string_view key = Skiplist::UserKey(current->Key());
  return std::string(key.data(), key.size());
}

std::string SortedIterator::Value() {
  if (!Valid())
    return "";
  pmem::obj::string_view value = current->Value();
  return std::string(value.data(), value.size());
}

Status SortedCollectionRebuilder::Rebuild(const KVEngine *engine) {
  std::vector<std::future<Status>> fs;
  if (engine->configs_.opt_large_sorted_collection_restore &&
      engine->skiplists_.size() > 0) {
    thread_cache_node_.resize(engine->configs_.max_write_threads);
    UpdateEntriesOffset(engine);
    for (uint8_t h = 1; h <= kMaxHeight; ++h) {
      for (uint32_t j = 0; j < engine->configs_.max_write_threads; ++j) {
        fs.push_back(
            std::async(std::launch::async, [j, h, this, engine]() -> Status {
              while (true) {
                SkiplistNode *cur_node = GetSortedOffset(h);
                if (!cur_node) {
                  break;
                }
                if (h == 1) {
                  Status s = DealWithFirstHeight(j, cur_node, engine);
                  if (s != Status::Ok) {
                    return s;
                  }
                } else {
                  DealWithOtherHeight(j, cur_node, h, engine->pmem_allocator_);
                }
              }
              LinkedNode(j, h, engine);
              return Status::Ok;
            }));
      }
      for (auto &f : fs) {
        Status s = f.get();
        if (s != Status::Ok) {
          return s;
        }
      }
      fs.clear();
      for (auto &kv : entries_offsets_) {
        kv.second.is_visited = false;
      }
#ifdef DEBUG_CHECK
      GlobalLogger.Info("Restoring skiplist height %d\n", h);
      GlobalLogger.Info("Check skiplist connecton\n");
      for (auto skiplist : engine->skiplists_) {
        Status s = skiplist->CheckConnection(h);
        if (s != Status::Ok) {
          return s;
        }
      }
#endif
    }
  } else {
    for (auto s : engine->skiplists_) {
      fs.push_back(std::async(&Skiplist::Rebuild, s));
    }
    for (auto &f : fs) {
      Status s = f.get();
      if (s != Status::Ok) {
        return s;
      }
    }
  }
  return Status::Ok;
}

void SortedCollectionRebuilder::LinkedNode(uint64_t thread_id, int height,
                                           const KVEngine *engine) {
  for (auto v : thread_cache_node_[thread_id]) {
    if (v->Height() < height) {
      continue;
    }
    uint64_t next_offset = v->record->next;
    DLRecord *next_record =
        engine->pmem_allocator_->offset2addr<DLRecord>(next_offset);
    assert(next_record != nullptr);
    // TODO jiayu: maybe cache data entry
    if (next_record->entry.meta.type != SortedHeaderRecord &&
        v->Next(height) == nullptr) {
      SkiplistNode *next_node = nullptr;
      // if height == 1, need to scan pmem data_entry
      if (height == 1) {
        while (next_record->entry.meta.type != SortedHeaderRecord) {
          HashEntry hash_entry;
          DataEntry data_entry;
          HashEntry *entry_ptr = nullptr;
          pmem::obj::string_view key = next_record->Key();
          Status s = engine->hash_table_->SearchForRead(
              engine->hash_table_->GetHint(key), key, SortedRecordType,
              &entry_ptr, &hash_entry, &data_entry);
          assert(s == Status::Ok &&
                 "It should be in hash_table when reseting entries_offset map");
          next_offset = next_record->next;
          next_record =
              engine->pmem_allocator_->offset2addr<DLRecord>(next_offset);

          if (hash_entry.header.offset_type == HashOffsetType::Skiplist) {
            next_node = ((Skiplist *)hash_entry.offset)->header();
          } else if (hash_entry.header.offset_type ==
                     HashOffsetType::SkiplistNode) {
            next_node = (SkiplistNode *)hash_entry.offset;
          } else {
            continue;
          }
          if (next_node->Height() >= height) {
            break;
          }
        }
      } else {
        SkiplistNode *pnode = v->Next(height - 1).RawPointer();
        while (pnode) {
          if (pnode->Height() >= height) {
            next_node = pnode;
            break;
          }
          pnode = pnode->Next(height - 1).RawPointer();
        }
      }
      if (next_node) {
        v->RelaxedSetNext(height, next_node);
      }
    }
  }
  thread_cache_node_[thread_id].clear();
}

SkiplistNode *SortedCollectionRebuilder::GetSortedOffset(int height) {
  std::lock_guard<SpinMutex> kv_mux(map_mu_);
  for (auto &kv : entries_offsets_) {
    if (!kv.second.is_visited &&
        kv.second.visited_node->Height() >= height - 1) {
      kv.second.is_visited = true;
      return kv.second.visited_node;
    }
  }
  return nullptr;
}

Status SortedCollectionRebuilder::DealWithFirstHeight(uint64_t thread_id,
                                                      SkiplistNode *cur_node,
                                                      const KVEngine *engine) {
  DLRecord *vesiting_record = cur_node->record;
  while (true) {
    uint64_t next_offset = vesiting_record->next;
    DLRecord *next_record =
        engine->pmem_allocator_->offset2addr<DLRecord>(next_offset);
    assert(next_record != nullptr);
    if (next_record->entry.meta.type == SortedHeaderRecord) {
      cur_node->RelaxedSetNext(1, nullptr);
      break;
    }
    // continue to build connention
    if (entries_offsets_.find(next_offset) == entries_offsets_.end()) {
      HashEntry hash_entry;
      DataEntry data_entry;
      HashEntry *entry_ptr = nullptr;
      pmem::obj::string_view key = next_record->Key();
      Status s = engine->hash_table_->SearchForRead(
          engine->hash_table_->GetHint(key), key, SortedRecordType, &entry_ptr,
          &hash_entry, &data_entry);
      if (s != Status::Ok) {
        GlobalLogger.Error(
            "the node should be already created during data restoring\n");
        return Status::Abort;
      }
      vesiting_record = next_record;

      SkiplistNode *next_node = nullptr;
      assert(hash_entry.header.offset_type == HashOffsetType::SkiplistNode ||
             hash_entry.header.offset_type == HashOffsetType::DLRecord &&
                 "next entry should be skiplistnode type or data_entry type!");
      if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
        next_node = (SkiplistNode *)hash_entry.offset;
      }
      // excepte data_entry node (height = 0)
      if (next_node) {
        if (next_node->Height() >= 1) {
          cur_node->RelaxedSetNext(1, next_node);
          next_node->RelaxedSetNext(1, nullptr);
          cur_node = next_node;
        }
      }
    } else {
      cur_node->RelaxedSetNext(1, nullptr);
      thread_cache_node_[thread_id].insert(cur_node);
      break;
    }
  }
  return Status::Ok;
}

void SortedCollectionRebuilder::DealWithOtherHeight(
    uint64_t thread_id, SkiplistNode *cur_node, int height,
    const std::shared_ptr<PMEMAllocator> &pmem_allocator) {
  SkiplistNode *visited_node = cur_node;
  bool first_visited = true;
  while (true) {
    if (visited_node->Height() >= height) {
      if (first_visited) {
        cur_node = visited_node;
        first_visited = false;
      }
      if (cur_node != visited_node) {
        cur_node->RelaxedSetNext(height, visited_node);
        visited_node->RelaxedSetNext(height, nullptr);
        cur_node = visited_node;
      }
    }

    SkiplistNode *next_node =
        visited_node->Height() >= height
            ? visited_node->Next(height - 1).RawPointer()
            : visited_node->Next(visited_node->Height()).RawPointer();
    if (next_node == nullptr) {
      if (cur_node->Height() >= height) {
        cur_node->RelaxedSetNext(height, nullptr);
        thread_cache_node_[thread_id].insert(cur_node);
      }
      break;
    }
    // continue to find next
    uint64_t next_offset = pmem_allocator->addr2offset(next_node->record);
    if (entries_offsets_.find(next_offset) == entries_offsets_.end()) {
      visited_node = next_node;
    } else {
      if (cur_node->Height() >= height) {
        cur_node->RelaxedSetNext(height, nullptr);
        thread_cache_node_[thread_id].insert(cur_node);
      }
      break;
    }
  }
}

void SortedCollectionRebuilder::UpdateEntriesOffset(const KVEngine *engine) {
  std::unordered_map<uint64_t, SkiplistNodeInfo> new_kvs;
  std::unordered_map<uint64_t, SkiplistNodeInfo>::iterator it =
      entries_offsets_.begin();
  while (it != entries_offsets_.end()) {
    DataEntry data_entry;
    HashEntry *entry_ptr = nullptr;
    HashEntry hash_entry;
    SkiplistNode *node = nullptr;
    DLRecord *cur_record =
        engine->pmem_allocator_->offset2addr<DLRecord>(it->first);
    pmem::obj::string_view key = cur_record->Key();

    Status s = engine->hash_table_->SearchForRead(
        engine->hash_table_->GetHint(key), key, SortedRecordType, &entry_ptr,
        &hash_entry, &data_entry);
    assert(s == Status::Ok || s == Status::NotFound);
    if (s == Status::NotFound ||
        hash_entry.header.offset_type == HashOffsetType::DLRecord) {
      it = entries_offsets_.erase(it);
      continue;
    }
    if (hash_entry.header.offset_type == HashOffsetType::Skiplist) {
      node = ((Skiplist *)hash_entry.offset)->header();
    } else if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
      node = (SkiplistNode *)hash_entry.offset;
    }
    assert(node && "should be not empty!");

    // remove old kv;
    if (data_entry.meta.timestamp > cur_record->entry.meta.timestamp) {
      it = entries_offsets_.erase(it);
      new_kvs.insert(
          {engine->pmem_allocator_->addr2offset(node->record), {false, node}});
    } else {
      it->second.visited_node = node;
      it++;
    }
  }
  entries_offsets_.insert(new_kvs.begin(), new_kvs.end());
}

} // namespace KVDK_NAMESPACE