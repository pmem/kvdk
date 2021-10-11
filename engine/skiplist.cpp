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
  return Skiplist::UserKey(data_entry->Key());
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
  DLDataEntry data_entry;
  for (uint8_t i = 1; i <= kMaxHeight; i++) {
    splice.prevs[i] = header_;
    splice.prev_data_entry = header_->data_entry;
  }
  while (1) {
    HashEntry *entry_base = nullptr;
    uint64_t next_offset = splice.prev_data_entry->next;
    DLDataEntry *next_data_entry =
        (DLDataEntry *)pmem_allocator_->offset2addr(next_offset);
    assert(next_data_entry != nullptr);
    if (next_data_entry == header()->data_entry) {
      break;
    }

    pmem::obj::string_view key = next_data_entry->Key();
    Status s = hash_table_->Search(hash_table_->GetHint(key), key,
                                   SortedDataRecord, &hash_entry, &data_entry,
                                   &entry_base, HashTable::SearchPurpose::Read);
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
    splice.prev_data_entry = next_data_entry;
  }
  return Status::Ok;
}

void Skiplist::Seek(const pmem::obj::string_view &key, Splice *result_splice) {
  result_splice->seeking_list = this;
  header_->SeekNode(key, header_->Height(), 1, result_splice);
  assert(result_splice->prevs[1] != nullptr);
  DLDataEntry *prev_data_entry = result_splice->prevs[1]->data_entry;
  DLDataEntry *next_data_entry = nullptr;
  while (1) {
    next_data_entry =
        (DLDataEntry *)pmem_allocator_->offset2addr(prev_data_entry->next);
    assert(next_data_entry != nullptr);
    if (next_data_entry == header()->data_entry) {
      break;
    }

    int cmp = compare_string_view(key, UserKey(next_data_entry->Key()));
    if (cmp > 0) {
      prev_data_entry = next_data_entry;
    } else {
      break;
    }
  }
  result_splice->next_data_entry = next_data_entry;
  result_splice->prev_data_entry = prev_data_entry;
}

uint64_t SkiplistNode::GetSkipListId() {
  uint64_t id;
  memcpy_8(&id, data_entry->Key().data());
  return id;
}

Status Skiplist::CheckConnection(int height) {
  SkiplistNode *cur_node = header_;
  DLDataEntry *cur_data_entry = cur_node->data_entry;
  while (true) {
    SkiplistNode *next_node = cur_node->Next(height).RawPointer();
    uint64_t next_offset = cur_data_entry->next;
    DLDataEntry *next_data_entry =
        (DLDataEntry *)pmem_allocator_->offset2addr(next_offset);
    assert(next_data_entry != nullptr);
    if (next_data_entry == header()->data_entry) {
      if (next_node != nullptr) {
        GlobalLogger.Error("when next pmem data entry is skiplist header, the "
                           "next node should be nullptr\n");
        return Status::Abort;
      }
      break;
    }
    HashEntry hash_entry;
    DLDataEntry data_entry;
    HashEntry *entry_base = nullptr;
    pmem::obj::string_view key = next_data_entry->Key();
    Status s = hash_table_->Search(hash_table_->GetHint(key), key,
                                   SortedDataRecord, &hash_entry, &data_entry,
                                   &entry_base, HashTable::SearchPurpose::Read);
    assert(s == Status::Ok && "search node fail!");

    if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
      SkiplistNode *dram_node = (SkiplistNode *)hash_entry.offset;
      if (next_node == nullptr) {
        if (dram_node->Height() >= height) {
          GlobalLogger.Error(
              "when next_node is nullptr, the dram data entry should be "
              "DLDataEntry or dram node's height < cur_node's height\n");
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
    cur_data_entry = next_data_entry;
  }
  return Status::Ok;
}

bool Skiplist::FindAndLockWritePos(Splice *splice,
                                   const pmem::obj::string_view &insert_key,
                                   const HashTable::KeyHashHint &hint,
                                   std::vector<SpinMutex *> &spins,
                                   DLDataEntry *updated_data_entry) {
  spins.clear();
  DLDataEntry *prev;
  DLDataEntry *next;
  if (updated_data_entry != nullptr) {
    prev =
        (DLDataEntry *)(pmem_allocator_->offset2addr(updated_data_entry->prev));
    next =
        (DLDataEntry *)(pmem_allocator_->offset2addr(updated_data_entry->next));
    splice->prev_data_entry = prev;
    splice->next_data_entry = next;
  } else {
    Seek(insert_key, splice);

    prev = splice->prev_data_entry;
    next = splice->next_data_entry;
    assert(prev == header_->data_entry ||
           compare_string_view(Skiplist::UserKey(prev->Key()), insert_key) < 0);
  }

  uint64_t prev_offset = pmem_allocator_->addr2offset(prev);
  uint64_t next_offset = pmem_allocator_->addr2offset(next);

  // sequentially lock to prevent deadlock
  auto cmp = [](const SpinMutex *s1, const SpinMutex *s2) {
    return (uint64_t)s1 < (uint64_t)s2;
  };
  auto prev_hint = hash_table_->GetHint(prev->Key());
  if (prev_hint.spin != hint.spin) {
    spins.push_back(prev_hint.spin);
  }
  assert(next != nullptr);
  auto next_hint = hash_table_->GetHint(next->Key());
  if (next_hint.spin != hint.spin && next_hint.spin != prev_hint.spin) {
    spins.push_back(next_hint.spin);
  }
  std::sort(spins.begin(), spins.end(), cmp);
  for (int i = 0; i < spins.size(); i++) {
    if (spins[i]->try_lock()) {
    } else {
      for (int j = 0; j < i; j++) {
        spins[j]->unlock();
      }
      spins.clear();
      return false;
    }
  }

  if (updated_data_entry) {
    assert(updated_data_entry->prev == pmem_allocator_->addr2offset(prev));
    assert(updated_data_entry->next == pmem_allocator_->addr2offset(next));
  }

  // Check the list has changed before we successfully locked
  // For update, we do not need to check because the key is already locked
  if (!updated_data_entry &&
      (prev->next != next_offset || next->prev != prev_offset)) {
    for (auto &m : spins) {
      m->unlock();
    }
    spins.clear();
    return false;
  }

  return true;
}

void Skiplist::DeleteDataEntry(DLDataEntry *deleting_entry,
                               Splice *delete_splice, SkiplistNode *dram_node) {
  DLDataEntry *prev = delete_splice->prev_data_entry;
  DLDataEntry *next = delete_splice->next_data_entry;
  assert(prev->next == pmem_allocator_->addr2offset(deleting_entry));
  // For repair in recovery due to crashes during pointers changing, we should
  // first unlink deleting entry from prev's next
  prev->next = pmem_allocator_->addr2offset(next);
  pmem_persist(&prev->next, 8);
  assert(next != nullptr);
  assert(next->prev == pmem_allocator_->addr2offset(deleting_entry));
  next->prev = pmem_allocator_->addr2offset(prev);
  pmem_persist(&next->prev, 8);

  if (dram_node) {
    dram_node->MarkAsRemoved();
  }
}

SkiplistNode *
Skiplist::InsertDataEntry(Splice *insert_splice, DLDataEntry *inserting_entry,
                          const pmem::obj::string_view &inserting_key,
                          SkiplistNode *data_node, bool is_update) {
  uint64_t inserting_entry_offset =
      pmem_allocator_->addr2offset(inserting_entry);
  DLDataEntry *prev = insert_splice->prev_data_entry;
  DLDataEntry *next = insert_splice->next_data_entry;
  assert(is_update || prev->next == pmem_allocator_->addr2offset(next));
  // For repair in recovery due to crashes during pointers changing, we should
  // first link inserting entry to prev's next
  prev->next = inserting_entry_offset;
  pmem_persist(&prev->next, 8);
  assert(next != nullptr);
  assert(is_update || next->prev == pmem_allocator_->addr2offset(prev));
  next->prev = inserting_entry_offset;
  pmem_persist(&next->prev, 8);

  // new dram node
  if (!is_update) {
    assert(data_node == nullptr);
    auto height = Skiplist::RandomHeight();
    data_node = SkiplistNode::NewNode(inserting_key, inserting_entry, height);
    for (int i = 1; i <= height; i++) {
      while (1) {
        auto now_next = insert_splice->prevs[i]->Next(i);
        // if next has been changed or been deleted, re-compute
        if (now_next.RawPointer() == insert_splice->nexts[i] &&
            now_next.GetTag() == 0) {
          data_node->RelaxedSetNext(i, insert_splice->nexts[i]);
          if (insert_splice->prevs[i]->CASNext(i, insert_splice->nexts[i],
                                               data_node)) {
            break;
          }
        } else {
          insert_splice->Recompute(inserting_key, i);
        }
      }
    }
  } else {
    if (data_node != nullptr) {
      data_node->data_entry = inserting_entry;
    }
  }
  return data_node;
}

void SortedIterator::Seek(const std::string &key) {
  assert(skiplist_);
  Splice splice(skiplist_);
  skiplist_->Seek(key, &splice);
  current = splice.next_data_entry;
}

void SortedIterator::SeekToFirst() {
  uint64_t first = skiplist_->header()->data_entry->next;
  current = (DLDataEntry *)pmem_allocator_->offset2addr(first);
}

void SortedIterator::SeekToLast() {
  uint64_t last = skiplist_->header()->data_entry->prev;
  current = (DLDataEntry *)pmem_allocator_->offset2addr(last);
}

void SortedIterator::Next() {
  if (!Valid()) {
    return;
  }
  current = (DLDataEntry *)pmem_allocator_->offset2addr(current->next);
}

void SortedIterator::Prev() {
  if(!Valid()){
    return;
  }
  current = (DLDataEntry *)(pmem_allocator_->offset2addr(current->prev));
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
    uint64_t next_offset = v->data_entry->next;
    DLDataEntry *next_data_entry =
        (DLDataEntry *)engine->pmem_allocator_->offset2addr(next_offset);
    assert(next_data_entry != nullptr);
    if (next_data_entry->type != SortedHeaderRecord &&
        v->Next(height) == nullptr) {
      SkiplistNode *next_node = nullptr;
      // if height == 1, need to scan pmem data_entry
      if (height == 1) {
        while (next_data_entry->type != SortedHeaderRecord) {
          HashEntry hash_entry;
          DLDataEntry data_entry;
          HashEntry *entry_base = nullptr;
          pmem::obj::string_view key = next_data_entry->Key();
          Status s = engine->hash_table_->Search(
              engine->hash_table_->GetHint(key), key, SortedDataEntryType,
              &hash_entry, &data_entry, &entry_base,
              HashTable::SearchPurpose::Read);
          assert(s == Status::Ok &&
                 "It should be in hash_table when reseting entries_offset map");
          next_offset = next_data_entry->next;
          next_data_entry =
              (DLDataEntry *)engine->pmem_allocator_->offset2addr(next_offset);

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
  DLDataEntry *visit_data_entry = cur_node->data_entry;
  while (true) {
    uint64_t next_offset = visit_data_entry->next;
    DLDataEntry *next_data_entry =
        (DLDataEntry *)engine->pmem_allocator_->offset2addr(next_offset);
    assert(next_data_entry != nullptr);
    if (next_data_entry->type == SortedHeaderRecord) {
      cur_node->RelaxedSetNext(1, nullptr);
      break;
    }
    // continue to build connention
    if (entries_offsets_.find(next_offset) == entries_offsets_.end()) {
      thread_local HashEntry hash_entry;
      thread_local DLDataEntry data_entry;
      HashEntry *entry_base = nullptr;
      pmem::obj::string_view key = next_data_entry->Key();
      Status s = engine->hash_table_->Search(
          engine->hash_table_->GetHint(key), key, SortedDataEntryType,
          &hash_entry, &data_entry, &entry_base,
          HashTable::SearchPurpose::Read);
      if (s != Status::Ok) {
        GlobalLogger.Error(
            "the node should be already created during data restoring\n");
        return Status::Abort;
      }
      visit_data_entry = next_data_entry;

      SkiplistNode *next_node = nullptr;
      assert(hash_entry.header.offset_type == HashOffsetType::SkiplistNode ||
             hash_entry.header.offset_type == HashOffsetType::DLDataEntry &&
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
    uint64_t next_offset = pmem_allocator->addr2offset(next_node->data_entry);
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
    DLDataEntry data_entry;
    HashEntry *entry_base = nullptr;
    HashEntry hash_entry;
    SkiplistNode *node = nullptr;
    DLDataEntry *cur_data_entry =
        (DLDataEntry *)engine->pmem_allocator_->offset2addr(it->first);
    pmem::obj::string_view key = cur_data_entry->Key();

    Status s = engine->hash_table_->Search(
        engine->hash_table_->GetHint(key), key, SortedDataEntryType,
        &hash_entry, &data_entry, &entry_base, HashTable::SearchPurpose::Read);
    assert(s == Status::Ok || s == Status::NotFound);
    if (s == Status::NotFound ||
        hash_entry.header.offset_type == HashOffsetType::DLDataEntry) {
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
    if (data_entry.timestamp > cur_data_entry->timestamp) {
      it = entries_offsets_.erase(it);
      new_kvs.insert({engine->pmem_allocator_->addr2offset(node->data_entry),
                      {false, node}});
    } else {
      it->second.visited_node = node;
      it++;
    }
  }
  entries_offsets_.insert(new_kvs.begin(), new_kvs.end());
}

} // namespace KVDK_NAMESPACE