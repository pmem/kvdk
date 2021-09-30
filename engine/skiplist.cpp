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

Status Skiplist::Rebuild() {
  Splice splice;
  HashEntry hash_entry;
  DLDataEntry data_entry;
  for (int i = 1; i <= kMaxHeight; i++) {
    splice.prevs[i] = header_;
    splice.prev_data_entry = header_->data_entry;
  }
  while (1) {
    HashEntry *entry_base = nullptr;
    uint64_t next_offset = splice.prev_data_entry->next;
    if (next_offset == kNullPmemOffset) {
      break;
    }
    // TODO: check failure

    DLDataEntry *next_data_entry =
        (DLDataEntry *)pmem_allocator_->offset2addr(next_offset);
    pmem::obj::string_view key = next_data_entry->Key();
    Status s = hash_table_->Search(
        hash_table_->GetHint(key), key, SortedDataRecord | SortedDeleteRecord,
        &hash_entry, &data_entry, &entry_base, HashTable::SearchPurpose::Read);
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

uint64_t SkiplistNode::GetSkipListId() {
  uint64_t id;
  memcpy_8(&id, data_entry->Key().data());
  return id;
}

void Skiplist::CheckConnection(int height) {
  SkiplistNode *cur_node = header_;
  DLDataEntry *cur_data_entry = cur_node->data_entry;
  while (true) {
    SkiplistNode *next_node = cur_node->Next(height);
    uint64_t next_offset = cur_data_entry->next;
    if (next_offset == kNullPmemOffset) {
      assert(next_node == nullptr && "when next offset is kNullPmemOffset, the "
                                     "next node should be nullptr\n");
      break;
    }
    HashEntry hash_entry;
    DLDataEntry data_entry;
    HashEntry *entry_base = nullptr;
    DLDataEntry *next_data_entry =
        (DLDataEntry *)pmem_allocator_->offset2addr(next_offset);
    pmem::obj::string_view key = next_data_entry->Key();
    Status s = hash_table_->Search(
        hash_table_->GetHint(key), key, SortedDataRecord | SortedDeleteRecord,
        &hash_entry, &data_entry, &entry_base, HashTable::SearchPurpose::Read);
    assert(s == Status::Ok && "search node fail!");

    if (hash_entry.header.offset_type == HashOffsetType::SkiplistNode) {
      SkiplistNode *dram_node = (SkiplistNode *)hash_entry.offset;
      if (next_node == nullptr) {
        assert(dram_node->Height() < height &&
               "when next_node is nullptr, the dram data entry should be "
               "DLDataEntry or dram node's height < cur_node's height ");
      } else {
        if (dram_node->Height() >= height) {
          assert((dram_node->Height() == next_node->Height() &&
                  dram_node->GetSkipListId() == next_node->GetSkipListId() &&
                  dram_node->UserKey() == next_node->UserKey()) &&
                 "two skiplist node should be equal!");
          cur_node = next_node;
        }
      }
    }
    cur_data_entry = next_data_entry;
  }
}

void Skiplist::Seek(const pmem::obj::string_view &key, Splice *splice) {
  SkiplistNode *prev = header_;
  SkiplistNode *tmp;
  // TODO: do not search from max height every time
  for (int i = kMaxHeight; i >= 1; i--) {
    while (1) {
      tmp = prev->Next(i);
      if (tmp == nullptr) {
        splice->nexts[i] = nullptr;
        splice->prevs[i] = prev;
        break;
      }
      int cmp = compare_string_view(key, tmp->UserKey());

      if (cmp > 0) {
        prev = tmp;
      } else {
        splice->nexts[i] = tmp;
        splice->prevs[i] = prev;
        break;
      }
    }
  }

  DLDataEntry *prev_data_entry = prev->data_entry;
  while (1) {
    uint64_t next_data_entry_offset = prev_data_entry->next;
    if (next_data_entry_offset == kNullPmemOffset) {
      splice->prev_data_entry = prev_data_entry;
      splice->next_data_entry = nullptr;
      break;
    }
    DLDataEntry *next_data_entry =
        (DLDataEntry *)pmem_allocator_->offset2addr(next_data_entry_offset);
    int cmp = compare_string_view(key, UserKey(next_data_entry->Key()));
    if (cmp > 0) {
      prev_data_entry = next_data_entry;
    } else {
      splice->next_data_entry = next_data_entry;
      splice->prev_data_entry = prev_data_entry;
      break;
    }
  }
}

bool Skiplist::FindAndLockWritePos(Splice *splice,
                                   const pmem::obj::string_view &insert_key,
                                   const HashTable::KeyHashHint &hint,
                                   std::vector<SpinMutex *> &spins,
                                   DLDataEntry *updated_data_entry) {
  {
    spins.clear();
    DLDataEntry *prev;
    DLDataEntry *next;
    if (updated_data_entry != nullptr) {
      prev = (DLDataEntry *)(pmem_allocator_->offset2addr(
          updated_data_entry->prev));
      next = (DLDataEntry *)(pmem_allocator_->offset2addr(
          updated_data_entry->next));
      splice->prev_data_entry = prev;
      splice->next_data_entry = next;
    } else {
      Seek(insert_key, splice);
      prev = splice->prev_data_entry;
      next = splice->next_data_entry;
      assert(prev == header_->data_entry ||
             compare_string_view(Skiplist::UserKey(prev->Key()), insert_key) <
                 0);
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
    if (next != nullptr) {
      auto next_hint = hash_table_->GetHint(next->Key());
      if (next_hint.spin != hint.spin && next_hint.spin != prev_hint.spin) {
        spins.push_back(next_hint.spin);
      }
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

    // Check the list has changed before we successfully locked
    // For update, we do not need to check because the key is already locked
    if (!updated_data_entry &&
        (prev->next != next_offset || (next && next->prev != prev_offset))) {
      for (auto &m : spins) {
        m->unlock();
      }
      spins.clear();
      return false;
    }

    return true;
  }
}

void Skiplist::DeleteDataEntry(Splice *delete_splice,
                               const pmem::obj::string_view &deleting_key,
                               SkiplistNode *dram_node) {
  delete_splice->prev_data_entry->next =
      pmem_allocator_->addr2offset(delete_splice->next_data_entry);
  pmem_persist(&delete_splice->prev_data_entry->next, 8);
  if (delete_splice->next_data_entry) {
    delete_splice->next_data_entry->prev =
        pmem_allocator_->addr2offset(delete_splice->prev_data_entry);
    pmem_persist(&delete_splice->next_data_entry->prev, 8);
  }

  assert(dram_node);
  for (int i = 1; i <= dram_node->height; i++) {
    while (1) {
      if (delete_splice->prevs[i]->CASNext(i, dram_node,
                                           delete_splice->nexts[i])) {
        break;
      }
      delete_splice->Recompute(deleting_key, i);
    }
  }
}

SkiplistNode *
Skiplist::InsertDataEntry(Splice *insert_splice, DLDataEntry *inserting_entry,
                          const pmem::obj::string_view &inserting_key,
                          SkiplistNode *data_node, bool is_update) {
  uint64_t entry_offset = pmem_allocator_->addr2offset(inserting_entry);
  insert_splice->prev_data_entry->next = entry_offset;
  pmem_persist(&insert_splice->prev_data_entry->next, 8);
  if (__glibc_likely(insert_splice->next_data_entry != nullptr)) {
    insert_splice->next_data_entry->prev = entry_offset;
    pmem_persist(&insert_splice->next_data_entry->prev, 8);
  }

  // new dram node
  if (!is_update) {
    assert(data_node == nullptr);
    auto height = Skiplist::RandomHeight();
    if (height > 0) {
      data_node = SkiplistNode::NewNode(inserting_key, inserting_entry, height);
      for (int i = 1; i <= height; i++) {
        while (1) {
          data_node->RelaxedSetNext(i, insert_splice->nexts[i]);
          if (insert_splice->prevs[i]->CASNext(i, insert_splice->nexts[i],
                                               data_node)) {
            break;
          }
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

void SortedCollectionRebuilder::Rebuild(const KVEngine *engine) {
  if (engine->configs_.restore_large_sorted_collection &&
      engine->skiplists_.size() > 0) {
    thread_cache_node_.resize(engine->configs_.max_write_threads);
    UpdateEntriesOffset(engine);
    for (int h = 1; h <= kMaxHeight; ++h) {
      std::vector<std::future<void>> fv;
      for (uint32_t j = 0; j < engine->configs_.max_write_threads; ++j) {
        fv.push_back(std::async(std::launch::async, [j, h, this, engine]() {
          while (true) {
            SkiplistNode *cur_node = GetSortedOffset(h);
            if (!cur_node) {
              break;
            }
            if (h == 1) {
              DealWithFirstHeight(j, cur_node, engine);
            } else {
              DealWithOtherHeight(j, cur_node, h, engine->pmem_allocator_);
            }
          }
          LinkedNode(j, h, engine);
        }));
      }
      for (auto &f : fv) {
        f.get();
      }
      fv.clear();
      for (auto &kv : entries_offsets_) {
        kv.second.is_visited = false;
      }
      GlobalLogger.Info("Restoring skiplist height %d\n", h);
#ifdef DEBUG_CHECK
      GlobalLogger.Info("Check skiplist connecton\n");
      for (auto s : engine->skiplists_) {
        s->CheckConnection(h);
      }
#endif
    }
  } else {
    std::vector<std::future<Status>> fs;
    for (auto s : engine->skiplists_) {
      fs.push_back(std::async(&Skiplist::Rebuild, s));
    }
    for (auto &f : fs) {
      Status s = f.get();
      assert(s == Status::Ok);
    }
  }
}

void SortedCollectionRebuilder::LinkedNode(uint64_t thread_id, int height,
                                           const KVEngine *engine) {
  for (auto v : thread_cache_node_[thread_id]) {
    if (v->Height() < height) {
      continue;
    }
    if (v->data_entry->next != kNullPmemOffset && v->Next(height) == nullptr) {
      SkiplistNode *next_node = nullptr;
      // if height == 1, need to scan pmem data_entry
      if (height == 1) {
        uint64_t next_offset = v->data_entry->next;
        while (next_offset != kNullPmemOffset) {
          HashEntry hash_entry;
          DLDataEntry data_entry;
          HashEntry *entry_base = nullptr;
          DLDataEntry *next_data_entry =
              (DLDataEntry *)engine->pmem_allocator_->offset2addr(next_offset);
          pmem::obj::string_view key = next_data_entry->Key();
          Status s = engine->hash_table_->Search(
              engine->hash_table_->GetHint(key), key, SortedDataEntryType,
              &hash_entry, &data_entry, &entry_base,
              HashTable::SearchPurpose::Read);
          assert(s == Status::Ok &&
                 "It should be in hash_table when reseting entries_offset map");
          next_offset = next_data_entry->next;

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
        SkiplistNode *pnode = v->Next(height - 1);
        while (pnode) {
          if (pnode->Height() >= height) {
            next_node = pnode;
            break;
          }
          pnode = pnode->Next(height - 1);
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

void SortedCollectionRebuilder::DealWithFirstHeight(uint64_t thread_id,
                                                    SkiplistNode *cur_node,
                                                    const KVEngine *engine) {
  DLDataEntry *visit_data_entry = cur_node->data_entry;
  while (true) {
    uint64_t next_offset = visit_data_entry->next;
    if (next_offset == kNullPmemOffset) {
      cur_node->RelaxedSetNext(1, nullptr);
      break;
    }
    // continue to build connention
    if (entries_offsets_.find(next_offset) == entries_offsets_.end()) {
      DLDataEntry *next_data_entry =
          (DLDataEntry *)engine->pmem_allocator_->offset2addr(next_offset);

      thread_local HashEntry hash_entry;
      thread_local DLDataEntry data_entry;
      HashEntry *entry_base = nullptr;
      pmem::obj::string_view key = next_data_entry->Key();
      Status s = engine->hash_table_->Search(
          engine->hash_table_->GetHint(key), key, SortedDataEntryType,
          &hash_entry, &data_entry, &entry_base,
          HashTable::SearchPurpose::Read);
      assert(s == Status::Ok &&
             "the node should be already created during data restoring");

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

    SkiplistNode *next_node = visited_node->Height() >= height
                                  ? visited_node->Next(height - 1)
                                  : visited_node->Next(visited_node->Height());
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