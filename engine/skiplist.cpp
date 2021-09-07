/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <libpmem.h>

#include "algorithm"
#include "hash_table.hpp"
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

bool Skiplist::SeekNode(const SkiplistNode *node, Splice *splice) {
  if (node == nullptr) {
    return false;
  }
  SkiplistNode *prev = header_;
  SkiplistNode *tmp;

  for (int i = node->height; i >= 1; i--) {
    while (1) {
      tmp = prev->Next(i).RawPointer();
      // Not exist
      if (tmp == nullptr) {
        return false;
      }

      if (tmp == node) {
        splice->nexts[i] = tmp;
        splice->prevs[i] = prev;
        break;
      }
    }
  }

  splice->next_data_entry = node->data_entry;
  splice->prev_data_entry =
      (DLDataEntry *)pmem_allocator_->offset2addr(node->data_entry->prev);
  return true;
}

void Skiplist::SeekKey(const pmem::obj::string_view &key, Splice *splice) {
  SkiplistNode *prev = header_;
  SkiplistNode *tmp;
  // TODO: do not search from max height every time
  for (int i = kMaxHeight; i >= 1; i--) {
    while (1) {
      tmp = prev->Next(i).RawPointer();
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
      SeekKey(insert_key, splice);
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
    data_node = SkiplistNode::NewNode(inserting_key, inserting_entry, height);
    for (int i = 1; i <= height; i++) {
      while (1) {
        auto now_next = insert_splice->prevs[i]->Next(i);
        if (now_next.RawPointer() == insert_splice->nexts[i]) {
          // Some thread is doing deletion, wait
          if (now_next.Tag() != 0) {
            continue;
          }
          data_node->RelaxedSetNext(i, insert_splice->nexts[i]);
          if (insert_splice->prevs[i]->CASNext(i, insert_splice->nexts[i],
                                               data_node)) {
            break;
          }
        } else {
          // Next of prev node changed
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
} // namespace KVDK_NAMESPACE