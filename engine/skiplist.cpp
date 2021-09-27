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

void SkiplistNode::SeekKey(const pmem::obj::string_view &key,
                           uint16_t start_height, uint16_t end_height,
                           Splice *result_splice) {
  assert(height >= start_height && end_height >= 1);
  SkiplistNode *prev = this;
  PointerWithTag<SkiplistNode> next;
  for (int i = start_height; i >= end_height; i--) {
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
          assert(result_splice->header);
          prev = result_splice->header;
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

      // Phisically remove deleted nodes from skiplist
      auto next_next = next->Next(i);
      if (next_next.GetTag()) {
        // if prev is marked deleted before cas, cas will be faied, and prev
        // will be roll back in next round
        prev->CASNext(i, next, next_next.RawPointer());
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
}

Status Skiplist::Rebuild() {
  Splice splice(header());
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

void Skiplist::SeekKey(const pmem::obj::string_view &key,
                       Splice *result_splice) {
  result_splice->header = header_;
  header_->SeekKey(key, header_->Height(), 1, result_splice);
  assert(result_splice->prevs[1] != nullptr);
  DLDataEntry *prev_data_entry = result_splice->prevs[1]->data_entry;
  while (1) {
    uint64_t next_data_entry_offset = prev_data_entry->next;
    if (next_data_entry_offset == kNullPmemOffset) {
      result_splice->prev_data_entry = prev_data_entry;
      result_splice->next_data_entry = nullptr;
      break;
    }
    DLDataEntry *next_data_entry =
        (DLDataEntry *)pmem_allocator_->offset2addr(next_data_entry_offset);
    int cmp = compare_string_view(key, UserKey(next_data_entry->Key()));
    if (cmp > 0) {
      prev_data_entry = next_data_entry;
    } else {
      result_splice->next_data_entry = next_data_entry;
      result_splice->prev_data_entry = prev_data_entry;
      break;
    }
  }
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
    SeekKey(insert_key, splice);

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

  if (updated_data_entry) {
    assert(updated_data_entry->prev == pmem_allocator_->addr2offset(prev));
    assert(updated_data_entry->next == pmem_allocator_->addr2offset(next));
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

void Skiplist::DeleteDataEntry(DLDataEntry *deleting_entry,
                               Splice *delete_splice, SkiplistNode *dram_node) {
  DLDataEntry *prev = delete_splice->prev_data_entry;
  DLDataEntry *next = delete_splice->next_data_entry;
  assert(prev->next == pmem_allocator_->addr2offset(deleting_entry));
  // For repair in recovery due to crashes during pointers changing, we should
  // first unlink deleting entry from prev's next
  prev->next = pmem_allocator_->addr2offset(next);
  pmem_persist(&prev->next, 8);
  if (next) {
    assert(next->prev == pmem_allocator_->addr2offset(deleting_entry));
    next->prev = pmem_allocator_->addr2offset(prev);
    pmem_persist(&next->prev, 8);
  }

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
  pmem_memcpy_persist(&insert_splice->prev_data_entry->next,
                      &inserting_entry_offset, 8);
  if (next != nullptr) {
    assert(is_update || next->prev == pmem_allocator_->addr2offset(prev));
    pmem_memcpy_persist(&next->prev, &inserting_entry_offset, 8);
  }

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
  Splice splice(skiplist_->header());
  skiplist_->SeekKey(key, &splice);
  current = splice.next_data_entry;
}

void SortedIterator::SeekToFirst() {
  uint64_t first = skiplist_->header()->data_entry->next;
  current = (DLDataEntry *)pmem_allocator_->offset2addr(first);
}

bool SortedIterator::Next() {
  if (!Valid()) {
    return false;
  }
  current = (DLDataEntry *)pmem_allocator_->offset2addr(current->next);
  return current != nullptr;
}

bool SortedIterator::Prev() {
  if (!Valid()) {
    return false;
  }

  current = (DLDataEntry *)(pmem_allocator_->offset2addr(current->prev));

  if (current == skiplist_->header()->data_entry) {
    current = nullptr;
    return false;
  }

  return true;
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
} // namespace KVDK_NAMESPACE