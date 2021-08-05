/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "hash_table.hpp"
#include "malloc.h"
#include "pmemdb/db.hpp"
#include "structures.hpp"
#include "utils.hpp"
#include <assert.h>

namespace PMEMDB_NAMESPACE {
static const int kMaxHeight = MAX_SKIPLIST_LEVEL;

class Skiplist {
public:
  Node *header_;
  SpinMutex spin_;

  Skiplist(SortedDataEntry *header, char *pmem) : pmem_log_(pmem) {
    header_ = (Node *)malloc(sizeof(Node) + kMaxHeight * 8);
    if (!header_)
      exit(1);
    for (int i = 0; i < kMaxHeight; i++) {
      header_->RelaxedSetNext(i, nullptr);
    }
    header_->data_entry = header;
  }

  ~Skiplist() {
    if(header_) {
      Node *to_delete = header_;
      while (to_delete) {
        Node *next = to_delete->Next(0);
        free(to_delete);
        to_delete = next;
      }
    }
  }

  static int RandomHeight() {
    int height = 0;
    while (height < kMaxHeight && fast_random() & 1) {
      height++;
    }

    return height;
  }

  struct Splice {
    Node *nexts[kMaxHeight];
    Node *prevs[kMaxHeight];
    SortedDataEntry *prev_data_entry;
    SortedDataEntry *next_data_entry;

    void Recompute(const Slice &key, int l) {
      while (1) {
        Node *tmp = prevs[l]->Next(l);
        if (tmp == nullptr) {
          nexts[l] = nullptr;
          break;
        }

        int cmp = Slice::compare(
            key, Slice((char *)tmp->data_entry + sizeof(SortedDataEntry),
                       tmp->data_entry->k_size));

        if (cmp >= 0) {
          prevs[l] = tmp;
        } else {
          nexts[l] = tmp;
          break;
        }
      }
    }
  };

  void Seek(const Slice &key, Splice *splice);

  void Rebuild(HashTable *hash_table) {
    Splice splice;
    HashEntry hash_entry;
    HashEntry *entry_base;
    SortedDataEntry data_entry;
    for (int i = 0; i < kMaxHeight; i++) {
      splice.prevs[i] = header_;
      splice.prev_data_entry = header_->data_entry;
    }
    while (1) {
      uint64_t next_offset = splice.prev_data_entry->next;
      if (next_offset == NULL_PMEM_OFFSET) {
        break;
      }
      // TODO: check failure
      SortedDataEntry *next_data_entry =
          (SortedDataEntry *)(pmem_log_ + next_offset);
      Slice key = next_data_entry->Key();
      hash_table->Search(0, key, hash_key(key.data(), key.size()),
                         SORTED_DATA_RECORD | SORTED_DELETE_RECORD, &hash_entry,
                         &data_entry, &entry_base, false, nullptr);
      Node *node = (Node *)hash_entry.offset;
      int height = (malloc_usable_size(node) - sizeof(Node)) / 8;
      for (int i = 0; i < height; i++) {
        splice.prevs[i]->RelaxedSetNext(i, node);
        node->RelaxedSetNext(i, nullptr);
        splice.prevs[i] = node;
      }
      splice.prev_data_entry = next_data_entry;
    }
  }

private:
  char *pmem_log_;
};

class SortedIterator : public Iterator {
public:
  SortedIterator(Skiplist *skiplist, char *log)
      : skiplist_(skiplist), pmem_value_log_(log), current(nullptr) {}

  virtual void Seek(const std::string &key) override {
    assert(skiplist_);
    Skiplist::Splice splice;
    skiplist_->Seek(key, &splice);
    current = splice.next_data_entry;
  }

  virtual void SeekToFirst() override {
    uint64_t first = skiplist_->header_->data_entry->next;
    current = (first == NULL_PMEM_OFFSET)
                  ? nullptr
                  : (SortedDataEntry *)(pmem_value_log_ + first);
  }

  virtual bool Valid() override { return current != nullptr; }

  virtual bool Next() override {
    if (!Valid()) {
      return false;
    }

    if (current->next == NULL_PMEM_OFFSET) {
      current = nullptr;
      return false;
    } else {
      current = (SortedDataEntry *)(current->next + pmem_value_log_);
      return true;
    }
  }

  virtual bool Prev() override {
    if (!Valid()) {
      return false;
    }

    current = (SortedDataEntry *)(current->prev + pmem_value_log_);
    if (current == skiplist_->header_->data_entry) {
      current = nullptr;
      return false;
    }

    return true;
  }

  virtual std::string Key() override {
    if (!Valid())
      return "";
    return std::string((char *)(current + 1), current->k_size);
  }

  virtual std::string Value() override {
    if (!Valid())
      return "";
    return std::string((char *)(current + 1) + current->k_size,
                       current->v_size);
  }

private:
  Skiplist *skiplist_;
  char *pmem_value_log_;
  SortedDataEntry *current;
};
} // namespace PMEMDB_NAMESPACE
