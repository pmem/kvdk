/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <algorithm>
#include <assert.h>
#include <cstdint>

#include "hash_table.hpp"
#include "kvdk/engine.hpp"
#include "structures.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {
static const int kMaxHeight = 32;
static const uint16_t kCacheLevel = 3;

struct Splice;

/* Format:
 * next pointers | DataEntry on pmem | level | cached key size | cached key
 * We only cache key if level > kCache level or there are enough space in
 * the end of malloced space to cache the key (4B here).
 * */
struct SkiplistNode {
public:
  // Tagged pointers means this node has been logically removed from the list
  std::atomic<TaggedPointer<SkiplistNode>> next[0];
  // Doubly linked data entry on PMem
  DLDataEntry *data_entry;
  // TODO: save memory
  uint16_t height;
  uint16_t cached_key_size;
  // 4 bytes for alignment, the actually allocated size may > 4
  char cached_key[4];

  static void DeleteNode(SkiplistNode *node) { free(node->heap_space_start()); }

  static SkiplistNode *NewNode(const pmem::obj::string_view &key,
                               DLDataEntry *entry_on_pmem, uint16_t l) {
    size_t size;
    if (l >= kCacheLevel && key.size() > 4) {
      size = sizeof(SkiplistNode) + 8 * l + key.size() - 4;
    } else {
      size = sizeof(SkiplistNode) + 8 * l;
    }
    void *space = malloc(size);
    SkiplistNode *node = (SkiplistNode *)((char *)space + 8 * l);
    if (node != nullptr) {
      node->data_entry = entry_on_pmem;
      node->height = l;
      node->MaybeCacheKey(key);
    }
    return node;
  }

  void SeekKey(const pmem::obj::string_view &key, uint16_t start_height,
               uint16_t end_height, Splice *result_splice);

  uint16_t Height() { return height; }

  pmem::obj::string_view UserKey();

  TaggedPointer<SkiplistNode> Next(int l) {
    assert(l > 0);
    return next[-l].load(std::memory_order_acquire);
  }

  bool CASNext(int l, TaggedPointer<SkiplistNode> expected,
               TaggedPointer<SkiplistNode> x) {
    assert(l > 0);
    return (next[-l].compare_exchange_strong(expected, x));
  }

  TaggedPointer<SkiplistNode> RelaxedNext(int l) {
    assert(l > 0);
    return next[-l].load(std::memory_order_relaxed);
  }

  void SetNext(int l, TaggedPointer<SkiplistNode> x) {
    assert(l > 0);
    next[-l].store(x, std::memory_order_release);
  }

  void RelaxedSetNext(int l, TaggedPointer<SkiplistNode> x) {
    assert(l > 0);
    next[-l].store(x, std::memory_order_relaxed);
  }

  void MarkAsRemoved() {
    for (int l = 1; l <= height; l++) {
      while (1) {
        auto next = RelaxedNext(l);
        auto tagged = TaggedPointer<SkiplistNode>(next.RawPointer(), 1);
        if (CASNext(l, next, tagged)) {
          // GlobalLogger.Error("node %lu tag after remove %u\n",
          // (uint64_t)this,
          //  Next(l).Tag());
          break;
        }
      }
    }
  }

private:
  SkiplistNode() {}

  void MaybeCacheKey(const pmem::obj::string_view &key) {
    if (height >= kCacheLevel || key.size() <= 4) {
      cached_key_size = key.size();
      memcpy(cached_key, key.data(), key.size());
    } else {
      cached_key_size = 0;
    }
  }

  void *heap_space_start() { return (char *)this - height * 8; }
};

class Skiplist : public PersistentList {
public:
  Skiplist(DLDataEntry *h, const std::string &n, uint64_t i,
           const std::shared_ptr<PMEMAllocator> &pmem_allocator,
           std::shared_ptr<HashTable> hash_table)
      : name_(n), id_(i), pmem_allocator_(pmem_allocator),
        hash_table_(hash_table) {
    header_ = SkiplistNode::NewNode(n, h, kMaxHeight);
    for (int i = 1; i <= kMaxHeight; i++) {
      header_->RelaxedSetNext(i, nullptr);
    }
  }

  ~Skiplist() {
    if (header_) {
      SkiplistNode *to_delete = header_;
      while (to_delete) {
        SkiplistNode *next = to_delete->Next(1).RawPointer();
        SkiplistNode::DeleteNode(to_delete);
        to_delete = next;
      }
    }
  }

  uint64_t id() override { return id_; }

  const std::string &name() { return name_; }

  SkiplistNode *header() { return header_; }

  static int RandomHeight() {
    int height = 0;
    while (height < kMaxHeight && fast_random_64() & 1) {
      height++;
    }

    return height;
  }

  inline static pmem::obj::string_view
  UserKey(const pmem::obj::string_view &skiplist_key) {
    return pmem::obj::string_view(skiplist_key.data() + 8,
                                  skiplist_key.size() - 8);
  }

  void SeekKey(const pmem::obj::string_view &key, Splice *splice);

  bool SeekNode(const SkiplistNode *node, Splice *splice);

  Status Rebuild();

  bool FindAndLockWritePos(Splice *splice,
                           const pmem::obj::string_view &insert_key,
                           const HashTable::KeyHashHint &hint,
                           std::vector<SpinMutex *> &spins,
                           DLDataEntry *updated_data_entry);

  // Return nullptr if inserted a new height 0 node
  SkiplistNode *InsertDataEntry(Splice *insert_splice,
                                DLDataEntry *inserting_entry,
                                const pmem::obj::string_view &inserting_key,
                                SkiplistNode *data_node, bool is_update);

  void DeleteDataEntry(Splice *delete_splice, SkiplistNode *dram_node);

private:
  SkiplistNode *header_;
  std::string name_;
  uint64_t id_;
  std::shared_ptr<HashTable> hash_table_;
  std::shared_ptr<PMEMAllocator> pmem_allocator_;
};

class SortedIterator : public Iterator {
public:
  SortedIterator(Skiplist *skiplist,
                 const std::shared_ptr<PMEMAllocator> &pmem_allocator)
      : skiplist_(skiplist), pmem_allocator_(pmem_allocator), current(nullptr) {
  }

  virtual void Seek(const std::string &key) override;

  virtual void SeekToFirst() override;

  virtual bool Valid() override { return (current != nullptr); }

  virtual bool Next() override;

  virtual bool Prev() override;

  virtual std::string Key() override;

  virtual std::string Value() override;

private:
  Skiplist *skiplist_;
  std::shared_ptr<PMEMAllocator> pmem_allocator_;
  DLDataEntry *current;
};

struct Splice {
  SkiplistNode *header;
  SkiplistNode *nexts[kMaxHeight + 1];
  SkiplistNode *prevs[kMaxHeight + 1];
  DLDataEntry *prev_data_entry;
  DLDataEntry *next_data_entry;

  void Recompute(const pmem::obj::string_view &key, int l) {
    SkiplistNode *start_node;
    uint16_t start_height = l;
    while (1) {
      if (prevs[start_height] == nullptr) {
        assert(header != nullptr);
        start_node = header;
        break;
      }

      if (prevs[start_height]->Next(start_height).Tag() != 0) {
        start_height++;
        if (start_height > kMaxHeight) {
          assert(header != nullptr);
          start_node = header;
          break;
        }
      } else {
        start_node = prevs[start_height];
        break;
      }
    }

    start_node->SeekKey(key, start_height, l, this);
  }
};
} // namespace KVDK_NAMESPACE
