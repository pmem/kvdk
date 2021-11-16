/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cassert>
#include <cstdint>

#include <algorithm>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "kvdk/engine.hpp"

#include "hash_table.hpp"
#include "structures.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {
static const uint8_t kMaxHeight = 32;
static const uint8_t kCacheHeight = 3;

struct Splice;

/* Format:
 * next pointers | DLRecord on pmem | height | cached key size |
 * cached key We only cache key if height > kCache height or there are enough
 * space in the end of malloced space to cache the key (4B here).
 * */
struct SkiplistNode {
public:
  // Tagged pointers means this node has been logically removed from the list
  std::atomic<PointerWithTag<SkiplistNode>> next[0];
  // Doubly linked record on PMem
  DLRecord *record;
  // TODO: save memory
  uint16_t cached_key_size;
  uint8_t height;
  // How many height this node are linked on its skiplist. If this node is
  // phisically remove from some height of a skiplist, then valid_links-=1.
  // valid_links==0 means this node is removed from every height of the
  // skiplist, free this node.
  std::atomic<uint8_t> valid_links{0};
  // 4 bytes for alignment, the actually allocated size may > 4
  char cached_key[4];

  static void DeleteNode(SkiplistNode *node) { free(node->heap_space_start()); }

  static SkiplistNode *NewNode(const StringView &key, DLRecord *record_on_pmem,
                               uint8_t height) {
    size_t size;
    if (height >= kCacheHeight && key.size() > 4) {
      size = sizeof(SkiplistNode) + 8 * height + key.size() - 4;
    } else {
      size = sizeof(SkiplistNode) + 8 * height;
    }
    SkiplistNode *node = nullptr;
    void *space = malloc(size);
    if (space != nullptr) {
      node = (SkiplistNode *)((char *)space + 8 * height);
      node->record = record_on_pmem;
      node->height = height;
      // make sure this will be linked to skiplist at all the height after
      // creation
      node->valid_links = height;
      node->MaybeCacheKey(key);
    }
    return node;
  }

  uint16_t Height() { return height; }

  StringView UserKey();

  uint64_t SkiplistId();

  PointerWithTag<SkiplistNode> Next(int l) {
    assert(l > 0 && l <= height && "should be less than node's height");
    return next[-l].load(std::memory_order_acquire);
  }

  bool CASNext(int l, PointerWithTag<SkiplistNode> expected,
               PointerWithTag<SkiplistNode> x) {
    assert(l > 0 && l <= height && "should be less than node's height");
    return (next[-l].compare_exchange_strong(expected, x));
  }

  PointerWithTag<SkiplistNode> RelaxedNext(int l) {
    assert(l > 0 && l <= height && "should be less than node's height");
    return next[-l].load(std::memory_order_relaxed);
  }

  void SetNext(int l, PointerWithTag<SkiplistNode> x) {
    assert(l > 0 && l <= height && "should be less than node's height");
    next[-l].store(x, std::memory_order_release);
  }

  void RelaxedSetNext(int l, PointerWithTag<SkiplistNode> x) {
    assert(l > 0 && l <= height && "should be less than node's height");
    next[-l].store(x, std::memory_order_relaxed);
  }

  // Logically delete node by tag next pointers from bottom to top
  void MarkAsRemoved() {
    for (int l = 1; l <= height; l++) {
      while (1) {
        auto next = RelaxedNext(l);
        // This node alread tagged by another thread
        if (next.GetTag()) {
          continue;
        }
        auto tagged = PointerWithTag<SkiplistNode>(next.RawPointer(), 1);
        if (CASNext(l, next, tagged)) {
          break;
        }
      }
    }
  }

private:
  SkiplistNode() {}

  void MaybeCacheKey(const StringView &key) {
    if (height >= kCacheHeight || key.size() <= 4) {
      cached_key_size = key.size();
      memcpy(cached_key, key.data(), key.size());
    } else {
      cached_key_size = 0;
    }
  }

  void *heap_space_start() { return (char *)this - height * 8; }
};

class Skiplist : public PersistentList, public CompStrategy<Skiplist> {
public:
  Skiplist(DLRecord *h, const std::string &n, uint64_t i,
           const std::shared_ptr<PMEMAllocator> &pmem_allocator,
           std::shared_ptr<HashTable> hash_table)
      : name_(n), id_(i), pmem_allocator_(pmem_allocator),
        hash_table_(hash_table) {
    header_ = SkiplistNode::NewNode(n, h, kMaxHeight);
    for (uint8_t i = 1; i <= kMaxHeight; i++) {
      header_->RelaxedSetNext(i, nullptr);
    }
    if (GetCollectionCompFuncMap().find(n) !=
        GetCollectionCompFuncMap().end()) {
      SetCompStrategy(GetCollectionCompFuncMap()[n]);
    }
    SetCollectionCompFunc(n, cmp_ctx.key_cmp, cmp_ctx.val_cmp,
                          cmp_ctx.priority_key);
  }

  ~Skiplist() {
    if (header_) {
      SkiplistNode *to_delete = header_;
      while (to_delete) {
        SkiplistNode *next = to_delete->Next(1).RawPointer();
        SkiplistNode::DeleteNode(to_delete);
        to_delete = next;
      }
      std::lock_guard<SpinMutex> lg_a(pending_delete_nodes_spin_);
      for (SkiplistNode *node : pending_deletion_nodes_) {
        SkiplistNode::DeleteNode(node);
      }
      pending_deletion_nodes_.clear();
      std::lock_guard<SpinMutex> lg_b(obsolete_nodes_spin_);
      for (SkiplistNode *node : obsolete_nodes_) {
        SkiplistNode::DeleteNode(node);
      }
      obsolete_nodes_.clear();
    }
  }

  static uint8_t RandomHeight() {
    uint8_t height = 0;
    while (height < kMaxHeight && fast_random_64() & 1) {
      height++;
    }

    return height;
  }

  uint64_t id() override { return id_; }

  const std::string &name() { return name_; }

  SkiplistNode *header() { return header_; }

  std::string InternalKey(const pmem::obj::string_view &key) {
    return PersistentList::ListKey(key, id_);
  }

  inline static pmem::obj::string_view
  UserKey(const pmem::obj::string_view &skiplist_key) {
    return pmem::obj::string_view(skiplist_key.data() + 8,
                                  skiplist_key.size() - 8);
  }

  inline static pmem::obj::string_view UserKey(const SkiplistNode *node) {
    assert(node != nullptr);
    if (node->cached_key_size > 0) {
      return pmem::obj::string_view(node->cached_key, node->cached_key_size);
    }
    return UserKey(node->record->Key());
  }

  inline static pmem::obj::string_view UserKey(const DLRecord *record) {
    assert(record != nullptr);
    return UserKey(record->Key());
  }

  inline static uint64_t
  SkiplistId(const pmem::obj::string_view &skiplist_key) {
    uint64_t id;
    memcpy_8(&id, skiplist_key.data());
    return id;
  }

  inline static uint64_t SkiplistId(const DLRecord *record) {
    assert(record != nullptr);
    uint64_t id = 0;
    switch (record->entry.meta.type) {
    case RecordType::SortedDataRecord:
      id = SkiplistId(record->Key());
      break;
    case RecordType::SortedHeaderRecord:
      memcpy_8(&id, record->Value().data());
      break;
    default:
      kvdk_assert(false, "Wrong type in SkiplistId");
      break;
    }
    return id;
  }

  inline static uint64_t SkiplistId(const SkiplistNode *node) {
    assert(node != nullptr);
    return SkiplistId(node->record);
  }

  // Start position of "key" on both dram and PMem node in the skiplist, and
  // store position in "result_splice". If "key" existing, the next pointers in
  // splice point to node of "key"
  void Seek(const StringView &key, StringView value, Splice *result_splice);

  Status Rebuild();

  // Insert a new key "key" to skiplist,
  //
  // space_to_write: PMem space entry to store new record.
  // dram_node: If height of new record > 0, store new dram node to it,
  // otherwise store nullptr instead
  // inserting_key_lock: lock of inserting key, should be already locked while
  // calling this function
  //
  // Return true on success, return false on fail.
  bool Insert(const pmem::obj::string_view &key,
              const pmem::obj::string_view &value,
              const SizedSpaceEntry &space_to_write, uint64_t timestamp,
              SkiplistNode **dram_node, const SpinMutex *inserting_key_lock);

  // Update "key" in skiplist
  //
  // space_to_write: PMem space entry to store new record
  // updated_record: existing record of updating key
  // dram_node: dram node of existing record, if it's a height 0 record, then
  // pass nullptr
  // updating_key_lock: lock of updating key, should be already locked while
  // calling this function
  //
  // Return true on success, return false on fail.
  bool Update(const pmem::obj::string_view &key,
              const pmem::obj::string_view &value,
              const DLRecord *updated_record,
              const SizedSpaceEntry &space_to_write, uint64_t timestamp,
              SkiplistNode *dram_node, const SpinMutex *updating_key_lock);

  // Delete "key" from skiplist
  //
  // deleted_record:existing record of deleting key
  // dram_node:dram node of existing record, if it's a height 0 record, then
  // pass nullptr
  // deleting_key_lock: lock of deleting key, should be already locked while
  // calling this function
  //
  // Return true on success, return false on fail.
  bool Delete(const pmem::obj::string_view &key, DLRecord *deleted_record,
              SkiplistNode *dram_node, const SpinMutex *deleting_key_lock);

  void ObsoleteNodes(const std::vector<SkiplistNode *> nodes) {
    std::lock_guard<SpinMutex> lg(obsolete_nodes_spin_);
    for (SkiplistNode *node : nodes) {
      obsolete_nodes_.push_back(node);
    }
  }

  void PurgeObsoletedNodes() {
    std::lock_guard<SpinMutex> lg_a(pending_delete_nodes_spin_);
    if (pending_deletion_nodes_.size() > 0) {
      for (SkiplistNode *node : pending_deletion_nodes_) {
        SkiplistNode::DeleteNode(node);
      }
      pending_deletion_nodes_.clear();
    }

    std::lock_guard<SpinMutex> lg_b(obsolete_nodes_spin_);
    obsolete_nodes_.swap(pending_deletion_nodes_);
  }

  Status CheckConnection(int height);

  // Start seek from start node, find dram position of "key" in the skiplist
  // between height "start_height" and "end"_height", and store position in
  // "result_splice", if "key" existing, the next pointers in splice point to
  // node of "key"
  void SeekNode(const StringView &key, const StringView &value,
                SkiplistNode *start_node, uint8_t start_height,
                uint8_t end_height, Splice *result_splice);

private:
  // Insert DLRecord "inserting" between "prev" and "next"
  void InsertDLRecord(DLRecord *prev, DLRecord *next, DLRecord *inserting);

  // Find and lock skiplist position to insert "key"
  //
  // Store prev dram nodes and prev/next PMem DLRecord in "splice", lock
  // prev DLRecord and manage the lock with "prev_record_lock".
  //
  // The "insert_key" should be already locked before call this function
  bool FindInsertPos(Splice *splice,
                     const pmem::obj::string_view &inserting_key,
                     const StringView &inserting_value,
                     const SpinMutex *inserting_key_lock,
                     std::unique_lock<SpinMutex> *prev_record_lock);

  // Find and lock skiplist position to update"key".
  //
  // Store prev/next PMem DLRecord in "splice", lock prev DLRecord and manage
  // the lock with "prev_record_lock".
  //
  //  The "updated_key" should be already locked before call this function
  bool FindUpdatePos(Splice *splice, const pmem::obj::string_view &updating_key,
                     const StringView &updating_value,
                     const SpinMutex *updating_key_lock,
                     const DLRecord *updated_record,
                     std::unique_lock<SpinMutex> *prev_record_lock);

  bool FindDeletePos(Splice *splice, const pmem::obj::string_view &deleting_key,
                     const SpinMutex *deleting_key_lock,
                     const DLRecord *deleted_record,
                     std::unique_lock<SpinMutex> *prev_record_lock) {
    return FindUpdatePos(splice, deleting_key, "", deleting_key_lock,
                         deleted_record, prev_record_lock);
  }

  int compare(const StringView &aKey, const StringView &bKey,
              const StringView &aVal, const StringView &bVal) {
    return cmp_ctx.priority_key
               ? (Comparekey(aKey, bKey) != 0 ? Comparekey(aKey, bKey)
                                              : CompareValue(aVal, bVal))
               : (CompareValue(aVal, bVal) != 0 ? CompareValue(aVal, bVal)
                                                : Comparekey(aKey, bKey));
  }

  SkiplistNode *header_;
  std::string name_;
  uint64_t id_;
  std::shared_ptr<HashTable> hash_table_;
  std::shared_ptr<PMEMAllocator> pmem_allocator_;
  // nodes that unlinked on every height
  std::vector<SkiplistNode *> obsolete_nodes_;
  // to avoid concurrent access a just deleted node, a node can be safely
  // deleted only if a certain interval is passes after being moved from
  // obsolete_nodes_ to pending_deletion_nodes_, this is guaranteed by
  // background thread of kvdk instance
  std::vector<SkiplistNode *> pending_deletion_nodes_;
  // protect obsolete_nodes_
  SpinMutex obsolete_nodes_spin_;
  // protect pending_deletion_nodes_
  SpinMutex pending_delete_nodes_spin_;
};

class SortedIterator : public Iterator {
public:
  SortedIterator(Skiplist *skiplist,
                 const std::shared_ptr<PMEMAllocator> &pmem_allocator)
      : skiplist_(skiplist), pmem_allocator_(pmem_allocator), current(nullptr) {
  }

  virtual void Seek(const std::string &key) override;

  virtual void SeekToFirst() override;

  virtual void SeekToLast() override;

  virtual bool Valid() override {
    return (current != nullptr && current != skiplist_->header()->record);
  }

  virtual void Next() override;

  virtual void Prev() override;

  virtual std::string Key() override;

  virtual std::string Value() override;

private:
  Skiplist *skiplist_;
  std::shared_ptr<PMEMAllocator> pmem_allocator_;
  DLRecord *current;
};

// A helper struct for seeking skiplist
struct Splice {
  // Seeking skiplist
  Skiplist *seeking_list;
  std::array<SkiplistNode *, kMaxHeight + 1> nexts;
  std::array<SkiplistNode *, kMaxHeight + 1> prevs;
  DLRecord *prev_pmem_record{nullptr};
  DLRecord *next_pmem_record{nullptr};

  Splice(Skiplist *s) : seeking_list(s) {}

  void Recompute(const StringView &key, const StringView &value, uint8_t l) {
    SkiplistNode *start_node;
    uint8_t start_height = l;
    while (1) {
      if (start_height > kMaxHeight || prevs[start_height] == nullptr) {
        assert(seeking_list != nullptr);
        start_height = kMaxHeight;
        start_node = seeking_list->header();
      } else if (prevs[start_height]->Next(start_height).GetTag()) {
        // If prev on this height has been deleted, roll back to higher height
        start_height++;
        continue;
      } else {
        start_node = prevs[start_height];
      }
      seeking_list->SeekNode(key, value, start_node, start_height, l, this);
      return;
    }
  }
};

class KVEngine;
class SortedCollectionRebuilder {
public:
  SortedCollectionRebuilder() = default;
  Status DealWithFirstHeight(uint64_t thread_id, SkiplistNode *cur_node,
                             const KVEngine *engine);

  void
  DealWithOtherHeight(uint64_t thread_id, SkiplistNode *cur_node, int heightm,
                      const std::shared_ptr<PMEMAllocator> &pmem_allocator);

  SkiplistNode *GetSortedOffset(int height);

  void LinkedNode(uint64_t thread_id, int height, const KVEngine *engine);

  Status Rebuild(const KVEngine *engine);

  void UpdateEntriesOffset(const KVEngine *engine);

  void SetEntriesOffsets(uint64_t entry_offset, bool is_visited,
                         SkiplistNode *node) {
    entries_offsets_.insert({entry_offset, {is_visited, node}});
  }

private:
  struct SkiplistNodeInfo {
    bool is_visited;
    SkiplistNode *visited_node;
  };
  SpinMutex map_mu_;
  std::vector<std::unordered_set<SkiplistNode *>> thread_cache_node_;
  std::unordered_map<uint64_t, SkiplistNodeInfo> entries_offsets_;
};

} // namespace KVDK_NAMESPACE
