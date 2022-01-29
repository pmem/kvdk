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
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {
class KVEngine;
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

  CollectionIDType SkiplistID();

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

// A persistent sorted collection implemented as skiplist struct, data organized
// sorted by key
//
// The lowest level of the skiplist are persisted on PMem along with key and
// values, while higher level nodes are stored in DRAM, and re-construct at
// recovery.
// The insert and seek operations are indexed by the multi-level links and
// implemented in O(logn) time. Meanwhile, the skiplist nodes is also indexed by
// the global hash table, so the updates/delete and point read operations can be
// indexed by hash table and implemented in ~O(1) time
class Skiplist : public Collection {
public:
  Skiplist(DLRecord *h, const std::string &name, CollectionIDType id,
           const std::shared_ptr<PMEMAllocator> &pmem_allocator,
           std::shared_ptr<HashTable> hash_table)
      : Collection(name, id), pmem_allocator_(pmem_allocator),
        hash_table_(hash_table) {
    header_ = SkiplistNode::NewNode(name, h, kMaxHeight);
    for (uint8_t i = 1; i <= kMaxHeight; i++) {
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

  SkiplistNode *header() { return header_; }

  inline static StringView UserKey(const SkiplistNode *node) {
    assert(node != nullptr);
    if (node->cached_key_size > 0) {
      return StringView(node->cached_key, node->cached_key_size);
    }
    return CollectionUtils::ExtractUserKey(node->record->Key());
  }

  inline static StringView UserKey(const DLRecord *record) {
    assert(record != nullptr);
    return CollectionUtils::ExtractUserKey(record->Key());
  }

  inline static CollectionIDType SkiplistID(const SkiplistNode *node) {
    assert(node != nullptr);
    return SkiplistID(node->record);
  }

  inline static CollectionIDType SkiplistID(const DLRecord *record) {
    assert(record != nullptr);
    switch (record->entry.meta.type) {
    case RecordType::SortedDataRecord:
    case RecordType::SortedDeleteRecord:
      return CollectionUtils::ExtractID(record->Key());
      break;
    case RecordType::SortedHeaderRecord:
      return CollectionUtils::string2ID(record->Value());
    default:
      kvdk_assert(false, "Wrong type in SkiplistID");
      GlobalLogger.Error("Wrong type in SkiplistID");
    }
    return 0;
  }

  // Start seek from this node, find dram position of "key" in the skiplist
  // between height "start_height" and "end"_height", and store position in
  // "result_splice", if "key" existing, the next pointers in splice point to
  // node of "key"
  void SeekNode(const StringView &key, SkiplistNode *start_node,
                uint8_t start_height, uint8_t end_height,
                Splice *result_splice);

  // Start position of "key" on both dram and PMem node in the skiplist, and
  // store position in "result_splice". If "key" existing, the next pointers in
  // splice point to node of "key"
  void Seek(const StringView &key, Splice *result_splice);

  // Insert a new key "key" to the skiplist,
  //
  // space_to_write: PMem space entry to store new record.
  // dram_node: If height of new record > 0, store new dram node to it,
  // otherwise store nullptr instead
  // inserting_key_lock: lock of inserting key, should be locked before call
  // this function
  //
  // Return true on success, return false on fail.
  bool Insert(const StringView &key, const StringView &value,
              const SpinMutex *inserting_key_lock, TimeStampType timestamp,
              SkiplistNode **dram_node, const SpaceEntry &space_to_write);

  // Update "key" in the skiplist
  //
  // space_to_write: PMem space entry to store new record
  // updated_record: existing record of updating key
  // dram_node: dram node of existing record, if it's a height 0 record, then
  // pass nullptr
  // updating_record_lock: lock of updating record, should be locked before call
  // this function
  //
  // Return true on success, return false on fail.
  bool Update(const StringView &key, const StringView &value,
              const DLRecord *updating_record,
              const SpinMutex *updating_record_lock, TimeStampType timestamp,
              SkiplistNode *dram_node, const SpaceEntry &space_to_write);

  // Delete "key" from the skiplist by replace it with a delete record
  //
  // deleted_record:existing record of deleting key
  // dram_node:dram node of existing record, if it's a height 0 record, then
  // pass nullptr
  // deleting_key_lock: lock of deleting key, should be locked before call this
  // function
  //
  // Return true on success, return false on fail.
  bool Delete(const StringView &key, DLRecord *deleting_record,
              const SpinMutex *deleting_record_lock, TimeStampType timestamp,
              SkiplistNode *dram_node, const SpaceEntry &space_to_write);

  // Purge a dl record from its skiplist by remove it from linkage
  //
  // purged_record:existing record to purge
  // dram_node:dram node of purging record, if it's a height 0 record, then
  // pass nullptr
  // purging_record_lock: lock of purging_record, should be locked before call
  // this function
  //
  // Return true on success, return false on fail.
  static bool Purge(DLRecord *purging_record,
                    const SpinMutex *purging_record_lock,
                    SkiplistNode *dram_node, PMEMAllocator *pmem_allocator,
                    HashTable *hash_table);

  // Replace "old_record" from its skiplist with "replacing_record", please make
  // sure the key order is correct after replace
  //
  // old_record: existing record to be replaced
  // replacing_record: new reocrd to replace the older one
  // dram_node:dram node of old record, if it's a height 0 record, then
  // pass nullptr
  // old_record_lock: lock of old_record, should be locked before call
  // this function
  //
  // Return true on success, return false on fail.
  static bool Replace(DLRecord *old_record, DLRecord *replacing_record,
                      const SpinMutex *old_record_lock, SkiplistNode *dram_node,
                      PMEMAllocator *pmem_allocator, HashTable *hash_table);

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

  // Link DLRecord "linking" between "prev" and "next"
  static void LinkDLRecord(DLRecord *prev, DLRecord *next, DLRecord *linking,
                           PMEMAllocator *pmem_allocator);

  // Find position of "searching_record" in its skiplist and lock its previous
  // node
  //
  // dram_node: dram node of searching_record, if it's a height 0 record, then
  // pass nullptr
  // searching_record_lock: lock of searching record, should be locked before
  // call this function
  //
  // Return true on success, return false on fail.
  static bool
  SearchAndLockRecordPos(Splice *splice, const DLRecord *searching_record,
                         const SpinMutex *searching_record_lock,
                         std::unique_lock<SpinMutex> *prev_record_lock,
                         PMEMAllocator *pmem_allocator, HashTable *hash_table);

  void SetCompareFunc(CompFunc comp_func) { compare_func_ = comp_func; }

  // Build a skiplist node for "pmem_record"
  static SkiplistNode *NewNodeBuild(DLRecord *pmem_record);

private:
  inline void LinkDLRecord(DLRecord *prev, DLRecord *next, DLRecord *linking) {
    return LinkDLRecord(prev, next, linking, pmem_allocator_.get());
  }

  // Find and lock skiplist position to insert "key"
  //
  // Store prev dram nodes and prev/next PMem DLRecord in "splice", lock
  // prev DLRecord and manage the lock with "prev_record_lock".
  //
  // The "insert_key" should be already locked before call this function
  bool searchAndLockInsertPos(Splice *splice, const StringView &inserting_key,
                              const SpinMutex *inserting_key_lock,
                              std::unique_lock<SpinMutex> *prev_record_lock);

  // Search and lock skiplist position to update"key".
  //
  // Store prev/next PMem DLRecord in "splice", lock prev DLRecord and manage
  // the lock with "prev_record_lock".
  //
  //  The "updated_key" should be already locked before call this function
  bool searchAndLockUpdatePos(Splice *splice, const DLRecord *updating_record,
                              const SpinMutex *updating_record_lock,
                              std::unique_lock<SpinMutex> *prev_record_lock) {
    return SearchAndLockRecordPos(splice, updating_record, updating_record_lock,
                                  prev_record_lock, pmem_allocator_.get(),
                                  hash_table_.get());
  }

  bool searchAndLockDeletePos(Splice *splice, const DLRecord *deleting_record,
                              const SpinMutex *deleting_record_lock,
                              std::unique_lock<SpinMutex> *prev_record_lock) {
    return SearchAndLockRecordPos(splice, deleting_record, deleting_record_lock,
                                  prev_record_lock, pmem_allocator_.get(),
                                  hash_table_.get());
  }

  bool ValidateDLRecord(const DLRecord *record) {
    DLRecord *prev = pmem_allocator_->offset2addr<DLRecord>(record->prev);
    return prev != nullptr &&
           prev->next == pmem_allocator_->addr2offset(record) &&
           SkiplistID(record) == ID();
  }

  int compare(const StringView &src_key, const StringView &target_key) {
    return compare_func_(src_key, target_key);
  }

  SkiplistNode *header_;
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
  CompFunc compare_func_ = compare_string_view;
};

class SortedIterator : public Iterator {
public:
  SortedIterator(Skiplist *skiplist,
                 const std::shared_ptr<PMEMAllocator> &pmem_allocator,
                 SnapshotImpl *snapshot, bool own_snapshot)
      : skiplist_(skiplist), pmem_allocator_(pmem_allocator), current_(nullptr),
        snapshot_(snapshot), own_snapshot_(own_snapshot) {}

  virtual void Seek(const std::string &key) override;

  virtual void SeekToFirst() override;

  virtual void SeekToLast() override;

  virtual bool Valid() override {
    return (current_ != nullptr && current_ != skiplist_->header()->record);
  }

  virtual void Next() override;

  virtual void Prev() override;

  virtual std::string Key() override;

  virtual std::string Value() override;

private:
  friend KVEngine;
  DLRecord *findValidVersion(DLRecord *pmem_record);

  Skiplist *skiplist_;
  std::shared_ptr<PMEMAllocator> pmem_allocator_;
  DLRecord *current_;
  SnapshotImpl *snapshot_;
  bool own_snapshot_;
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

  void Recompute(const StringView &key, uint8_t l) {
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
      seeking_list->SeekNode(key, start_node, start_height, l, this);
      return;
    }
  }
};
class KVEngine;
class SortedCollectionRebuilder {
public:
  SortedCollectionRebuilder(PMEMAllocator *pmem_allocator,
                            HashTable *hash_table, bool opt_parallel_rebuild,
                            uint64_t num_rebuild_threads,
                            const CheckPoint &checkpoint)
      : pmem_allocator_(pmem_allocator), hash_table_(hash_table),
        checkpoint_(checkpoint), opt_parallel_rebuild_(opt_parallel_rebuild),
        num_rebuild_threads_(num_rebuild_threads){};

  Status
  RebuildLinkage(const std::vector<std::shared_ptr<Skiplist>> &skiplists);

  void AddRecordForParallelRebuild(uint64_t record_offset, bool is_visited,
                                   SkiplistNode *node) {
    record_offsets_.insert({record_offset, {is_visited, node}});
  }

  DLRecord *FindValidVersion(DLRecord *pmem_record,
                             std::vector<DLRecord *> *invalid_version_records);

private:
  Status repairSkiplistLinkage(Skiplist *skiplist);

  Status parallelRepairSkiplistLinkage();

  Status updateRecordOffsets();

  SkiplistNode *getSortedOffset(int height);

  void linkedNode(uint64_t thread_id, int height);

  Status dealWithFirstHeight(uint64_t thread_id, SkiplistNode *cur_node);

  void dealWithOtherHeight(uint64_t thread_id, SkiplistNode *cur_node,
                           int heightm);

  void batchPurgeAndFree(std::vector<DLRecord *> &pmem_records) {
    std::vector<SpaceEntry> to_free;
    for (DLRecord *pmem_record : pmem_records) {
      pmem_record->Destroy();
      to_free.emplace_back(pmem_allocator_->addr2offset_checked(pmem_record),
                           pmem_record->entry.header.record_size);
    }
    pmem_allocator_->BatchFree(to_free);
  }

  struct SkiplistNodeInfo {
    bool visited;
    SkiplistNode *node;
  };
  SpinMutex map_mu_;
  std::vector<std::unordered_set<SkiplistNode *>> thread_cache_node_;
  std::unordered_map<uint64_t, SkiplistNodeInfo> record_offsets_;
  PMEMAllocator *pmem_allocator_;
  HashTable *hash_table_;
  uint64_t num_rebuild_threads_;
  bool opt_parallel_rebuild_;
  CheckPoint checkpoint_;
};

} // namespace KVDK_NAMESPACE
