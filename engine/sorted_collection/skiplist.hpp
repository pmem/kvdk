/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "../hash_table.hpp"
#include "../structures.hpp"
#include "../utils/utils.hpp"
#include "kvdk/engine.hpp"
#include "rebuilder.hpp"

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
  enum class NodeStatus : uint8_t {
    Normal = 0,
    Deleted = 1,
  };
  // Tagged pointers means this node has been logically removed from the list
  std::atomic<PointerWithTag<SkiplistNode, NodeStatus>> next[0];
  // Doubly linked record on PMem
  DLRecord* record;
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

  static void DeleteNode(SkiplistNode* node) { free(node->heap_space_start()); }

  static SkiplistNode* NewNode(const StringView& key, DLRecord* record_on_pmem,
                               uint8_t height) {
    size_t size;
    if (height >= kCacheHeight && key.size() > 4) {
      size = sizeof(SkiplistNode) + 8 * height + key.size() - 4;
    } else {
      size = sizeof(SkiplistNode) + 8 * height;
    }
    SkiplistNode* node = nullptr;
    void* space = malloc(size);
    if (space != nullptr) {
      node = (SkiplistNode*)((char*)space + 8 * height);
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

  PointerWithTag<SkiplistNode, NodeStatus> Next(int l) {
    assert(l > 0 && l <= height && "should be less than node's height");
    return next[-l].load(std::memory_order_acquire);
  }

  bool CASNext(int l, PointerWithTag<SkiplistNode, NodeStatus> expected,
               PointerWithTag<SkiplistNode, NodeStatus> x) {
    assert(l > 0 && l <= height && "should be less than node's height");
    return (next[-l].compare_exchange_strong(expected, x));
  }

  PointerWithTag<SkiplistNode, NodeStatus> RelaxedNext(int l) {
    assert(l > 0 && l <= height && "should be less than node's height");
    return next[-l].load(std::memory_order_relaxed);
  }

  void SetNext(int l, PointerWithTag<SkiplistNode, NodeStatus> x) {
    assert(l > 0 && l <= height && "should be less than node's height");
    next[-l].store(x, std::memory_order_release);
  }

  void RelaxedSetNext(int l, PointerWithTag<SkiplistNode, NodeStatus> x) {
    assert(l > 0 && l <= height && "should be less than node's height");
    next[-l].store(x, std::memory_order_relaxed);
  }

  // Logically delete node by tag next pointers from bottom to top
  void MarkAsRemoved() {
    for (int l = 1; l <= height; l++) {
      while (1) {
        auto next = RelaxedNext(l);
        // This node alread tagged by another thread
        if (next.GetTag() == NodeStatus::Deleted) {
          continue;
        }
        auto tagged = PointerWithTag<SkiplistNode, NodeStatus>(
            next.RawPointer(), NodeStatus::Deleted);
        if (CASNext(l, next, tagged)) {
          break;
        }
      }
    }
  }

 private:
  SkiplistNode() {}

  void MaybeCacheKey(const StringView& key) {
    if (height >= kCacheHeight || key.size() <= 4) {
      cached_key_size = key.size();
      memcpy(cached_key, key.data(), key.size());
    } else {
      cached_key_size = 0;
    }
  }

  void* heap_space_start() { return (char*)this - height * 8; }
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
// Each skiplist has a header record persisted on PMem, the key of header record
// is the skiplist name, the value of header record is encoded by skiplist id
// and configs
class Skiplist : public Collection {
 public:
  // Result of a write operation
  struct WriteResult {
    Status s = Status::Ok;
    DLRecord* existing_record = nullptr;
    DLRecord* write_record = nullptr;
    SkiplistNode* dram_node = nullptr;
    HashEntry* hash_entry_ptr = nullptr;
  };

  Skiplist(DLRecord* h, const std::string& name, CollectionIDType id,
           Comparator comparator, std::shared_ptr<PMEMAllocator> pmem_allocator,
           std::shared_ptr<HashTable> hash_table, bool index_with_hashtable);

  ~Skiplist();

  SkiplistNode* Header() { return header_; }

  bool IndexWithHashtable() { return index_with_hashtable_; }

  ExpireTimeType GetExpireTime() const final {
    return header_->record->expired_time;
  }

  bool HasExpired() const final {
    return TimeUtils::CheckIsExpired(GetExpireTime());
  }

  Status SetExpireTime(ExpireTimeType expired_time) final;

  // Set "key, value" to the skiplist
  //
  // key_hash_hint_locked: hash table hint of the setting key, the lock of hint
  // should already been locked
  // timestamp: kvdk engine timestamp of this operation
  //
  // Return Ok on success, with the writed pmem record, its dram node and
  // updated pmem record if it existed, return Fail if there is thread
  // contension
  WriteResult Set(const StringView& key, const StringView& value,
                  const HashTable::KeyHashHint& key_hash_hint_locked,
                  TimeStampType timestamp);

  // Get value of "key" from the skiplist
  Status Get(const StringView& key, std::string* value);

  // Delete "key" from the skiplist by replace it with a delete record
  //
  // key_hash_hint_locked: hash table hint of the deleting key, the lock of hint
  // should already been locked
  // timestamp: kvdk engine timestamp of this operation
  //
  // Return Ok on success, with the writed pmem delete record, its dram node and
  // deleted pmem record if existed, return Fail if there is thread contension
  WriteResult Delete(const StringView& key,
                     const HashTable::KeyHashHint& key_hash_hint_locked,
                     TimeStampType timestamp);

  // Seek position of "key" on both dram and PMem node in the skiplist, and
  // store position in "result_splice". If "key" existing, the next pointers in
  // splice point to node of "key"
  void Seek(const StringView& key, Splice* result_splice);

  // Start seek from "start_node", find dram position of "key" in the skiplist
  // between height "start_height" and "end"_height", and store position in
  // "result_splice", if "key" existing, the next pointers in splice point to
  // node of "key"
  void SeekNode(const StringView& key, SkiplistNode* start_node,
                uint8_t start_height, uint8_t end_height,
                Splice* result_splice);

  // Destroy and free the whole skiplist, including skiplist nodes and pmem
  // records.
  void Destroy();

  // check node linkage and hash index
  Status CheckIndex();

  void CleanObsoletedNodes();

  // Check if record correctly linked on list
  static bool CheckRecordLinkage(DLRecord* record,
                                 PMEMAllocator* pmem_allocator) {
    uint64_t offset = pmem_allocator->addr2offset_checked(record);
    DLRecord* prev =
        pmem_allocator->offset2addr_checked<DLRecord>(record->prev);
    DLRecord* next =
        pmem_allocator->offset2addr_checked<DLRecord>(record->next);
    return prev->next == offset && next->prev == offset;
  }

  // Purge a dl record from its skiplist by remove it from linkage
  //
  // purged_record:existing record to purge
  // dram_node:dram node of purging record, if it's a height 0 record, then
  // pass nullptr
  // purging_record_lock: lock of purging_record, should be locked before call
  // this function
  //
  // Return true on success, return false on fail.
  static bool Purge(DLRecord* purging_record,
                    const SpinMutex* purging_record_lock,
                    SkiplistNode* dram_node, PMEMAllocator* pmem_allocator,
                    HashTable* hash_table);

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
  static bool Replace(DLRecord* old_record, DLRecord* new_record,
                      const SpinMutex* old_record_lock, SkiplistNode* dram_node,
                      PMEMAllocator* pmem_allocator, HashTable* hash_table);

  // Build a skiplist node for "pmem_record"
  static SkiplistNode* NewNodeBuild(DLRecord* pmem_record);

  // Format:
  // id (8 bytes) | configs
  static std::string EncodeSortedCollectionValue(
      CollectionIDType id, const SortedCollectionConfigs& s_configs);

  static Status DecodeSortedCollectionValue(StringView value_str,
                                            CollectionIDType& id,
                                            SortedCollectionConfigs& s_configs);

  inline static StringView UserKey(const SkiplistNode* node) {
    assert(node != nullptr);
    if (node->cached_key_size > 0) {
      return StringView(node->cached_key, node->cached_key_size);
    }
    return ExtractUserKey(node->record->Key());
  }

  inline static StringView UserKey(const DLRecord* record) {
    assert(record != nullptr);
    return ExtractUserKey(record->Key());
  }

  inline static CollectionIDType SkiplistID(const SkiplistNode* node) {
    assert(node != nullptr);
    return SkiplistID(node->record);
  }

  inline static CollectionIDType SkiplistID(const DLRecord* record) {
    assert(record != nullptr);
    switch (record->entry.meta.type) {
      case RecordType::SortedDataRecord:
      case RecordType::SortedDeleteRecord:
        return ExtractID(record->Key());
        break;
      case RecordType::SortedHeaderRecord:
        return DecodeID(record->Value());
      default:
        kvdk_assert(false, "Wrong type in SkiplistID");
        GlobalLogger.Error("Wrong type in SkiplistID");
    }
    return 0;
  }

 private:
  WriteResult setImplNoHash(const StringView& key, const StringView& value,
                            const SpinMutex* locked_key_lock,
                            TimeStampType timestamp);

  WriteResult setImplWithHash(const StringView& key, const StringView& value,
                              const HashTable::KeyHashHint& locked_hash_hint,
                              TimeStampType timestamp);

  WriteResult deleteImplNoHash(const StringView& key,
                               const SpinMutex* locked_key_lock,
                               TimeStampType timestamp);

  WriteResult deleteImplWithHash(const StringView& key,
                                 const HashTable::KeyHashHint& locked_hash_hint,
                                 TimeStampType timestamp);

  // Link DLRecord "linking" between "prev" and "next"
  static void linkDLRecord(DLRecord* prev, DLRecord* next, DLRecord* linking,
                           PMEMAllocator* pmem_allocator);

  inline void linkDLRecord(DLRecord* prev, DLRecord* next, DLRecord* linking) {
    return linkDLRecord(prev, next, linking, pmem_allocator_.get());
  }

  // lock skiplist position to insert "key" by locking
  // prev DLRecord and manage the lock with "prev_record_lock".
  //
  // The "insert_key" should be already locked before call this function
  bool lockInsertPosition(const StringView& inserting_key,
                          DLRecord* prev_record, DLRecord* next_record,
                          const SpinMutex* inserting_key_lock,
                          std::unique_lock<SpinMutex>* prev_record_lock);

  // lock skiplist position of "record" by locking its prev DLRecord and manage
  // the lock with "prev_record_lock".
  //
  // The key of "record" itself should be already locked before call
  // this function
  static bool lockRecordPosition(const DLRecord* record,
                                 const SpinMutex* record_key_lock,
                                 std::unique_lock<SpinMutex>* prev_record_lock,
                                 PMEMAllocator* pmem_allocator,
                                 HashTable* hash_table);

  bool lockRecordPosition(const DLRecord* record,
                          const SpinMutex* record_key_lock,
                          std::unique_lock<SpinMutex>* prev_record_lock) {
    return lockRecordPosition(record, record_key_lock, prev_record_lock,
                              pmem_allocator_.get(), hash_table_.get());
  }

  bool validateDLRecord(const DLRecord* record) {
    DLRecord* prev = pmem_allocator_->offset2addr<DLRecord>(record->prev);
    return prev != nullptr &&
           prev->next == pmem_allocator_->addr2offset(record) &&
           SkiplistID(record) == ID();
  }

  int compare(const StringView& src_key, const StringView& target_key) {
    return comparator_(src_key, target_key);
  }

  void obsoleteNodes(const std::vector<SkiplistNode*> nodes) {
    std::lock_guard<SpinMutex> lg(obsolete_nodes_spin_);
    for (SkiplistNode* node : nodes) {
      obsolete_nodes_.push_back(node);
    }
  }

  static uint8_t randomHeight() {
    uint8_t height = 0;
    while (height < kMaxHeight && fast_random_64() & 1) {
      height++;
    }

    return height;
  }

  void destroyRecords();

  void destroyNodes();

  SkiplistNode* header_;
  std::shared_ptr<HashTable> hash_table_;
  std::shared_ptr<PMEMAllocator> pmem_allocator_;
  bool index_with_hashtable_;
  // nodes that unlinked on every height
  std::vector<SkiplistNode*> obsolete_nodes_;
  // to avoid concurrent access a just deleted node, a node can be safely
  // deleted only if a certain interval is passes after being moved from
  // obsolete_nodes_ to pending_deletion_nodes_, this is guaranteed by
  // background thread of kvdk instance
  std::vector<SkiplistNode*> pending_deletion_nodes_;
  // protect obsolete_nodes_
  SpinMutex obsolete_nodes_spin_;
  // protect pending_deletion_nodes_
  SpinMutex pending_delete_nodes_spin_;
  Comparator comparator_ = compare_string_view;
};

// A helper struct for locating a skiplist position
//
// nexts: next nodes on DRAM of a key position, or node of the key if it existed
// prevs: prev nodes on DRAM of a key position
// prev_pmem_record: previous record on PMem of a key position
// next_pmem_record: next record on PMem of a key position, or record of the key
// if it existed
//
// TODO: maybe we only need prev position
struct Splice {
  // Seeking skiplist
  Skiplist* seeking_list;
  std::array<SkiplistNode*, kMaxHeight + 1> nexts;
  std::array<SkiplistNode*, kMaxHeight + 1> prevs;
  DLRecord* prev_pmem_record{nullptr};
  DLRecord* next_pmem_record{nullptr};

  Splice(Skiplist* s) : seeking_list(s) {}

  void Recompute(const StringView& key, uint8_t l) {
    SkiplistNode* start_node;
    uint8_t start_height = l;
    while (1) {
      if (start_height > kMaxHeight || prevs[start_height] == nullptr) {
        assert(seeking_list != nullptr);
        start_height = kMaxHeight;
        start_node = seeking_list->Header();
      } else if (prevs[start_height]->Next(start_height).GetTag() ==
                 SkiplistNode::NodeStatus::Deleted) {
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

}  // namespace KVDK_NAMESPACE
