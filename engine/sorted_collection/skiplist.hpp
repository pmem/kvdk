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

#include "../alias.hpp"
#include "../collection.hpp"
#include "../hash_table.hpp"
#include "../lock_table.hpp"
#include "../structures.hpp"
#include "../utils/utils.hpp"
#include "../write_batch_impl.hpp"
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
           Comparator comparator, PMEMAllocator* pmem_allocator,
           HashTable* hash_table, LockTable* lock_table,
           bool index_with_hashtable);

  ~Skiplist();

  SkiplistNode* HeaderNode() { return header_; }

  DLRecord* HeaderRecord() { return header_->record; }

  bool IndexWithHashtable() { return index_with_hashtable_; }

  ExpireTimeType GetExpireTime() const final {
    return header_->record->expired_time;
  }

  bool HasExpired() const final {
    return TimeUtils::CheckIsExpired(GetExpireTime());
  }

  // TODO jiayu: use lock table for skiplist so will don't need to pass outsider
  // locks for write operations, and write operations wont fail anymore
  WriteResult SetExpireTime(ExpireTimeType expired_time,
                            TimeStampType timestamp);

  Status SetExpireTime(ExpireTimeType expired_time) final;

  Status MarkAsDeleted();

  // Put "key, value" to the skiplist
  //
  // timestamp: kvdk engine timestamp of this operation
  //
  // Return Ok on success, with the writed pmem record, its dram node and
  // updated pmem record if it existed, return Fail if there is thread
  // contension
  //
  // Notice: the setting key should already been locked by engine
  WriteResult Put(const StringView& key, const StringView& value,
                  TimeStampType timestamp);

  // Get value of "key" from the skiplist
  Status Get(const StringView& key, std::string* value);

  // Delete "key" from the skiplist by replace it with a delete record
  //
  // timestamp: kvdk engine timestamp of this operation
  //
  // Return Ok on success, with the writed pmem delete record, its dram node and
  // deleted pmem record if existed
  //
  // Notice: the deleting key should already been locked by engine
  WriteResult Delete(const StringView& key, TimeStampType timestamp);

  // Prepare args for batch write, store lookup/seek result and pmem space to
  // write new reocrd in args
  //
  // Args:
  // * args: prepared by kvdk engine, the op and ts field must be assigned
  //
  // Return:
  // Ok on success
  // PMemOverflow if no enough pmem space
  // MemoryOverflow if no enough dram space
  //
  // Notice: args.elem (the writing key) should already been locked by engine
  Status PrepareWrite(SortedWriteArgs& args);

  // Do batch write according to args
  //
  // Args:
  // * args: batch write args generated by PrepareWrite()
  //
  // Return:
  // Status Ok on success, with the writed pmem delete record, its dram node and
  // deleted pmem record if existed
  WriteResult Write(SortedWriteArgs& args);

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

  // Destroy and free the whole skiplist with old version list.
  void DestroyAll();

  // check node linkage and hash index
  Status CheckIndex();

  void CleanObsoletedNodes();

  // Return number of elements in skiplist
  size_t Size();

  void UpdateSize(int64_t delta);

  int Compare(const StringView& src_key, const StringView& target_key) {
    return comparator_(src_key, target_key);
  }

  static bool IsSkiplistRecord(DLRecord* record) {
    auto type = record->entry.meta.type;
    return type == SortedElem || type == SortedElemDelete ||
           type == SortedHeader || type == SortedHeaderDelete;
  }

  // Check if record correctly linked on list
  static bool CheckRecordLinkage(DLRecord* record,
                                 PMEMAllocator* pmem_allocator) {
    return CheckRecordPrevLinkage(record, pmem_allocator) &&
           CheckReocrdNextLinkage(record, pmem_allocator);
  }

  static bool CheckReocrdNextLinkage(DLRecord* record,
                                     PMEMAllocator* pmem_allocator) {
    kvdk_assert(IsSkiplistRecord(record),
                "check linkage of a non skiplist rececord");

    uint64_t offset = pmem_allocator->addr2offset_checked(record);
    DLRecord* next =
        pmem_allocator->offset2addr_checked<DLRecord>(record->next);

    auto check_linkage = [&]() { return next->prev == offset; };

    auto check_type = [&]() { return IsSkiplistRecord(next); };

    auto check_id = [&]() {
      auto next_id = Skiplist::SkiplistID(next);
      auto record_id = Skiplist::SkiplistID(record);
      return record_id == next_id;
    };

    return check_linkage() && check_type() && check_id();
  }

  static bool CheckRecordPrevLinkage(DLRecord* record,
                                     PMEMAllocator* pmem_allocator) {
    kvdk_assert(IsSkiplistRecord(record),
                "check linkage of a non skiplist rececord");
    uint64_t offset = pmem_allocator->addr2offset_checked(record);
    DLRecord* prev =
        pmem_allocator->offset2addr_checked<DLRecord>(record->prev);

    auto check_linkage = [&]() { return prev->next == offset; };

    auto check_type = [&]() { return IsSkiplistRecord(prev); };

    auto check_id = [&]() {
      auto prev_id = Skiplist::SkiplistID(prev);
      auto record_id = Skiplist::SkiplistID(record);
      return record_id == prev_id;
    };

    return check_linkage() && check_type() && check_id();
  }

  // Remove a dl record from its skiplist by unlinking
  //
  // Args:
  // * purged_record:existing record to purge
  // * dram_node:dram node of purging record, if it's a height 0 record, then
  // pass nullptr
  //
  // Return:
  // * true on success
  // * false if purging_record not linked on a skiplist
  //
  // Notice: key of the purging record should already been locked by engine
  static bool Remove(DLRecord* purging_record, SkiplistNode* dram_node,
                     PMEMAllocator* pmem_allocator, LockTable* lock_table);

  // Replace "old_record" from its skiplist with "replacing_record", please make
  // sure the key order is correct after replace
  //
  // Args:
  // * old_record: existing record to be replaced
  // * replacing_record: new reocrd to replace the older one
  // * dram_node:dram node of old record, if it's a height 0 record, then
  // pass nullptr
  //
  // Return:
  // * true on success
  // * false if old_record not linked on a skiplist
  //
  // Notice: key of the replacing record should already been locked by engine
  static bool Replace(DLRecord* old_record, DLRecord* new_record,
                      SkiplistNode* dram_node, PMEMAllocator* pmem_allocator,
                      LockTable* lock_table);

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
      case RecordType::SortedElem:
      case RecordType::SortedElemDelete:
        return ExtractID(record->Key());
        break;
      case RecordType::SortedHeader:
      case RecordType::SortedHeaderDelete:
        return DecodeID(record->Value());
      default:
        GlobalLogger.Error("Wrong record type %d in SkiplistID",
                           record->entry.meta.type);
        kvdk_assert(false, "Wrong type in SkiplistID");
    }
    return 0;
  }

 private:
  WriteResult putImplNoHash(const StringView& key, const StringView& value,
                            TimeStampType timestamp);

  WriteResult putImplWithHash(const StringView& key, const StringView& value,
                              TimeStampType timestamp);

  // put impl with prepared seek result and pmem space
  WriteResult putPreparedNoHash(Splice& seek_result, const StringView& key,
                                const StringView& value,
                                TimeStampType timestamp,
                                const SpaceEntry& space);

  // put impl with prepared lookup result and pmem space
  WriteResult putPreparedWithHash(const HashTable::LookupResult& lookup_result,
                                  const StringView& key,
                                  const StringView& value,
                                  TimeStampType timestamp,
                                  const SpaceEntry& space);
  WriteResult deleteImplNoHash(const StringView& key, TimeStampType timestamp);

  WriteResult deleteImplWithHash(const StringView& key,
                                 TimeStampType timestamp);

  // put impl with prepared existing record and pmem space
  WriteResult deletePreparedNoHash(DLRecord* existing_record,
                                   SkiplistNode* dram_node,
                                   const StringView& key,
                                   TimeStampType timestamp,
                                   const SpaceEntry& space);

  // put impl with prepared lookup result of existing record and pmem space
  WriteResult deletePreparedWithHash(
      const HashTable::LookupResult& lookup_result, const StringView& key,
      TimeStampType timestamp, const SpaceEntry& space);

  // Link DLRecord "linking" between "prev" and "next"
  static void linkDLRecord(DLRecord* prev, DLRecord* next, DLRecord* linking,
                           PMEMAllocator* pmem_allocator);

  inline void linkDLRecord(DLRecord* prev, DLRecord* next, DLRecord* linking) {
    return linkDLRecord(prev, next, linking, pmem_allocator_);
  }

  // lock skiplist position to insert "key" by locking prev DLRecord and manage
  // the lock with "prev_record_lock".
  //
  // Return true on success, return false if linkage of prev_record and
  // next_record changed before succefully acquire lock
  bool lockInsertPosition(const StringView& inserting_key,
                          DLRecord* prev_record, DLRecord* next_record,
                          LockTable::ULockType* prev_record_lock);

  // lock skiplist position of "record" by locking its prev DLRecord and the
  // record itself
  // Notice: we do not check if record is still correctly linked
  static LockTable::GuardType lockRecordPosition(const DLRecord* record,
                                                 PMEMAllocator* pmem_allocator,
                                                 LockTable* lock_table);

  // lock skiplist position of "record" by locking its prev DLRecord and the
  // record itself
  // Notice: record must be a on list record, e.g. correctly linked by its
  // predecessor
  LockTable::GuardType lockOnListRecord(const DLRecord* record) {
    while (true) {
      auto guard = lockRecordPosition(record, pmem_allocator_, record_locks_);
      DLRecord* prev =
          pmem_allocator_->offset2addr_checked<DLRecord>(record->prev);
      if (prev->next == pmem_allocator_->addr2offset_checked(record)) {
        return guard;
      }
    }
  }

  bool validateDLRecord(const DLRecord* record) {
    DLRecord* prev = pmem_allocator_->offset2addr<DLRecord>(record->prev);
    return prev != nullptr &&
           prev->next == pmem_allocator_->addr2offset(record) &&
           SkiplistID(record) == ID();
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

  // Destroy sorted records, not including old version list.
  void destroyRecords();

  void destroyNodes();

  // Destroy all sorted records including old version list.
  void destroyAllRecords();

  static LockTable::HashType recordHash(const DLRecord* record) {
    kvdk_assert(record != nullptr, "");
    return XXH3_64bits(record, sizeof(const DLRecord*));
  }

  std::atomic<size_t> size_;
  Comparator comparator_ = compare_string_view;
  PMEMAllocator* pmem_allocator_;
  // TODO: use specified hash table for each skiplist
  HashTable* hash_table_;
  // locks to protect modification of records
  LockTable* record_locks_;
  bool index_with_hashtable_;
  bool deleted_;
  SkiplistNode* header_;
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
        start_node = seeking_list->HeaderNode();
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
