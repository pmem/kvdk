/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "../alias.hpp"
#include "skiplist.hpp"

namespace KVDK_NAMESPACE {
class KVEngine;

// Used to rebuild skiplist DRAM index and clean invalid records in recovery
// It support parallel rebuild with segment based rebuild or list based rebuild,
// segment based rebuild optimzied for rebuild a small number of large skiplist,
// while list based rebuild optimzied for rebuild massive skiplists
//
// segment_based_rebuild: use segment based rebuild if set true, otherwise
// rebuild with list based rebuild
// num_rebuild_threads: max number of rebuild threads, the parallel rebuild
// threads is min(kvdk access threads, num_rebuild_threads)
// checkpoint: rebuild skiplists to the checkpoint version if it's valid
class SortedCollectionRebuilder {
 public:
  SortedCollectionRebuilder(KVEngine* kv_engine, bool segment_based_rebuild,
                            uint64_t num_rebuild_threads,
                            const CheckPoint& checkpoint);

  // Rebuild result of skiplists
  //
  // rebuild_skiplists: succeffully rebuilded skiplists
  // max_id: max id of "rebuild_skiplists"
  struct RebuildResult {
    Status s = Status::Ok;
    CollectionIDType max_id = 0;
    std::unordered_map<CollectionIDType, std::shared_ptr<Skiplist>>
        rebuild_skiplits;
  };

  // Rebuild DRAM index for skiplists and free invalid records.
  RebuildResult Rebuild();

  // Add a skiplist data/delete record to rebuilder
  Status AddElement(DLRecord* record);

  // Add a skiplist header to rebuilder
  Status AddHeader(DLRecord* record);

 private:
  struct RebuildSegment {
    bool visited;
    SkiplistNode* start_node;
  };

  bool recoverToCheckpoint() { return checkpoint_.Valid(); }

  // Find the version of pmem_record under checkpoint version if checkpoint
  // exist, otherwise return pmem_record itself. Return nullptr if no valid
  // version of pmem_record exist.
  // If invalid_version_records is not null, put all invalid versions into it.
  DLRecord* findValidVersion(DLRecord* pmem_record,
                             std::vector<DLRecord*>* invalid_version_records);

  // Rebuild DRAM index based on skiplists, i.e., every recovery thread rebuilds
  // a whole skiplist one by one in parallel
  Status listBasedIndexRebuild();

  // Rebuild DRAM index based on skiplist segments, i.e., every recovery thread
  // rebuilds a segment of a skiplist one by one in parallel
  Status segmentBasedIndexRebuild();

  // Rebuild index for a skiplist
  //
  // Used in list based index rebuild
  Status rebuildSkiplistIndex(Skiplist* skiplist);

  // Add a recovery segment start from "start_node"
  //
  // Used in segment based index rebuild
  void addRecoverySegment(SkiplistNode* start_node);

  // Build/link first level dram nodes and build hash index for a recovery
  // segment
  //
  // Used in segment based index rebuild
  Status rebuildSegmentIndex(SkiplistNode* start_node, bool build_hash_index);

  // Link high level dram nodes of a skiplist after build the first level
  //
  // Used in segment based index rebuild
  Status linkHighDramNodes(Skiplist* skiplist);

  // Segment based dram nodes link
  //
  // Used in segment based index rebuild (not used for now)
  void linkSegmentDramNodes(SkiplistNode* start_node, int height);

  void cleanInvalidRecords();

  // Check if a record collectly linked in PMem list
  bool checkRecordLinkage(DLRecord* record);

  // Check if a record collectly linked in PMem list, and repair linkage if able
  bool checkAndRepairRecordLinkage(DLRecord* record);

  // insert hash index "ptr" for "key", the key should be locked before call
  // this function
  Status insertHashIndex(const StringView& key, void* ptr,
                         PointerType index_type);

  void addUnlinkedRecord(DLRecord* pmem_record) {
    assert(access_thread.id >= 0);
    rebuilder_thread_cache_[access_thread.id].unlinked_records.push_back(
        pmem_record);
  }

  struct ThreadCache {
    // For segment based rebuild
    std::unordered_map<uint64_t, int> visited_skiplists{};

    // For clean unlinked records in checkpoint recovery
    std::vector<DLRecord*> unlinked_records{};
  };

  KVEngine* kv_engine_;
  CheckPoint checkpoint_;
  bool segment_based_rebuild_;
  uint64_t num_rebuild_threads_;
  std::vector<ThreadCache> rebuilder_thread_cache_;
  std::unordered_map<DLRecord*, RebuildSegment> recovery_segments_{};
  // skiplists that need to rebuild index
  std::unordered_map<CollectionIDType, std::shared_ptr<Skiplist>>
      rebuild_skiplits_{};
  // skiplists that either newer than checkpoint or expired, need to be
  // destroyed
  std::unordered_map<CollectionIDType, std::shared_ptr<Skiplist>>
      invalid_skiplists_{};
  SpinMutex lock_;
  CollectionIDType max_recovered_id_ = 0;
  // Select elements as a segment start point for segment based rebuild every
  // kRestoreSkiplistStride elements per skiplist
  const uint64_t kRestoreSkiplistStride = 10000;
};
}  // namespace KVDK_NAMESPACE