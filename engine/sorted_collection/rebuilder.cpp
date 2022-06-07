/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "rebuilder.hpp"

#include <future>

#include "../kv_engine.hpp"

namespace KVDK_NAMESPACE {
SortedCollectionRebuilder::SortedCollectionRebuilder(
    KVEngine* kv_engine, bool segment_based_rebuild,
    uint64_t num_rebuild_threads, const CheckPoint& checkpoint)
    : kv_engine_(kv_engine),
      checkpoint_(checkpoint),
      segment_based_rebuild_(segment_based_rebuild),
      num_rebuild_threads_(std::min(num_rebuild_threads,
                                    kv_engine->configs_.max_access_threads)),
      recovery_segments_(),
      rebuild_skiplits_(),
      invalid_skiplists_() {
  rebuilder_thread_cache_.resize(num_rebuild_threads_);
}

SortedCollectionRebuilder::RebuildResult SortedCollectionRebuilder::Rebuild() {
  RebuildResult ret;
  ret.s = initRebuildLists();
  if (ret.s != Status::Ok || rebuild_skiplits_.size() == 0) {
    return ret;
  }

  if (segment_based_rebuild_) {
    ret.s = segmentBasedIndexRebuild();
  } else {
    ret.s = listBasedIndexRebuild();
  }

  if (ret.s == Status::Ok) {
    ret.max_id = max_recovered_id_;
    ret.rebuild_skiplits.swap(rebuild_skiplits_);
  }
  cleanInvalidRecords();
  return ret;
}

Status SortedCollectionRebuilder::AddHeader(DLRecord* header_record) {
  assert(header_record->entry.meta.type == SortedHeader ||
         header_record->entry.meta.type == SortedHeaderDelete);

  bool linked_record = checkAndRepairRecordLinkage(header_record);

  if (!linked_record) {
    if (!recoverToCheckpoint()) {
      kv_engine_->purgeAndFree(header_record);
    } else {
      // We do not know if this is a checkpoint version record, so we can't free
      // it here
      addUnlinkedRecord(header_record);
    }
    return Status::Ok;
  }

  std::lock_guard<SpinMutex> lg(lock_);
  linked_headers_.emplace_back(header_record);
  return Status::Ok;
}

Status SortedCollectionRebuilder::AddElement(DLRecord* record) {
  kvdk_assert(record->entry.meta.type == SortedElem ||
                  record->entry.meta.type == SortedElemDelete,
              "wrong record type in RestoreSkiplistRecord");
  bool linked_record = checkAndRepairRecordLinkage(record);

  if (!linked_record) {
    if (!recoverToCheckpoint()) {
      kv_engine_->purgeAndFree(record);
    } else {
      // We do not know if this is a checkpoint version record, so we can't free
      // it here
      addUnlinkedRecord(record);
    }
  } else {
    if (segment_based_rebuild_ &&
        ++rebuilder_thread_cache_[access_thread.id]
                    .visited_skiplists[Skiplist::SkiplistID(record)] %
                kRestoreSkiplistStride ==
            0 &&
        findCheckpointVersion(record) == record &&
        record->entry.meta.type == SortedElem) {
      SkiplistNode* start_node = nullptr;
      while (start_node == nullptr) {
        // Always build dram node for a recovery segment start record
        start_node = Skiplist::NewNodeBuild(record);
      }
      addRecoverySegment(start_node);
    }
  }
  return Status::Ok;
}

Status SortedCollectionRebuilder::initRebuildLists() {
  PMEMAllocator* pmem_allocator = kv_engine_->pmem_allocator_.get();
  Status s = kv_engine_->MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  // Keep headers with same id together for recognize outdated ones
  auto cmp = [](const DLRecord* header1, const DLRecord* header2) {
    auto id1 = Skiplist::SkiplistID(header1);
    auto id2 = Skiplist::SkiplistID(header2);
    if (id1 == id2) {
      return header1->entry.meta.timestamp < header2->entry.meta.timestamp;
    }
    return id1 < id2;
  };
  std::sort(linked_headers_.begin(), linked_headers_.end(), cmp);

  for (size_t i = 0; i < linked_headers_.size(); i++) {
    DLRecord* header_record = linked_headers_[i];
    if (i + 1 < linked_headers_.size() &&
        Skiplist::SkiplistID(header_record) ==
            Skiplist::SkiplistID(linked_headers_[i + 1])) {
      // There are newer version of this header, it indicates system crashed
      // while updating header of a empty skiplist in previous run before break
      // header linkage.
      kvdk_assert(
          header_record->prev == header_record->next &&
              header_record->prev == pmem_allocator->addr2offset(header_record),
          "outdated header record with valid linkage should always "
          "point to it self");
      // Break the linkage
      auto newer_offset = pmem_allocator->addr2offset(linked_headers_[i + 1]);
      header_record->PersistPrevNT(newer_offset);
      kvdk_assert(
          !Skiplist::CheckRecordPrevLinkage(header_record, pmem_allocator) &&
              !Skiplist::CheckReocrdNextLinkage(header_record, pmem_allocator),
          "");
      addUnlinkedRecord(header_record);
      continue;
    }

    // Decode header
    std::string collection_name = string_view_2_string(header_record->Key());
    CollectionIDType id;
    SortedCollectionConfigs s_configs;
    Status s = Skiplist::DecodeSortedCollectionValue(header_record->Value(), id,
                                                     s_configs);
    if (s != Status::Ok) {
      GlobalLogger.Error(
          "Decode id and configs of sorted collection %s error\n",
          collection_name.c_str());
      return s;
    }

    auto comparator =
        kv_engine_->comparators_.GetComparator(s_configs.comparator_name);
    if (comparator == nullptr) {
      GlobalLogger.Error(
          "Compare function %s of restoring sorted collection %s is not "
          "registered\n",
          s_configs.comparator_name.c_str(), collection_name.c_str());
      return Status::Abort;
    }

    max_recovered_id_ = std::max(max_recovered_id_, id);

    // Check version and rebuild index
    DLRecord* valid_version_record = findCheckpointVersion(header_record);
    std::shared_ptr<Skiplist> skiplist;
    if (valid_version_record == nullptr ||
        Skiplist::SkiplistID(valid_version_record) != id) {
      // No valid version, or valid version header belongs to another linked
      // skiplist with same name
      skiplist =
          std::
              make_shared<Skiplist>(header_record, collection_name, id,
                                    comparator, pmem_allocator,
                                    kv_engine_->hash_table_.get(),
                                    kv_engine_->skiplist_locks_.get(), false /* we do not build hash index for a invalid skiplist as it will be destroyed soon */);
      {
        std::lock_guard<SpinMutex> lg(lock_);
        invalid_skiplists_[id] = skiplist;
      }
    } else {
      auto ul = kv_engine_->hash_table_->AcquireLock(collection_name);

      if (valid_version_record != header_record) {
        bool success = Skiplist::Replace(header_record, valid_version_record,
                                         nullptr, pmem_allocator,
                                         kv_engine_->skiplist_locks_.get());
        kvdk_assert(success, "headers in rebuild should passed linkage check");
        addUnlinkedRecord(header_record);
      }

      bool outdated =
          valid_version_record->entry.meta.type == SortedHeaderDelete ||
          TimeUtils::CheckIsExpired(valid_version_record->GetExpireTime());

      if (outdated) {
        skiplist = std::make_shared<Skiplist>(
            valid_version_record, collection_name, id, comparator,
            pmem_allocator, kv_engine_->hash_table_.get(),
            kv_engine_->skiplist_locks_.get(), false);
        {
          std::lock_guard<SpinMutex> lg(lock_);
          invalid_skiplists_[id] = skiplist;
        }
      } else {
        skiplist = std::make_shared<Skiplist>(
            valid_version_record, collection_name, id, comparator,
            pmem_allocator, kv_engine_->hash_table_.get(),
            kv_engine_->skiplist_locks_.get(), s_configs.index_with_hashtable);
        {
          std::lock_guard<SpinMutex> lg(lock_);
          rebuild_skiplits_[id] = skiplist;
        }
        if (segment_based_rebuild_) {
          // Always use header as a recovery segment
          addRecoverySegment(skiplist->HeaderNode());
        }

        valid_version_record->PersistOldVersion(kNullPMemOffset);
        // Always build hash index for skiplist
        s = insertHashIndex(skiplist->Name(), skiplist.get(),
                            PointerType::Skiplist);
        if (s != Status::Ok) {
          GlobalLogger.Error("Insert skiplist to hashtable error in  recovery");
          return s;
        }
      }
    }
  }
  linked_headers_.clear();
  return s;
}

Status SortedCollectionRebuilder::segmentBasedIndexRebuild() {
  GlobalLogger.Info("segment based rebuild start\n");
  std::vector<std::future<Status>> fs;

  auto rebuild_segments_index = [&]() -> Status {
    Status s = this->kv_engine_->MaybeInitAccessThread();
    if (s != Status::Ok) {
      return s;
    }
    defer(this->kv_engine_->ReleaseAccessThread());
    for (auto iter = this->recovery_segments_.begin();
         iter != this->recovery_segments_.end(); iter++) {
      if (!iter->second.visited) {
        std::lock_guard<SpinMutex> lg(this->lock_);
        if (!iter->second.visited) {
          iter->second.visited = true;
        } else {
          continue;
        }
      } else {
        continue;
      }

      auto rebuild_skiplist_iter = rebuild_skiplits_.find(
          Skiplist::SkiplistID(iter->second.start_node->record));
      if (rebuild_skiplist_iter == rebuild_skiplits_.end()) {
        // this start point belong to a invalid skiplist
        kvdk_assert(
            invalid_skiplists_.find(Skiplist::SkiplistID(
                iter->second.start_node->record)) != invalid_skiplists_.end(),
            "Start record of a recovery segment should belong to a skiplist");
      } else {
        Status s = rebuildSegmentIndex(iter->second.start_node,
                                       rebuild_skiplist_iter->second.get());
        if (s != Status::Ok) {
          return s;
        }
      }
    }
    return Status::Ok;
  };

  GlobalLogger.Info("build segment index\n");
  for (uint32_t thread_num = 0; thread_num < num_rebuild_threads_;
       ++thread_num) {
    fs.push_back(std::async(rebuild_segments_index));
  }
  for (auto& f : fs) {
    Status s = f.get();
    if (s != Status::Ok) {
      return s;
    }
  }
  fs.clear();
  GlobalLogger.Info("link dram nodes\n");

  size_t i = 0;
  for (auto& s : rebuild_skiplits_) {
    i++;
    fs.push_back(std::async(&SortedCollectionRebuilder::linkHighDramNodes, this,
                            s.second.get()));
    if (i % num_rebuild_threads_ == 0 || i == rebuild_skiplits_.size()) {
      for (auto& f : fs) {
        Status s = f.get();
        if (s != Status::Ok) {
          return s;
        }
      }
      fs.clear();
    }
  }

  recovery_segments_.clear();
  GlobalLogger.Info("segment based rebuild done\n");

  return Status::Ok;
}

Status SortedCollectionRebuilder::rebuildSegmentIndex(SkiplistNode* start_node,
                                                      Skiplist* segment_owner) {
  Status s;
  bool build_hash_index = segment_owner->IndexWithHashtable();
  size_t num_elems = 0;
  // First insert hash index for the start node
  if (start_node->record != segment_owner->HeaderRecord()) {
    kvdk_assert((start_node->record->entry.meta.type & SortedHeaderType) == 0,
                "Wrong start node of skiplist segment");
    num_elems++;
    if (build_hash_index) {
      s = insertHashIndex(start_node->record->Key(), start_node,
                          PointerType::SkiplistNode);
      if (s != Status::Ok) {
        return s;
      }
    }
  }
  kvdk_assert(findCheckpointVersion(start_node->record) == start_node->record,
              "start node of a recovery segment must be valid verion");
  start_node->record->PersistOldVersion(kNullPMemOffset);

  SkiplistNode* cur_node = start_node;
  DLRecord* cur_record = cur_node->record;
  while (true) {
    DLRecord* next_record =
        kv_engine_->pmem_allocator_->offset2addr_checked<DLRecord>(
            cur_record->next);
    if (next_record == segment_owner->HeaderRecord()) {
      cur_node->RelaxedSetNext(1, nullptr);
      break;
    }

    auto iter = recovery_segments_.find(next_record);
    if (iter == recovery_segments_.end()) {
      HashEntry hash_entry;
      DataEntry data_entry;
      StringView internal_key = next_record->Key();

      auto ul = kv_engine_->hash_table_->AcquireLock(internal_key);
      DLRecord* valid_version_record = findCheckpointVersion(next_record);
      if (valid_version_record == nullptr ||
          valid_version_record->entry.meta.type == SortedElemDelete) {
        bool success = Skiplist::Remove(next_record, nullptr,
                                        kv_engine_->pmem_allocator_.get(),
                                        kv_engine_->skiplist_locks_.get());
        kvdk_assert(success, "elems in rebuild should passed linkage check");
        addUnlinkedRecord(next_record);
      } else {
        if (valid_version_record != next_record) {
          bool success =
              Skiplist::Replace(next_record, valid_version_record, nullptr,
                                kv_engine_->pmem_allocator_.get(),
                                kv_engine_->skiplist_locks_.get());
          kvdk_assert(success, "elems in rebuild should passed linkage check");
          addUnlinkedRecord(next_record);
        }
        num_elems++;

        assert(valid_version_record != nullptr);
        SkiplistNode* dram_node = Skiplist::NewNodeBuild(valid_version_record);
        if (dram_node != nullptr) {
          cur_node->RelaxedSetNext(1, dram_node);
          dram_node->RelaxedSetNext(1, nullptr);
          cur_node = dram_node;
        }

        if (build_hash_index) {
          if (dram_node) {
            s = insertHashIndex(internal_key, dram_node,
                                PointerType::SkiplistNode);
          } else {
            s = insertHashIndex(internal_key, valid_version_record,
                                PointerType::DLRecord);
          }

          if (s != Status::Ok) {
            return s;
          }
        }
        valid_version_record->PersistOldVersion(kNullPMemOffset);
        cur_record = valid_version_record;
      }
    } else {
      // link end node of this segment to adjacent segment
      if ((iter->second.start_node->record->entry.meta.type &
           SortedHeaderType) == 0) {
        cur_node->RelaxedSetNext(1, iter->second.start_node);
      } else {
        cur_node->RelaxedSetNext(1, nullptr);
      }
      break;
    }
  }
  segment_owner->UpdateSize(num_elems);
  return Status::Ok;
}

void SortedCollectionRebuilder::linkSegmentDramNodes(SkiplistNode* start_node,
                                                     int height) {
  assert(height > 1);
  while (start_node->Height() < height) {
    start_node = start_node->RelaxedNext(height - 1).RawPointer();
    if (start_node == nullptr || recovery_segments_.find(start_node->record) !=
                                     recovery_segments_.end()) {
      return;
    }
  }
  SkiplistNode* cur_node = start_node;
  SkiplistNode* next_node = cur_node->RelaxedNext(height - 1).RawPointer();
  assert(start_node && start_node->Height() >= height);
  while (true) {
    if (next_node == nullptr) {
      cur_node->RelaxedSetNext(height, nullptr);
      break;
    }

    if (recovery_segments_.find(next_node->record) !=
        recovery_segments_.end()) {
      // link end point of this segment
      while (true) {
        if (next_node == nullptr || next_node->Height() >= height) {
          cur_node->RelaxedSetNext(height, next_node);
          break;
        } else {
          next_node = next_node->RelaxedNext(height - 1).RawPointer();
        }
      }
      break;
    }

    if (next_node->Height() >= height) {
      cur_node->RelaxedSetNext(height, next_node);
      next_node->RelaxedSetNext(height, nullptr);
      cur_node = next_node;
    }
    next_node = next_node->RelaxedNext(height - 1).RawPointer();
  }
}

Status SortedCollectionRebuilder::linkHighDramNodes(Skiplist* skiplist) {
  Splice splice(skiplist);
  for (uint8_t i = 1; i <= kMaxHeight; i++) {
    splice.prevs[i] = skiplist->HeaderNode();
  }

  SkiplistNode* next_node = splice.prevs[1]->RelaxedNext(1).RawPointer();
  while (next_node != nullptr) {
    assert(splice.prevs[1]->RelaxedNext(1).RawPointer() == next_node);
    splice.prevs[1] = next_node;
    if (next_node->Height() > 1) {
      for (uint8_t i = 2; i <= next_node->Height(); i++) {
        splice.prevs[i]->RelaxedSetNext(i, next_node);
        splice.prevs[i] = next_node;
      }
    }
    next_node = next_node->RelaxedNext(1).RawPointer();
  }
  for (uint8_t i = 1; i <= kMaxHeight; i++) {
    splice.prevs[i]->RelaxedSetNext(i, nullptr);
  }

  return Status::Ok;
}

Status SortedCollectionRebuilder::rebuildSkiplistIndex(Skiplist* skiplist) {
  Status s = kv_engine_->MaybeInitAccessThread();
  if (s != Status::Ok) {
    GlobalLogger.Error("too many threads repair skiplist linkage\n");
    return s;
  }
  defer(kv_engine_->ReleaseAccessThread());

  if (s != Status::Ok) {
    return s;
  }

  size_t num_elems = 0;

  Splice splice(skiplist);
  HashEntry hash_entry;
  for (uint8_t i = 1; i <= kMaxHeight; i++) {
    splice.prevs[i] = skiplist->HeaderNode();
    splice.prev_pmem_record = skiplist->HeaderRecord();
  }

  while (true) {
    uint64_t next_offset = splice.prev_pmem_record->next;
    DLRecord* next_record =
        kv_engine_->pmem_allocator_->offset2addr_checked<DLRecord>(next_offset);
    if (next_record == skiplist->HeaderRecord()) {
      break;
    }

    StringView internal_key = next_record->Key();
    auto ul = kv_engine_->hash_table_->AcquireLock(internal_key);
    DLRecord* valid_version_record = findCheckpointVersion(next_record);

    if (valid_version_record == nullptr ||
        valid_version_record->entry.meta.type == SortedElemDelete) {
      // purge invalid version record from list
      bool success = Skiplist::Remove(next_record, nullptr,
                                      kv_engine_->pmem_allocator_.get(),
                                      kv_engine_->skiplist_locks_.get());
      kvdk_assert(success, "elems in rebuild should passed linkage check");
      addUnlinkedRecord(next_record);
    } else {
      if (valid_version_record != next_record) {
        // repair linkage of checkpoint version
        bool success =
            Skiplist::Replace(next_record, valid_version_record, nullptr,
                              kv_engine_->pmem_allocator_.get(),
                              kv_engine_->skiplist_locks_.get());
        kvdk_assert(success, "elems in rebuild should passed linkage check");
        addUnlinkedRecord(next_record);
      }
      num_elems++;

      // Rebuild dram node
      assert(valid_version_record != nullptr);
      SkiplistNode* dram_node = Skiplist::NewNodeBuild(valid_version_record);

      if (dram_node != nullptr) {
        auto height = dram_node->Height();
        for (uint8_t i = 1; i <= height; i++) {
          splice.prevs[i]->RelaxedSetNext(i, dram_node);
          dram_node->RelaxedSetNext(i, nullptr);
          splice.prevs[i] = dram_node;
        }
      }

      // Rebuild hash index
      if (skiplist->IndexWithHashtable()) {
        Status s;
        if (dram_node) {
          s = insertHashIndex(internal_key, dram_node,
                              PointerType::SkiplistNode);
        } else {
          s = insertHashIndex(internal_key, valid_version_record,
                              PointerType::DLRecord);
        }

        if (s != Status::Ok) {
          return s;
        }
      }

      valid_version_record->PersistOldVersion(kNullPMemOffset);
      splice.prev_pmem_record = valid_version_record;
    }
  }
  skiplist->UpdateSize(num_elems);
  return Status::Ok;
}

Status SortedCollectionRebuilder::listBasedIndexRebuild() {
  std::vector<std::future<Status>> fs;
  size_t i = 0;
  for (auto skiplist : rebuild_skiplits_) {
    i++;
    fs.push_back(std::async(&SortedCollectionRebuilder::rebuildSkiplistIndex,
                            this, skiplist.second.get()));
    if (i % num_rebuild_threads_ == 0 || i == rebuild_skiplits_.size()) {
      for (auto& f : fs) {
        Status s = f.get();
        if (s != Status::Ok) {
          return s;
        }
      }
      fs.clear();
    }
  }

  return Status::Ok;
}

bool SortedCollectionRebuilder::checkRecordLinkage(DLRecord* record) {
  return Skiplist::CheckRecordLinkage(record,
                                      kv_engine_->pmem_allocator_.get());
}

bool SortedCollectionRebuilder::checkAndRepairRecordLinkage(DLRecord* record) {
  PMEMAllocator* pmem_allocator = kv_engine_->pmem_allocator_.get();

  // The next linkage is correct. If the prev linkage is correct too, the
  // record linkage is ok. If the prev linkage is not correct, it will be
  // repaired by the correct prodecessor soon, so directly return true here.
  if (Skiplist::CheckReocrdNextLinkage(record, pmem_allocator)) {
    return true;
  }
  // If only prev linkage is correct, then repair the next linkage
  if (Skiplist::CheckRecordPrevLinkage(record, pmem_allocator)) {
    DLRecord* next =
        pmem_allocator->offset2addr_checked<DLRecord>(record->next);
    next->prev = pmem_allocator->addr2offset_checked(record);
    pmem_persist(&next->prev, sizeof(PMemOffsetType));
    return true;
  }

  return false;
}

void SortedCollectionRebuilder::cleanInvalidRecords() {
  std::vector<SpaceEntry> to_free;

  // clean unlinked records
  for (auto& thread_cache : rebuilder_thread_cache_) {
    for (DLRecord* pmem_record : thread_cache.unlinked_records) {
      if (!Skiplist::IsSkiplistRecord(pmem_record) ||
          !checkRecordLinkage(pmem_record)) {
        pmem_record->Destroy();
        to_free.emplace_back(
            kv_engine_->pmem_allocator_->addr2offset_checked(pmem_record),
            pmem_record->entry.header.record_size);
      }
    }
    kv_engine_->pmem_allocator_->BatchFree(to_free);
    to_free.clear();
    thread_cache.unlinked_records.clear();
  }

  // clean invalid skiplists
  for (auto& s : invalid_skiplists_) {
    s.second->Destroy();
  }
  invalid_skiplists_.clear();
}

void SortedCollectionRebuilder::addRecoverySegment(SkiplistNode* start_node) {
  if (segment_based_rebuild_) {
    std::lock_guard<SpinMutex> lg(lock_);
    recovery_segments_.insert({start_node->record, {false, start_node}});
  }
}

Status SortedCollectionRebuilder::insertHashIndex(const StringView& key,
                                                  void* index_ptr,
                                                  PointerType index_type) {
  // TODO: ttl
  uint16_t search_type_mask = 0;
  RecordType record_type = RecordType::Empty;
  if (index_type == PointerType::DLRecord) {
    search_type_mask = SortedElemType;
    record_type = static_cast<DLRecord*>(index_ptr)->entry.meta.type;
  } else if (index_type == PointerType::SkiplistNode) {
    search_type_mask = SortedElemType;
    record_type =
        static_cast<SkiplistNode*>(index_ptr)->record->entry.meta.type;
  } else if (index_type == PointerType::Skiplist) {
    search_type_mask = SortedHeaderType;
    record_type = SortedHeader;
  } else {
    kvdk_assert(false, "Wrong type in sorted collection rebuilder");
  }

  auto lookup_result =
      kv_engine_->hash_table_->Lookup<true>(key, search_type_mask);

  switch (lookup_result.s) {
    case Status::NotFound: {
      kv_engine_->hash_table_->Insert(lookup_result, record_type, index_ptr,
                                      index_type);
      return Status::Ok;
    }
    case Status::Ok: {
      GlobalLogger.Error(
          "Rebuild skiplist error, hash entry of sorted records should not "
          "be "
          "inserted before rebuild\n");
      return Status::Abort;
    }

    case Status::MemoryOverflow: {
      return lookup_result.s;
    }

    default:
      std::abort();  // never reach
  }

  return Status::Ok;
}

DLRecord* SortedCollectionRebuilder::findCheckpointVersion(
    DLRecord* pmem_record) {
  kvdk_assert(pmem_record != nullptr,
              "pass nullptr to SortedCollectionRebuilder::findValidVersion");
  if (!recoverToCheckpoint()) {
    return pmem_record;
  }
  CollectionIDType id = Skiplist::SkiplistID(pmem_record);
  DLRecord* curr = pmem_record;
  while (curr != nullptr &&
         curr->entry.meta.timestamp > checkpoint_.CheckpointTS()) {
    curr =
        kv_engine_->pmem_allocator_->offset2addr<DLRecord>(curr->old_version);

    kvdk_assert(curr == nullptr || curr->Validate(),
                "Broken checkpoint: invalid older version sorted record");
    kvdk_assert(
        curr == nullptr || equal_string_view(curr->Key(), pmem_record->Key()),
        "Broken checkpoint: key of older version sorted data is "
        "not same as new "
        "version");

    if (curr && Skiplist::SkiplistID(curr) != id) {
      curr = nullptr;
    }
  }
  return curr;
}
}  // namespace KVDK_NAMESPACE