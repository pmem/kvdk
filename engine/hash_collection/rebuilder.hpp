/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <future>

#include "../alias.hpp"
#include "../write_batch_impl.hpp"
#include "hash_list.hpp"

namespace KVDK_NAMESPACE {
class KVEngine;

class HashListRebuilder {
 public:
  struct RebuildResult {
    Status s = Status::Ok;
    CollectionIDType max_id = 0;
    std::unordered_map<CollectionIDType, std::shared_ptr<HashList>>
        rebuilt_hlists;
  };

  HashListRebuilder(PMEMAllocator* pmem_allocator, HashTable* hash_table,
                    LockTable* lock_table, ThreadManager* thread_manager,
                    uint64_t num_rebuild_threads, const CheckPoint& checkpoint)
      : recovery_utils_(pmem_allocator),
        rebuilder_thread_cache_(num_rebuild_threads),
        pmem_allocator_(pmem_allocator),
        hash_table_(hash_table),
        lock_table_(lock_table),
        thread_manager_(thread_manager),
        num_rebuild_threads_(num_rebuild_threads),
        checkpoint_(checkpoint) {}

  Status AddElem(DLRecord* elem_record) {
    bool linked_record = recovery_utils_.CheckAndRepairLinkage(elem_record);
    if (!linked_record) {
      if (recoverToCheckPoint()) {
        // We do not know if this is a checkpoint version record, so we can't
        // free
        // it here
        addUnlinkedRecord(elem_record);
      } else {
        pmem_allocator_->PurgeAndFree<DLRecord>(elem_record);
      }
    }
    return Status::Ok;
  }

  Status AddHeader(DLRecord* header_record) {
    bool linked_record = recovery_utils_.CheckAndRepairLinkage(header_record);
    if (!linked_record) {
      if (recoverToCheckPoint()) {
        // We do not know if this is a checkpoint version record, so we can't
        // free
        // it here
        addUnlinkedRecord(header_record);
      } else {
        pmem_allocator_->PurgeAndFree<DLRecord>(header_record);
      }
    } else {
      linked_headers_.emplace_back(header_record);
    }
    return Status::Ok;
  }

  RebuildResult Rebuild() {
    RebuildResult ret;
    ret.s = initRebuildLists();
    if (ret.s != Status::Ok) {
      return ret;
    }
    std::vector<std::future<Status>> fs;
    size_t i = 0;
    for (auto hlist : rebuild_hlists_) {
      i++;
      fs.push_back(std::async(&HashListRebuilder::rebuildIndex, this,
                              hlist.second.get()));
      if (i % num_rebuild_threads_ == 0 || i == rebuild_hlists_.size()) {
        for (auto& f : fs) {
          ret.s = f.get();
          if (ret.s != Status::Ok) {
            break;
          }
        }
        fs.clear();
      }
    }

    if (ret.s == Status::Ok) {
      ret.rebuilt_hlists.swap(rebuild_hlists_);
      ret.max_id = max_recovered_id_;
    }
    cleanInvalidRecords();
    return ret;
  }

  Status Rollback(const BatchWriteLog::HashLogEntry& log) {
    DLRecord* elem = pmem_allocator_->offset2addr_checked<DLRecord>(log.offset);
    // We only check prev linkage as a valid prev linkage indicate valid prev
    // and next pointers on the record, so we can safely do remove/replace
    if (elem->Validate() && recovery_utils_.CheckPrevLinkage(elem)) {
      if (elem->old_version != kNullPMemOffset) {
        bool success = DLList::Replace(
            elem,
            pmem_allocator_->offset2addr_checked<DLRecord>(elem->old_version),
            pmem_allocator_, lock_table_);
        kvdk_assert(success, "Replace should success as we checked linkage");
      } else {
        bool success = DLList::Remove(elem, pmem_allocator_, lock_table_);
        kvdk_assert(success, "Remove should success as we checked linkage");
      }
    }

    elem->Destroy();
    return Status::Ok;
  }

 private:
  bool recoverToCheckPoint() { return checkpoint_.Valid(); }

  Status initRebuildLists() {
    Status s = thread_manager_->MaybeInitThread(access_thread);
    if (s != Status::Ok) {
      return s;
    }

    // Keep headers with same id together for recognize outdated ones
    auto cmp = [](const DLRecord* header1, const DLRecord* header2) {
      auto id1 = HashList::FetchID(header1);
      auto id2 = HashList::FetchID(header2);
      if (id1 == id2) {
        return header1->GetTimestamp() < header2->GetTimestamp();
      }
      return id1 < id2;
    };
    std::sort(linked_headers_.begin(), linked_headers_.end(), cmp);

    for (size_t i = 0; i < linked_headers_.size(); i++) {
      DLRecord* header_record = linked_headers_[i];
      if (i + 1 < linked_headers_.size() &&
          HashList::FetchID(header_record) ==
              HashList::FetchID(linked_headers_[i + 1])) {
        // There are newer version of this header, it indicates system crashed
        // while updating header of a empty skiplist in previous run before
        // break header linkage.
        kvdk_assert(header_record->prev == header_record->next &&
                        header_record->prev ==
                            pmem_allocator_->addr2offset(header_record),
                    "outdated header record with valid linkage should always "
                    "point to it self");
        // Break the linkage
        auto newer_offset =
            pmem_allocator_->addr2offset(linked_headers_[i + 1]);
        header_record->PersistPrevNT(newer_offset);
        kvdk_assert(!recovery_utils_.CheckPrevLinkage(header_record) &&
                        !recovery_utils_.CheckNextLinkage(header_record),
                    "");
        addUnlinkedRecord(header_record);
        continue;
      }

      auto collection_name = header_record->Key();
      CollectionIDType id = HashList::FetchID(header_record);
      max_recovered_id_ = std::max(max_recovered_id_, id);

      DLRecord* valid_version_record = findCheckpointVersion(header_record);
      std::shared_ptr<HashList> hlist;
      if (valid_version_record == nullptr ||
          HashList::FetchID(valid_version_record) != id) {
        hlist = std::make_shared<HashList>(header_record, collection_name, id,
                                           pmem_allocator_, hash_table_,
                                           lock_table_);
        {
          std::lock_guard<SpinMutex> lg(lock_);
          invalid_hlists_[id] = hlist;
        }
      } else {
        auto ul = hash_table_->AcquireLock(collection_name);
        if (valid_version_record != header_record) {
          bool success = DLList::Replace(header_record, valid_version_record,
                                         pmem_allocator_, lock_table_);
          kvdk_assert(success,
                      "headers in rebuild should passed linkage check");
          addUnlinkedRecord(header_record);
        }

        hlist = std::make_shared<HashList>(valid_version_record,
                                           collection_name, id, pmem_allocator_,
                                           hash_table_, lock_table_);
        kvdk_assert(hlist != nullptr, "");

        bool outdated =
            valid_version_record->GetRecordStatus() == RecordStatus::Outdated ||
            valid_version_record->HasExpired();

        {
          std::lock_guard<SpinMutex> lg(lock_);
          if (outdated) {
            invalid_hlists_[id] = hlist;
          } else {
            rebuild_hlists_[id] = hlist;
          }
        }
        // TODO no need always to persist old version
        valid_version_record->PersistOldVersion(kNullPMemOffset);

        if (!outdated) {
          auto lookup_result = hash_table_->Insert(
              collection_name, RecordType::HashHeader, RecordStatus::Normal,
              hlist.get(), PointerType::HashList);
          switch (lookup_result.s) {
            case Status::Ok: {
              GlobalLogger.Error(
                  "Rebuild hlist error, hash entry of hlist records should "
                  "not be inserted before rebuild\n");
              return Status::Abort;
            }

            case Status::NotFound: {
              break;
            }
            default: {
              return lookup_result.s;
            }
          }
        }
      }
    }
    linked_headers_.clear();
    return Status::Ok;
  }

  DLRecord* findCheckpointVersion(DLRecord* pmem_record) {
    if (!recoverToCheckPoint()) {
      return pmem_record;
    }

    CollectionIDType id = HashList::FetchID(pmem_record);
    DLRecord* curr = pmem_record;
    while (curr != nullptr &&
           curr->GetTimestamp() > checkpoint_.CheckpointTS()) {
      curr = pmem_allocator_->offset2addr<DLRecord>(curr->old_version);
      kvdk_assert(curr == nullptr || curr->Validate(),
                  "Broken checkpoint: invalid older version sorted record");
      kvdk_assert(
          curr == nullptr || equal_string_view(curr->Key(), pmem_record->Key()),
          "Broken checkpoint: key of older version sorted data is "
          "not same as new "
          "version");
      if (curr && HashList::FetchID(curr) != id) {
        curr = nullptr;
      }
    }
    return curr;
  }

  Status rebuildIndex(HashList* hlist) {
    thread_manager_->MaybeInitThread(access_thread);

    size_t num_elems = 0;

    auto iter = hlist->GetDLList()->GetRecordIterator();
    iter->SeekToFirst();
    while (iter->Valid()) {
      DLRecord* curr = iter->Record();
      iter->Next();
      auto internal_key = curr->Key();
      auto ul = hash_table_->AcquireLock(internal_key);
      DLRecord* valid_version_record = findCheckpointVersion(curr);
      if (valid_version_record == nullptr ||
          valid_version_record->GetRecordStatus() == RecordStatus::Outdated) {
        bool success = hlist->GetDLList()->Remove(curr);
        kvdk_assert(success, "elems in rebuild should passed linkage check");
        addUnlinkedRecord(curr);
      } else {
        if (valid_version_record != curr) {
          bool success =
              hlist->GetDLList()->Replace(curr, valid_version_record);
          kvdk_assert(success, "elems in rebuild should passed linkage check");
          addUnlinkedRecord(curr);
        }
        num_elems++;

        auto lookup_result = hash_table_->Insert(
            internal_key, RecordType::HashElem, RecordStatus::Normal,
            valid_version_record, PointerType::DLRecord);
        switch (lookup_result.s) {
          case Status::Ok: {
            GlobalLogger.Error(
                "Rebuild hlist error, hash entry of hlist records should "
                "not be inserted before rebuild\n");
            return Status::Abort;
          }

          case Status::NotFound: {
            break;
          }
          default: {
            return lookup_result.s;
          }
        }

        valid_version_record->PersistOldVersion(kNullPMemOffset);
      }
    }
    hlist->UpdateSize(num_elems);
    return Status::Ok;
  }

  void addUnlinkedRecord(DLRecord* pmem_record) {
    kvdk_assert(access_thread.id >= 0, "");
    rebuilder_thread_cache_[access_thread.id % rebuilder_thread_cache_.size()]
        .unlinked_records.push_back(pmem_record);
  }

  void cleanInvalidRecords() {
    std::vector<SpaceEntry> to_free;

    // clean unlinked records
    for (auto& thread_cache : rebuilder_thread_cache_) {
      for (DLRecord* pmem_record : thread_cache.unlinked_records) {
        if (!recovery_utils_.CheckLinkage(pmem_record)) {
          pmem_record->Destroy();
          to_free.emplace_back(
              pmem_allocator_->addr2offset_checked(pmem_record),
              pmem_record->GetRecordSize());
        }
      }
      pmem_allocator_->BatchFree(to_free);
      to_free.clear();
      thread_cache.unlinked_records.clear();
    }

    // clean invalid skiplists
    for (auto& s : invalid_hlists_) {
      s.second->Destroy();
    }
    invalid_hlists_.clear();
  }

  struct ThreadCache {
    std::vector<DLRecord*> unlinked_records{};
  };

  DLListRecoveryUtils<HashList> recovery_utils_;
  std::vector<ThreadCache> rebuilder_thread_cache_;
  std::vector<DLRecord*> linked_headers_;
  PMEMAllocator* pmem_allocator_;
  HashTable* hash_table_;
  LockTable* lock_table_;
  ThreadManager* thread_manager_;
  const size_t num_rebuild_threads_;
  CheckPoint checkpoint_;
  SpinMutex lock_;
  std::unordered_map<CollectionIDType, std::shared_ptr<HashList>>
      invalid_hlists_;
  std::unordered_map<CollectionIDType, std::shared_ptr<HashList>>
      rebuild_hlists_;
  CollectionIDType max_recovered_id_;
};
}  // namespace KVDK_NAMESPACE