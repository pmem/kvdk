/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <future>

#include "../alias.hpp"
#include "hash_list.hpp"

namespace KVDK_NAMESPACE {
class KVEngine;

class HashListRebuilder {
 public:
  struct RebuildResult {
    Status s = Status::Ok;
    CollectionIDType max_id = 0;
    std::set<HashList*, Collection::TTLCmp> rebuilt_hlists;
  };

  HashListRebuilder(PMEMAllocator* pmem_allocator, HashTable* hash_table,
                    LockTable* lock_table, ThreadManager* thread_manager,
                    uint64_t num_rebuild_threads, const CheckPoint& checkpoint)
      : rebuilder_thread_cache_(num_rebuild_threads),
        pmem_allocator_(pmem_allocator),
        hash_table_(hash_table),
        lock_table_(lock_table),
        thread_manager_(thread_manager),
        num_rebuild_threads_(num_rebuild_threads),
        checkpoint_(checkpoint) {}

  Status AddElem(DLRecord* elem_record) {
    bool linked_record = DLListRebuilderHelper::CheckAndRepairLinkage(
        elem_record, pmem_allocator_);
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
    bool linked_record = DLListRebuilderHelper::CheckAndRepairLinkage(
        header_record, pmem_allocator_);
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
      GlobalLogger.Debug("Add header %s status %u address %lu\n",
                         string_view_2_string(header_record->Key()).c_str(),
                         header_record->GetRecordStatus(),
                         pmem_allocator_->addr2offset_checked(header_record));
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
      fs.push_back(
          std::async(&HashListRebuilder::rebuildIndex, this, hlist.second));
      ret.rebuilt_hlists.insert(hlist.second);
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
    ret.max_id = max_recovered_id_;
    GlobalLogger.Debug("Rebuild finish, rebuilt %lu\n",
                       ret.rebuilt_hlists.size());
    return ret;
  }

 private:
  bool recoverToCheckPoint() { return checkpoint_.Valid(); }

  Status initRebuildLists() {
    GlobalLogger.Debug("Init %lu rebuild hash lists\n", linked_headers_.size());
    for (size_t i = 0; i < linked_headers_.size(); i++) {
      DLRecord* header_record = linked_headers_[i];
      // if (i + 1 < linked_headers_.size() /*&& xx*/) {
      // continue;
      // }

      auto collection_name = header_record->Key();
      CollectionIDType id = Collection::DecodeID(header_record->Value());
      max_recovered_id_ = std::max(max_recovered_id_, id);

      DLRecord* valid_version_record = findCheckpointVersion(header_record);
      HashList* hlist;
      if (valid_version_record == nullptr ||
          HashList::HashListID(valid_version_record) != id) {
        hlist = new HashList(header_record, collection_name, id,
                             pmem_allocator_, hash_table_, lock_table_);
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

        hlist = new HashList(valid_version_record, collection_name, id,
                             pmem_allocator_, hash_table_, lock_table_);
        kvdk_assert(hlist != nullptr, "");

        bool outdated =
            valid_version_record->GetRecordStatus() == RecordStatus::Outdated ||
            valid_version_record->HasExpired();
        GlobalLogger.Debug("Outdated? %d\n", outdated);

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
              hlist, PointerType::HashList);
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
          GlobalLogger.Debug("rebuild hlist %s ok\n",
                             string_view_2_string(collection_name).c_str());
        }
        // TODO continue
      }
    }
    linked_headers_.clear();
    return Status::Ok;
  }

  DLRecord* findCheckpointVersion(DLRecord* pmem_record) {
    if (!recoverToCheckPoint()) {
      return pmem_record;
    }

    CollectionIDType id = HashList::HashListID(pmem_record);
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
      if (curr && HashList::HashListID(curr) != id) {
        curr = nullptr;
      }
    }
    return curr;
  }

  Status rebuildIndex(HashList* hlist) {
    Status s = thread_manager_->MaybeInitThread(access_thread);
    if (s != Status::Ok) {
      return s;
    }
    defer(thread_manager_->Release(access_thread));
    size_t num_elems = 0;
    DLRecord* prev = hlist->HeaderRecord();
    while (true) {
      DLRecord* curr =
          pmem_allocator_->offset2addr_checked<DLRecord>(prev->next);
      if (curr == hlist->HeaderRecord()) {
        break;
      }
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
        prev = valid_version_record;
      }
    }
    hlist->UpdateSize(num_elems);
    return Status::Ok;
  }

  void addUnlinkedRecord(DLRecord* pmem_record) {
    assert(access_thread.id >= 0);
    rebuilder_thread_cache_[access_thread.id].unlinked_records.push_back(
        pmem_record);
  }

  void cleanInvalidRecords() {
    std::vector<SpaceEntry> to_free;

    // clean unlinked records
    for (auto& thread_cache : rebuilder_thread_cache_) {
      for (DLRecord* pmem_record : thread_cache.unlinked_records) {
        if (!DLList::CheckLinkage(pmem_record, pmem_allocator_)) {
          pmem_record->Destroy();
          to_free.emplace_back(
              pmem_allocator_->addr2offset_checked(pmem_record),
              pmem_record->entry.header.record_size);
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

  std::vector<ThreadCache> rebuilder_thread_cache_;
  std::vector<DLRecord*> linked_headers_;
  PMEMAllocator* pmem_allocator_;
  HashTable* hash_table_;
  LockTable* lock_table_;
  ThreadManager* thread_manager_;
  const size_t num_rebuild_threads_;
  CheckPoint checkpoint_;
  SpinMutex lock_;
  std::unordered_map<CollectionIDType, HashList*> invalid_hlists_;
  std::unordered_map<CollectionIDType, HashList*> rebuild_hlists_;
  CollectionIDType max_recovered_id_;
};
}  // namespace KVDK_NAMESPACE