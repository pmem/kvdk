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
      : pmem_allocator_(pmem_allocator),
        hash_table_(hash_table),
        lock_table_(lock_table),
        thread_manager_(thread_manager),
        num_rebuild_threads_(num_rebuild_threads),
        checkpoint_(checkpoint) {}

  Status AddElem(DLRecord* record) {
    // TODO check and repair linkage
    bool linked_record = true;
    if (!linked_record) {
      if (recoverToCheckPoint()) {
      } else {
        pmem_allocator_->PurgeAndFree<DLRecord>(record);
      }
    }
    return Status::Ok;
  }

  Status AddHeader(DLRecord* record) {
    // TODO check and repair linkage
    bool linked_record = true;
    if (!linked_record) {
      if (recoverToCheckPoint()) {
      } else {
        pmem_allocator_->PurgeAndFree<DLRecord>(record);
      }
    } else {
      linked_headers_.emplace_back(record);
    }
    return Status::Ok;
  }

  RebuildResult Rebuild() {
    RebuildResult ret;
    std::vector<std::future<Status>> fs;
    size_t i = 0;
    for (auto hlist : rebuild_hlists_) {
      i++;
      fs.push_back(
          std::async(&HashListRebuilder::rebuildIndex, this, hlist.second));
      ret.rebuilt_hlists.insert(hlist.second);
      if (i % num_rebuild_threads_ == 0 || i == rebuild_hlists_.size()) {
        for (auto& f : fs) {
          Status s = f.get();
          if (s != Status::Ok) {
            ret.s = s;
            return ret;
          }
        }
        fs.clear();
      }
    }
    ret.max_id = max_recovered_id_;
    return ret;
  }

 private:
  bool recoverToCheckPoint() { return checkpoint_.Valid(); }

  Status initRebuildLists() {
    for (size_t i = 0; i < linked_headers_.size(); i++) {
      DLRecord* header_record = linked_headers_[i];
      if (i + 1 < linked_headers_.size() /*&& xx*/) {
        continue;
      }

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
          // TODO add unlinked record
        }

        hlist = new HashList(valid_version_record, collection_name, id,
                             pmem_allocator_, hash_table_, lock_table_);

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
        // TODO insert hash index
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
    // TODO Init access thread
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
        // TODO add unlinked record
      } else {
        if (valid_version_record != curr) {
          bool success =
              hlist->GetDLList()->Replace(curr, valid_version_record);
          kvdk_assert(success, "elems in rebuild should passed linkage check");
          //   addUnlinkedRecord(next_record);
        }
        num_elems++;

        // insert hash index

        valid_version_record->PersistOldVersion(kNullPMemOffset);
        prev = valid_version_record;
      }
    }
    hlist->UpdateSize(num_elems);
    return Status::Ok;
  }

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