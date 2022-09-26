/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <future>

#include "../alias.hpp"
#include "../write_batch_impl.hpp"
#include "list.hpp"

namespace KVDK_NAMESPACE {
class KVEngine;

class ListRebuilder {
 public:
  struct RebuildResult {
    Status s = Status::Ok;
    CollectionIDType max_id = 0;
    std::unordered_map<CollectionIDType, std::shared_ptr<List>> rebuilt_lists;
  };

  ListRebuilder(PMEMAllocator* pmem_allocator, HashTable* hash_table,
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
    kvdk_assert(elem_record->GetRecordType() == RecordType::ListElem, "");
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
    for (auto list : rebuild_lists_) {
      i++;
      fs.push_back(
          std::async(&ListRebuilder::rebuildIndex, this, list.second.get()));
      if (i % num_rebuild_threads_ == 0 || i == rebuild_lists_.size()) {
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
      ret.rebuilt_lists.swap(rebuild_lists_);
      ret.max_id = max_recovered_id_;
    }
    cleanInvalidRecords();
    return ret;
  }

  Status Rollback(const BatchWriteLog::ListLogEntry& log) {
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
    for (size_t i = 0; i < linked_headers_.size(); i++) {
      DLRecord* header_record = linked_headers_[i];
      // if (i + 1 < linked_headers_.size() /*&& xx*/) {
      // continue;
      // }

      auto collection_name = header_record->Key();
      CollectionIDType id = Collection::DecodeID(header_record->Value());
      max_recovered_id_ = std::max(max_recovered_id_, id);

      DLRecord* valid_version_record = findCheckpointVersion(header_record);
      std::shared_ptr<List> list;
      if (valid_version_record == nullptr ||
          List::FetchID(valid_version_record) != id) {
        list = std::make_shared<List>(header_record, collection_name, id,
                                      pmem_allocator_, lock_table_);
        {
          std::lock_guard<SpinMutex> lg(lock_);
          invalid_lists_[id] = list;
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

        list = std::make_shared<List>(valid_version_record, collection_name, id,
                                      pmem_allocator_, lock_table_);
        kvdk_assert(list != nullptr, "");

        bool outdated =
            valid_version_record->GetRecordStatus() == RecordStatus::Outdated ||
            valid_version_record->HasExpired();

        {
          std::lock_guard<SpinMutex> lg(lock_);
          if (outdated) {
            invalid_lists_[id] = list;
          } else {
            rebuild_lists_[id] = list;
          }
        }
        // TODO no need always to persist old version
        valid_version_record->PersistOldVersion(kNullPMemOffset);

        if (!outdated) {
          auto lookup_result = hash_table_->Insert(
              collection_name, RecordType::ListHeader, RecordStatus::Normal,
              list.get(), PointerType::List);
          switch (lookup_result.s) {
            case Status::Ok: {
              GlobalLogger.Error(
                  "Rebuild list error, hash entry of list records should "
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

    CollectionIDType id = List::FetchID(pmem_record);
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
      if (curr && List::FetchID(curr) != id) {
        curr = nullptr;
      }
    }
    return curr;
  }

  Status rebuildIndex(List* list) {
    auto ul = list->AcquireLock();
    Status s = thread_manager_->MaybeInitThread(access_thread);
    if (s != Status::Ok) {
      return s;
    }
    defer(thread_manager_->Release(access_thread));
    auto iter = list->GetDLList()->GetRecordIterator();
    iter->SeekToFirst();
    while (iter->Valid()) {
      DLRecord* curr = iter->Record();
      iter->Next();
      DLRecord* valid_version_record = findCheckpointVersion(curr);
      if (valid_version_record == nullptr ||
          valid_version_record->GetRecordStatus() == RecordStatus::Outdated) {
        bool success = list->GetDLList()->Remove(curr);
        kvdk_assert(success, "elems in rebuild should passed linkage check");
        addUnlinkedRecord(curr);
      } else {
        if (valid_version_record != curr) {
          bool success = list->GetDLList()->Replace(curr, valid_version_record);
          kvdk_assert(success, "elems in rebuild should passed linkage check");
          addUnlinkedRecord(curr);
        }
        valid_version_record->PersistOldVersion(kNullPMemOffset);
        list->AddLiveRecord(valid_version_record, ListPos::Back);
      }
    }
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
    for (auto& s : invalid_lists_) {
      {
        auto ul = s.second->AcquireLock();
        s.second->Destroy();
      }
    }
    invalid_lists_.clear();
  }

  struct ThreadCache {
    std::vector<DLRecord*> unlinked_records{};
  };

  DLListRecoveryUtils<List> recovery_utils_;
  std::vector<ThreadCache> rebuilder_thread_cache_;
  std::vector<DLRecord*> linked_headers_;
  PMEMAllocator* pmem_allocator_;
  HashTable* hash_table_;
  LockTable* lock_table_;
  ThreadManager* thread_manager_;
  const size_t num_rebuild_threads_;
  CheckPoint checkpoint_;
  SpinMutex lock_;
  std::unordered_map<CollectionIDType, std::shared_ptr<List>> invalid_lists_;
  std::unordered_map<CollectionIDType, std::shared_ptr<List>> rebuild_lists_;
  CollectionIDType max_recovered_id_;
};
}  // namespace KVDK_NAMESPACE