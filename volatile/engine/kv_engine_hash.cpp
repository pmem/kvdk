/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "hash_collection/iterator.hpp"
#include "kv_engine.hpp"

namespace KVDK_NAMESPACE {
Status KVEngine::HashCreate(StringView collection) {
  auto thread_holder = AcquireAccessThread();

  if (!checkKeySize(collection)) {
    return Status::InvalidDataSize;
  }

  std::shared_ptr<HashList> hlist = nullptr;
  return buildHashlist(collection, hlist);
}

Status KVEngine::buildHashlist(const StringView& collection,
                               std::shared_ptr<HashList>& hlist) {
  auto ul = hash_table_->AcquireLock(collection);
  auto holder = version_controller_.GetLocalSnapshotHolder();
  TimestampType new_ts = holder.Timestamp();
  auto lookup_result = lookupKey<true>(collection, RecordType::HashHeader);
  if (lookup_result.s == Status::NotFound ||
      lookup_result.s == Status::Outdated) {
    DLRecord* existing_header =
        lookup_result.s == Outdated
            ? lookup_result.entry.GetIndex().hlist->HeaderRecord()
            : nullptr;
    CollectionIDType id = collection_id_.fetch_add(1);
    std::string value_str = HashList::EncodeID(id);
    SpaceEntry space =
        kv_allocator_->Allocate(DLRecord::RecordSize(collection, value_str));
    if (space.size == 0) {
      return Status::MemoryOverflow;
    }
    // dl list is circular, so the next and prev pointers of
    // header point to itself
    DLRecord* data_record = DLRecord::PersistDLRecord(
        kv_allocator_->offset2addr_checked(space.offset), space.size, new_ts,
        RecordType::HashHeader, RecordStatus::Normal,
        kv_allocator_->addr2offset(existing_header), space.offset, space.offset,
        collection, value_str);
    hlist = std::make_shared<HashList>(data_record, collection, id,
                                       kv_allocator_.get(), hash_table_.get(),
                                       dllist_locks_.get());
    kvdk_assert(hlist != nullptr, "");
    addHashlistToMap(hlist);
    insertKeyOrElem(lookup_result, RecordType::HashHeader, RecordStatus::Normal,
                    hlist.get());
    return Status::Ok;
  } else {
    return lookup_result.s == Status::Ok ? Status::Existed : lookup_result.s;
  }
}

Status KVEngine::HashDestroy(StringView collection) {
  auto thread_holder = AcquireAccessThread();

  if (!checkKeySize(collection)) {
    return Status::InvalidDataSize;
  }

  auto ul = hash_table_->AcquireLock(collection);
  auto snapshot_holder = version_controller_.GetLocalSnapshotHolder();
  auto new_ts = snapshot_holder.Timestamp();
  HashList* hlist;
  Status s = hashListFind(collection, &hlist);
  if (s == Status::Ok) {
    DLRecord* header = hlist->HeaderRecord();
    kvdk_assert(header->GetRecordType() == RecordType::HashHeader, "");
    StringView value = header->Value();
    auto request_size = DLRecord::RecordSize(collection, value);
    SpaceEntry space = kv_allocator_->Allocate(request_size);
    if (space.size == 0) {
      return Status::MemoryOverflow;
    }
    DLRecord* data_record = DLRecord::PersistDLRecord(
        kv_allocator_->offset2addr_checked(space.offset), space.size, new_ts,
        RecordType::HashHeader, RecordStatus::Outdated,
        kv_allocator_->addr2offset_checked(header), header->prev, header->next,
        collection, value);
    bool success = hlist->Replace(header, data_record);
    kvdk_assert(success, "existing header should be linked on its hlist");
    hash_table_->Insert(collection, RecordType::HashHeader,
                        RecordStatus::Outdated, hlist, PointerType::HashList);
    {
      std::unique_lock<std::mutex> hlist_lock(hlists_mu_);
      expirable_hlists_.emplace(hlist);
    }
  }
  return s;
}

Status KVEngine::HashSize(StringView collection, size_t* len) {
  if (!checkKeySize(collection)) {
    return Status::InvalidDataSize;
  }
  auto thread_holder = AcquireAccessThread();

  auto token = version_controller_.GetLocalSnapshotHolder();
  HashList* hlist;
  Status s = hashListFind(collection, &hlist);
  if (s != Status::Ok) {
    return s;
  }
  *len = hlist->Size();
  return Status::Ok;
}

Status KVEngine::HashGet(StringView collection, StringView key,
                         std::string* value) {
  auto thread_holder = AcquireAccessThread();

  // Hold current snapshot in this thread
  auto holder = version_controller_.GetLocalSnapshotHolder();

  HashList* hlist;
  Status s = hashListFind(collection, &hlist);
  if (s == Status::Ok) {
    s = hlist->Get(key, value);
  }
  return s;
}

Status KVEngine::HashPut(StringView collection, StringView key,
                         StringView value) {
  auto thread_holder = AcquireAccessThread();

  // Hold current snapshot in this thread
  auto holder = version_controller_.GetLocalSnapshotHolder();

  HashList* hlist;
  Status s = hashListFind(collection, &hlist);
  if (s == Status::Ok) {
    std::string collection_key(hlist->InternalKey(key));
    if (!checkKeySize(collection_key) || !checkValueSize(value)) {
      s = Status::InvalidDataSize;
    } else {
      auto ul = hash_table_->AcquireLock(collection_key);
      auto ret =
          hlist->Put(key, value, version_controller_.GetCurrentTimestamp());
      if (ret.s == Status::Ok && ret.existing_record &&
          hlist->TryCleaningLock()) {
        removeAndCacheOutdatedVersion<DLRecord>(ret.write_record);
        hlist->ReleaseCleaningLock();
      }
      tryCleanCachedOutdatedRecord();
      s = ret.s;
    }
  }
  return s;
}

Status KVEngine::HashDelete(StringView collection, StringView key) {
  auto thread_holder = AcquireAccessThread();

  // Hold current snapshot in this thread
  auto holder = version_controller_.GetLocalSnapshotHolder();

  HashList* hlist;
  Status s = hashListFind(collection, &hlist);
  if (s == Status::Ok) {
    std::string collection_key(hlist->InternalKey(key));
    if (!checkKeySize(collection_key)) {
      s = Status::InvalidDataSize;
    } else {
      auto ul = hash_table_->AcquireLock(collection_key);
      auto ret = hlist->Delete(key, version_controller_.GetCurrentTimestamp());
      if (ret.s == Status::Ok && ret.existing_record && ret.write_record &&
          hlist->TryCleaningLock()) {
        removeAndCacheOutdatedVersion(ret.write_record);
        hlist->ReleaseCleaningLock();
      }
      tryCleanCachedOutdatedRecord();
      s = ret.s;
    }
  }
  return s;
}

Status KVEngine::HashModify(StringView collection, StringView key,
                            ModifyFunc modify_func, void* cb_args) {
  auto thread_holder = AcquireAccessThread();

  // Hold current snapshot in this thread
  auto holder = version_controller_.GetLocalSnapshotHolder();
  HashList* hlist;
  Status s = hashListFind(collection, &hlist);
  if (s == Status::Ok) {
    std::string internal_key(hlist->InternalKey(key));
    auto ul = hash_table_->AcquireLock(internal_key);
    auto ret = hlist->Modify(key, modify_func, cb_args,
                             version_controller_.GetCurrentTimestamp());
    s = ret.s;
    if (s == Status::Ok && ret.existing_record && ret.write_record &&
        hlist->TryCleaningLock()) {
      removeAndCacheOutdatedVersion<DLRecord>(ret.write_record);
      hlist->ReleaseCleaningLock();
    }
    tryCleanCachedOutdatedRecord();
  }
  return s;
}

HashIterator* KVEngine::HashIteratorCreate(StringView collection,
                                           Snapshot* snapshot, Status* status) {
  Status s{Status::Ok};
  HashIterator* ret(nullptr);
  if (!checkKeySize(collection)) {
    s = Status::InvalidDataSize;
  }

  if (s == Status::Ok) {
    bool create_snapshot = snapshot == nullptr;
    if (create_snapshot) {
      snapshot = GetSnapshot(false);
    }
    HashList* hlist;
    Status s = hashListFind(collection, &hlist);
    if (s == Status::Ok) {
      ret = new HashIteratorImpl(hlist, static_cast<SnapshotImpl*>(snapshot),
                                 create_snapshot);
    } else if (create_snapshot) {
      ReleaseSnapshot(snapshot);
    }
  }
  if (status) {
    *status = s;
  }
  return ret;
}

void KVEngine::HashIteratorRelease(HashIterator* hash_iter) {
  if (hash_iter == nullptr) {
    GlobalLogger.Info("pass a nullptr in KVEngine::HashIteratorRelease!\n");
    return;
  }
  HashIteratorImpl* iter = static_cast<HashIteratorImpl*>(hash_iter);
  if (iter->own_snapshot_) {
    ReleaseSnapshot(iter->snapshot_);
  }
  delete iter;
}

Status KVEngine::hashListFind(StringView collection, HashList** hlist) {
  // Callers should acquire the access token or snapshot.
  // Lockless lookup for the collection
  auto result = lookupKey<false>(collection, RecordType::HashHeader);
  if (result.s == Status::Outdated) {
    return Status::NotFound;
  }
  if (result.s != Status::Ok) {
    return result.s;
  }
  (*hlist) = result.entry.GetIndex().hlist;
  return Status::Ok;
}

Status KVEngine::hashWritePrepare(HashWriteArgs& args, TimestampType ts) {
  return args.hlist->PrepareWrite(args, ts);
}

Status KVEngine::hashListWrite(HashWriteArgs& args) {
  return args.hlist->Write(args).s;
}

Status KVEngine::hashListPublish(HashWriteArgs const&) { return Status::Ok; }

}  // namespace KVDK_NAMESPACE