/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kv_engine.hpp"

namespace KVDK_NAMESPACE {
Status KVEngine::HashCreate(StringView collection) {
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(collection)) {
    return Status::InvalidDataSize;
  }

  auto ul = hash_table_->AcquireLock(collection);
  auto holder = version_controller_.GetLocalSnapshotHolder();
  TimeStampType new_ts = holder.Timestamp();
  auto lookup_result = lookupKey<true>(collection, RecordType::HashHeader);
  if (lookup_result.s == Status::NotFound ||
      lookup_result.s == Status::Outdated) {
    DLRecord* existing_header =
        lookup_result.s == Outdated
            ? lookup_result.entry.GetIndex().hlist->HeaderRecord()
            : nullptr;
    CollectionIDType id = list_id_.fetch_add(1);
    std::string value_str = HashList::EncodeID(id);
    SpaceEntry space =
        pmem_allocator_->Allocate(DLRecord::RecordSize(collection, value_str));
    if (space.size == 0) {
      return Status::PmemOverflow;
    }
    // dl list is circular, so the next and prev pointers of
    // header point to itself
    DLRecord* pmem_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr_checked(space.offset), space.size, new_ts,
        RecordType::HashHeader, RecordStatus::Normal,
        pmem_allocator_->addr2offset(existing_header), space.offset,
        space.offset, collection, value_str);

    HashList* hlist =
        new HashList(pmem_record, collection, id, pmem_allocator_.get(),
                     hash_table_.get(), dllist_locks_.get());
    kvdk_assert(hlist != nullptr, "");
    {
      std::lock_guard<std::mutex> lg(hlists_mu_);
      hash_lists_.insert(hlist);
    }
    insertKeyOrElem(lookup_result, RecordType::HashHeader, RecordStatus::Normal,
                    hlist);
    return Status::Ok;
  } else {
    return lookup_result.s;
  }
}

Status KVEngine::HashDestroy(StringView collection) {
  auto s = MaybeInitAccessThread();
  defer(ReleaseAccessThread());
  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(collection)) {
    return Status::InvalidDataSize;
  }

  auto ul = hash_table_->AcquireLock(collection);
  auto snapshot_holder = version_controller_.GetLocalSnapshotHolder();
  auto new_ts = snapshot_holder.Timestamp();
  HashList* hlist;
  s = hashListFind(collection, &hlist);
  if (s == Status::Ok) {
    DLRecord* header = hlist->HeaderRecord();
    kvdk_assert(header->GetRecordType() == RecordType::HashHeader, "");
    StringView value = header->Value();
    auto request_size = DLRecord::RecordSize(collection, value);
    SpaceEntry space = pmem_allocator_->Allocate(request_size);
    if (space.size == 0) {
      return Status::PmemOverflow;
    }
    DLRecord* pmem_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr_checked(space.offset), space.size, new_ts,
        RecordType::HashHeader, RecordStatus::Outdated,
        pmem_allocator_->addr2offset_checked(header), header->prev,
        header->next, collection, value);
    bool success = hlist->Replace(header, pmem_record);
    kvdk_assert(success, "existing header should be linked on its hlist");
    hash_table_->Insert(collection, RecordType::HashHeader,
                        RecordStatus::Outdated, hlist, PointerType::HashList);
  }
  return s;
}

Status KVEngine::HashLength(StringView collection, size_t* len) {
  if (!CheckKeySize(collection)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

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
  Status s = MaybeInitAccessThread();

  if (s != Status::Ok) {
    return s;
  }

  // Hold current snapshot in this thread
  auto holder = version_controller_.GetLocalSnapshotHolder();

  HashList* hlist;
  s = hashListFind(collection, &hlist);
  if (s == Status::Ok) {
    s = hlist->Get(key, value);
  }
  return s;
}

Status KVEngine::HashPut(StringView collection, StringView key,
                         StringView value) {
  Status s = MaybeInitAccessThread();

  if (s != Status::Ok) {
    return s;
  }

  // Hold current snapshot in this thread
  auto holder = version_controller_.GetLocalSnapshotHolder();

  HashList* hlist;
  s = hashListFind(collection, &hlist);
  if (s == Status::Ok) {
    std::string collection_key(hlist->InternalKey(key));
    if (!CheckKeySize(collection_key) || !CheckValueSize(value)) {
      s = Status::InvalidDataSize;
    } else {
      auto ul = hash_table_->AcquireLock(collection_key);
      auto ret =
          hlist->Put(key, value, version_controller_.GetCurrentTimestamp());
      if (ret.s == Status::Ok && ret.existing_record) {
        removeAndCacheOutdatedVersion<DLRecord>(ret.write_record);
      }
      tryCleanCachedOutdatedRecord();
      s = ret.s;
    }
  }
  return s;
}

Status KVEngine::HashDelete(StringView collection, StringView key) {
  Status s = MaybeInitAccessThread();

  if (s != Status::Ok) {
    return s;
  }

  // Hold current snapshot in this thread
  auto holder = version_controller_.GetLocalSnapshotHolder();

  HashList* hlist;
  s = hashListFind(collection, &hlist);
  if (s == Status::Ok) {
    std::string collection_key(hlist->InternalKey(key));
    if (!CheckKeySize(collection_key)) {
      s = Status::InvalidDataSize;
    } else {
      auto ul = hash_table_->AcquireLock(collection_key);
      auto ret = hlist->Delete(key, version_controller_.GetCurrentTimestamp());
      if (ret.s == Status::Ok && ret.existing_record && ret.write_record) {
        removeAndCacheOutdatedVersion(ret.write_record);
      }
      tryCleanCachedOutdatedRecord();
      s = ret.s;
    }
  }
  return s;
}

Status KVEngine::HashModify(StringView collection, StringView key,
                            ModifyFunc modify_func, void* cb_args) {
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  // Hold current snapshot in this thread
  auto holder = version_controller_.GetLocalSnapshotHolder();
  HashList* hlist;
  s = hashListFind(collection, &hlist);
  if (s == Status::Ok) {
    std::string internal_key(hlist->InternalKey(key));
    auto ul = hash_table_->AcquireLock(internal_key);
    auto ret = hlist->Modify(key, modify_func, cb_args,
                             version_controller_.GetCurrentTimestamp());
    s = ret.s;
    if (s == Status::Ok && ret.existing_record && ret.write_record) {
      removeAndCacheOutdatedVersion<DLRecord>(ret.write_record);
    }
    tryCleanCachedOutdatedRecord();
  }
  return s;
}

std::unique_ptr<HashIterator> KVEngine::HashCreateIterator(StringView key,
                                                           Status* status) {
  if (!CheckKeySize(key)) {
    return nullptr;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return nullptr;
  }

  auto snapshot = GetSnapshot(false);
  HashList* hlist;
  Status s = hashListFind(key, &hlist);
  if (status != nullptr) {
    *status = s;
  }
  if (s != Status::Ok) {
    ReleaseSnapshot(snapshot);
    return nullptr;
  }
  return std::unique_ptr<HashIteratorImpl>{
      new HashIteratorImpl{hlist, static_cast<SnapshotImpl*>(snapshot), true}};
}

Status KVEngine::hashListFind(StringView key, HashList** hlist) {
  // Callers should acquire the access token or snapshot.
  // Lockless lookup for the collection
  auto result = lookupKey<false>(key, RecordType::HashHeader);
  if (result.s == Status::Outdated) {
    return Status::NotFound;
  }
  if (result.s != Status::Ok) {
    return result.s;
  }
  (*hlist) = result.entry.GetIndex().hlist;
  return Status::Ok;
}

Status KVEngine::restoreHashElem(DLRecord* rec) {
  return hash_rebuilder_->AddElem(rec);
}

Status KVEngine::restoreHashHeader(DLRecord* rec) {
  return hash_rebuilder_->AddHeader(rec);
}

Status KVEngine::hashWritePrepare(HashWriteArgs& args, TimeStampType ts) {
  return args.hlist->PrepareWrite(args, ts);
}

Status KVEngine::hashListWrite(HashWriteArgs& args) {
  return args.hlist->Write(args).s;
}

Status KVEngine::hashListPublish(HashWriteArgs const& args) {
  return Status::Ok;
}

Status KVEngine::hashListRollback(BatchWriteLog::HashLogEntry const& log) {
  DLRecord* elem = pmem_allocator_->offset2addr_checked<DLRecord>(log.offset);
  // We only check prev linkage as a valid prev linkage indicate valid prev and
  // next pointers on the record, so we can safely do remove/replace
  if (elem->Validate() &&
      DLList::CheckPrevLinkage(elem, pmem_allocator_.get())) {
    if (elem->old_version != kNullPMemOffset) {
      bool success = DLList::Replace(
          elem,
          pmem_allocator_->offset2addr_checked<DLRecord>(elem->old_version),
          pmem_allocator_.get(), dllist_locks_.get());
      kvdk_assert(success, "Replace should success as we checked linkage");
    } else {
      bool success =
          DLList::Remove(elem, pmem_allocator_.get(), dllist_locks_.get());
      kvdk_assert(success, "Remove should success as we checked linkage");
    }
  }

  elem->Destroy();
  return Status::Ok;
}

}  // namespace KVDK_NAMESPACE