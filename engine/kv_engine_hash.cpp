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
  auto lookup_result = lookupKey<true>(collection, RecordType::SortedHeader);
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
    // PMem level of dl list is circular, so the next and prev pointers of
    // header point to itself
    DLRecord* pmem_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr_checked(space.offset), space.size, new_ts,
        RecordType::HashRecord, RecordStatus::Normal,
        pmem_allocator_->addr2offset(existing_header), space.offset,
        space.offset, collection, value_str);

    HashList* hlist =
        new HashList(pmem_record, collection, id, pmem_allocator_.get(),
                     hash_table_.get(), skiplist_locks_.get());
    {
      std::lock_guard<std::mutex> lg(hlists_mu_);
      hash_lists_.insert(hlist);
    }
    // TODO add hlist to set
    insertKeyOrElem(lookup_result, RecordType::HashRecord, RecordStatus::Normal,
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
    kvdk_assert(header->GetRecordType() == RecordType::HashRecord, "");
    StringView value = header->Value();
    auto request_size = DLRecord::RecordSize(collection, value);
    SpaceEntry space = pmem_allocator_->Allocate(request_size);
    if (space.size == 0) {
      return Status::PmemOverflow;
    }
    DLRecord* pmem_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr_checked(space.offset), space.size, new_ts,
        RecordType::HashRecord, RecordStatus::Outdated,
        pmem_allocator_->addr2offset_checked(header), header->prev,
        header->next, collection, value);
    bool success = hlist->Replace(header, pmem_record);
    kvdk_assert(success, "existing header should be linked on its hlist");
    // insert to hash table
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
  Status s = hashListFind(collection, &hlist);
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
  Status s = hashListFind(collection, &hlist);
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
  Status s = hashListFind(collection, &hlist);
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

Status KVEngine::HashModify(StringView key, StringView field,
                            ModifyFunc modify_func, void* cb_args) {
  return Status::Ok;
}

std::unique_ptr<HashIterator> KVEngine::HashCreateIterator(StringView key,
                                                           Status* status) {
  if (!CheckKeySize(key)) {
    return nullptr;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return nullptr;
  }

  auto snapshot = version_controller_.GetGlobalSnapshotToken();
  HashList* hlist;
  Status s = hashListFind(key, &hlist);
  if (status != nullptr) {
    *status = s;
  }
  if (s != Status::Ok) {
    return nullptr;
  }
  return std::unique_ptr<HashIteratorImpl>{
      new HashIteratorImpl{hlist, std::move(snapshot)}};
}

Status KVEngine::hashListFind(StringView key, HashList** hlist) {
  // Callers should acquire the access token or snapshot.
  // Lockless lookup for the collection
  auto result = lookupKey<false>(key, RecordType::HashRecord);
  if (result.s == Status::Outdated) {
    return Status::NotFound;
  }
  if (result.s != Status::Ok) {
    return result.s;
  }
  (*hlist) = result.entry.GetIndex().hlist;
  return Status::Ok;
}

Status KVEngine::hashListRestoreElem(DLRecord* rec) {
  if (!hash_list_builder_->AddListElem(rec)) {
    // Broken record, don't put in HashTable.
    // Rebuilder will delete it after recovery is done.
    return Status::Ok;
  }

  StringView internal_key = rec->Key();
  auto guard = hash_table_->AcquireLock(internal_key);
  auto result = lookupElem<true>(internal_key, RecordType::HashElem);
  if (!(result.s == Status::Ok || result.s == Status::NotFound)) {
    return result.s;
  }
  kvdk_assert(result.s == Status::NotFound, "Impossible!");
  insertKeyOrElem(result, RecordType::HashElem, RecordStatus::Normal, rec);

  return Status::Ok;
}

Status KVEngine::hashListRestoreList(DLRecord* rec) {
  hash_list_builder_->AddListRecord(rec);
  return Status::Ok;
}

Status KVEngine::hashListRegisterRecovered() {
  CollectionIDType max_id = 0;
  for (HashList* hlist : hash_lists_) {
    auto guard = hash_table_->AcquireLock(hlist->Name());
    Status s = registerCollection(hlist);
    if (s != Status::Ok) {
      return s;
    }
    max_id = std::max(max_id, hlist->ID());
  }
  CollectionIDType old = list_id_.load();
  while (max_id >= old && !list_id_.compare_exchange_strong(old, max_id + 1)) {
  }
  return Status::Ok;
}

Status hashWritePrepare(HashWriteArgs& args, TimeStampType ts) {
  return args.hlist->PrepareWrite(args, ts);
}

Status KVEngine::hashListWrite(HashWriteArgs& args) {
  return args.hlist->Write(args).s;
}

Status KVEngine::hashListPublish(HashWriteArgs const& args) {
  return Status::Ok;
}

Status KVEngine::hashListRollback(BatchWriteLog::HashLogEntry const& log) {
  switch (log.op) {
    case BatchWriteLog::Op::Delete: {
      kvdk_assert(log.new_offset == kNullPMemOffset, "");
      DLRecord* old_rec = static_cast<DLRecord*>(
          pmem_allocator_->offset2addr_checked(log.old_offset));
      hash_list_builder_->RollbackDeletion(old_rec);
      break;
    }
    case BatchWriteLog::Op::Put: {
      kvdk_assert(log.old_offset == kNullPMemOffset, "");
      DLRecord* new_rec = static_cast<DLRecord*>(
          pmem_allocator_->offset2addr_checked(log.new_offset));
      hash_list_builder_->RollbackEmplacement(new_rec);
      break;
    }
    case BatchWriteLog::Op::Replace: {
      DLRecord* old_rec = static_cast<DLRecord*>(
          pmem_allocator_->offset2addr_checked(log.old_offset));
      DLRecord* new_rec = static_cast<DLRecord*>(
          pmem_allocator_->offset2addr_checked(log.new_offset));
      hash_list_builder_->RollbackReplacement(new_rec, old_rec);
      break;
    }
  }
  return Status::Ok;
}

}  // namespace KVDK_NAMESPACE