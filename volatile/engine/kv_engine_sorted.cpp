/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kv_engine.hpp"
#include "sorted_collection/iterator.hpp"

namespace KVDK_NAMESPACE {
Status KVEngine::SortedCreate(const StringView collection_name,
                              const SortedCollectionConfigs& s_configs) {
  auto thread_holder = AcquireAccessThread();

  if (!checkKeySize(collection_name)) {
    return Status::InvalidDataSize;
  }

  std::shared_ptr<Skiplist> skiplist = nullptr;

  return buildSkiplist(collection_name, s_configs, skiplist);
}

Status KVEngine::buildSkiplist(const StringView& collection_name,
                               const SortedCollectionConfigs& s_configs,
                               std::shared_ptr<Skiplist>& skiplist) {
  auto ul = hash_table_->AcquireLock(collection_name);
  auto holder = version_controller_.GetLocalSnapshotHolder();
  TimestampType new_ts = holder.Timestamp();
  auto lookup_result =
      lookupKey<true>(collection_name, RecordType::SortedHeader);
  if (lookup_result.s == NotFound || lookup_result.s == Outdated) {
    DLRecord* existing_header =
        lookup_result.s == Outdated
            ? lookup_result.entry.GetIndex().skiplist->HeaderRecord()
            : nullptr;
    auto comparator = comparators_.GetComparator(s_configs.comparator_name);
    if (comparator == nullptr) {
      GlobalLogger.Error("Compare function %s is not registered\n",
                         s_configs.comparator_name);
      return Status::Abort;
    }
    CollectionIDType id = collection_id_.fetch_add(1);
    std::string value_str =
        Skiplist::EncodeSortedCollectionValue(id, s_configs);
    uint32_t request_size =
        sizeof(DLRecord) + collection_name.size() + value_str.size();
    SpaceEntry space_entry = kv_allocator_->Allocate(request_size);
    if (space_entry.size == 0) {
      return Status::MemoryOverflow;
    }

    // Data level of dl list is circular, so the next and prev pointers of
    // header point to itself
    DLRecord* data_record = DLRecord::PersistDLRecord(
        kv_allocator_->offset2addr(space_entry.offset), space_entry.size,
        new_ts, RecordType::SortedHeader, RecordStatus::Normal,
        kv_allocator_->addr2offset(existing_header), space_entry.offset,
        space_entry.offset, collection_name, value_str);

    skiplist = std::make_shared<Skiplist>(
        data_record, string_view_2_string(collection_name), id, comparator,
        kv_allocator_.get(), skiplist_node_allocator_.get(), hash_table_.get(),
        dllist_locks_.get(), s_configs.index_with_hashtable);
    addSkiplistToMap(skiplist);
    insertKeyOrElem(lookup_result, RecordType::SortedHeader,
                    RecordStatus::Normal, skiplist.get());
  } else {
    return lookup_result.s == Status::Ok ? Status::Existed : lookup_result.s;
  }
  return Status::Ok;
}

Status KVEngine::SortedDestroy(const StringView collection_name) {
  auto thread_holder = AcquireAccessThread();

  auto ul = hash_table_->AcquireLock(collection_name);
  auto snapshot_holder = version_controller_.GetLocalSnapshotHolder();
  auto new_ts = snapshot_holder.Timestamp();
  auto lookup_result =
      lookupKey<false>(collection_name, RecordType::SortedHeader);
  if (lookup_result.s == Status::Ok) {
    Skiplist* skiplist = lookup_result.entry.GetIndex().skiplist;
    DLRecord* header = skiplist->HeaderRecord();
    assert(header->GetRecordType() == RecordType::SortedHeader);
    StringView value = header->Value();
    auto request_size =
        sizeof(DLRecord) + collection_name.size() + value.size();
    SpaceEntry space_entry = kv_allocator_->Allocate(request_size);
    if (space_entry.size == 0) {
      return Status::MemoryOverflow;
    }
    DLRecord* data_record = DLRecord::PersistDLRecord(
        kv_allocator_->offset2addr_checked(space_entry.offset),
        space_entry.size, new_ts, RecordType::SortedHeader,
        RecordStatus::Outdated, kv_allocator_->addr2offset_checked(header),
        header->prev, header->next, collection_name, value, 0);
    bool success =
        Skiplist::Replace(header, data_record, skiplist->HeaderNode(),
                          kv_allocator_.get(), dllist_locks_.get());
    kvdk_assert(success, "existing header should be linked on its skiplist");
    insertKeyOrElem(lookup_result, RecordType::SortedHeader,
                    RecordStatus::Outdated, skiplist);
    {
      std::unique_lock<std::mutex> skiplist_lock(skiplists_mu_);
      expirable_skiplists_.emplace(skiplist);
    }
  } else if (lookup_result.s == Status::Outdated ||
             lookup_result.s == Status::NotFound) {
    lookup_result.s = Status::Ok;
  }
  return lookup_result.s;
}

Status KVEngine::SortedSize(const StringView collection, size_t* size) {
  auto thread_holder = AcquireAccessThread();

  auto holder = version_controller_.GetLocalSnapshotHolder();

  Skiplist* skiplist = nullptr;
  auto ret = lookupKey<false>(collection, RecordType::SortedHeader);
  if (ret.s != Status::Ok) {
    return ret.s == Status::Outdated ? Status::NotFound : ret.s;
  }

  kvdk_assert(ret.entry.GetIndexType() == PointerType::Skiplist,
              "pointer type of skiplist in hash entry should be skiplist");
  skiplist = ret.entry.GetIndex().skiplist;
  *size = skiplist->Size();
  return Status::Ok;
}

Status KVEngine::SortedGet(const StringView collection,
                           const StringView user_key, std::string* value) {
  auto thread_holder = AcquireAccessThread();

  // Hold current snapshot in this thread
  auto holder = version_controller_.GetLocalSnapshotHolder();

  Skiplist* skiplist = nullptr;
  auto ret = lookupKey<false>(collection, RecordType::SortedHeader);
  if (ret.s != Status::Ok) {
    return ret.s == Status::Outdated ? Status::NotFound : ret.s;
  }

  kvdk_assert(ret.entry.GetIndexType() == PointerType::Skiplist,
              "pointer type of skiplist in hash entry should be skiplist");
  skiplist = ret.entry.GetIndex().skiplist;

  assert(skiplist);
  return skiplist->Get(user_key, value);
}

Status KVEngine::SortedPut(const StringView collection,
                           const StringView user_key, const StringView value) {
  auto thread_holder = AcquireAccessThread();

  auto snapshot_holder = version_controller_.GetLocalSnapshotHolder();

  Skiplist* skiplist = nullptr;

  auto ret = lookupKey<false>(collection, RecordType::SortedHeader);
  if (ret.s != Status::Ok) {
    return ret.s == Status::Outdated ? Status::NotFound : ret.s;
  }

  kvdk_assert(ret.entry.GetIndexType() == PointerType::Skiplist,
              "pointer type of skiplist in hash entry should be skiplist");
  skiplist = ret.entry.GetIndex().skiplist;
  return sortedPutImpl(skiplist, user_key, value);
}

Status KVEngine::SortedDelete(const StringView collection,
                              const StringView user_key) {
  auto thread_holder = AcquireAccessThread();

  // Hold current snapshot in this thread
  auto holder = version_controller_.GetLocalSnapshotHolder();

  Skiplist* skiplist = nullptr;
  auto ret = lookupKey<false>(collection, RecordType::SortedHeader);
  if (ret.s != Status::Ok) {
    return (ret.s == Status::Outdated || ret.s == Status::NotFound) ? Status::Ok
                                                                    : ret.s;
  }

  kvdk_assert(ret.entry.GetIndexType() == PointerType::Skiplist,
              "pointer type of skiplist in hash entry should be skiplist");
  skiplist = ret.entry.GetIndex().skiplist;

  return sortedDeleteImpl(skiplist, user_key);
}

SortedIterator* KVEngine::SortedIteratorCreate(const StringView collection,
                                               Snapshot* snapshot, Status* s) {
  Skiplist* skiplist;
  bool create_snapshot = snapshot == nullptr;
  if (create_snapshot) {
    snapshot = GetSnapshot(false);
  }
  // find collection
  auto res = lookupKey<false>(collection, RecordType::SortedHeader);
  if (s != nullptr) {
    *s = (res.s == Status::Outdated) ? Status::NotFound : res.s;
  }
  if (res.s == Status::Ok) {
    skiplist = res.entry_ptr->GetIndex().skiplist;
    return new SortedIteratorImpl(skiplist, kv_allocator_.get(),
                                  static_cast<SnapshotImpl*>(snapshot),
                                  create_snapshot);
  } else {
    if (create_snapshot) {
      ReleaseSnapshot(snapshot);
    }
    return nullptr;
  }
}

void KVEngine::SortedIteratorRelease(SortedIterator* sorted_iterator) {
  if (sorted_iterator == nullptr) {
    GlobalLogger.Info("pass a nullptr in KVEngine::SortedIteratorRelease!\n");
    return;
  }
  SortedIteratorImpl* iter = static_cast<SortedIteratorImpl*>(sorted_iterator);
  if (iter->own_snapshot_) {
    ReleaseSnapshot(iter->snapshot_);
  }
  delete iter;
}

Status KVEngine::sortedDeleteImpl(Skiplist* skiplist,
                                  const StringView& user_key) {
  std::string collection_key(skiplist->InternalKey(user_key));
  if (!checkKeySize(collection_key)) {
    return Status::InvalidDataSize;
  }

  auto ul = hash_table_->AcquireLock(collection_key);
  TimestampType new_ts = version_controller_.GetCurrentTimestamp();

  auto ret = skiplist->Delete(user_key, new_ts);

  if (ret.existing_record && ret.write_record && skiplist->TryCleaningLock()) {
    removeAndCacheOutdatedVersion(ret.write_record);
    skiplist->ReleaseCleaningLock();
  }
  tryCleanCachedOutdatedRecord();

  return ret.s;
}

Status KVEngine::sortedPutImpl(Skiplist* skiplist, const StringView& user_key,
                               const StringView& value) {
  std::string collection_key(skiplist->InternalKey(user_key));
  if (!checkKeySize(collection_key) || !checkValueSize(value)) {
    return Status::InvalidDataSize;
  }

  auto ul = hash_table_->AcquireLock(collection_key);
  TimestampType new_ts = version_controller_.GetCurrentTimestamp();
  auto ret = skiplist->Put(user_key, value, new_ts);

  // Collect outdated version records
  if (ret.existing_record && skiplist->TryCleaningLock()) {
    removeAndCacheOutdatedVersion<DLRecord>(ret.write_record);
    skiplist->ReleaseCleaningLock();
  }
  tryCleanCachedOutdatedRecord();

  return ret.s;
}

Status KVEngine::sortedWritePrepare(SortedWriteArgs& args, TimestampType ts) {
  return args.skiplist->PrepareWrite(args, ts);
}

Status KVEngine::sortedWrite(SortedWriteArgs& args) {
  // Notice Jiayu: we do not handle delay free here as is no longer need soon
  return args.skiplist->Write(args).s;
}

Status KVEngine::sortedWritePublish(SortedWriteArgs const&) {
  return Status::Ok;
}
}  // namespace KVDK_NAMESPACE
