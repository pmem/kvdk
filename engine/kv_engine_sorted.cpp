/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kv_engine.hpp"
#include "sorted_collection/iterator.hpp"

namespace KVDK_NAMESPACE {
Status KVEngine::CreateSortedCollection(
    const StringView collection_name,
    const SortedCollectionConfigs& s_configs) {
  Status s = MaybeInitAccessThread();
  defer(ReleaseAccessThread());
  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(collection_name)) {
    return Status::InvalidDataSize;
  }

  // TODO jiayu: package sorted collection creation and destroy in Skiplist
  // class
  auto hint = hash_table_->GetHint(collection_name);
  std::lock_guard<SpinMutex> lg(*hint.spin);
  auto holder = version_controller_.GetLocalSnapshotHolder();
  TimeStampType new_ts = holder.Timestamp();
  auto ret =
      lookupKey<true>(collection_name, SortedHeader | SortedHeaderDelete);
  if (ret.s == NotFound || ret.s == Outdated) {
    DLRecord* existing_header =
        ret.s == Outdated ? ret.entry.GetIndex().skiplist->HeaderRecord()
                          : nullptr;
    auto comparator = comparators_.GetComparator(s_configs.comparator_name);
    if (comparator == nullptr) {
      GlobalLogger.Error("Compare function %s is not registered\n",
                         s_configs.comparator_name);
      return Status::Abort;
    }
    CollectionIDType id = list_id_.fetch_add(1);
    std::string value_str =
        Skiplist::EncodeSortedCollectionValue(id, s_configs);
    uint32_t request_size =
        sizeof(DLRecord) + collection_name.size() + value_str.size();
    SpaceEntry space_entry = pmem_allocator_->Allocate(request_size);
    if (space_entry.size == 0) {
      return Status::PmemOverflow;
    }
    // PMem level of skiplist is circular, so the next and prev pointers of
    // header point to itself
    DLRecord* pmem_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr(space_entry.offset), space_entry.size,
        new_ts, SortedHeader, pmem_allocator_->addr2offset(existing_header),
        space_entry.offset, space_entry.offset, collection_name, value_str);

    auto skiplist = std::make_shared<Skiplist>(
        pmem_record, string_view_2_string(collection_name), id, comparator,
        pmem_allocator_.get(), hash_table_.get(), skiplist_locks_.get(),
        s_configs.index_with_hashtable);
    addSkiplistToMap(skiplist);
    hash_table_->Insert(hint, ret.entry_ptr, SortedHeader, skiplist.get(),
                        PointerType::Skiplist);
  } else {
    // Todo (jiayu): handle expired skiplist
    // Todo (jiayu): what if skiplist exists but comparator not match?
    return ret.s;
  }

  return Status::Ok;
}

Status KVEngine::DestroySortedCollection(const StringView collection_name) {
  auto s = MaybeInitAccessThread();
  defer(ReleaseAccessThread());
  if (s != Status::Ok) {
    return s;
  }
  auto hint = hash_table_->GetHint(collection_name);
  std::unique_lock<SpinMutex> ul(*hint.spin);
  auto snapshot_holder = version_controller_.GetLocalSnapshotHolder();
  auto new_ts = snapshot_holder.Timestamp();
  auto ret = lookupKey<false>(collection_name,
                              static_cast<uint16_t>(RecordType::SortedHeader));
  if (ret.s == Status::Ok) {
    Skiplist* skiplist = ret.entry.GetIndex().skiplist;
    DLRecord* header = skiplist->HeaderRecord();
    assert(header->entry.meta.type == SortedHeader);
    StringView value = header->Value();
    auto request_size =
        sizeof(DLRecord) + collection_name.size() + value.size();
    SpaceEntry space_entry = pmem_allocator_->Allocate(request_size);
    if (space_entry.size == 0) {
      return Status::PmemOverflow;
    }
    DLRecord* pmem_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr_checked(space_entry.offset),
        space_entry.size, new_ts, SortedHeaderDelete,
        pmem_allocator_->addr2offset_checked(header), header->prev,
        header->next, collection_name, value);
    Skiplist::Replace(header, pmem_record, skiplist->HeaderNode(),
                      pmem_allocator_.get(), skiplist_locks_.get());
    hash_table_->Insert(hint, ret.entry_ptr, SortedHeaderDelete, skiplist,
                        PointerType::Skiplist);
    hash_table_->UpdateEntryStatus(ret.entry_ptr, KeyStatus::Expired);

    ul.unlock();
    delayFree(OldDataRecord{header, new_ts});
  } else if (ret.s == Status::Outdated || ret.s == Status::NotFound) {
    ret.s = Status::Ok;
  }
  return ret.s;
}

Status KVEngine::SortedSize(const StringView collection, size_t* size) {
  Status s = MaybeInitAccessThread();

  if (s != Status::Ok) {
    return s;
  }

  auto holder = version_controller_.GetLocalSnapshotHolder();

  Skiplist* skiplist = nullptr;
  auto ret = lookupKey<false>(collection, SortedHeader | SortedHeaderDelete);
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
  Status s = MaybeInitAccessThread();

  if (s != Status::Ok) {
    return s;
  }

  // Hold current snapshot in this thread
  auto holder = version_controller_.GetLocalSnapshotHolder();

  Skiplist* skiplist = nullptr;
  auto ret = lookupKey<false>(collection, SortedHeader | SortedHeaderDelete);
  if (ret.s != Status::Ok) {
    return ret.s == Status::Outdated ? Status::NotFound : ret.s;
  }

  kvdk_assert(ret.entry.GetIndexType() == PointerType::Skiplist,
              "pointer type of skiplist in hash entry should be skiplist");
  skiplist = ret.entry.GetIndex().skiplist;

  assert(skiplist);
  return skiplist->Get(user_key, value);
}

Status KVEngine::SortedSet(const StringView collection,
                           const StringView user_key, const StringView value) {
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  auto snapshot_holder = version_controller_.GetLocalSnapshotHolder();

  Skiplist* skiplist = nullptr;

  auto ret = lookupKey<false>(collection, SortedHeader | SortedHeaderDelete);
  if (ret.s != Status::Ok) {
    return ret.s == Status::Outdated ? Status::NotFound : ret.s;
  }

  kvdk_assert(ret.entry.GetIndexType() == PointerType::Skiplist,
              "pointer type of skiplist in hash entry should be skiplist");
  skiplist = ret.entry.GetIndex().skiplist;
  return SortedSetImpl(skiplist, user_key, value);
}

Status KVEngine::SortedDelete(const StringView collection,
                              const StringView user_key) {
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }
  // Hold current snapshot in this thread
  auto holder = version_controller_.GetLocalSnapshotHolder();

  Skiplist* skiplist = nullptr;
  auto ret = lookupKey<false>(collection, SortedHeader | SortedHeaderDelete);
  // GlobalLogger.Debug("ret.s is %d\n", ret.s);
  if (ret.s != Status::Ok) {
    return (ret.s == Status::Outdated || ret.s == Status::NotFound) ? Status::Ok
                                                                    : ret.s;
  }

  kvdk_assert(ret.entry.GetIndexType() == PointerType::Skiplist,
              "pointer type of skiplist in hash entry should be skiplist");
  skiplist = ret.entry.GetIndex().skiplist;

  return SDeleteImpl(skiplist, user_key);
}

Iterator* KVEngine::NewSortedIterator(const StringView collection,
                                      Snapshot* snapshot) {
  Skiplist* skiplist;
  // find collection
  auto res = lookupKey<false>(collection, SortedHeader);
  if (res.s == Status::Ok) {
    skiplist = res.entry_ptr->GetIndex().skiplist;
  }

  bool create_snapshot = snapshot == nullptr;
  if (create_snapshot) {
    snapshot = GetSnapshot(false);
  }

  return res.s == Status::Ok
             ? new SortedIterator(skiplist, pmem_allocator_.get(),
                                  static_cast<SnapshotImpl*>(snapshot),
                                  create_snapshot)
             : nullptr;
}

void KVEngine::ReleaseSortedIterator(Iterator* sorted_iterator) {
  if (sorted_iterator == nullptr) {
    GlobalLogger.Info("pass a nullptr in KVEngine::ReleaseSortedIterator!\n");
    return;
  }
  SortedIterator* iter = static_cast<SortedIterator*>(sorted_iterator);
  if (iter->own_snapshot_) {
    ReleaseSnapshot(iter->snapshot_);
  }
  delete iter;
}

Status KVEngine::SDeleteImpl(Skiplist* skiplist, const StringView& user_key) {
  std::string collection_key(skiplist->InternalKey(user_key));
  if (!CheckKeySize(collection_key)) {
    return Status::InvalidDataSize;
  }

  auto hint = hash_table_->GetHint(collection_key);

  std::unique_lock<SpinMutex> ul(*hint.spin);
  TimeStampType new_ts = version_controller_.GetCurrentTimestamp();

  auto ret = skiplist->Delete(user_key, new_ts);
  if (ret.hash_entry_ptr != nullptr) {
    hash_table_->UpdateEntryStatus(ret.hash_entry_ptr, KeyStatus::Expired);
  }
  ul.unlock();
  if (ret.s == Status::Ok) {
    if (ret.write_record != nullptr) {
      kvdk_assert(ret.existing_record != nullptr &&
                      ret.existing_record->entry.meta.type == SortedElem,
                  "Wrong existing record type while insert a delete reocrd for "
                  "sorted collection");
      delayFree(OldDataRecord{ret.existing_record, new_ts});
    }
  }
  return ret.s;
}

Status KVEngine::SortedSetImpl(Skiplist* skiplist, const StringView& user_key,
                               const StringView& value) {
  std::string collection_key(skiplist->InternalKey(user_key));
  if (!CheckKeySize(collection_key) || !CheckValueSize(value)) {
    return Status::InvalidDataSize;
  }

  auto hint = hash_table_->GetHint(collection_key);
  std::unique_lock<SpinMutex> ul(*hint.spin);
  TimeStampType new_ts = version_controller_.GetCurrentTimestamp();
  auto ret = skiplist->Set(user_key, value, new_ts);

  if (ret.s == Status::Ok) {
    if (ret.existing_record &&
        ret.existing_record->entry.meta.type == SortedElem) {
      ul.unlock();
      delayFree(OldDataRecord{ret.existing_record, new_ts});
    }
  }
  return ret.s;
}

Status KVEngine::restoreSortedHeader(DLRecord* header) {
  return sorted_rebuilder_->AddHeader(header);
}

Status KVEngine::restoreSortedElem(DLRecord* elem) {
  return sorted_rebuilder_->AddElement(elem);
}
}  // namespace KVDK_NAMESPACE