#include "kv_engine.hpp"
#include "utils/sync_point.hpp"

namespace KVDK_NAMESPACE {
Status KVEngine::ListCreate(StringView list_name) {
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  if (!CheckKeySize(list_name)) {
    return Status::InvalidDataSize;
  }

  std::shared_ptr<List> list = nullptr;
  return buildList(list_name, list);
}

Status KVEngine::buildList(const StringView& list_name,
                           std::shared_ptr<List>& list) {
  auto ul = hash_table_->AcquireLock(list_name);
  auto holder = version_controller_.GetLocalSnapshotHolder();
  TimeStampType new_ts = holder.Timestamp();
  auto lookup_result = lookupKey<true>(list_name, RecordType::ListRecord);
  if (lookup_result.s == Status::NotFound ||
      lookup_result.s == Status::Outdated) {
    DLRecord* existing_header =
        lookup_result.s == Outdated
            ? lookup_result.entry.GetIndex().hlist->HeaderRecord()
            : nullptr;
    CollectionIDType id = collection_id_.fetch_add(1);
    std::string value_str = List::EncodeID(id);
    SpaceEntry space =
        pmem_allocator_->Allocate(DLRecord::RecordSize(list_name, value_str));
    if (space.size == 0) {
      return Status::PmemOverflow;
    }
    // dl list is circular, so the next and prev pointers of
    // header point to itself
    DLRecord* pmem_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr_checked(space.offset), space.size, new_ts,
        RecordType::ListRecord, RecordStatus::Normal,
        pmem_allocator_->addr2offset(existing_header), space.offset,
        space.offset, list_name, value_str);
    list = std::make_shared<List>(pmem_record, list_name, id,
                                  pmem_allocator_.get(), dllist_locks_.get());
    kvdk_assert(list != nullptr, "");
    addListToMap(list);
    insertKeyOrElem(lookup_result, RecordType::ListRecord, RecordStatus::Normal,
                    list.get());
    return Status::Ok;
  } else {
    return lookup_result.s;
  }
}

Status KVEngine::ListDestroy(StringView collection) {
  if (!CheckKeySize(collection)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto ul = hash_table_->AcquireLock(collection);
  auto snapshot_holder = version_controller_.GetLocalSnapshotHolder();
  auto new_ts = snapshot_holder.Timestamp();
  List* list;
  Status s = listFind(collection, &list);
  if (s == Status::Ok) {
    DLRecord* header = list->HeaderRecord();
    kvdk_assert(header->GetRecordType() == RecordType::ListRecord, "");
    StringView value = header->Value();
    auto request_size = DLRecord::RecordSize(collection, value);
    SpaceEntry space = pmem_allocator_->Allocate(request_size);
    if (space.size == 0) {
      return Status::PmemOverflow;
    }
    DLRecord* pmem_record = DLRecord::PersistDLRecord(
        pmem_allocator_->offset2addr_checked(space.offset), space.size, new_ts,
        RecordType::ListRecord, RecordStatus::Outdated,
        pmem_allocator_->addr2offset_checked(header), header->prev,
        header->next, collection, value);
    bool success = list->Replace(header, pmem_record);
    kvdk_assert(success, "existing header should be linked on its list");
    hash_table_->Insert(collection, RecordType::ListRecord,
                        RecordStatus::Outdated, list, PointerType::List);
  }
  return s;
}

Status KVEngine::ListSize(StringView key, size_t* sz) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();

  List* list;
  Status s = listFind(key, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
  *sz = list->Size();
  return Status::Ok;
}

Status KVEngine::ListPushFront(StringView collection, StringView elem) {
  if (!CheckKeySize(collection) || !CheckValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(collection, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

  return list->PushFront(elem, version_controller_.GetCurrentTimestamp()).s;
}

Status KVEngine::ListPushBack(StringView list_name, StringView elem) {
  if (!CheckKeySize(list_name) || !CheckValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(list_name, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

  return list->PushBack(elem, version_controller_.GetCurrentTimestamp()).s;
}

Status KVEngine::ListPopFront(StringView key, std::string* elem) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(key, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

  auto ret = list->PopFront(version_controller_.GetCurrentTimestamp());
  if (ret.existing_record == nullptr) {
    /// TODO: NotFound does not properly describe the situation
    return Status::NotFound;
  }

  if (ret.s == Status::Ok) {
    elem->assign(ret.existing_record->Value().data(),
                 ret.existing_record->Value().size());
  }
  return ret.s;
}

Status KVEngine::ListPopBack(StringView key, std::string* elem) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(key, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

  auto ret = list->PopBack(version_controller_.GetCurrentTimestamp());
  if (ret.existing_record == nullptr) {
    /// TODO: NotFound does not properly describe the situation
    return Status::NotFound;
  }

  if (ret.s == Status::Ok) {
    elem->assign(ret.existing_record->Value().data(),
                 ret.existing_record->Value().size());
  }
  return ret.s;
}

Status KVEngine::ListBatchPushFront(StringView key,
                                    std::vector<std::string> const& elems) {
  return ListBatchPushFront(
      key, std::vector<StringView>{elems.begin(), elems.end()});
}

Status KVEngine::ListBatchPushFront(StringView key,
                                    std::vector<StringView> const& elems) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  if (elems.size() > BatchWriteLog::Capacity()) {
    return Status::InvalidBatchSize;
  }
  for (auto const& elem : elems) {
    if (!CheckValueSize(elem)) {
      return Status::InvalidDataSize;
    }
  }
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }
  s = maybeInitBatchLogFile();
  if (s != Status::Ok) {
    return s;
  }
  return listBatchPushImpl(key, 0, elems);
}

Status KVEngine::ListBatchPushBack(StringView key,
                                   std::vector<std::string> const& elems) {
  return ListBatchPushBack(key,
                           std::vector<StringView>{elems.begin(), elems.end()});
}

Status KVEngine::ListBatchPushBack(StringView list_name,
                                   std::vector<StringView> const& elems) {
  if (!CheckKeySize(list_name)) {
    return Status::InvalidDataSize;
  }
  if (elems.size() > BatchWriteLog::Capacity()) {
    return Status::InvalidBatchSize;
  }
  for (auto const& elem : elems) {
    if (!CheckValueSize(elem)) {
      return Status::InvalidDataSize;
    }
  }
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }
  s = maybeInitBatchLogFile();
  if (s != Status::Ok) {
    return s;
  }
  return listBatchPushImpl(list_name, -1, elems);
}

Status KVEngine::ListBatchPopFront(StringView key, size_t n,
                                   std::vector<std::string>* elems) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }
  s = maybeInitBatchLogFile();
  if (s != Status::Ok) {
    return s;
  }
  return listBatchPopImpl(key, 0, n, elems);
}

Status KVEngine::ListBatchPopBack(StringView key, size_t n,
                                  std::vector<std::string>* elems) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }
  s = maybeInitBatchLogFile();
  if (s != Status::Ok) {
    return s;
  }
  return listBatchPopImpl(key, -1, n, elems);
}

Status KVEngine::ListMove(StringView src, int src_pos, StringView dst,
                          int dst_pos, std::string* elem) {
  if ((src_pos != 0 && src_pos != -1) || (dst_pos != 0 && dst_pos != -1)) {
    return Status::InvalidArgument;
  }
  if (!CheckKeySize(src) || !CheckKeySize(dst)) {
    return Status::InvalidDataSize;
  }
  Status s = MaybeInitAccessThread();
  if (s != Status::Ok) {
    return s;
  }

  s = maybeInitBatchLogFile();
  if (s != Status::Ok) {
    return s;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* src_list;
  List* dst_list;
  /// TODO: we must guarantee a consistent view for the List.
  /// The same holds for other collections.
  /// No collection should be expired, created or deleted during BatchWrite.
  s = listFind(src, &src_list);
  if (s != Status::Ok) {
    return s;
  }
  if (src != dst) {
    s = listFind(dst, &dst_list);
    if (s != Status::Ok) {
      return s;
    }
  } else {
    dst_list = src_list;
  }

  std::unique_lock<std::recursive_mutex> guard1;
  std::unique_lock<std::recursive_mutex> guard2;
  if (src_list < dst_list) {
    guard1 = src_list->AcquireLock();
    guard2 = dst_list->AcquireLock();
  } else if (src_list > dst_list) {
    guard1 = dst_list->AcquireLock();
    guard2 = src_list->AcquireLock();
  } else {
    kvdk_assert(src == dst, "");
    guard1 = src_list->AcquireLock();
  }

  if (src == dst && src_pos == dst_pos) {
    s = src_pos == 0 ? src_list->Front(elem) : src_list->Back(elem);
    return s;
  }

  auto bw_token = version_controller_.GetBatchWriteToken();
  BatchWriteLog log;
  log.SetTimestamp(bw_token.Timestamp());

  // TODO first log
  List::WriteResult ret;
  if (src_pos == 0) {
    ret = src_list->PopFront(bw_token.Timestamp());
  } else {
    ret = src_list->PopBack(bw_token.Timestamp());
  }

  if (ret.s == Status::Ok) {
    elem->assign(ret.existing_record->Value().data(),
                 ret.existing_record->Value().size());
  } else {
    return ret.s;
  }

  auto space = pmem_allocator_->Allocate(DLRecord::RecordSize("", *elem) +
                                         sizeof(CollectionIDType));
  if (space.size == 0) {
    return Status::PmemOverflow;
  }

  log.ListDelete(pmem_allocator_->addr2offset_checked(ret.existing_record));
  log.ListEmplace(space.offset);

  auto& tc = engine_thread_cache_[access_thread.id];
  log.EncodeTo(tc.batch_log);
  BatchWriteLog::MarkProcessing(tc.batch_log);

  TEST_CRASH_POINT("KVEngine::ListMove", "");
  if (dst_pos == 0) {
    dst_list->PushFront(*elem, bw_token.Timestamp(), space);
  } else {
    dst_list->PushBack(*elem, bw_token.Timestamp(), space);
  }

  BatchWriteLog::MarkCommitted(tc.batch_log);
  // TODO free existing record
  return Status::Ok;
}

Status KVEngine::ListInsertBefore(StringView list_name, StringView elem,
                                  StringView pos) {
  if (!CheckValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(list_name, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

  return list
      ->InsertBefore(elem, pos, version_controller_.GetCurrentTimestamp())
      .s;
}

Status KVEngine::ListInsertAfter(StringView collection, StringView key,
                                 StringView pos) {
  if (!CheckValueSize(key)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(collection, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

  return list->InsertAfter(key, pos, version_controller_.GetCurrentTimestamp())
      .s;
}

Status KVEngine::ListErase(StringView list_name, uint64_t pos) {
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(list_name, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
  return list->Erase(pos).s;
}

// Replace the element at pos
Status KVEngine::ListReplace(StringView collection, uint64_t pos,
                             StringView elem) {
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(collection, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
  return list->Replace(pos, elem, version_controller_.GetCurrentTimestamp()).s;
}

std::unique_ptr<ListIterator> KVEngine::ListCreateIterator(
    StringView collection, Snapshot* snapshot, Status* status) {
  Status s{Status::Ok};
  std::unique_ptr<ListIterator> ret(nullptr);
  if (!CheckKeySize(collection)) {
    s = Status::InvalidDataSize;
  }

  if (s == Status::Ok) {
    bool create_snapshot = snapshot == nullptr;
    if (create_snapshot) {
      snapshot = GetSnapshot(false);
    }
    List* list;
    Status s = listFind(collection, &list);
    if (s == Status::Ok) {
      ret = std::unique_ptr<ListIteratorImpl>(new ListIteratorImpl(
          this, list, static_cast<SnapshotImpl*>(snapshot), create_snapshot));
    } else if (create_snapshot) {
      ReleaseSnapshot(snapshot);
    }
  }

  if (status) {
    *status = s;
  }
  return ret;
}

Status KVEngine::listRestoreElem(DLRecord* pmp_record) {
  return list_rebuilder_->AddElem(pmp_record);
}

Status KVEngine::listRestoreList(DLRecord* pmp_record) {
  return list_rebuilder_->AddHeader(pmp_record);
}

Status KVEngine::listFind(StringView key, List** list) {
  auto result = lookupKey<false>(key, RecordType::ListRecord);
  if (result.s == Status::Outdated) {
    return Status::NotFound;
  }
  if (result.s != Status::Ok) {
    return result.s;
  }
  (*list) = result.entry.GetIndex().list;
  return Status::Ok;
}

Status KVEngine::listBatchPushImpl(StringView key, int pos,
                                   std::vector<StringView> const& elems) {
  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(key, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

  auto bw_token = version_controller_.GetBatchWriteToken();
  BatchWriteLog log;
  log.SetTimestamp(bw_token.Timestamp());

  std::vector<SpaceEntry> spaces;
  auto ReleaseResources = [&]() {
// Don't Free() if we simulate a crash.
#ifndef KVDK_ENABLE_CRASHPOINT
    for (auto space : spaces) {
      pmem_allocator_->Free(space);
    }
#endif
  };
  defer(ReleaseResources());

  for (auto const& elem : elems) {
    SpaceEntry space = pmem_allocator_->Allocate(
        sizeof(DLRecord) + sizeof(CollectionIDType) + elem.size());
    if (space.size == 0) {
      return Status::PmemOverflow;
    }
    spaces.push_back(space);
    log.ListEmplace(space.offset);
  }

  auto& tc = engine_thread_cache_[access_thread.id];
  log.EncodeTo(tc.batch_log);
  BatchWriteLog::MarkProcessing(tc.batch_log);
  if (pos == 0) {
    for (size_t i = 0; i < elems.size(); i++) {
      list->PushFront(elems[i], bw_token.Timestamp(), spaces[i]);
      TEST_CRASH_POINT("KVEngine::listBatchPushImpl", "");
    }
  } else {
    kvdk_assert(pos == -1, "");
    for (size_t i = 0; i < elems.size(); i++) {
      list->PushBack(elems[i], bw_token.Timestamp(), spaces[i]);
      TEST_CRASH_POINT("KVEngine::listBatchPushImpl", "");
    }
  }
  BatchWriteLog::MarkCommitted(tc.batch_log);
  spaces.clear();
  return Status::Ok;
}

Status KVEngine::listBatchPopImpl(StringView key, int pos, size_t n,
                                  std::vector<std::string>* elems) {
  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(key, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

  auto bw_token = version_controller_.GetBatchWriteToken();
  BatchWriteLog log;
  log.SetTimestamp(bw_token.Timestamp());

  elems->clear();
  size_t nn = n;
  while (nn > 0) {
    auto ret = pos == 0 ? list->PopFront(bw_token.Timestamp())
                        : list->PopBack(bw_token.Timestamp());
    if (ret.s != Status::Ok) {
      break;
    }
    StringView elem = ret.existing_record->Value();
    elems->emplace_back(elem.data(), elem.size());
    log.ListDelete(pmem_allocator_->addr2offset_checked(ret.write_record));
    --nn;
  }

  auto& tc = engine_thread_cache_[access_thread.id];
  log.EncodeTo(tc.batch_log);
  BatchWriteLog::MarkProcessing(tc.batch_log);

  BatchWriteLog::MarkCommitted(tc.batch_log);
  return Status::Ok;
}

Status KVEngine::listRollback(BatchWriteLog::ListLogEntry const& log) {
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
