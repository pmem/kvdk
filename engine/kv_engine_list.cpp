#include "kv_engine.hpp"
#include "utils/sync_point.hpp"

namespace KVDK_NAMESPACE {
Status KVEngine::ListCreate(StringView key) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto guard = hash_table_->AcquireLock(key);
  auto result = lookupKey<true>(key, RecordType::ListRecord);
  if (result.s == Status::Ok) {
    return Status::Existed;
  }
  if (result.s != Status::NotFound && result.s != Status::Outdated) {
    return result.s;
  }
  SpaceEntry space = pmem_allocator_->Allocate(sizeof(DLRecord) + key.size() +
                                               sizeof(CollectionIDType));
  if (space.size == 0) {
    return Status::PmemOverflow;
  }
  List* list = new List{};
  list->Init(pmem_allocator_.get(), space,
             version_controller_.GetCurrentTimestamp(), key,
             list_id_.fetch_add(1), nullptr);
  List* old_list = nullptr;
  if (result.s == Status::Outdated) {
    old_list = result.entry.GetIndex().list;
    list->AddOldVersion(old_list);
  }
  {
    std::lock_guard<std::mutex> guard2{lists_mu_};
    if (old_list != nullptr) {
      lists_.erase(old_list);
    }
    lists_.emplace(list);
  }
  insertKeyOrElem(result, RecordType::ListRecord, list);
  return Status::Ok;
}

Status KVEngine::ListDestroy(StringView key) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();
  auto guard = hash_table_->AcquireLock(key);
  std::unique_lock<std::recursive_mutex> guard2;
  List* list;
  Status s = listFind(key, &list);
  if (s != Status::Ok) {
    return s;
  }
  return listExpire(list, 0);
}

Status KVEngine::ListLength(StringView key, size_t* sz) {
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

Status KVEngine::ListPushFront(StringView key, StringView elem) {
  if (!CheckKeySize(key) || !CheckValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  /// TODO: (Ziyan) use gargage collection mechanism from version controller
  /// to perform these operations lockless.
  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(key, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

  SpaceEntry space = pmem_allocator_->Allocate(
      sizeof(DLRecord) + sizeof(CollectionIDType) + elem.size());
  if (space.size == 0) {
    return Status::PmemOverflow;
  }

  list->PushFront(space, version_controller_.GetCurrentTimestamp(), "", elem);
  return Status::Ok;
}

Status KVEngine::ListPushBack(StringView key, StringView elem) {
  if (!CheckKeySize(key) || !CheckValueSize(elem)) {
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

  SpaceEntry space = pmem_allocator_->Allocate(
      sizeof(DLRecord) + sizeof(CollectionIDType) + elem.size());
  if (space.size == 0) {
    return Status::PmemOverflow;
  }

  list->PushBack(space, version_controller_.GetCurrentTimestamp(), "", elem);
  return Status::Ok;
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
  /// TODO: NotFound does not properly describe the situation
  if (list->Size() == 0) {
    return Status::NotFound;
  }

  StringView sw = list->Front()->Value();
  elem->assign(sw.data(), sw.size());
  list->PopFront([&](DLRecord* rec) { delayFree(rec); });
  return Status::Ok;
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
  if (list->Size() == 0) {
    return Status::NotFound;
  }

  StringView sw = list->Back()->Value();
  elem->assign(sw.data(), sw.size());
  list->PopBack([&](DLRecord* rec) { delayFree(rec); });
  return Status::Ok;
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

Status KVEngine::ListBatchPushBack(StringView key,
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
  return listBatchPushImpl(key, -1, elems);
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

  if (src_list->Size() == 0) {
    return Status::NotFound;
  }

  List::Iterator iter = (src_pos == 0) ? src_list->Front() : src_list->Back();
  StringView sw = iter->Value();
  elem->assign(sw.data(), sw.size());
  if (src == dst && src_pos == dst_pos) {
    return Status::Ok;
  }

  auto space = pmem_allocator_->Allocate(DLRecord::RecordSize("", sw) +
                                         sizeof(CollectionIDType));
  if (space.size == 0) {
    return Status::PmemOverflow;
  }

  auto bw_token = version_controller_.GetBatchWriteToken();
  BatchWriteLog log;
  log.SetTimestamp(bw_token.Timestamp());
  log.ListDelete(iter.Offset());
  log.ListEmplace(space.offset);

  auto& tc = engine_thread_cache_[access_thread.id];
  log.EncodeTo(tc.batch_log);
  BatchWriteLog::MarkProcessing(tc.batch_log);

  if (src_pos == 0) {
    src_list->PopFront([&](DLRecord*) { return; });
  } else {
    src_list->PopBack([&](DLRecord*) { return; });
  }
  TEST_CRASH_POINT("KVEngine::ListMove", "");
  if (dst_pos == 0) {
    dst_list->PushFront(space, bw_token.Timestamp(), "", sw);
  } else {
    dst_list->PushBack(space, bw_token.Timestamp(), "", sw);
  }

  BatchWriteLog::MarkCommitted(tc.batch_log);
  delayFree(iter.Address());
  return Status::Ok;
}

Status KVEngine::ListInsertBefore(std::unique_ptr<ListIterator> const& pos,
                                  StringView elem) {
  if (!CheckValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  ListIteratorImpl* iter = dynamic_cast<ListIteratorImpl*>(pos.get());
  kvdk_assert(iter != nullptr, "Invalid iterator!");

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(iter->Owner()->Name(), &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
  kvdk_assert(list == iter->Owner(), "Iterator outdated!");

  SpaceEntry space = pmem_allocator_->Allocate(
      sizeof(DLRecord) + sizeof(CollectionIDType) + elem.size());
  if (space.size == 0) {
    return Status::PmemOverflow;
  }

  iter->Rep() = list->EmplaceBefore(
      space, iter->Rep(), version_controller_.GetCurrentTimestamp(), "", elem);
  return Status::Ok;
}

Status KVEngine::ListInsertAfter(std::unique_ptr<ListIterator> const& pos,
                                 StringView elem) {
  if (!CheckValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  ListIteratorImpl* iter = dynamic_cast<ListIteratorImpl*>(pos.get());
  kvdk_assert(iter != nullptr, "Invalid iterator!");

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(iter->Owner()->Name(), &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
  kvdk_assert(list == iter->Owner(), "Iterator outdated!");

  SpaceEntry space = pmem_allocator_->Allocate(
      sizeof(DLRecord) + sizeof(CollectionIDType) + elem.size());
  if (space.size == 0) {
    return Status::PmemOverflow;
  }

  iter->Rep() = list->EmplaceAfter(
      space, iter->Rep(), version_controller_.GetCurrentTimestamp(), "", elem);
  return Status::Ok;
}

Status KVEngine::ListErase(std::unique_ptr<ListIterator> const& pos) {
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  ListIteratorImpl* iter = dynamic_cast<ListIteratorImpl*>(pos.get());
  kvdk_assert(iter != nullptr, "Invalid iterator!");

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(iter->Owner()->Name(), &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
  kvdk_assert(list == iter->Owner(), "Iterator outdated!");
  kvdk_assert(iter->Valid(), "Trying to erase invalid iterator!");

  iter->Rep() =
      list->Erase(iter->Rep(), [&](DLRecord* rec) { delayFree(rec); });
  return Status::Ok;
}

// Replace the element at pos
Status KVEngine::ListReplace(std::unique_ptr<ListIterator> const& pos,
                             StringView elem) {
  if (!CheckValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  ListIteratorImpl* iter = dynamic_cast<ListIteratorImpl*>(pos.get());
  kvdk_assert(iter != nullptr, "Invalid iterator!");

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(iter->Owner()->Name(), &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
  kvdk_assert(list == iter->Owner(), "Iterator outdated!");

  SpaceEntry space = pmem_allocator_->Allocate(
      sizeof(DLRecord) + sizeof(CollectionIDType) + elem.size());
  if (space.size == 0) {
    return Status::PmemOverflow;
  }
  iter->Rep() = list->Replace(space, iter->Rep(),
                              version_controller_.GetCurrentTimestamp(), "",
                              elem, [&](DLRecord* rec) { delayFree(rec); });
  return Status::Ok;
}

std::unique_ptr<ListIterator> KVEngine::ListCreateIterator(StringView key,
                                                           Status* status) {
  if (!CheckKeySize(key)) {
    return nullptr;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return nullptr;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(key, &list);
  if (status != nullptr) {
    *status = s;
  }
  if (s != Status::Ok) {
    return nullptr;
  }
  auto guard = list->AcquireLock();
  return std::unique_ptr<ListIteratorImpl>{new ListIteratorImpl{list}};
}

Status KVEngine::listRestoreElem(DLRecord* pmp_record) {
  list_builder_->AddListElem(pmp_record);
  return Status::Ok;
}

Status KVEngine::listRestoreList(DLRecord* pmp_record) {
  list_builder_->AddListRecord(pmp_record);
  return Status::Ok;
}

Status KVEngine::listRegisterRecovered() {
  CollectionIDType max_id = 0;
  for (List* list : lists_) {
    auto guard = hash_table_->AcquireLock(list->Name());
    Status s = registerCollection(list);
    if (s != Status::Ok) {
      return s;
    }
    max_id = std::max(max_id, list->ID());
  }
  CollectionIDType old = list_id_.load();
  while (max_id >= old && !list_id_.compare_exchange_strong(old, max_id + 1)) {
  }
  return Status::Ok;
}

Status KVEngine::listExpire(List* list, ExpireTimeType t) {
  std::lock_guard<std::mutex> guard(lists_mu_);
  lists_.erase(list);
  Status s = list->SetExpireTime(t);
  lists_.insert(list);
  return s;
}

Status KVEngine::listDestroy(List* list) {
  std::vector<SpaceEntry> entries;
  auto PushPending = [&](DLRecord* rec) {
    SpaceEntry space{pmem_allocator_->addr2offset_checked(rec),
                     rec->entry.header.record_size};
    entries.push_back(space);
  };
  if (list->OldVersion() != nullptr) {
    auto old_list = list->OldVersion();
    list->RemoveOldVersion();
    listDestroy(old_list);
  }
  while (list->Size() > 0) {
    list->PopFront(PushPending);
  }
  list->Destroy(PushPending);
  pmem_allocator_->BatchFree(entries);
  delete list;
  return Status::Ok;
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
      list->PushFront(spaces[i], bw_token.Timestamp(), "", elems[i]);
      TEST_CRASH_POINT("KVEngine::listBatchPushImpl", "");
    }
  } else {
    kvdk_assert(pos == -1, "");
    for (size_t i = 0; i < elems.size(); i++) {
      list->PushBack(spaces[i], bw_token.Timestamp(), "", elems[i]);
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
  if (pos == 0) {
    for (auto iter = list->Front(); iter != list->Tail(); ++iter) {
      StringView elem = iter->Value();
      elems->emplace_back(elem.data(), elem.size());
      log.ListDelete(iter.Offset());
      --nn;
      if (nn == 0) {
        break;
      }
    }
  } else {
    kvdk_assert(pos == -1, "");
    for (auto iter = list->Back(); iter != list->Head(); --iter) {
      StringView elem = iter->Value();
      elems->emplace_back(elem.data(), elem.size());
      log.ListDelete(iter.Offset());
      --nn;
      if (nn == 0) {
        break;
      }
    }
  }

  std::vector<DLRecord*> old_records;
  auto& tc = engine_thread_cache_[access_thread.id];
  log.EncodeTo(tc.batch_log);
  BatchWriteLog::MarkProcessing(tc.batch_log);
  if (pos == 0) {
    for (size_t i = 0; i < n && list->Size() > 0; i++) {
      list->PopFront([&](DLRecord* rec) { old_records.push_back(rec); });
      TEST_CRASH_POINT("KVEngine::listBatchPopImpl", "");
    }
  } else {
    for (size_t i = 0; i < n && list->Size() > 0; i++) {
      list->PopBack([&](DLRecord* rec) { old_records.push_back(rec); });
      TEST_CRASH_POINT("KVEngine::listBatchPopImpl", "");
    }
  }
  BatchWriteLog::MarkCommitted(tc.batch_log);
  for (auto rec : old_records) {
    delayFree(rec);
  }
  return Status::Ok;
}

Status KVEngine::listRollback(BatchWriteLog::ListLogEntry const& log) {
  DLRecord* rec =
      static_cast<DLRecord*>(pmem_allocator_->offset2addr_checked(log.offset));
  switch (log.op) {
    case BatchWriteLog::Op::Delete: {
      list_builder_->RollbackDeletion(rec);
      break;
    }
    case BatchWriteLog::Op::Put: {
      list_builder_->RollbackEmplacement(rec);
      break;
    }
    default: {
      kvdk_assert(false, "Invalid operation int log");
      return Status::Abort;
    }
  }
  return Status::Ok;
}

}  // namespace KVDK_NAMESPACE
