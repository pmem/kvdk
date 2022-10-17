/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include "kv_engine.hpp"
#include "list_collection/iterator.hpp"
#include "utils/sync_point.hpp"

namespace KVDK_NAMESPACE {
Status KVEngine::ListCreate(StringView list_name) {
  auto thread_holder = AcquireAccessThread();

  if (!checkKeySize(list_name)) {
    return Status::InvalidDataSize;
  }

  std::shared_ptr<List> list = nullptr;
  return buildList(list_name, list);
}

Status KVEngine::buildList(const StringView& list_name,
                           std::shared_ptr<List>& list) {
  auto ul = hash_table_->AcquireLock(list_name);
  auto holder = version_controller_.GetLocalSnapshotHolder();
  TimestampType new_ts = holder.Timestamp();
  auto lookup_result = lookupKey<true>(list_name, RecordType::ListHeader);
  if (lookup_result.s == Status::NotFound ||
      lookup_result.s == Status::Outdated) {
    DLRecord* existing_header =
        lookup_result.s == Outdated
            ? lookup_result.entry.GetIndex().hlist->HeaderRecord()
            : nullptr;
    CollectionIDType id = collection_id_.fetch_add(1);
    std::string value_str = List::EncodeID(id);
    SpaceEntry space =
        kv_allocator_->Allocate(DLRecord::RecordSize(list_name, value_str));
    if (space.size == 0) {
      return Status::MemoryOverflow;
    }
    // dl list is circular, so the next and prev pointers of
    // header point to itself
    DLRecord* data_record = DLRecord::PersistDLRecord(
        kv_allocator_->offset2addr_checked(space.offset), space.size, new_ts,
        RecordType::ListHeader, RecordStatus::Normal,
        kv_allocator_->addr2offset(existing_header), space.offset, space.offset,
        list_name, value_str);
    list = std::make_shared<List>(data_record, list_name, id,
                                  kv_allocator_.get(), dllist_locks_.get());
    kvdk_assert(list != nullptr, "");
    addListToMap(list);
    insertKeyOrElem(lookup_result, RecordType::ListHeader, RecordStatus::Normal,
                    list.get());
    return Status::Ok;
  } else {
    return lookup_result.s == Status::Ok ? Status::Existed : lookup_result.s;
  }
}

Status KVEngine::ListDestroy(StringView collection) {
  if (!checkKeySize(collection)) {
    return Status::InvalidDataSize;
  }
  auto thread_holder = AcquireAccessThread();

  auto ul = hash_table_->AcquireLock(collection);
  auto snapshot_holder = version_controller_.GetLocalSnapshotHolder();
  auto new_ts = snapshot_holder.Timestamp();
  List* list;
  Status s = listFind(collection, &list);
  if (s == Status::Ok) {
    DLRecord* header = list->HeaderRecord();
    kvdk_assert(header->GetRecordType() == RecordType::ListHeader, "");
    StringView value = header->Value();
    auto request_size = DLRecord::RecordSize(collection, value);
    SpaceEntry space = kv_allocator_->Allocate(request_size);
    if (space.size == 0) {
      return Status::MemoryOverflow;
    }
    DLRecord* data_record = DLRecord::PersistDLRecord(
        kv_allocator_->offset2addr_checked(space.offset), space.size, new_ts,
        RecordType::ListHeader, RecordStatus::Outdated,
        kv_allocator_->addr2offset_checked(header), header->prev, header->next,
        collection, value);
    bool success = list->Replace(header, data_record);
    kvdk_assert(success, "existing header should be linked on its list");
    hash_table_->Insert(collection, RecordType::ListHeader,
                        RecordStatus::Outdated, list, PointerType::List);
    {
      std::unique_lock<std::mutex> list_lock(lists_mu_);
      expirable_lists_.emplace(list);
    }
  }
  return s;
}

Status KVEngine::ListSize(StringView list_name, size_t* sz) {
  if (!checkKeySize(list_name)) {
    return Status::InvalidDataSize;
  }
  auto thread_holder = AcquireAccessThread();

  auto token = version_controller_.GetLocalSnapshotHolder();

  List* list;
  Status s = listFind(list_name, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
  *sz = list->Size();
  return Status::Ok;
}

Status KVEngine::ListPushFront(StringView collection, StringView elem) {
  if (!checkKeySize(collection) || !checkValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  auto thread_holder = AcquireAccessThread();

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
  if (!checkKeySize(list_name) || !checkValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  auto thread_holder = AcquireAccessThread();

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(list_name, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

  return list->PushBack(elem, version_controller_.GetCurrentTimestamp()).s;
}

Status KVEngine::ListPopFront(StringView list_name, std::string* elem) {
  if (!checkKeySize(list_name)) {
    return Status::InvalidDataSize;
  }
  auto thread_holder = AcquireAccessThread();

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(list_name, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

  auto ret = list->PopFront(version_controller_.GetCurrentTimestamp());

  if (ret.s == Status::Ok) {
    kvdk_assert(ret.existing_record && ret.write_record, "");
    if (elem) {
      elem->assign(ret.existing_record->Value().data(),
                   ret.existing_record->Value().size());
    }
    if (list->TryCleaningLock()) {
      removeAndCacheOutdatedVersion(ret.write_record);
      list->ReleaseCleaningLock();
    }
  }
  tryCleanCachedOutdatedRecord();
  return ret.s;
}

Status KVEngine::ListPopBack(StringView list_name, std::string* elem) {
  if (!checkKeySize(list_name)) {
    return Status::InvalidDataSize;
  }
  auto thread_holder = AcquireAccessThread();

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(list_name, &list);
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
    kvdk_assert(ret.existing_record && ret.write_record, "");
    if (elem) {
      elem->assign(ret.existing_record->Value().data(),
                   ret.existing_record->Value().size());
    }
    kvdk_assert(ret.existing_record && ret.write_record, "");

    if (list->TryCleaningLock()) {
      removeAndCacheOutdatedVersion(ret.write_record);
      list->ReleaseCleaningLock();
    }
  }
  tryCleanCachedOutdatedRecord();
  return ret.s;
}

Status KVEngine::ListBatchPushFront(StringView list_name,
                                    std::vector<std::string> const& elems) {
  return ListBatchPushFront(
      list_name, std::vector<StringView>{elems.begin(), elems.end()});
}

Status KVEngine::ListBatchPushFront(StringView list_name,
                                    std::vector<StringView> const& elems) {
  if (!checkKeySize(list_name)) {
    return Status::InvalidDataSize;
  }
  if (elems.size() > BatchWriteLog::Capacity()) {
    return Status::InvalidBatchSize;
  }
  for (auto const& elem : elems) {
    if (!checkValueSize(elem)) {
      return Status::InvalidDataSize;
    }
  }
  auto thread_holder = AcquireAccessThread();

  Status s = maybeInitBatchLogFile();
  if (s != Status::Ok) {
    return s;
  }
  return listBatchPushImpl(list_name, ListPos::Front, elems);
}

Status KVEngine::ListBatchPushBack(StringView list_name,
                                   std::vector<std::string> const& elems) {
  return ListBatchPushBack(list_name,
                           std::vector<StringView>{elems.begin(), elems.end()});
}

Status KVEngine::ListBatchPushBack(StringView list_name,
                                   std::vector<StringView> const& elems) {
  if (!checkKeySize(list_name)) {
    return Status::InvalidDataSize;
  }
  if (elems.size() > BatchWriteLog::Capacity()) {
    return Status::InvalidBatchSize;
  }
  for (auto const& elem : elems) {
    if (!checkValueSize(elem)) {
      return Status::InvalidDataSize;
    }
  }
  auto thread_holder = AcquireAccessThread();

  Status s = maybeInitBatchLogFile();
  if (s != Status::Ok) {
    return s;
  }
  return listBatchPushImpl(list_name, ListPos::Back, elems);
}

Status KVEngine::ListBatchPopFront(StringView list_name, size_t n,
                                   std::vector<std::string>* elems) {
  if (!checkKeySize(list_name)) {
    return Status::InvalidDataSize;
  }
  auto thread_holder = AcquireAccessThread();

  Status s = maybeInitBatchLogFile();
  if (s != Status::Ok) {
    return s;
  }
  return listBatchPopImpl(list_name, ListPos::Front, n, elems);
}

Status KVEngine::ListBatchPopBack(StringView list_name, size_t n,
                                  std::vector<std::string>* elems) {
  if (!checkKeySize(list_name)) {
    return Status::InvalidDataSize;
  }
  auto thread_holder = AcquireAccessThread();

  Status s = maybeInitBatchLogFile();
  if (s != Status::Ok) {
    return s;
  }

  return listBatchPopImpl(list_name, ListPos::Back, n, elems);
}

Status KVEngine::ListMove(StringView src, ListPos src_pos, StringView dst,
                          ListPos dst_pos, std::string* elem) {
  if (!checkKeySize(src) || !checkKeySize(dst)) {
    return Status::InvalidDataSize;
  }
  auto thread_holder = AcquireAccessThread();

  Status s = maybeInitBatchLogFile();
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
    s = src_pos == ListPos::Front ? src_list->Front(elem)
                                  : src_list->Back(elem);
    return s;
  }

  auto bw_token = version_controller_.GetBatchWriteToken();
  BatchWriteLog log;
  log.SetTimestamp(bw_token.Timestamp());
  std::vector<std::string> elems;

  auto pop_args =
      src_list->PreparePopN((ListPos)src_pos, 1, bw_token.Timestamp(), &elems);
  if (pop_args.s != Status::Ok) {
    return pop_args.s;
  }
  kvdk_assert(elems.size() == 1, "");
  elem->swap(elems[0]);
  std::vector<StringView> elems_view{StringView(elem->data(), elem->size())};
  auto push_args = dst_list->PreparePushN((ListPos)dst_pos, elems_view,
                                          bw_token.Timestamp());
  if (push_args.s != Status::Ok) {
    return push_args.s;
  }

  log.ListDelete(pop_args.spaces[0].offset);
  log.ListEmplace(push_args.spaces[0].offset);
  auto& tc = engine_thread_cache_[ThreadManager::ThreadID() %
                                  configs_.max_access_threads];
  log.EncodeTo(tc.batch_log);

  BatchWriteLog::MarkProcessing(tc.batch_log);

  s = src_list->PopN(pop_args);
  kvdk_assert(s == Status::Ok, "pop n always success");
  TEST_CRASH_POINT("KVEngine::ListMove", "");
  s = dst_list->PushN(push_args);
  kvdk_assert(s == Status::Ok, "push n always success");

  BatchWriteLog::MarkCommitted(tc.batch_log);
  return Status::Ok;
}

Status KVEngine::ListInsertAt(StringView list_name, StringView elem,
                              long index) {
  if (!checkValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  auto thread_holder = AcquireAccessThread();
  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(list_name, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
  return list->InsertAt(elem, index, version_controller_.GetCurrentTimestamp())
      .s;
}

Status KVEngine::ListInsertBefore(StringView list_name, StringView elem,
                                  StringView pos) {
  if (!checkValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  auto thread_holder = AcquireAccessThread();

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

Status KVEngine::ListInsertAfter(StringView collection, StringView elem,
                                 StringView dst) {
  if (!checkValueSize(elem)) {
    return Status::InvalidDataSize;
  }
  auto thread_holder = AcquireAccessThread();

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(collection, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

  return list->InsertAfter(elem, dst, version_controller_.GetCurrentTimestamp())
      .s;
}

Status KVEngine::ListErase(StringView list_name, long index,
                           std::string* elem) {
  auto thread_holder = AcquireAccessThread();

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(list_name, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
  auto ret = list->Erase(index, version_controller_.GetCurrentTimestamp());
  if (ret.s == Status::Ok) {
    kvdk_assert(ret.existing_record && ret.write_record, "");
    if (elem) {
      elem->assign(ret.existing_record->Value().data(),
                   ret.existing_record->Value().size());
    }
    if (list->TryCleaningLock()) {
      removeAndCacheOutdatedVersion(ret.write_record);
      list->ReleaseCleaningLock();
    }
  }
  tryCleanCachedOutdatedRecord();
  return ret.s;
}

// Replace the element at pos
Status KVEngine::ListReplace(StringView collection, long index,
                             StringView elem) {
  auto thread_holder = AcquireAccessThread();

  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(collection, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();
  return list->Update(index, elem, version_controller_.GetCurrentTimestamp()).s;
}

ListIterator* KVEngine::ListIteratorCreate(StringView collection,
                                           Snapshot* snapshot, Status* status) {
  Status s{Status::Ok};
  ListIterator* ret(nullptr);
  if (!checkKeySize(collection)) {
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
      ret = new ListIteratorImpl(list, static_cast<SnapshotImpl*>(snapshot),
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

void KVEngine::ListIteratorRelease(ListIterator* list) {
  if (list == nullptr) {
    GlobalLogger.Info("pass a nullptr in KVEngine::SortedIteratorRelease!\n");
    return;
  }
  ListIteratorImpl* iter = static_cast<ListIteratorImpl*>(list);
  if (iter->own_snapshot_) {
    ReleaseSnapshot(iter->snapshot_);
  }
  delete iter;
}

Status KVEngine::listFind(StringView list_name, List** list) {
  auto result = lookupKey<false>(list_name, RecordType::ListHeader);
  if (result.s == Status::Outdated) {
    return Status::NotFound;
  }
  if (result.s != Status::Ok) {
    return result.s;
  }
  (*list) = result.entry.GetIndex().list;
  return Status::Ok;
}

Status KVEngine::listBatchPushImpl(StringView list_name, ListPos pos,
                                   std::vector<StringView> const& elems) {
  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(list_name, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

  auto bw_token = version_controller_.GetBatchWriteToken();
  BatchWriteLog log;
  log.SetTimestamp(bw_token.Timestamp());

  auto push_n_args = list->PreparePushN(pos, elems, bw_token.Timestamp());
  if (push_n_args.s != Status::Ok) {
    return push_n_args.s;
  }

  for (auto& space : push_n_args.spaces) {
    log.ListEmplace(space.offset);
  }

  auto& tc = engine_thread_cache_[ThreadManager::ThreadID() %
                                  configs_.max_access_threads];
  log.EncodeTo(tc.batch_log);
  BatchWriteLog::MarkProcessing(tc.batch_log);

  s = list->PushN(push_n_args);
  kvdk_assert(s == Status::Ok, "PushN always success");
  BatchWriteLog::MarkCommitted(tc.batch_log);
  return s;
}

Status KVEngine::listBatchPopImpl(StringView list_name, ListPos pos, size_t n,
                                  std::vector<std::string>* elems) {
  auto token = version_controller_.GetLocalSnapshotHolder();
  List* list;
  Status s = listFind(list_name, &list);
  if (s != Status::Ok) {
    return s;
  }
  auto guard = list->AcquireLock();

  auto bw_token = version_controller_.GetBatchWriteToken();
  BatchWriteLog log;
  log.SetTimestamp(bw_token.Timestamp());

  auto pop_n_args = list->PreparePopN(pos, n, bw_token.Timestamp(), elems);
  if (pop_n_args.s != Status::Ok) {
    return pop_n_args.s;
  }

  for (auto& space : pop_n_args.spaces) {
    log.ListDelete(space.offset);
  }

  auto& tc = engine_thread_cache_[ThreadManager::ThreadID() %
                                  configs_.max_access_threads];
  log.EncodeTo(tc.batch_log);
  BatchWriteLog::MarkProcessing(tc.batch_log);

  s = list->PopN(pop_n_args);
  kvdk_assert(s == Status::Ok, "PopN always success with lock");
  BatchWriteLog::MarkCommitted(tc.batch_log);
  return s;
}

}  // namespace KVDK_NAMESPACE
