/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "kv_engine.hpp"

namespace KVDK_NAMESPACE {
Status KVEngine::HashLength(StringView key, size_t* len) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();
  HashList* hlist;
  Status s = hashListFind(key, &hlist, false);
  if (s != Status::Ok) {
    return s;
  }
  *len = hlist->Size();
  return Status::Ok;
}

Status KVEngine::HashGet(StringView key, StringView field, std::string* value) {
  auto get_func = [&](StringView const* resp, StringView*, void*) {
    if (resp != nullptr) {
      value->assign(resp->data(), resp->size());
    }
    return ModifyOperation::Noop;
  };
  return hashModifyImpl<hashModifyImplCaller::HashGet>(key, field, get_func,
                                                       nullptr);
}

Status KVEngine::HashSet(StringView key, StringView field, StringView value) {
  auto set_func = [&](StringView const*, StringView* new_val, void*) {
    *new_val = value;
    return ModifyOperation::Write;
  };

  return hashModifyImpl<hashModifyImplCaller::HashSet>(key, field, set_func,
                                                       nullptr);
}

Status KVEngine::HashDelete(StringView key, StringView field) {
  auto delete_func = [&](StringView const*, StringView*, void*) {
    return ModifyOperation::Delete;
  };

  Status s = hashModifyImpl<hashModifyImplCaller::HashDelete>(
      key, field, delete_func, nullptr);
  if (s == Status::NotFound) {
    return Status::Ok;
  }
  return s;
}

Status KVEngine::HashModify(StringView key, StringView field,
                            ModifyFunc modify_func, void* cb_args) {
  std::string buffer;
  auto modify = [&](StringView const* old_value, StringView* new_value,
                    void* args) {
    ModifyOperation op;
    if (old_value != nullptr) {
      std::string old_val{old_value->data(), old_value->size()};
      op = modify_func(&old_val, &buffer, args);
    } else {
      op = modify_func(nullptr, &buffer, args);
    }
    *new_value = buffer;
    return op;
  };

  return hashModifyImpl<hashModifyImplCaller::HashModify>(key, field, modify,
                                                          cb_args);
}

template <KVEngine::hashModifyImplCaller caller, typename ModifyFunction>
Status KVEngine::hashModifyImpl(StringView key, StringView field,
                                ModifyFunction modify_func, void* cb_args) {
  if (!CheckKeySize(key) || !CheckKeySize(field)) {
    return Status::InvalidDataSize;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }

  constexpr bool init_nx = (caller == hashModifyImplCaller::HashModify ||
                            caller == hashModifyImplCaller::HashSet);
  constexpr bool hash_get = (caller == hashModifyImplCaller::HashGet);

  // This token guarantees a valid view of the hlist and its elements.
  auto token = version_controller_.GetLocalSnapshotHolder();
  // HashDelete() and HashGet() does not need to initialize a new HashList if
  // none exists. HashModify() and HashSet() will always initialize a new
  // HashList.
  HashList* hlist;
  Status s = hashListFind(key, &hlist, init_nx);
  if (s != Status::Ok) {
    // Fail to create List for HashModify() or HashSet(),
    // or NotFound for HashDelete() or HashGet()
    return s;
  }

  std::string internal_key = hlist->InternalKey(field);
  std::unique_lock<SpinMutex> guard;
  if (!hash_get) {
    guard = hash_table_->AcquireLock(internal_key);
  }

  LookupResult result =
      lookupElem<!hash_get>(internal_key, RecordType::HashElem);
  if (!(result.s == Status::Ok || result.s == Status::NotFound)) {
    return result.s;
  }
  StringView new_value;
  StringView old_value;
  StringView* p_old_value = nullptr;
  if (result.s == Status::Ok) {
    DLRecord* old_rec = result.entry.GetIndex().dl_record;
    old_value = old_rec->Value();
    p_old_value = &old_value;
  }

  switch (modify_func(p_old_value, &new_value, cb_args)) {
    case ModifyOperation::Write: {
      kvdk_assert(caller == hashModifyImplCaller::HashModify ||
                      caller == hashModifyImplCaller::HashSet,
                  "");
      if (!CheckValueSize(new_value)) {
        return Status::InvalidDataSize;
      }
      TimeStampType ts = token.Timestamp();
      auto space = pmem_allocator_->Allocate(
          sizeof(DLRecord) + internal_key.size() + new_value.size());
      if (space.size == 0) {
        return Status::PmemOverflow;
      }
      void* addr = pmem_allocator_->offset2addr_checked(space.offset);
      if (result.s == Status::NotFound) {
        if (std::hash<StringView>{}(field) % 2 == 0) {
          hlist->PushFrontWithLock(space, ts, field, new_value);
        } else {
          hlist->PushBackWithLock(space, ts, field, new_value);
        }
      } else {
        kvdk_assert(result.s == Status::Ok, "");
        DLRecord* old_rec = result.entry.GetIndex().dl_record;
        auto pos = hlist->MakeIterator(old_rec);
        hlist->ReplaceWithLock(space, pos, ts, field, new_value,
                               [&](DLRecord* rec) { delayFree(rec, ts); });
      }
      insertKeyOrElem(result, internal_key, RecordType::HashElem, addr);
      return Status::Ok;
    }
    case ModifyOperation::Delete: {
      kvdk_assert(caller == hashModifyImplCaller::HashModify ||
                      caller == hashModifyImplCaller::HashDelete,
                  "");
      if (result.s == Status::Ok) {
        removeKeyOrElem(result);
        hlist->EraseWithLock(
            result.entry.GetIndex().dl_record,
            [&](DLRecord* rec) { delayFree(rec, token.Timestamp()); });
      }
      return result.s;
    }
    case ModifyOperation::Noop: {
      kvdk_assert(caller == hashModifyImplCaller::HashModify ||
                      caller == hashModifyImplCaller::HashGet,
                  "");
      return result.s;
    }
    case ModifyOperation::Abort: {
      kvdk_assert(caller == hashModifyImplCaller::HashModify, "");
      return Status::Abort;
    }
    default: {
      kvdk_assert(false, "Invalid Operation!");
      return Status::Abort;
    }
  }
}

std::unique_ptr<HashIterator> KVEngine::HashCreateIterator(StringView key) {
  if (!CheckKeySize(key)) {
    return nullptr;
  }
  if (MaybeInitAccessThread() != Status::Ok) {
    return nullptr;
  }

  auto token = version_controller_.GetLocalSnapshotHolder();
  HashList* hlist;
  Status s = hashListFind(key, &hlist, false);
  if (s != Status::Ok) {
    return nullptr;
  }
  return std::unique_ptr<HashIteratorImpl>{new HashIteratorImpl{
      hlist, version_controller_.GetGlobalSnapshotToken()}};
}

Status KVEngine::hashListFind(StringView key, HashList** hlist, bool init_nx) {
  // Callers should acquire the access token.

  // Lockless lookup for the collection
  {
    auto result = lookupKey<false>(key, RecordType::HashRecord);
    if (result.s != Status::Ok && result.s != Status::NotFound &&
        result.s != Status::Outdated) {
      return result.s;
    }
    if (result.s == Status::Ok) {
      (*hlist) = result.entry.GetIndex().hlist;
      return Status::Ok;
    }
    if (!init_nx) {
      // Uninitialized or Deleted
      return Status::NotFound;
    }
  }

  // Uninitialized or Deleted, initialize new one.
  // Collection is first erased from HashTable then Destroy()ed.
  {
    auto guard2 = hash_table_->AcquireLock(key);
    auto result = lookupKey<true>(key, RecordType::HashRecord);
    if (result.s != Status::Ok && result.s != Status::NotFound &&
        result.s != Status::Outdated) {
      return result.s;
    }
    if (result.s == Status::Ok) {
      (*hlist) = result.entry.GetIndex().hlist;
      return Status::Ok;
    }
    // No other thread have created one, create one here.
    auto space = pmem_allocator_->Allocate(sizeof(DLRecord) + key.size() +
                                           sizeof(CollectionIDType));
    if (space.size == 0) {
      return Status::PmemOverflow;
    }
    *hlist = new HashList{};
    (*hlist)->Init(pmem_allocator_.get(), space,
                   version_controller_.GetCurrentTimestamp(), key,
                   list_id_.fetch_add(1), hash_list_locks_.get());
    {
      std::lock_guard<std::mutex> guard2{hlists_mu_};
      hash_lists_.emplace(*hlist);
    }
    insertKeyOrElem(result, key, RecordType::HashRecord, *hlist);
    return Status::Ok;
  }
}

Status KVEngine::hashListRestoreElem(DLRecord* rec) {
  if (!hash_list_builder_->AddListElem(rec)) {
    // Broken record, don't put in HashTable.
    // Rebuilder will delete it after recovery is done.
    return Status::Ok;
  }

  auto internal_key = rec->Key();
  auto guard = hash_table_->AcquireLock(internal_key);
  LookupResult result = lookupElem<true>(internal_key, RecordType::HashElem);
  if (!(result.s == Status::Ok || result.s == Status::NotFound)) {
    return result.s;
  }
  kvdk_assert(result.s == Status::NotFound, "Impossible!");
  insertKeyOrElem(result, internal_key, RecordType::HashElem, rec);

  return Status::Ok;
}

Status KVEngine::hashListRestoreList(DLRecord* rec) {
  hash_list_builder_->AddListRecord(rec);
  return Status::Ok;
}

Status KVEngine::hashListRegisterRecovered() {
  CollectionIDType max_id = 0;
  for (auto& hlist : hash_lists_) {
    auto key = hlist->Name();
    auto guard = hash_table_->AcquireLock(key);
    LookupResult ret = lookupKey<true>(key, RecordType::HashRecord);
    kvdk_assert(ret.s == Status::NotFound, "");
    if (ret.s == Status::Ok) {
      return Status::Abort;
    }
    if (ret.s != Status::NotFound) {
      return ret.s;
    }
    insertKeyOrElem(ret, key, RecordType::HashRecord, hlist);
    max_id = std::max(max_id, hlist->ID());
  }
  auto old = list_id_.load();
  while (max_id >= old && !list_id_.compare_exchange_strong(old, max_id + 1)) {
  }
  return Status::Ok;
}

Status KVEngine::hashListDestroy(HashList* hlist,
                                 std::function<void(DLRecord*)> free_func) {
  while (hlist->Size() != 0) {
    auto internal_key = hlist->Front()->Key();
    {
      auto guard = hash_table_->AcquireLock(internal_key);
      kvdk_assert(hlist->Front()->Key() == internal_key, "");
      LookupResult ret = lookupElem<false>(internal_key, RecordType::HashElem);
      kvdk_assert(ret.s == Status::Ok, "");
      removeKeyOrElem(ret);
      hlist->PopFront([&](DLRecord* rec) { free_func(rec); });
    }
  }
  hlist->Destroy([&](DLRecord* rec) { free_func(rec); });
  delete hlist;
  return Status::Ok;
}

}  // namespace KVDK_NAMESPACE