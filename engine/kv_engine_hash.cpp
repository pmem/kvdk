#include "kv_engine.hpp"

namespace KVDK_NAMESPACE {
Status KVEngine::HashLength(StringView key, size_t* len) {
  if (!CheckKeySize(key)) {
    return Status::InvalidDataSize;
  }
  HashList* hlist;
  Status s = hashListFind(key, &hlist, false);
  if (s != Status::Ok) {
    return s;
  }
  *len = hlist->Size();
  return Status::Ok;
}

Status KVEngine::HashGet(StringView key, StringView field, std::string* value) {
  if (!CheckKeySize(key) || !CheckKeySize(field)) {
    return Status::InvalidDataSize;
  }

  HashList* hlist;
  Status s = hashListFind(key, &hlist, false);
  if (s != Status::Ok) {
    return s;
  }
  LookupResult result =
      lookupImpl<false>(hlist->InternalKey(field), RecordType::HashElem);
  if (result.s != Status::Ok) {
    return result.s;
  }
  StringView val = result.entry.GetIndex().dl_record->Value();
  value->assign(val.data(), val.size());
  return Status::Ok;
}

Status KVEngine::HashSet(StringView key, StringView field, StringView value) {
  if (!CheckKeySize(key) || !CheckKeySize(field) || !CheckValueSize(value)) {
    return Status::InvalidDataSize;
  }

  HashList* hlist;
  Status s = hashListFind(key, &hlist, true);
  if (s != Status::Ok) {
    return s;
  }

  auto internal_key = hlist->InternalKey(field);
  auto guard = hash_table_->AcquireLock(internal_key);
  LookupResult result = lookupImpl<true>(internal_key, RecordType::HashElem);
  if (!(result.s == Status::Ok || result.s == Status::NotFound)) {
    return result.s;
  }

  /// TODO: delayFree instead of purgeAndFree
  TimeStampType ts = version_controller_.GetCurrentTimestamp();
  auto space = pmem_allocator_->Allocate(sizeof(DLRecord) +
                                         internal_key.size() + value.size());
  void* addr = pmem_allocator_->offset2addr_checked(space.offset);
  if (result.s == Status::NotFound) {
    if (std::hash<std::thread::id>{}(std::this_thread::get_id()) % 2 == 0) {
      hlist->PushFront(space, ts, field, value);
    } else {
      hlist->PushBack(space, ts, field, value);
    }
  } else {
    DLRecord* old_rec = result.entry.GetIndex().dl_record;
    hlist->Replace(space, old_rec, ts, field, value,
                   [&](DLRecord* rec) { purgeAndFree(rec); });
  }
  insertImpl(result, internal_key, RecordType::HashElem, addr);
  return Status::Ok;
}

Status KVEngine::HashDelete(StringView key, StringView field) {
  if (!CheckKeySize(key) || !CheckKeySize(field)) {
    return Status::InvalidDataSize;
  }
  HashList* hlist;
  Status s = hashListFind(key, &hlist, false);
  if (s == Status::NotFound) {
    return Status::Ok;
  }
  if (s != Status::Ok) {
    return s;
  }

  auto guard = hash_table_->AcquireLock(hlist->InternalKey(field));
  LookupResult ret =
      removeImpl(hlist->InternalKey(field), RecordType::HashElem);
  guard.unlock();
  if (ret.s == Status::NotFound) {
    return Status::Ok;
  }
  if (ret.s != Status::Ok) {
    return ret.s;
  }
  hlist->Erase(ret.entry.GetIndex().dl_record,
               [&](DLRecord* rec) { purgeAndFree(rec); });
  return Status::Ok;
}

std::unique_ptr<HashIterator> KVEngine::HashMakeIterator(StringView key) {
  if (!CheckKeySize(key)) {
    return nullptr;
  }
  HashList* hlist;
  Status s = hashListFind(key, &hlist, false);
  if (s != Status::Ok) {
    return nullptr;
  }
  return std::unique_ptr<HashIteratorImpl>{new HashIteratorImpl{hlist}};
}

Status KVEngine::hashListFind(StringView key, HashList** hlist, bool init_nx) {
  if (MaybeInitAccessThread() != Status::Ok) {
    return Status::TooManyAccessThreads;
  }
  {
    auto result = lookupKey<false>(key, RecordType::HashRecord);
    if (result.s != Status::Ok && result.s != Status::NotFound) {
      return result.s;
    }
    if (result.s == Status::Ok) {
      (*hlist) = result.entry.GetIndex().hlist;
      if ((*hlist)->Valid()) {
        // Active and successfully locked
        return Status::Ok;
      }
      // Inactive, already destroyed by other thread.
      // The inactive List will be removed from HashTable
      // by caller that destroys it with HashTable locked.
    }
    if (!init_nx) {
      // Uninitialized or Inactive
      return Status::NotFound;
    }
  }

  // Uninitialized or Inactive, initialize new one
  {
    auto guard2 = hash_table_->AcquireLock(key);
    auto result = lookupKey<false>(key, RecordType::HashRecord);
    if (result.s != Status::Ok && result.s != Status::NotFound) {
      return result.s;
    }
    if (result.s == Status::Ok) {
      (*hlist) = result.entry.GetIndex().hlist;
      kvdk_assert((*hlist)->Valid(), "Invalid list should have been removed!");
      return Status::Ok;
    }
    // No other thread have created one, create one here.
    std::uint64_t ts = version_controller_.GetCurrentTimestamp();
    CollectionIDType id = list_id_.fetch_add(1);
    auto space = pmem_allocator_->Allocate(sizeof(DLRecord) + key.size() +
                                           sizeof(CollectionIDType));
    if (space.size == 0) {
      return Status::PmemOverflow;
    }
    *hlist = new HashList{};
    (*hlist)->Init(pmem_allocator_.get(), space, ts, key, id,
                   hash_list_locks_.get());
    {
      std::lock_guard<std::mutex> guard2{list_mu_};
      hash_lists_.emplace_back(*hlist);
    }
    return registerCollection(*hlist);
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
  LookupResult result = lookupImpl<true>(internal_key, RecordType::HashElem);
  if (!(result.s == Status::Ok || result.s == Status::NotFound)) {
    return result.s;
  }
  kvdk_assert(result.s == Status::NotFound, "Impossible!");
  insertImpl(result, internal_key, RecordType::HashElem, rec);

  return Status::Ok;
}

Status KVEngine::hashListRestoreList(DLRecord* rec) {
  hash_list_builder_->AddListRecord(rec);
  return Status::Ok;
}

Status KVEngine::hashListRegisterRecovered() {
  CollectionIDType max_id = 0;
  for (auto const& hlist : hash_lists_) {
    auto guard = hash_table_->AcquireLock(hlist->Name());
    Status s = registerCollection(hlist.get());
    if (s != Status::Ok) {
      return s;
    }
    max_id = std::max(max_id, hlist->ID());
  }
  auto old = list_id_.load();
  while (max_id >= old && !list_id_.compare_exchange_strong(old, max_id + 1)) {
  }
  return Status::Ok;
}

Status KVEngine::hashListDestroy(HashList* hlist) {
  kvdk_assert(hlist->Valid(), "");
  while (hlist->Size() != 0) {
    auto internal_key = hlist->Front()->Key();
    auto guard = hash_table_->AcquireLock(internal_key);
    LookupResult ret = removeImpl(internal_key, RecordType::HashElem);
    kvdk_assert(ret.s == Status::Ok, "");
    kvdk_assert(ret.entry.GetIndex().dl_record == hlist->Front().Address(), "");
    hlist->PopFront([&](DLRecord* rec) { purgeAndFree(rec); });
  }
  hlist->Destroy([&](DLRecord* rec) { purgeAndFree(rec); });
  return Status::Ok;
}

}  // namespace KVDK_NAMESPACE