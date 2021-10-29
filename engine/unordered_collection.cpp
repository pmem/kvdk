#include "unordered_collection.hpp"

namespace KVDK_NAMESPACE {
UnorderedCollection::UnorderedCollection(HashTable *hash_table_p,
                                         PMEMAllocator *pmem_allocator_p,
                                         std::string const name,
                                         CollectionIDType id,
                                         TimeStampType timestamp)
    : hash_table_ptr{hash_table_p}, collection_record_ptr{nullptr},
      dlinked_list{pmem_allocator_p, timestamp, id2View(id), StringView{""}},
      collection_name{name}, collection_id{id}, timestamp{timestamp} {
  {
    auto list_record_space = dlinked_list.pmem_allocator_ptr->Allocate(
        sizeof(DLRecord) + collection_name.size() + sizeof(CollectionIDType));
    if (list_record_space.size == 0) {
      dlinked_list.PurgeAndFree(dlinked_list.Head().GetCurrentAddress());
      dlinked_list.PurgeAndFree(dlinked_list.Tail().GetCurrentAddress());
      dlinked_list.head_pmmptr = nullptr;
      dlinked_list.tail_pmmptr = nullptr;
      throw std::bad_alloc{};
    }
    PMemOffsetType offset_list_record = list_record_space.space_entry.offset;
    collection_record_ptr = DLRecord::PersistDLRecord(
        dlinked_list.pmem_allocator_ptr->offset2addr_checked(
            offset_list_record),
        list_record_space.size, timestamp, RecordType::DlistRecord,
        dlinked_list.Head().GetCurrentOffset(),
        dlinked_list.Tail().GetCurrentOffset(), collection_name,
        id2View(collection_id));
  }
}

UnorderedCollection::UnorderedCollection(HashTable *hash_table_p,
                                         PMEMAllocator *pmem_allocator_p,
                                         DLRecord *pmp_dlist_record)
    : hash_table_ptr{hash_table_p}, collection_record_ptr{pmp_dlist_record},
      dlinked_list{
          pmem_allocator_p,
          pmem_allocator_p->offset2addr_checked<DLRecord>(
              pmp_dlist_record->prev),
          pmem_allocator_p->offset2addr_checked<DLRecord>(
              pmp_dlist_record->next),
      },
      collection_name{string_view_2_string(pmp_dlist_record->Key())},
      collection_id{view2ID(pmp_dlist_record->Value())},
      timestamp{pmp_dlist_record->entry.meta.timestamp} {}

EmplaceReturn UnorderedCollection::Emplace(TimeStampType timestamp,
                                           StringView const key,
                                           StringView const value,
                                           LockType const &lock) {
  thread_local DLRecord *last_emplacement_pos = nullptr;

  iterator new_record = makeInternalIterator(nullptr);
  LockPair lock_prev_and_next;

  auto internal_key = makeInternalKey(key);

  if (isValidRecord(last_emplacement_pos)) {
    iterator prev = makeInternalIterator(last_emplacement_pos);
    --prev;
    iterator next = makeInternalIterator(last_emplacement_pos);
    if (!lockPositions(prev, next, lock, lock_prev_and_next) ||
        !isAdjacent(prev, next) || !isValidRecord(last_emplacement_pos)) {
      last_emplacement_pos = nullptr;
      return EmplaceReturn{};
    }
    new_record =
        dlinked_list.EmplaceBefore(next, timestamp, internal_key, value);
  } else {
    KeyHashType hash = hash_str(key.data(), key.size());
    if (hash % 2 == 0) {
      iterator prev = dlinked_list.Head();
      iterator next = dlinked_list.Head();
      ++next;
      if (!lockPositions(prev, next, lock, lock_prev_and_next) ||
          !isAdjacent(prev, next)) {
        return EmplaceReturn{};
      }
      new_record = dlinked_list.EmplaceFront(timestamp, internal_key, value);
    } else {
      iterator prev = dlinked_list.Tail();
      --prev;
      iterator next = dlinked_list.Tail();
      if (!lockPositions(prev, next, lock, lock_prev_and_next) ||
          !isAdjacent(prev, next)) {
        return EmplaceReturn{};
      }
      new_record = dlinked_list.EmplaceBack(timestamp, internal_key, value);
    }
  }

  last_emplacement_pos = new_record.GetCurrentAddress();

  return EmplaceReturn{new_record.GetCurrentOffset(), EmplaceReturn::FailOffset,
                       true};
}

EmplaceReturn UnorderedCollection::Replace(DLRecord *pos,
                                           TimeStampType timestamp,
                                           StringView const key,
                                           StringView const value,
                                           LockType const &lock) {
  kvdk_assert(checkID(pos) && isValidRecord(pos),
              "Trying to replace invalid record!");

  iterator old = makeInternalIterator(pos);
  iterator prev{old};
  --prev;
  iterator next{old};
  ++next;

  LockPair lock_prev_and_next;
  if (!lockPositions(prev, next, lock, lock_prev_and_next))
    return EmplaceReturn{};

  iterator curr =
      dlinked_list.Replace(old, timestamp, makeInternalKey(key), value);

  return EmplaceReturn{curr.GetCurrentOffset(), old.GetCurrentOffset(), true};
}

EmplaceReturn UnorderedCollection::Erase(DLRecord *pos, LockType const &lock) {
  kvdk_assert(checkID(pos) && isValidRecord(pos),
              "Trying to erase invalid record!");

  iterator old = makeInternalIterator(pos);
  iterator prev{old};
  --prev;
  iterator next{old};
  ++next;

  LockPair lock_prev_and_next;
  if (!lockPositions(prev, next, lock, lock_prev_and_next))
    return EmplaceReturn{};

  dlinked_list.Erase(old);

  return EmplaceReturn{EmplaceReturn::FailOffset, old.GetCurrentOffset(), true};
}

UnorderedIterator::UnorderedIterator(
    std::shared_ptr<UnorderedCollection> sp_coll)
    : collection_shrdptr{sp_coll},
      internal_iterator{sp_coll->dlinked_list.Head()}, valid{false} {}

UnorderedIterator::UnorderedIterator(
    std::shared_ptr<UnorderedCollection> sp_coll, DLRecord *pmp)
    : collection_shrdptr{sp_coll},
      internal_iterator{sp_coll->makeInternalIterator(pmp)}, valid{false} {
  kvdk_assert(
      pmp,
      "Explicit Constructor of UnorderedIterator does not accept nullptr!");
  collection_shrdptr->isValidRecord(pmp);
  valid = (pmp->entry.meta.type == RecordType::DlistDataRecord);
  return;
}

void UnorderedIterator::internalNext() {
  if (!internal_iterator.valid()) {
    goto FATAL_FAILURE;
  }
  switch (static_cast<RecordType>(internal_iterator->entry.meta.type)) {
  case RecordType::DlistHeadRecord:
  case RecordType::DlistDataRecord: {
    break;
  }
  case RecordType::DlistRecord:
  case RecordType::DlistTailRecord:
  default: {
    goto FATAL_FAILURE;
  }
  }

  ++internal_iterator;
  while (internal_iterator.valid()) {
    valid = false;
    switch (internal_iterator->entry.meta.type) {
    case RecordType::DlistDataRecord: {
      valid = true;
      return;
    }
    case RecordType::DlistTailRecord: {
      valid = false;
      return;
    }
    case RecordType::DlistHeadRecord:
    case RecordType::DlistRecord:
    default: {
      goto FATAL_FAILURE;
    }
    }
  }
FATAL_FAILURE:
  kvdk_assert(false, "UnorderedIterator::internalNext() fails!");
}

void UnorderedIterator::internalPrev() {
  if (!internal_iterator.valid()) {
    goto FATAL_FAILURE;
  }
  switch (static_cast<RecordType>(internal_iterator->entry.meta.type)) {
  case RecordType::DlistTailRecord:
  case RecordType::DlistDataRecord: {
    break;
  }
  case RecordType::DlistHeadRecord:
  case RecordType::DlistRecord:
  default: {
    goto FATAL_FAILURE;
  }
  }

  --internal_iterator;
  while (internal_iterator.valid()) {
    valid = false;
    switch (internal_iterator->entry.meta.type) {
    case RecordType::DlistDataRecord: {
      valid = true;
      return;
    }
    case RecordType::DlistHeadRecord: {
      valid = false;
      return;
    }
    case RecordType::DlistTailRecord:
    case RecordType::DlistRecord:
    default: {
      goto FATAL_FAILURE;
    }
    }
  }
FATAL_FAILURE:
  kvdk_assert(false, "UnorderedIterator::internalPrev() fails!");
}
} // namespace KVDK_NAMESPACE
