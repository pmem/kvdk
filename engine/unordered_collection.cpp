#include "unordered_collection.hpp"

namespace KVDK_NAMESPACE {
HashTable* UnorderedCollection::hash_table_ptr = nullptr;

UnorderedCollection::UnorderedCollection(
  std::string const name, 
  CollectionIDType id,
  TimeStampType timestamp)
    : collection_record_ptr{nullptr},
      dlinked_list{timestamp, id2View(id), StringView{""}},
      collection_name{name}, collection_id{id}, timestamp{timestamp} {
  {
    auto list_record_space = DLinkedListType::pmem_allocator_ptr->Allocate(
        sizeof(DLRecord) + collection_name.size() + sizeof(CollectionIDType));
    if (list_record_space.size == 0) {
      DLinkedListType::Deallocate(dlinked_list.Head());
      DLinkedListType::Deallocate(dlinked_list.Tail());
      dlinked_list.head_pmmptr = nullptr;
      dlinked_list.tail_pmmptr = nullptr;
      throw std::bad_alloc{};
    }
    PMemOffsetType offset_list_record = list_record_space.space_entry.offset;
    collection_record_ptr = DLRecord::PersistDLRecord(
        DLinkedListType::pmem_allocator_ptr->offset2addr_checked(
            offset_list_record),
        list_record_space.size, timestamp, RecordType::DlistRecord,
        dlinked_list.Head().GetCurrentOffset(),
        dlinked_list.Tail().GetCurrentOffset(), 
        collection_name, id2View(collection_id));
  }
}

UnorderedCollection::UnorderedCollection(
      DLRecord *pmp_dlist_record)
    : collection_record_ptr{pmp_dlist_record},
      dlinked_list{
          DLinkedListType::pmem_allocator_ptr->offset2addr_checked<DLRecord>(
              pmp_dlist_record->prev),
          DLinkedListType::pmem_allocator_ptr->offset2addr_checked<DLRecord>(
              pmp_dlist_record->next),
      },
      collection_name{string_view_2_string(pmp_dlist_record->Key())},
      collection_id{view2ID(pmp_dlist_record->Value())},
      timestamp{pmp_dlist_record->entry.meta.timestamp} {}

EmplaceReturn
UnorderedCollection::EmplaceBefore(DLRecord *pos, std::uint64_t timestamp,
                                  StringView const key, StringView const value,
                                  LockType const &lock) {
  if (!checkUserSuppliedPmp(pos))
    return EmplaceReturn{};
  iterator prev{pos};
  --prev;
  iterator next{pos};
  LockPair lock_prev_and_next;
  if(!lockPositions(prev, next, lock, lock_prev_and_next))
    return EmplaceReturn{};
  if (!isAdjacent(prev, next))
    return EmplaceReturn{};

  iterator curr = dlinked_list.EmplaceBefore(next, timestamp, makeInternalKey(key), value);
  
  return EmplaceReturn{curr.GetCurrentOffset(), EmplaceReturn::FailOffset, true};
}

EmplaceReturn
UnorderedCollection::EmplaceAfter(DLRecord *pos, std::uint64_t timestamp,
                                  StringView const key, StringView const value,
                                  LockType const &lock) {
  if (!checkUserSuppliedPmp(pos))
    return EmplaceReturn{};
  iterator prev{pos};
  iterator next{pos};
  ++next;
  LockPair lock_prev_and_next;
  if(!lockPositions(prev, next, lock, lock_prev_and_next))
    return EmplaceReturn{};
  if (!isAdjacent(prev, next))
    return EmplaceReturn{};

  iterator curr = dlinked_list.EmplaceBefore(next, timestamp, makeInternalKey(key), value);
  
  return EmplaceReturn{curr.GetCurrentOffset(), EmplaceReturn::FailOffset, true};
}

EmplaceReturn
UnorderedCollection::EmplaceFront(std::uint64_t timestamp, StringView const key,
                                  StringView const value,
                                  LockType const &lock) {
  iterator prev{dlinked_list.Head()};
  iterator next{dlinked_list.Head()};
  ++next;
  LockPair lock_prev_and_next;
  if(!lockPositions(prev, next, lock, lock_prev_and_next))
    return EmplaceReturn{};
  if (!isAdjacent(prev, next))
    return EmplaceReturn{};

  iterator curr = dlinked_list.EmplaceBefore(next, timestamp, makeInternalKey(key), value);
  
  return EmplaceReturn{curr.GetCurrentOffset(), EmplaceReturn::FailOffset, true};
}

EmplaceReturn
UnorderedCollection::EmplaceBack(std::uint64_t timestamp, StringView const key,
                                 StringView const value,
                                 LockType const &lock) {
  iterator prev{dlinked_list.Tail()};
  --prev;
  iterator next{dlinked_list.Tail()};
  LockPair lock_prev_and_next;
  if(!lockPositions(prev, next, lock, lock_prev_and_next))
    return EmplaceReturn{};
  if (!isAdjacent(prev, next))
    return EmplaceReturn{};

  iterator curr = dlinked_list.EmplaceBefore(next, timestamp, makeInternalKey(key), value);
  
  return EmplaceReturn{curr.GetCurrentOffset(), EmplaceReturn::FailOffset, true};
}

/// key is also checked to match old key
EmplaceReturn
UnorderedCollection::Replace(DLRecord *pos, std::uint64_t timestamp,
                             StringView const key, StringView const value,
                             LockType const &lock) {
  if (!checkUserSuppliedPmp(pos))
    return EmplaceReturn{};
  iterator old{pos};
  iterator prev{pos};
  --prev;
  iterator next{pos};
  --next;

  LockPair lock_prev_and_next;
  if(!lockPositions(prev, next, lock, lock_prev_and_next))
    return EmplaceReturn{};

  iterator curr = dlinked_list.Replace(old, timestamp, makeInternalKey(key), value);

  return EmplaceReturn{curr.GetCurrentOffset(), old.GetCurrentOffset(), true};
}

EmplaceReturn
UnorderedCollection::Erase(DLRecord *pos,
                           LockType const &lock) {
  if (!checkUserSuppliedPmp(pos))
    return EmplaceReturn{};
  iterator old{pos};
  iterator prev{pos};
  --prev;
  iterator next{pos};
  --next;

  LockPair lock_prev_and_next;
  if(!lockPositions(prev, next, lock, lock_prev_and_next))
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
      internal_iterator{pmp},
      valid{false} {
  kvdk_assert(
      pmp,
      "Explicit Constructor of UnorderedIterator does not accept nullptr!");
  collection_shrdptr->checkUserSuppliedPmp(pmp);
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
