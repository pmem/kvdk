#include "unordered_collection.hpp"

namespace KVDK_NAMESPACE {
UnorderedCollection::UnorderedCollection(HashTable* hash_table_ptr,
                                         PMEMAllocator* pmem_allocator_p,
                                         std::string const name,
                                         CollectionIDType id,
                                         TimeStampType timestamp)
    : Collection(name, id),
      hash_table_ptr_{hash_table_ptr},
      collection_record_ptr_{nullptr},
      dlinked_list_{pmem_allocator_p, timestamp, EncodeID(id), StringView{""}},
      timestamp_{timestamp} {
  {
    auto list_record_space = dlinked_list_.pmem_allocator_ptr_->Allocate(
        sizeof(DLRecord) + Name().size() + sizeof(CollectionIDType));
    if (list_record_space.size == 0) {
      dlinked_list_.purgeAndFree(dlinked_list_.Head().GetCurrentAddress());
      dlinked_list_.purgeAndFree(dlinked_list_.Tail().GetCurrentAddress());
      dlinked_list_.head_pmmptr_ = nullptr;
      dlinked_list_.tail_pmmptr_ = nullptr;
      throw std::bad_alloc{};
    }
    PMemOffsetType offset_list_record = list_record_space.offset;
    collection_record_ptr_ = DLRecord::PersistDLRecord(
        dlinked_list_.pmem_allocator_ptr_->offset2addr_checked(
            offset_list_record),
        list_record_space.size, timestamp, RecordType::DlistRecord,
        kNullPMemOffset, dlinked_list_.Head().GetCurrentOffset(),
        dlinked_list_.Tail().GetCurrentOffset(), Name(), EncodeID(ID()));
  }
}

UnorderedCollection::UnorderedCollection(HashTable* hash_table_ptr,
                                         PMEMAllocator* pmem_allocator_p,
                                         DLRecord* pmp_dlist_record)
    : Collection{string_view_2_string(pmp_dlist_record->Key()),
                 DecodeID(pmp_dlist_record->Value())},
      hash_table_ptr_{hash_table_ptr},
      collection_record_ptr_{pmp_dlist_record},
      dlinked_list_{
          pmem_allocator_p,
          pmem_allocator_p->offset2addr_checked<DLRecord>(
              pmp_dlist_record->prev),
          pmem_allocator_p->offset2addr_checked<DLRecord>(
              pmp_dlist_record->next),
      },
      timestamp_{pmp_dlist_record->entry.meta.timestamp} {}

ModifyReturn UnorderedCollection::Emplace(TimeStampType timestamp,
                                          StringView const key,
                                          StringView const value,
                                          LockType const& lock) {
  thread_local PMemOffsetType last_emplacement_offset = kNullPMemOffset;
  DLRecord* last_emplacement_pos =
      (last_emplacement_offset == kNullPMemOffset)
          ? nullptr
          : static_cast<DLRecord*>(
                dlinked_list_.pmem_allocator_ptr_->offset2addr_checked(
                    last_emplacement_offset));

  iterator new_record = makeInternalIterator(nullptr);
  LockPair lock_prev_and_next;

  auto internal_key = InternalKey(key);

  if (isValidRecord(last_emplacement_pos)) {
    iterator prev = makeInternalIterator(last_emplacement_pos);
    --prev;
    iterator next = makeInternalIterator(last_emplacement_pos);
    if (!lockPositions(prev, next, lock, lock_prev_and_next) ||
        !isAdjacent(prev, next) || !isValidRecord(last_emplacement_pos)) {
      // Locking failed or the cached position is modified
      // Invalidate the cache and try again
      last_emplacement_pos = nullptr;
      return ModifyReturn{};
    }
    new_record =
        dlinked_list_.EmplaceBefore(next, timestamp, internal_key, value);
    return ModifyReturn{new_record.GetCurrentOffset(), ModifyReturn::FailOffset,
                        true};
  } else {
    // No cached position for emplacing
    // Emplace front or back based on oddity of hash
    KeyHashType hash = hash_str(key.data(), key.size());
    if (hash % 2 == 0) {
      iterator prev = dlinked_list_.Head();
      iterator next = dlinked_list_.Head();
      ++next;
      if (!lockPositions(prev, next, lock, lock_prev_and_next) ||
          !isAdjacent(prev, next)) {
        return ModifyReturn{};
      }
      new_record = dlinked_list_.EmplaceFront(timestamp, internal_key, value);
      last_emplacement_pos = new_record.GetCurrentAddress();
      return ModifyReturn{new_record.GetCurrentOffset(),
                          ModifyReturn::FailOffset, true};
    } else {
      iterator prev = dlinked_list_.Tail();
      --prev;
      iterator next = dlinked_list_.Tail();
      if (!lockPositions(prev, next, lock, lock_prev_and_next) ||
          !isAdjacent(prev, next)) {
        return ModifyReturn{};
      }
      new_record = dlinked_list_.EmplaceBack(timestamp, internal_key, value);
      last_emplacement_pos = new_record.GetCurrentAddress();
      return ModifyReturn{new_record.GetCurrentOffset(),
                          ModifyReturn::FailOffset, true};
    }
  }
}

ModifyReturn UnorderedCollection::Replace(DLRecord* pos,
                                          TimeStampType timestamp,
                                          StringView const key,
                                          StringView const value,
                                          LockType const& lock) {
  kvdk_assert(checkID(pos) && isValidRecord(pos),
              "Trying to replace invalid record!");

  iterator old = makeInternalIterator(pos);
  iterator prev{old};
  --prev;
  iterator next{old};
  ++next;

  LockPair lock_prev_and_next;
  if (!lockPositions(prev, next, lock, lock_prev_and_next)) {
    return ModifyReturn{};
  }

  iterator curr =
      dlinked_list_.Replace(old, timestamp, InternalKey(key), value);

  return ModifyReturn{curr.GetCurrentOffset(), old.GetCurrentOffset(), true};
}

ModifyReturn UnorderedCollection::Erase(DLRecord* pos, LockType const& lock) {
  kvdk_assert(checkID(pos) && isValidRecord(pos),
              "Trying to erase invalid record!");

  iterator old = makeInternalIterator(pos);
  iterator prev{old};
  --prev;
  iterator next{old};
  ++next;

  LockPair lock_prev_and_next;
  if (!lockPositions(prev, next, lock, lock_prev_and_next)) {
    return ModifyReturn{};
  }

  dlinked_list_.Erase(old);

  return ModifyReturn{ModifyReturn::FailOffset, old.GetCurrentOffset(), true};
}

UnorderedIterator::UnorderedIterator(
    std::shared_ptr<UnorderedCollection> sp_coll)
    : collection_shrdptr{sp_coll},
      internal_iterator{sp_coll->dlinked_list_.Head()},
      valid{false} {}

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
}  // namespace KVDK_NAMESPACE
