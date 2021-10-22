#include "unordered_collection.hpp"

namespace KVDK_NAMESPACE {
UnorderedCollection::UnorderedCollection(
    std::shared_ptr<PMEMAllocator> sp_pmem_allocator,
    std::shared_ptr<HashTable> sp_hash_table, std::string const &name,
    std::uint64_t id, std::uint64_t timestamp)
    : sp_hash_table_{sp_hash_table}, p_pmem_allocator_{sp_pmem_allocator.get()},
      pmp_dlist_record_{nullptr}, dlinked_list_{sp_pmem_allocator, timestamp,
                                                id2View(id),
                                                pmem::obj::string_view{""}},
      name_{name}, id_{id}, time_stamp_{timestamp} {
  auto space_list_record = dlinked_list_.p_pmem_allocator_->Allocate(
      sizeof(DLRecord) + name_.size() + sizeof(decltype(id_)));
  if (space_list_record.size == 0) {
    DLinkedList::Deallocate(dlinked_list_.Head());
    DLinkedList::Deallocate(dlinked_list_.Tail());
    dlinked_list_.pmp_head_ = nullptr;
    dlinked_list_.pmp_tail_ = nullptr;
    throw std::bad_alloc{};
  }
  std::uint64_t offset_list_record = space_list_record.space_entry.offset;
  pmp_dlist_record_ = DLRecord::PersistDLRecord(
      dlinked_list_.p_pmem_allocator_->offset2addr_checked(offset_list_record),
      space_list_record.size, timestamp, RecordType::DlistRecord,
      dlinked_list_.Head().GetOffset(), dlinked_list_.Tail().GetOffset(), name_,
      id2View(id_));
}

UnorderedCollection::UnorderedCollection(
    std::shared_ptr<PMEMAllocator> sp_pmem_allocator,
    std::shared_ptr<HashTable> sp_hash_table, DLRecord *pmp_dlist_record)
    : sp_hash_table_{sp_hash_table}, p_pmem_allocator_{sp_pmem_allocator.get()},
      pmp_dlist_record_{pmp_dlist_record},
      dlinked_list_{
          sp_pmem_allocator,
          sp_pmem_allocator->offset2addr_checked<DLRecord>(
              pmp_dlist_record->prev),
          sp_pmem_allocator->offset2addr_checked<DLRecord>(
              pmp_dlist_record->next),
      },
      name_{string_view_2_string(pmp_dlist_record->Key())},
      id_{view2ID(pmp_dlist_record->Value())},
      time_stamp_{pmp_dlist_record->entry.meta.timestamp} {}

UnorderedIterator UnorderedCollection::First() {
  UnorderedIterator iter{shared_from_this()};
  iter.SeekToFirst();
  return iter;
}

UnorderedIterator UnorderedCollection::Last() {
  UnorderedIterator iter{shared_from_this()};
  iter.SeekToLast();
  return iter;
}

EmplaceReturn UnorderedCollection::EmplaceBefore(
    DLRecord *pmp, std::uint64_t timestamp, pmem::obj::string_view const key,
    pmem::obj::string_view const value, RecordType type,
    std::unique_lock<SpinMutex> const &lock) {
  if (!checkUserSuppliedPmp(pmp))
    return EmplaceReturn{};
  DListIterator iter_prev{dlinked_list_.p_pmem_allocator_, pmp};
  --iter_prev;
  DListIterator iter_next{dlinked_list_.p_pmem_allocator_, pmp};
  EmplaceReturn ret = emplaceBetween(iter_prev.pmp_curr_, iter_next.pmp_curr_,
                                     timestamp, key, value, type, lock, true);
  return ret;
}

EmplaceReturn UnorderedCollection::EmplaceAfter(
    DLRecord *pmp, std::uint64_t timestamp, pmem::obj::string_view const key,
    pmem::obj::string_view const value, RecordType type,
    std::unique_lock<SpinMutex> const &lock) {
  if (!checkUserSuppliedPmp(pmp))
    return EmplaceReturn{};
  DListIterator iter_prev{dlinked_list_.p_pmem_allocator_, pmp};
  DListIterator iter_next{dlinked_list_.p_pmem_allocator_, pmp};
  ++iter_next;

  EmplaceReturn ret = emplaceBetween(iter_prev.pmp_curr_, iter_next.pmp_curr_,
                                     timestamp, key, value, type, lock, true);
  return ret;
}

EmplaceReturn UnorderedCollection::EmplaceFront(
    std::uint64_t timestamp, pmem::obj::string_view const key,
    pmem::obj::string_view const value, RecordType type,
    std::unique_lock<SpinMutex> const &lock) {
  DListIterator iter_prev{dlinked_list_.Head()};
  DListIterator iter_next{dlinked_list_.Head()};
  ++iter_next;

  EmplaceReturn ret = emplaceBetween(iter_prev.pmp_curr_, iter_next.pmp_curr_,
                                     timestamp, key, value, type, lock, true);
  return ret;
}

EmplaceReturn UnorderedCollection::EmplaceBack(
    std::uint64_t timestamp, pmem::obj::string_view const key,
    pmem::obj::string_view const value, RecordType type,
    std::unique_lock<SpinMutex> const &lock) {
  DListIterator iter_prev{dlinked_list_.Tail()};
  --iter_prev;
  DListIterator iter_next{dlinked_list_.Tail()};

  EmplaceReturn ret = emplaceBetween(iter_prev.pmp_curr_, iter_next.pmp_curr_,
                                     timestamp, key, value, type, lock, true);
  return ret;
}

/// key is also checked to match old key
EmplaceReturn UnorderedCollection::Replace(
    DLRecord *pmp, std::uint64_t timestamp, pmem::obj::string_view const key,
    pmem::obj::string_view const value, RecordType type,
    std::unique_lock<SpinMutex> const &lock) {
  if (!checkUserSuppliedPmp(pmp))
    return EmplaceReturn{};
  DListIterator iter_prev{dlinked_list_.p_pmem_allocator_, pmp};
  --iter_prev;
  DListIterator iter_next{dlinked_list_.p_pmem_allocator_, pmp};
  ++iter_next;

  EmplaceReturn ret = emplaceBetween(iter_prev.pmp_curr_, iter_next.pmp_curr_,
                                     timestamp, key, value, type, lock, false);
  ret.offset_old = dlinked_list_.p_pmem_allocator_->addr2offset_checked(pmp);
  return ret;
}

EmplaceReturn
UnorderedCollection::Erase(DLRecord *pmp_record_to_delete,
                           std::unique_lock<SpinMutex> const &lock) {
  if (!checkUserSuppliedPmp(pmp_record_to_delete))
    return EmplaceReturn{};
  DListIterator iter{dlinked_list_.p_pmem_allocator_, pmp_record_to_delete};
  DListIterator iter_prev{iter};
  --iter_prev;
  DListIterator iter_next{iter};
  ++iter_next;

  checkLock(lock);

  // These locks may be invalidified after other threads insert another node!
  auto internal_key = pmp_record_to_delete->Key();
  SpinMutex *spin = lock.mutex();
  SpinMutex *spin1 = getMutex(iter_prev->Key());
  SpinMutex *spin2 = getMutex(internal_key);
  SpinMutex *spin3 = getMutex(iter_next->Key());

  using lock_t = std::unique_lock<SpinMutex>;
  lock_t lock_left;
  lock_t lock_middle;
  lock_t lock_right;

  if (spin1 != spin) {
    lock_left = lock_t{*spin1, std::defer_lock};
    if (!lock_left.try_lock())
      return EmplaceReturn{};
  }
  if (spin2 != spin && spin2 != spin1) {
    lock_middle = lock_t{*spin2, std::defer_lock};
    if (!lock_middle.try_lock())
      return EmplaceReturn{};
  }
  if (spin3 != spin && spin3 != spin1 && spin3 != spin2) {
    lock_right = lock_t{*spin3, std::defer_lock};
    if (!lock_right.try_lock())
      return EmplaceReturn{};
  }
  // acquired all locks
  // No need to check linkage.
  // No thread can modify linkage as long as caller has
  // acquired lock to middle node.

  DListIterator iter_old{iter};
  iter = dlinked_list_.Erase(iter);

  return EmplaceReturn{iter.GetOffset(), iter_old.GetOffset(), true};
}

EmplaceReturn UnorderedCollection::emplaceBetween(
    DLRecord *pmp_prev, DLRecord *pmp_next, std::uint64_t timestamp,
    pmem::obj::string_view const key, pmem::obj::string_view const value,
    RecordType type,
    std::unique_lock<SpinMutex> const
        &lock /* lock to prev or next or newly inserted, passed in and out. */,
    bool check_linkage) {
  checkLock(lock);
  checkEmplaceType(type);

  DListIterator iter_prev{dlinked_list_.p_pmem_allocator_, pmp_prev};
  DListIterator iter_next{dlinked_list_.p_pmem_allocator_, pmp_next};

  // These locks may be invalidified after other threads insert another node!
  std::string internal_key = GetInternalKey(key);
  SpinMutex *spin = lock.mutex();
  SpinMutex *spin1 = getMutex(iter_prev->Key());
  SpinMutex *spin2 = getMutex(internal_key);
  SpinMutex *spin3 = getMutex(iter_next->Key());

  using lock_t = std::unique_lock<SpinMutex>;
  lock_t lock_left;
  lock_t lock_middle;
  lock_t lock_right;
  std::vector<lock_t> locks;

  if (spin1 != spin) {
    lock_left = lock_t{*spin1, std::defer_lock};
    if (!lock_left.try_lock())
      return EmplaceReturn{};
  }
  if (spin2 != spin && spin2 != spin1) {
    lock_middle = lock_t{*spin2, std::defer_lock};
    if (!lock_middle.try_lock())
      return EmplaceReturn{};
  }
  if (spin3 != spin && spin3 != spin1 && spin3 != spin2) {
    lock_right = lock_t{*spin3, std::defer_lock};
    if (!lock_right.try_lock())
      return EmplaceReturn{};
  }

  // acquired all locks

  // Only when insertion should we check linkage
  if (check_linkage) {
    bool has_other_thread_modified = false;
    DListIterator iter_prev_copy{iter_prev};
    DListIterator iter_next_copy(iter_next);
    has_other_thread_modified =
        has_other_thread_modified || (++iter_prev_copy != iter_next);
    has_other_thread_modified =
        has_other_thread_modified || (--iter_next_copy != iter_prev);
    if (has_other_thread_modified) {
      return EmplaceReturn{};
    }
  }
  DListIterator iter = dlinked_list_.EmplaceBetween(
      iter_prev, iter_next, timestamp, internal_key, value, type);

  return EmplaceReturn{iter.GetOffset(), EmplaceReturn::FailOffset, true};
}

UnorderedIterator::UnorderedIterator(
    std::shared_ptr<UnorderedCollection> sp_coll)
    : sp_collection_{sp_coll},
      internal_iterator_{sp_coll->dlinked_list_.Head()}, valid_{false} {}

UnorderedIterator::UnorderedIterator(
    std::shared_ptr<UnorderedCollection> sp_coll, DLRecord *pmp)
    : sp_collection_{sp_coll},
      internal_iterator_{sp_collection_->dlinked_list_.p_pmem_allocator_, pmp},
      valid_{false} {
  kvdk_assert(
      pmp,
      "Explicit Constructor of UnorderedIterator does not accept nullptr!");
  sp_collection_->checkUserSuppliedPmp(pmp);
  valid_ = (pmp->entry.meta.type == RecordType::DlistDataRecord);
  return;
}

void UnorderedIterator::internalNext() {
  if (!internal_iterator_.valid()) {
    goto FATAL_FAILURE;
  }
  switch (static_cast<RecordType>(internal_iterator_->entry.meta.type)) {
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

  ++internal_iterator_;
  while (internal_iterator_.valid()) {
    valid_ = false;
    switch (internal_iterator_->entry.meta.type) {
    case RecordType::DlistDataRecord: {
      valid_ = true;
      return;
    }
    case RecordType::DlistTailRecord: {
      valid_ = false;
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
  if (!internal_iterator_.valid()) {
    goto FATAL_FAILURE;
  }
  switch (static_cast<RecordType>(internal_iterator_->entry.meta.type)) {
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

  --internal_iterator_;
  while (internal_iterator_.valid()) {
    valid_ = false;
    switch (internal_iterator_->entry.meta.type) {
    case RecordType::DlistDataRecord: {
      valid_ = true;
      return;
    }
    case RecordType::DlistHeadRecord: {
      valid_ = false;
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
