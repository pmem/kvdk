/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once
#include <immintrin.h>
#include <x86intrin.h>

#include <array>
#include <atomic>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <thread>

#include "alias.hpp"
#include "collection.hpp"
#include "data_record.hpp"
#include "lock_table.hpp"
#include "macros.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

constexpr PMemOffsetType NullPMemOffset = kNullPMemOffset;

template <RecordType, RecordType>
class GenericListBuilder;

template <RecordType ListType, RecordType DataType>
class GenericList final : public Collection {
 private:
  // For offset-address translation
  PMEMAllocator* alloc{nullptr};
  // Persisted ListType on PMem, contains List name(key) and id(value).
  DLRecord* list_record{nullptr};
  // First Element in List, nullptr indicates empty List
  DLRecord* first{nullptr};
  // Last Element in List, nullptr indicates empty List
  DLRecord* last{nullptr};
  // Size of list
  std::atomic_uint64_t sz{0U};
  // Recursive mutex to lock the List
  using LockType = std::recursive_mutex;
  LockType mu;
  // Lock table to lock individual elements
  // Redis List don't use lock_table, Redis Hash does.
  LockTable* lock_table{nullptr};

 public:
  // Deletion of a node in GenericList takes two steps.
  // First, the node is unlinked from list and markAsDirty(),
  // which can be detected by Iterator::dirty().
  // Iterator may go to a dirty() node by operator++() or operator--().
  // It's safe to read data on this dirty() node.
  // After any Iterator or lockless read threads can no longer access
  // this unlinked node, which is guaranteed by the snapshot system,
  // the node is Free()d by PMemAllocator.
  class Iterator {
   private:
    // owner must be initialized with a List
    GenericList const* owner;
    // curr == nullptr indicates Head() and Tail()
    DLRecord* curr{nullptr};

   public:
    // Always start at Head()/Tail()
    // Head() and Tail() is the same state,
    // just for convention this state has two names
    // Tail() for iterating forward
    // Head() for iterating backward
    explicit Iterator(GenericList const* o) : Iterator{o, nullptr} {}

    explicit Iterator(GenericList const* o, DLRecord* c) : owner{o}, curr{c} {
      debug_check();
    }

    Iterator() = delete;
    Iterator(Iterator const&) = default;
    Iterator(Iterator&&) = default;
    Iterator& operator=(Iterator const&) = default;
    Iterator& operator=(Iterator&&) = default;
    ~Iterator() = default;

    /// Increment and Decrement operators
    // Don't skip Delete Records, its up to caller to handle the situation.
    Iterator& operator++() {
      debug_check();
      if (curr == nullptr) {
        // Head(), goto Front()
        // Front() == Tail() if List is empty.
        curr = owner->first;
      } else if (curr->next != NullPMemOffset) {
        // Not Back(), goto next
        curr = owner->addressOf(curr->next);
      } else {
        // Back(), goto Tail()
        curr = nullptr;
      }
      debug_check();
      return *this;
    }

    Iterator const operator++(int) {
      Iterator old{*this};
      this->operator++();
      return old;
    }

    Iterator& operator--() {
      debug_check();
      if (curr == nullptr) {
        // Tail(), goto Back()
        // Back() == Head() if List is empty.
        curr = owner->last;
      } else if (curr->prev != NullPMemOffset) {
        // Not Front(), goto prev
        curr = owner->addressOf(curr->prev);
      } else {
        // Front(), goto Head()
        curr = nullptr;
      }
      debug_check();
      return *this;
    }

    Iterator const operator--(int) {
      Iterator old{*this};
      this->operator--();
      return old;
    }

    // It's valid to access address or offset of deleted record,
    // but invalid to access its contents.
    DLRecord* operator->() const { return Address(); }

    DLRecord& operator*() const { return *Address(); }

    DLRecord* Address() const {
      kvdk_assert(curr != nullptr, "");
      debug_check();
      return curr;
    }

    PMemOffsetType Offset() const { return owner->offsetOf(Address()); }

    static std::uint64_t Hash(void const* addr) {
      kvdk_assert(addr != nullptr, "");
      return XXH3_64bits(&addr, sizeof(void const*));
    }

    std::uint64_t Hash() const {
      void const* addr =
          (curr == nullptr) ? static_cast<void const*>(owner) : curr;
      return XXH3_64bits(&addr, sizeof(void const*));
    }

   private:
    friend GenericList;

    friend bool operator==(Iterator const& lhs, Iterator const& rhs) {
      return (lhs.owner == rhs.owner) && (lhs.curr == rhs.curr);
    }

    friend bool operator!=(Iterator const& lhs, Iterator const& rhs) {
      return !(lhs == rhs);
    }

    // Returns true if iterator stays in a possible state,
    // including iterator points to
    //  Head()/Tail()
    //  A Delete Record
    //  A Normal Record
    void debug_check() const {
#if KVDK_DEBUG_LEVEL > 0
      if (curr == nullptr) {
        return;
      }
      kvdk_assert(Collection::ExtractID(curr->Key()) == owner->ID(), "");
      kvdk_assert(
          (curr->entry.meta.type == DataType && curr->Validate()) || dirty(),
          "");
#endif  // KVDK_DEBUG_LEVEL > 0
    }

    bool dirty() const {
      return (curr != nullptr && owner->isRecordDirty(curr));
    }
  };

 public:
  // Connstruct an empty List on DRAM with no name and 0 as id
  // Must Init() or Restore() before further use.
  GenericList() : Collection{"", CollectionIDType{}} {}

  GenericList(GenericList const&) = delete;
  GenericList(GenericList&&) = delete;
  GenericList& operator=(GenericList const&) = delete;
  GenericList& operator=(GenericList&&) = delete;
  ~GenericList() {
    list_record = nullptr;
    alloc = nullptr;
  }

  // Initialize a List on PMem with PMEMAllocator a, pre-allocated space,
  // Creation time, List name and id.
  void Init(PMEMAllocator* a, SpaceEntry space, TimeStampType timestamp,
            StringView key, CollectionIDType id, LockTable* lt) {
    collection_name_.assign(key.data(), key.size());
    collection_id_ = id;
    alloc = a;
    list_record = DLRecord::PersistDLRecord(
        addressOf(space.offset), space.size, timestamp, ListType,
        NullPMemOffset, NullPMemOffset, NullPMemOffset, key, ID2String(id));
    lock_table = lt;
  }

  // Destroy the PMem record of an Empty List.
  template <typename DelayFree>
  void Destroy(DelayFree delay_free) {
    kvdk_assert(list_record != nullptr, "Destroy() uninitialized.");
    kvdk_assert(Size() == 0, "Only empty list can be Destroy()ed");
    kvdk_assert(!isRecordDirty(list_record), "double Destroy()!");
    markRecordAsDirty(list_record);
    delay_free(list_record);
  }

  // Restore a List already on PMem with its ListRecord, first and last element
  // and size This function is used by GenericListBuilder to restore the List.
  void Restore(PMEMAllocator* a, DLRecord* list_rec, DLRecord* fi, DLRecord* la,
               size_t n, LockTable* lt) {
    auto key = list_rec->Key();
    collection_name_.assign(key.data(), key.size());
    kvdk_assert(list_rec->Value().size() == sizeof(CollectionIDType), "");
    collection_id_ = ExtractID(list_rec->Value());
    alloc = a;
    list_record = list_rec;
    first = fi;
    last = la;
    sz.store(n);
    lock_table = lt;
  }

  LockType* Mutex() { return &mu; }

  std::unique_lock<LockType> AcquireLock() {
    return std::unique_lock<LockType>(mu);
  }

  ExpireTimeType GetExpireTime() const final {
    return list_record->GetExpireTime();
  }

  bool HasExpired() const final {
    return TimeUtils::CheckIsExpired(GetExpireTime());
  }

  Status SetExpireTime(ExpireTimeType time) final {
    list_record->PersistExpireTimeNT(time);
    return Status::Ok;
  }

  size_t Size() const { return sz.load(); }

  Iterator Front() const { return ++Head(); }

  Iterator Back() const { return --Tail(); }

  Iterator Head() const { return Iterator{this}; }

  Iterator Tail() const { return Iterator{this}; }

  Iterator Seek(std::int64_t index) {
    if (index >= 0) {
      auto iter = Front();
      while (index != 0 && iter != Tail()) {
        ++iter;
        --index;
      }
      return iter;
    } else {
      auto iter = Back();
      while (index != -1 && iter != Head()) {
        --iter;
        ++index;
      }
      return iter;
    }
  }

  template <typename DelayFree>
  Iterator Erase(Iterator pos, DelayFree delay_free) {
    return erase_impl(pos, delay_free);
  }

  // EraseWithLock() presumes that DLRecord* rec is secured by caller,
  // only one thread is calling EraseWithLock() on rec and rec is valid.
  template <typename DelayFree>
  Iterator EraseWithLock(DLRecord* rec, DelayFree delay_free) {
    Iterator pos{this, rec};
    LockTable::GuardType guard;
    lockPosAndPrev(pos, guard);
    return erase_impl(pos, delay_free);
  }

  template <typename DelayFree>
  void PopFront(DelayFree delay_free) {
    erase_impl(Front(), delay_free);
  }

  template <typename DelayFree>
  void PopBack(DelayFree delay_free) {
    erase_impl(Back(), delay_free);
  }

  Iterator EmplaceBefore(SpaceEntry space, Iterator pos,
                         TimeStampType timestamp, StringView key,
                         StringView value) {
    return emplace_impl(space, pos, timestamp, key, value);
  }

  Iterator EmplaceAfter(SpaceEntry space, Iterator pos, TimeStampType timestamp,
                        StringView key, StringView value) {
    return emplace_impl(space, ++pos, timestamp, key, value);
  }

  void PushFront(SpaceEntry space, TimeStampType timestamp, StringView key,
                 StringView value) {
    emplace_impl(space, Front(), timestamp, key, value);
  }

  // If new element is not locked, Delete() next may happens before
  // emplace_impl() returns, which is troublesome.
  // Thus we must lock the newly inserted element.
  void PushFrontWithLock(SpaceEntry space, TimeStampType timestamp,
                         StringView key, StringView value) {
    auto pos_hash = Iterator::Hash(addressOf(space.offset));
    auto guard = lock_table->MultiGuard({Head().Hash(), pos_hash});
    emplace_impl(space, Front(), timestamp, key, value);
  }

  void PushBack(SpaceEntry space, TimeStampType timestamp, StringView key,
                StringView value) {
    emplace_impl(space, Tail(), timestamp, key, value);
  }

  void PushBackWithLock(SpaceEntry space, TimeStampType timestamp,
                        StringView key, StringView value) {
    std::uint64_t pos_hash = Iterator::Hash(addressOf(space.offset));
    Iterator back = Back();
    LockTable::GuardType guard;
    while (true) {
      guard = lock_table->MultiGuard({back.Hash(), pos_hash});
      if (back != Back()) {
        guard.clear();
        back = Back();
        continue;
      }
      break;
    }
    emplace_impl(space, Tail(), timestamp, key, value);
  }

  template <typename DelayFree>
  Iterator Replace(SpaceEntry space, Iterator pos, TimeStampType timestamp,
                   StringView key, StringView value, DelayFree delay_free) {
    return replace_impl(space, pos, timestamp, key, value, delay_free);
  }

  // ReplaceWithLock() presumes that DLRecord* rec is secured by caller,
  // only one thread is calling ReplaceWithLock() on rec and rec is valid.
  template <typename DelayFree>
  Iterator ReplaceWithLock(SpaceEntry space, Iterator pos,
                           TimeStampType timestamp, StringView key,
                           StringView value, DelayFree delay_free) {
    LockTable::GuardType guard;
    lockPosAndPrev(pos, guard);
    return replace_impl(space, pos, timestamp, key, value, delay_free);
  }

  Iterator MakeIterator(DLRecord* rec) { return Iterator{this, rec}; }

 private:
  inline DLRecord* addressOf(PMemOffsetType offset) const {
    return static_cast<DLRecord*>(alloc->offset2addr_checked(offset));
  }

  inline PMemOffsetType offsetOf(DLRecord* rec) const {
    return alloc->addr2offset_checked(rec);
  }

  Iterator emplace_impl(SpaceEntry space, Iterator next,
                        TimeStampType timestamp, StringView key,
                        StringView value) {
    kvdk_assert(next == Tail() || ExtractID(next->Key()) == ID(),
                "Wrong List!");
    Iterator prev{next};
    --prev;
    kvdk_assert(!next.dirty() && !prev.dirty(), "");

    PMemOffsetType prev_off = (prev == Head()) ? NullPMemOffset : prev.Offset();
    PMemOffsetType next_off = (next == Tail()) ? NullPMemOffset : next.Offset();
    DLRecord* record = DLRecord::PersistDLRecord(
        addressOf(space.offset), space.size, timestamp, DataType,
        NullPMemOffset, prev_off, next_off, InternalKey(key), value);
    kvdk_assert(record->Validate(), "");

    if (prev == Head() && next == Tail()) {
      first = record;
      last = record;
    } else if (prev == Head()) {
      // PushFront()
      next->PersistPrevNT(space.offset);
      first = record;
    } else if (next == Tail()) {
      // PushBack()
      prev->PersistNextNT(space.offset);
      last = record;
    } else {
      // Emplace between two elements on PMem
      prev->PersistNextNT(space.offset);
      next->PersistPrevNT(space.offset);
    }
    ++sz;
    return Iterator{this, record};
  }

  template <typename DelayFree>
  Iterator erase_impl(Iterator pos, DelayFree delay_free) {
    kvdk_assert(pos != Head(), "Cannot erase Head()");
    kvdk_assert(Size() >= 1, "Cannot erase from empty List!");
    kvdk_assert(ExtractID(pos->Key()) == ID(), "Erase from wrong List!");

    Iterator prev{pos};
    --prev;
    Iterator next{pos};
    ++next;

    kvdk_assert(!prev.dirty() && !next.dirty(), "");

    kvdk_assert(Size() >= 1, "");
    if (prev == Head() && next == Tail()) {
      first = nullptr;
      last = nullptr;
    } else if (prev == Head()) {
      // Erase Front()
      first = next.Address();
      next->PersistPrevNT(NullPMemOffset);
    } else if (next == Tail()) {
      // Erase Back()
      last = prev.Address();
      prev->PersistNextNT(NullPMemOffset);
    } else {
      // Reverse procedure of emplace_impl() between two elements
      next->PersistPrevNT(prev.Offset());
      prev->PersistNextNT(next.Offset());
    }
    markRecordAsDirty(pos.Address());
    delay_free(pos.Address());
    --sz;
    return next;
  }

  template <typename DelayFree>
  Iterator replace_impl(SpaceEntry space, Iterator pos, TimeStampType timestamp,
                        StringView key, StringView value,
                        DelayFree delay_free) {
    kvdk_assert(ExtractID(pos->Key()) == ID(), "Wrong List!");
    Iterator prev{pos};
    --prev;
    Iterator next{pos};
    ++next;
    kvdk_assert(!prev.dirty() && !next.dirty(), "");

    PMemOffsetType prev_off = (prev == Head()) ? NullPMemOffset : prev.Offset();
    PMemOffsetType next_off = (next == Tail()) ? NullPMemOffset : next.Offset();
    DLRecord* record = DLRecord::PersistDLRecord(
        addressOf(space.offset), space.size, timestamp, DataType,
        NullPMemOffset, prev_off, next_off, InternalKey(key), value);
    if (prev == Head() && next == Tail()) {
      first = record;
      last = record;
    } else if (prev == Head()) {
      // Replace Front()
      first = record;
      next->PersistPrevNT(space.offset);
    } else if (next == Tail()) {
      // Replace Back()
      last = record;
      prev->PersistNextNT(space.offset);
    } else {
      prev->PersistNextNT(space.offset);
      next->PersistPrevNT(space.offset);
    }
    markRecordAsDirty(pos.Address());
    delay_free(pos.Address());
    return Iterator{this, record};
  }

  std::string serialize(DLRecord* rec) const {
    std::stringstream ss;
    ss << "Type:\t" << to_hex(rec->entry.meta.type) << "\t"
       << "Prev:\t" << to_hex(rec->prev) << "\t"
       << "Offset:\t" << to_hex(offsetOf(rec)) << "\t"
       << "Next:\t" << to_hex(rec->next) << "\t"
       << "ID:\t" << to_hex(ExtractID(rec->Key())) << "\t"
       << "Valid:\t" << rec->Validate() << "\t"
       << "Key: " << ExtractUserKey(rec->Key()) << "\t"
       << "Value: " << rec->Value();
    return ss.str();
  }

  friend std::ostream& operator<<(std::ostream& out, GenericList const& list) {
    out << "Contents of List:\n";
    for (Iterator iter = list.Front(); iter != list.Tail(); iter++) {
      out << list.serialize(iter.Address()) << "\n";
    }
    return out;
  }

  void lockPosAndPrev(Iterator pos, LockTable::GuardType& guard) {
    kvdk_assert(guard.empty(), "");
    Iterator prev{pos};
    --prev;
    guard = lock_table->MultiGuard({prev.Hash(), pos.Hash()});
    while (true) {
      Iterator prev_copy{pos};
      --prev_copy;
      if (prev != prev_copy) {
        guard.clear();
        prev = prev_copy;
        guard = lock_table->MultiGuard({prev.Hash(), pos.Hash()});
        continue;
      }
      kvdk_assert(++prev_copy == pos, "");
      break;
    }
  }

  friend class GenericListBuilder<ListType, DataType>;

  static void markRecordAsDirty(DLRecord* rec) {
    auto& entry = rec->entry;
    switch (entry.meta.type) {
      case RecordType::ListElem: {
        entry.meta.type = RecordType::ListDirtyElem;
        break;
      }
      case RecordType::HashElem: {
        entry.meta.type = RecordType::HashDirtyElem;
        break;
      }
      case RecordType::ListRecord: {
        entry.meta.type = RecordType::ListDirtyRecord;
        break;
      }
      case RecordType::HashRecord: {
        entry.meta.type = RecordType::HashDirtyRecord;
        break;
      }
      default: {
        kvdk_assert(false, "Unsupported!");
        std::abort();
      }
    }
    _mm_clwb(&entry.meta.type);
    _mm_mfence();
  }

  static void cleanRecordDirtyMark(DLRecord* rec) {
    auto& entry = rec->entry;
    switch (entry.meta.type) {
      case RecordType::ListDirtyElem: {
        entry.meta.type = RecordType::ListElem;
        break;
      }
      case RecordType::HashDirtyElem: {
        entry.meta.type = RecordType::HashElem;
        break;
      }
      case RecordType::ListDirtyRecord: {
        entry.meta.type = RecordType::ListRecord;
        break;
      }
      case RecordType::HashDirtyRecord: {
        entry.meta.type = RecordType::HashRecord;
        break;
      }
      default: {
        kvdk_assert(false, "Unsupported!");
        std::abort();
      }
    }
    _mm_clwb(&entry.meta.type);
    _mm_mfence();
  }

  static bool isRecordDirty(DLRecord* rec) {
    auto& entry = rec->entry;
    return (entry.meta.type == RecordType::ListDirtyElem) ||
           (entry.meta.type == RecordType::HashDirtyElem) ||
           (entry.meta.type == RecordType::ListDirtyRecord) ||
           (entry.meta.type == RecordType::HashDirtyRecord);
  }
};

template <RecordType ListType, RecordType DataType>
class GenericListBuilder final {
  static constexpr size_t NMiddlePoints = 1024;
  using List = GenericList<ListType, DataType>;

  PMEMAllocator* alloc;
  // Number of workers to count List elements
  size_t n_worker;
  std::set<List*, Collection::TTLCmp>* rebuilded_lists;
  LockTable* lock_table;
  // Resevoir for middle points
  // Middle points can be used for multi-thread interating through Lists
  std::atomic_uint64_t mpoint_cnt{0U};
  std::array<DLRecord*, NMiddlePoints> mpoints{};

  struct ListPrimer {
    DLRecord* list_record{nullptr};
    DLRecord* first{nullptr};
    DLRecord* last{nullptr};
    std::atomic_uint64_t size{0U};

    ListPrimer() = default;
    ListPrimer(ListPrimer const& other)
        : list_record{other.list_record},
          first{other.first},
          last{other.last},
          size{other.size.load()} {}
    ListPrimer(ListPrimer&& other) = delete;
    ListPrimer& operator=(ListPrimer const&) = delete;
    ListPrimer& operator=(ListPrimer&&) = delete;
    ~ListPrimer() = default;
  };
  std::vector<ListPrimer> primers{};
  RWLock primers_lock;

  std::vector<DLRecord*> brokens{};
  std::mutex brokens_lock{};

  // There are 4 types of DLRecord
  // 0. Unique: prev == Null && next == Null
  // 1. First: prev == Null && next != Null
  // 2. Last: prev != Null && next == Null
  // 3. Middle: prev != Null && next != Null
  enum class ListRecordType { Unique, First, Last, Middle };

  // There are several types of failure during GenericList operations
  // 1. PushFront/PushBack/PopFront/PopBack Failure
  //    Interruption in Pop() after unlink before purge, same as
  //    Interruption in Push() after persist before link
  //    For uniformity with 1-elem Pop crash, which cannot be detected
  //    Discard the node not linked to/unlinked from List
  // 2. ReplaceFront/ReplaceBack Failure
  //    Discard the unlinked node
  // 3. Replace Failure
  //    a. Node not linked from its prev and next purged directly
  //    b. Node not linked from prev but linked from next, this node is older
  //       and is saved to broken
  //       After all nodes repaired, this node is unlinked from prev and next
  //       and can be purged
  //    c. Node linked from prev but not linked from next, this node is newer
  //       and is repaired into List
  // 4. Insertion/Erase Failure
  //    a. Node not linked from its prev and next purged directly
  //    b. Node linked from prev but not linked from next is repaired into List
  // In conclusion,
  //    After a node is half linked into List, it will be repaired into List.
  //    This prevents dirty reads from iterator.

 public:
  explicit GenericListBuilder(PMEMAllocator* a,
                              std::set<List*, Collection::TTLCmp>* lists,
                              size_t num_worker, LockTable* lt)
      : alloc{a}, n_worker{num_worker}, rebuilded_lists{lists}, lock_table{lt} {
    kvdk_assert(lists != nullptr && lists->empty(), "");
    kvdk_assert(n_worker != 0, "");
    kvdk_assert(rebuilded_lists != nullptr, "Empty input!");
  }

  bool AddListRecord(DLRecord* lrec) {
    kvdk_assert(lrec->entry.meta.type == ListType, "");
    kvdk_assert(lrec->Value().size() == sizeof(CollectionIDType), "");
    CollectionIDType id = Collection::ExtractID(lrec->Value());
    maybeResizePrimers(id);

    primers_lock.lock_shared();
    auto& primer = primers.at(id);
    kvdk_assert(primer.list_record == nullptr, "");
    primer.list_record = lrec;
    primers_lock.unlock_shared();
    return true;
  }

  bool AddListElem(DLRecord* elem) {
    kvdk_assert(elem->entry.meta.type == DataType, "");
    switch (typeOf(elem)) {
      case ListRecordType::Unique: {
        return addUniqueElem(elem);
      }
      case ListRecordType::First: {
        return addFirstElem(elem);
      }
      case ListRecordType::Last: {
        return addLastElem(elem);
      }
      case ListRecordType::Middle: {
        return addMiddleElem(elem);
      }
      default: {
        kvdk_assert(false, "Unreachable");
        std::abort();
      }
    }
  }

  void RollbackReplacement(DLRecord* new_elem, DLRecord* old_elem) {
    kvdk_assert(new_elem->Validate(), "No need to rollback invalid element");
    kvdk_assert(old_elem->prev == new_elem->prev, "");
    kvdk_assert(old_elem->next == new_elem->next, "");

    if (old_elem->entry.meta.type == ListDirtyElem, "")
    {
      kvdk_assert(addressOf(old_elem->next)->prev == offsetOf(new_elem), "");
      kvdk_assert(addressOf(old_elem->prev)->next == offsetOf(new_elem), "");
      List::cleanRecordDirtyMark(old_elem);
    }
    kvdk_assert(old_elem->Validate(), "");

    // Link next back to old_elem
    if (addressOf(old_elem->next)->prev != offsetOf(old_elem)) {
      kvdk_assert(addressOf(old_elem->next)->prev == offsetOf(new_elem), "");
      kvdk_assert(addressOf(old_elem->prev)->next == offsetOf(new_elem), "");
      addressOf(old_elem->next)->PersistPrevNT(offsetOf(old_elem));
    }

    if (addressOf(old_elem->prev)->next != offsetOf(old_elem)) {
      kvdk_assert(addressOf(old_elem->next)->prev == offsetOf(new_elem), "");
      addressOf(old_elem->prev)->PersistNextNT(offsetOf(old_elem));
    }

    List::markRecordAsDirty(new_elem);
  }

  void RollbackEmplacement(DLRecord* new_elem) {
    kvdk_assert(new_elem->Validate(), "No need to rollback invalid element");

    // Unlink from next, like deletion
    if (addressOf(new_elem->next)->prev == offsetOf(new_elem)) {
      kvdk_assert(addressOf(new_elem->prev)->next == offsetOf(new_elem), "");
      addressOf(old_elem->next)->PersistPrevNT(new_elem->prev);
    }

    // Unlink from prev, like deletion
    if (addressOf(new_elem->prev)->next == offsetOf(new_elem)) {
      kvdk_assert(addressOf(new_elem->next)->prev == new_elem->prev, "");
      addressOf(old_elem->prev)->PersistNextNT(new_elem->next);
    }

    List::markRecordAsDirty(new_elem);
  }

  void RollbackDeletion(DLRecord* old_elem) {
    // Clean dirty mark
    if (old_elem->entry.meta.type == ListDirtyElem, "")
    {
      List::cleanRecordDirtyMark(old_elem);
    }
    kvdk_assert(old_elem->Validate(), "");

    // Link to prev, like insertion
    if (addressOf(old_elem->prev)->next != offsetOf(old_elem))
    {
      kvdk_assert(addressOf(old_elem->prev)->next == old_elem->next, "");
      kvdk_assert(addressOf(old_elem->next)->prev == old_elem->prev, "");
      addressOf(old_elem->prev)->PersistNextNT(offsetOf(old_elem));
    }
    
    // Link to next, like insertion
    if (addressOf(old_elem->next)->prev != offsetOf(old_elem)) {
      kvdk_assert(addressOf(old_elem->next)->prev == old_elem->prev, "");
      addressOf(old_elem->next)->PersistPrevNT(offsetOf(old_elem));
    }
  }

  void RebuildLists() {
    countListElems();
    for (auto const& primer : primers) {
      if (primer.list_record == nullptr) {
        kvdk_assert(primer.first == nullptr, "");
        kvdk_assert(primer.last == nullptr, "");
        kvdk_assert(primer.size.load() == 0U, "");
        continue;
      }
      List* list = new List{};
      list->Restore(alloc, primer.list_record, primer.first, primer.last,
                    primer.size.load(), lock_table);
      rebuilded_lists->emplace(list);
    }
  }

  template <typename FreeFunc>
  void CleanBrokens(FreeFunc free_func) {
    for (DLRecord* elem : brokens) {
      switch (typeOf(elem)) {
        case ListRecordType::Unique: {
          kvdk_assert(false, "Impossible!");
          break;
        }
        case ListRecordType::First: {
          kvdk_assert(!isValidFirst(elem), "");
          break;
        }
        case ListRecordType::Last: {
          kvdk_assert(!isValidLast(elem), "");
          break;
        }
        case ListRecordType::Middle: {
          kvdk_assert(isDiscardedMiddle(elem), "");
          break;
        }
      }
      List::markRecordAsDirty(elem);
      free_func(elem);
    }
  }

 private:
  inline DLRecord* addressOf(PMemOffsetType offset) const {
    return static_cast<DLRecord*>(alloc->offset2addr_checked(offset));
  }

  inline PMemOffsetType offsetOf(DLRecord* rec) const {
    return alloc->addr2offset_checked(rec);
  }

  ListRecordType typeOf(DLRecord* elem) {
    if (elem->prev == NullPMemOffset) {
      if (elem->next == NullPMemOffset) {
        return ListRecordType::Unique;
      } else {
        return ListRecordType::First;
      }
    } else {
      if (elem->next == NullPMemOffset) {
        return ListRecordType::Last;
      } else {
        return ListRecordType::Middle;
      }
    }
  }

  bool addUniqueElem(DLRecord* elem) {
    kvdk_assert(elem->prev == NullPMemOffset && elem->next == NullPMemOffset,
                "Not UniqueElem!");

    CollectionIDType id = Collection::ExtractID(elem->Key());
    maybeResizePrimers(id);

    auto guard = LockShared(primers_lock);
    auto& primer = primers.at(id);
    kvdk_assert(primer.first == nullptr, "");
    kvdk_assert(primer.last == nullptr, "");
    primer.first = elem;
    primer.last = elem;

    return true;
  }

  bool addFirstElem(DLRecord* elem) {
    kvdk_assert(elem->prev == NullPMemOffset && elem->next != NullPMemOffset,
                "Not FirstElem!");

    if (!isValidFirst(elem)) {
      std::lock_guard<std::mutex> guard{brokens_lock};
      brokens.push_back(elem);
      return false;
    }

    CollectionIDType id = Collection::ExtractID(elem->Key());
    maybeResizePrimers(id);

    auto guard = LockShared(primers_lock);
    auto& primer = primers.at(id);
    kvdk_assert(primer.first == nullptr, "");
    primer.first = elem;

    return true;
  }

  bool addLastElem(DLRecord* elem) {
    kvdk_assert(elem->next == NullPMemOffset && elem->prev != NullPMemOffset,
                "Not LastElem!");

    if (!isValidLast(elem)) {
      std::lock_guard<std::mutex> guard{brokens_lock};
      brokens.push_back(elem);
      return false;
    }

    CollectionIDType id = Collection::ExtractID(elem->Key());
    maybeResizePrimers(id);

    auto guard = LockShared(primers_lock);
    auto& primer = primers.at(id);
    kvdk_assert(primer.last == nullptr, "");
    primer.last = elem;

    return true;
  }

  bool addMiddleElem(DLRecord* elem) {
    kvdk_assert(elem->prev != NullPMemOffset && elem->next != NullPMemOffset,
                "Not MiddleElem!");

    if (!maybeTryFixMiddle(elem)) {
      std::lock_guard<std::mutex> guard{brokens_lock};
      brokens.push_back(elem);
      return false;
    }

    thread_local std::default_random_engine rengine{get_seed()};
    thread_local size_t local_cnt = 0;
    if (local_cnt++ % 1024 == 0) {
      // Reservoir algorithm to add middle points
      auto cnt = mpoint_cnt.fetch_add(1U);
      auto pos = cnt % NMiddlePoints;
      auto k = cnt / NMiddlePoints;
      // k-th point has posibility 1/(k+1) to replace previous point in
      // reservoir
      if (std::bernoulli_distribution{1.0 /
                                      static_cast<double>(k + 1)}(rengine)) {
        mpoints.at(pos) = elem;
      }
    }

    return true;
  }

  bool isValidFirst(DLRecord* elem) {
    return (addressOf(elem->next)->prev == offsetOf(elem));
  }

  bool isValidLast(DLRecord* elem) {
    return (addressOf(elem->prev)->next == offsetOf(elem));
  }

  // Check for discarded Middle
  bool isDiscardedMiddle(DLRecord* elem) {
    kvdk_assert(typeOf(elem) == ListRecordType::Middle, "Not a middle");
    return (offsetOf(elem) != addressOf(elem->prev)->next) &&
           (offsetOf(elem) != addressOf(elem->next)->prev);
  }

  // When false is returned, the node is put in temporary pool
  // and processed after all restoration is done
  bool maybeTryFixMiddle(DLRecord* elem) {
    if (offsetOf(elem) == addressOf(elem->prev)->next) {
      if (offsetOf(elem) == addressOf(elem->next)->prev) {
        // Normal Middle
        return true;
      } else {
        // Interrupted Replace/Emplace(newer),
        // repair newer record into List
        addressOf(elem->next)->PersistPrevNT(offsetOf(elem));
        return true;
      }
    } else {
      if (offsetOf(elem) == addressOf(elem->next)->prev) {
        // Interrupted Replace/Emplace(older),
        // discard older record
        return false;
      } else {
        // Not linked into List, discard
        return false;
      }
    }
  }

  void maybeResizePrimers(CollectionIDType id) {
    if (id >= primers.size()) {
      std::lock_guard<decltype(primers_lock)> guard{primers_lock};
      for (size_t i = primers.size(); i < (id + 1) * 3 / 2; i++) {
        primers.emplace_back();
      }
    }
  }

  void countListElems() {
    std::unordered_set<DLRecord*> start_points;
    for (auto const& primer : primers) {
      if (primer.list_record == nullptr) {
        continue;
      }
      start_points.emplace(primer.first);
    }
    for (DLRecord* mpoint : mpoints) {
      start_points.emplace(mpoint);
    }

    std::vector<std::vector<DLRecord*>> workloads{n_worker};
    size_t j = 0;
    for (DLRecord* rec : start_points) {
      if (rec != nullptr) {
        workloads[j++ % workloads.size()].push_back(rec);
      }
    }
    auto CountElems = [&](std::vector<DLRecord*> const& work_load) {
      std::unordered_map<CollectionIDType, size_t> cache{1024};
      for (DLRecord* rec : work_load) {
        while (true) {
          CollectionIDType id = Collection::ExtractID(rec->Key());
          ++cache[id];
          if (cache.load_factor() > 0.7) {
            for (auto const& pair : cache) {
              primers.at(pair.first).size.fetch_add(pair.second);
            }
            cache.clear();
          }
          if (rec->next == NullPMemOffset) {
            break;
          }
          rec = addressOf(rec->next);
          if (start_points.find(rec) != start_points.end()) {
            break;
          }
        }
      }
      for (auto const& pair : cache) {
        primers.at(pair.first).size.fetch_add(pair.second);
      }
    };
    std::vector<std::thread> workers;
    for (auto const& workload : workloads) {
      workers.emplace_back(CountElems, workload);
    }
    for (auto& worker : workers) {
      worker.join();
    }
  }
};

}  // namespace KVDK_NAMESPACE
