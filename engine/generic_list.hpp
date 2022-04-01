/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <immintrin.h>
#include <libpmem.h>
#include <x86intrin.h>

#include <array>
#include <atomic>
#include <deque>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <stdexcept>
#include <unordered_map>

#include "alias.hpp"
#include "collection.hpp"
#include "data_record.hpp"
#include "macros.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

constexpr PMemOffsetType NullPMemOffset = kNullPMemOffset;

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
  size_t sz{0U};
  // Recursive mutex to lock the List
  using LockType = std::recursive_mutex;
  LockType mu;

 public:
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
    explicit Iterator(GenericList const* o) : owner{o} {
      kvdk_assert(owner != nullptr, "Invalid iterator!");
    }

    Iterator() = delete;
    Iterator(Iterator const&) = default;
    Iterator(Iterator&&) = default;
    Iterator& operator=(Iterator const&) = default;
    Iterator& operator=(Iterator&&) = default;
    ~Iterator() = default;

    /// Increment and Decrement operators
    Iterator& operator++() {
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
      return *this;
    }

    Iterator const operator++(int) {
      Iterator old{*this};
      this->operator++();
      return old;
    }

    Iterator& operator--() {
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
      return *this;
    }

    Iterator const operator--(int) {
      Iterator old{*this};
      this->operator--();
      return old;
    }

    DLRecord& operator*() { return *curr; }

    DLRecord* operator->() { return curr; }

    PMemOffsetType Offset() const { return owner->offsetOf(Address()); }

    DLRecord* Address() const {
      kvdk_assert(curr != nullptr, "Trying to address dummy Iterator Head()!");
      return curr;
    }

   private:
    friend bool operator==(Iterator const& lhs, Iterator const& rhs) {
      return (lhs.owner == rhs.owner) && (lhs.curr == rhs.curr);
    }

    friend bool operator!=(Iterator const& lhs, Iterator const& rhs) {
      return !(lhs == rhs);
    }
  };

 public:
  // Default to an empty list with no name and 0 as id
  // Must Init() or Restore() before further use.
  GenericList() : Collection{"", CollectionIDType{}} {}

  GenericList(GenericList const&) = delete;
  GenericList(GenericList&&) = delete;
  GenericList& operator=(GenericList const&) = delete;
  GenericList& operator=(GenericList&&) = delete;
  ~GenericList() = default;

  // Initialize a List with pmem base address p_base, pre-space space,
  // Creation time, List name and id.
  void Init(PMEMAllocator* a, SpaceEntry space, TimeStampType timestamp,
            StringView const key, CollectionIDType id) {
    collection_name_.assign(key.data(), key.size());
    collection_id_ = id;
    alloc = a;
    list_record = DLRecord::PersistDLRecord(
        addressOf(space.offset), space.size, timestamp, RecordType::ListRecord,
        NullPMemOffset, NullPMemOffset, NullPMemOffset, key, ID2String(id));
  }

  template <typename ListDeleter>
  void Destroy(ListDeleter list_deleter) {
    kvdk_assert(sz == 0 && list_record != nullptr && first == nullptr &&
                    last == nullptr,
                "Only initialized empty List can be destroyed!");
    list_deleter(list_record);
    list_record = nullptr;
    alloc = nullptr;
  }

  bool Valid() const { return (list_record != nullptr); }

  // Restore a List with its ListRecord, first and last element and size
  // This function is used by GenericListBuilder to restore the List
  void Restore(PMEMAllocator* a, DLRecord* list_rec, DLRecord* fi, DLRecord* la,
               size_t n) {
    auto key = list_rec->Key();
    collection_name_.assign(key.data(), key.size());
    kvdk_assert(list_rec->Value().size() == sizeof(CollectionIDType), "");
    collection_id_ = ExtractID(list_rec->Value());
    alloc = a;
    list_record = list_rec;
    first = fi;
    last = la;
    sz = n;
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

  size_t Size() const { return sz; }

  Iterator Front() { return ++Head(); }

  Iterator Back() { return --Tail(); }

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

  template <typename ElemDeleter>
  Iterator Erase(Iterator pos, ElemDeleter elem_deleter) {
    kvdk_assert(pos != Head(), "Cannot erase Head()");
    kvdk_assert(sz >= 1, "Cannot erase from empty List!");
    kvdk_assert(ExtractID(pos->Key()) == ID(), "Erase from wrong List!");

    Iterator prev{pos};
    --prev;
    Iterator next{pos};
    ++next;

    if (sz == 1) {
      kvdk_assert(prev == Head() && next == Tail(), "Impossible!");
      first = nullptr;
      last = nullptr;
    } else if (prev == Head()) {
      // Erase Front()
      kvdk_assert(next != Tail(), "");
      first = next.Address();
      next->PersistPrevNT(NullPMemOffset);
    } else if (next == Tail()) {
      // Erase Back()
      kvdk_assert(prev != Head(), "");
      last = prev.Address();
      prev->PersistNextNT(NullPMemOffset);
    } else {
      kvdk_assert(prev != Head() && next != Tail(), "");
      // Reverse procedure of emplace_between() between two elements
      next->PersistPrevNT(prev.Offset());
      prev->PersistNextNT(next.Offset());
    }
    elem_deleter(pos.Address());
    --sz;
    return next;
  }

  template <typename ElemDeleter>
  void PopFront(ElemDeleter elem_deleter) {
    Erase(Front(), elem_deleter);
  }

  template <typename ElemDeleter>
  void PopBack(ElemDeleter elem_deleter) {
    Erase(Back(), elem_deleter);
  }

  void EmplaceBefore(SpaceEntry space, Iterator pos, TimeStampType timestamp,
                     StringView const key, StringView const value) {
    Iterator prev{pos};
    --prev;
    Iterator next{pos};
    emplace_between(space, prev, next, timestamp, key, value);
    ++sz;
  }

  void EmplaceAfter(SpaceEntry space, Iterator pos, TimeStampType timestamp,
                    StringView const key, StringView const value) {
    Iterator prev{pos};
    Iterator next{pos};
    ++next;
    emplace_between(space, prev, next, timestamp, key, value);
    ++sz;
  }

  void PushFront(SpaceEntry space, TimeStampType timestamp,
                 StringView const key, StringView const value) {
    emplace_between(space, Head(), Front(), timestamp, key, value);
    ++sz;
  }

  void PushBack(SpaceEntry space, TimeStampType timestamp, StringView const key,
                StringView const value) {
    emplace_between(space, Back(), Tail(), timestamp, key, value);
    ++sz;
  }

  template <typename ElemDeleter>
  void Replace(SpaceEntry space, Iterator pos, TimeStampType timestamp,
               StringView const key, StringView const value,
               ElemDeleter elem_deleter) {
    kvdk_assert(ExtractID(pos->Key()) == ID(), "Wrong List!");
    Iterator prev{pos};
    --prev;
    Iterator next{pos};
    ++next;
    emplace_between(space, prev, next, timestamp, key, value);
    elem_deleter(pos.Address());
  }

 private:
  inline DLRecord* addressOf(PMemOffsetType offset) const {
    return static_cast<DLRecord*>(alloc->offset2addr_checked(offset));
  }

  inline PMemOffsetType offsetOf(DLRecord* rec) const {
    return alloc->addr2offset_checked(rec);
  }

  Iterator emplace_between(SpaceEntry space, Iterator prev, Iterator next,
                           TimeStampType timestamp, StringView const key,
                           StringView const value) {
    kvdk_assert(++Iterator{prev} == next || ++++Iterator{prev} == next,
                "Should only insert or replace");

    PMemOffsetType prev_off = (prev == Head()) ? NullPMemOffset : prev.Offset();
    PMemOffsetType next_off = (next == Tail()) ? NullPMemOffset : next.Offset();
    DLRecord* record = DLRecord::PersistDLRecord(
        addressOf(space.offset), space.size, timestamp, DataType,
        NullPMemOffset, prev_off, next_off, InternalKey(key), value);

    if (sz == 0) {
      kvdk_assert(prev == Head() && next == Tail(), "Impossible!");
      first = record;
      last = record;
    } else if (prev == Head()) {
      // PushFront()
      kvdk_assert(next != Tail(), "");
      next->PersistPrevNT(space.offset);
      first = record;
    } else if (next == Tail()) {
      // PushBack()
      kvdk_assert(prev != Head(), "");
      prev->PersistNextNT(space.offset);
      last = record;
    } else {
      // Emplace between two elements on PMem
      kvdk_assert(prev != Head() && next != Tail(), "");
      prev->PersistNextNT(space.offset);
      next->PersistPrevNT(space.offset);
    }

    return (next == Tail()) ? --next : ++prev;
  }

  friend std::ostream& operator<<(std::ostream& out, GenericList const& list) {
    auto printRecord = [&](DLRecord* record) {
      out << "Type:\t" << to_hex(record->entry.meta.type) << "\t"
          << "Prev:\t" << to_hex(record->prev) << "\t"
          << "Offset:\t"
          << to_hex(reinterpret_cast<char*>(record) - list.pmem_base) << "\t"
          << "Next:\t" << to_hex(record->next) << "\t"
          << "ID:\t" << to_hex(Collection::ExtractID(record->Key())) << "\t"
          << "Key: " << Collection::ExtractUserKey(record->Key()) << "\t"
          << "Value: " << record->Value() << "\n";
    };

    out << "Contents of List:\n";
    Iterator iter = list.Head();
    for (Iterator iter = list.Head(); iter != list.Tail(); iter++) {
      printRecord(iter.record);
    }
    printRecord(iter.record);
    return out;
  }
};

template <RecordType ListType, RecordType DataType>
class GenericListBuilder final {
  static constexpr size_t NMiddlePoints = 1024;
  using List = GenericList<ListType, DataType>;

  PMEMAllocator* alloc;
  size_t n_worker{0};
  std::mutex mu;
  std::vector<std::unique_ptr<List>>* rebuilded_lists{nullptr};

  // Resevoir for middle points
  // Middle points can be used for multi-thread interating through Lists
  std::atomic_uint64_t mpoint_cnt{0U};
  std::array<DLRecord*, NMiddlePoints> mpoints{};

  struct ListPrimer {
    DLRecord* list_record{nullptr};
    DLRecord* unique{nullptr};
    DLRecord* first{nullptr};
    DLRecord* last{nullptr};
    std::atomic_uint64_t size{0U};

    ListPrimer() = default;
    ListPrimer(ListPrimer const& other)
        : list_record{other.list_record},
          unique{other.unique},
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
  //    b. Node not linked from prev but linked from next is linked from prev
  //       This node is older
  //    c. Node linked from prev but not linked from next is saved to broken
  //       This node is newer
  //       After all nodes repaired, this node is unlinked from prev and next
  //       and can be purged
  // 4. Insertion/Erase Failure
  //    a. Node not linked from its prev and next purged directly
  //    b. Node not linked from prev but linked from next is linked from prev
  //    c. Node linked from prev but not linked from next is saved to broken
  //       After all nodes repaired, this node is unlinked from prev and next
  //       and can be purged
  // In conclusion,
  // All unsuccessful Emplace/Push/Replace(before fully linked) will rollback
  // All unsuccessful Erase/Pop will be finished
  // This way, we avoid generating duplicate First/Last elements, which will
  // complicate the recovery procedure

 public:
  explicit GenericListBuilder(PMEMAllocator* a,
                              std::vector<std::unique_ptr<List>>* lists,
                              size_t num_worker)
      : alloc{a}, n_worker{num_worker}, rebuilded_lists{lists} {
    kvdk_assert(lists != nullptr && lists->empty(), "");
    kvdk_assert(n_worker != 0, "");
    kvdk_assert(rebuilded_lists != nullptr, "Empty input!");
  }

  void AddListRecord(DLRecord* lrec) {
    kvdk_assert(lrec->Value().size() == sizeof(CollectionIDType), "");
    CollectionIDType id = Collection::ExtractID(lrec->Value());
    maybeResizePrimers(id);

    primers_lock.lock_shared();
    kvdk_assert(primers.at(id).list_record == nullptr, "");
    primers.at(id).list_record = lrec;
    primers_lock.unlock_shared();
  }

  void AddListElem(DLRecord* elem) {
    kvdk_assert(elem->entry.meta.type == DataType, "");
    switch (typeOf(elem)) {
      case ListRecordType::Unique: {
        addUniqueElem(elem);
        break;
      }
      case ListRecordType::First: {
        addFirstElem(elem);
        break;
      }
      case ListRecordType::Last: {
        addLastElem(elem);
        break;
      }
      case ListRecordType::Middle: {
        addMiddleElem(elem);
        break;
      }
    }
    return;
  }

  template <typename Func>
  void ProcessCachedElems(Func f, void* args) {
    f(mpoints, args);
    return;
  }

  void RebuildLists() {
    for (auto const& primer : primers) {
      if (primer.list_record == nullptr) {
        kvdk_assert(primer.first == nullptr, "");
        kvdk_assert(primer.last == nullptr, "");
        kvdk_assert(primer.unique == nullptr, "");
        kvdk_assert(primer.size.load() == 0U, "");
        continue;
      }

      rebuilded_lists->emplace_back(new List{});
      switch (primer.size.load()) {
        case 0: {
          // Empty List
          kvdk_assert(primer.first == nullptr, "");
          kvdk_assert(primer.last == nullptr, "");
          kvdk_assert(primer.unique == nullptr, "");
          kvdk_assert(primer.size.load() == 0, "");
          rebuilded_lists->back()->Restore(alloc, primer.list_record, nullptr,
                                           nullptr, 0);
          break;
        }
        case 1: {
          // 1-elem List
          kvdk_assert(primer.first == nullptr, "");
          kvdk_assert(primer.last == nullptr, "");
          kvdk_assert(primer.unique != nullptr, "");
          kvdk_assert(primer.size.load() == 1, "");
          rebuilded_lists->back()->Restore(alloc, primer.list_record,
                                           primer.unique, primer.unique, 1);
          break;
        }
        default: {
          // k-elem List
          kvdk_assert(primer.first != nullptr, "");
          kvdk_assert(primer.last != nullptr, "");
          kvdk_assert(primer.unique == nullptr, "");
          rebuilded_lists->back()->Restore(alloc, primer.list_record,
                                           primer.first, primer.last,
                                           primer.size.load());
          break;
        }
      }
    }
  }

  template <typename ElemDeleter>
  void CleanBrokens(ElemDeleter elem_deleter) {
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
      elem_deleter(elem);
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

  void addUniqueElem(DLRecord* elem) {
    kvdk_assert(elem->prev == NullPMemOffset && elem->next == NullPMemOffset,
                "Not UniqueElem!");

    CollectionIDType id = Collection::ExtractID(elem->Key());
    maybeResizePrimers(id);

    primers_lock.lock_shared();
    kvdk_assert(primers.at(id).unique == nullptr, "");
    kvdk_assert(primers.at(id).first == nullptr, "");
    kvdk_assert(primers.at(id).last == nullptr, "");
    primers.at(id).unique = elem;
    primers.at(id).size.fetch_add(1U);
    primers_lock.unlock_shared();
  }

  void addFirstElem(DLRecord* elem) {
    kvdk_assert(elem->prev == NullPMemOffset && elem->next != NullPMemOffset,
                "Not FirstElem!");

    if (!isValidFirst(elem)) {
      std::lock_guard<std::mutex> guard{brokens_lock};
      brokens.push_back(elem);
      return;
    }

    CollectionIDType id = Collection::ExtractID(elem->Key());
    maybeResizePrimers(id);

    primers_lock.lock_shared();
    kvdk_assert(primers.at(id).first == nullptr, "");
    kvdk_assert(primers.at(id).unique == nullptr, "");
    primers.at(id).first = elem;
    primers.at(id).size.fetch_add(1U);
    primers_lock.unlock_shared();
  }

  void addLastElem(DLRecord* elem) {
    kvdk_assert(elem->next == NullPMemOffset && elem->prev != NullPMemOffset,
                "Not LastElem!");

    if (!isValidLast(elem)) {
      std::lock_guard<std::mutex> guard{brokens_lock};
      brokens.push_back(elem);
      return;
    }

    CollectionIDType id = Collection::ExtractID(elem->Key());
    maybeResizePrimers(id);

    primers_lock.lock_shared();
    kvdk_assert(primers.at(id).last == nullptr, "");
    kvdk_assert(primers.at(id).unique == nullptr, "");
    primers.at(id).last = elem;
    primers.at(id).size.fetch_add(1U);
    primers_lock.unlock_shared();
  }

  // Reservoir algorithm
  void addMiddleElem(DLRecord* elem) {
    kvdk_assert(elem->prev != NullPMemOffset && elem->next != NullPMemOffset,
                "Not MiddleElem!");

    if (!maybeTryFixMiddle(elem)) {
      std::lock_guard<std::mutex> guard{brokens_lock};
      brokens.push_back(elem);
      return;
    }

    CollectionIDType id = Collection::ExtractID(elem->Key());
    maybeResizePrimers(id);

    primers_lock.lock_shared();
    primers.at(id).size.fetch_add(1U);
    primers_lock.unlock_shared();

    thread_local std::default_random_engine rengine{get_seed()};

    auto cnt = mpoint_cnt.fetch_add(1U);
    auto pos = cnt % NMiddlePoints;
    auto k = cnt / NMiddlePoints;
    // k-th point has posibility 1/(k+1) to replace previous point in reservoir
    if (std::bernoulli_distribution{1.0 /
                                    static_cast<double>(k + 1)}(rengine)) {
      mpoints.at(pos) = elem;
    }
    return;
  }

  bool isValidFirst(DLRecord* elem) {
    if (addressOf(elem->next)->prev == NullPMemOffset) {
      // Interrupted PushFront()/PopFront()
      return false;
    } else if (addressOf(elem->next)->prev == offsetOf(elem)) {
      return true;
    } else {
      // Interrupted ReplaceFront()
      kvdk_assert(addressOf(addressOf(elem->next)->prev)->next == elem->next,
                  "");
      return false;
    }
  }

  bool isValidLast(DLRecord* elem) {
    if (addressOf(elem->prev)->next == NullPMemOffset) {
      // Interrupted PushBack()/PopBack()
      return false;
    } else if (addressOf(elem->prev)->next == offsetOf(elem)) {
      return true;
    } else {
      // Interrupted ReplaceBack()
      kvdk_assert(addressOf(addressOf(elem->prev)->next)->prev == elem->prev,
                  "");
      return false;
    }
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
        // Interrupted Replace/Emplace(newer), discard
        return false;
      }
    } else {
      if (offsetOf(elem) == addressOf(elem->next)->prev) {
        // Interrupted Replace/Emplace(older), repair into List
        addressOf(elem->prev)->PersistNextNT(offsetOf(elem));
        return true;
      } else {
        // Un-purged, discard
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
};

}  // namespace KVDK_NAMESPACE
