/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <atomic>
#include <array>
#include <deque>
#include <mutex>
#include <random>
#include <unordered_map>
#include <stdexcept>

#include <immintrin.h>
#include <x86intrin.h>

#include <libpmem.h>

#include <libpmemobj++/string_view.hpp>

#include "kvdk/collection.hpp"

#include "alias.hpp"
#include "macros.hpp"
#include "data_record.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

constexpr PMemOffsetType NullPMemOffset = kNullPMemOffset;
template <RecordType ListType, RecordType DataType>
class GenericList final : public Collection {
 private:
  // For offset-address translation
  char* pmem_base;
  // Persisted ListType on PMem, contains List name(key) and id(value).
  StringRecord* list_record;
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
    GenericList* owner;
    // curr == nullptr indicates Head() and Tail()
    DLRecord* curr{nullptr};

   public:
    // Always start at Head()/Tail()
    // Head() and Tail() is the same state, 
    // just for convention this state has two names
    // Tail() for iterating forward
    // Head() for iterating backward
    explicit Iterator(GenericList* o) : owner{o} 
    {
      kvdk_assert(owner != nullptr, "Invalid iterator!");
    }
    
    Iterator() = delete;
    Iterator(Iterator const&) = default;
    Iterator(Iterator &&) = default;
    Iterator& operator=(Iterator const&) = default;
    Iterator& operator=(Iterator &&) = default;

    /// Increment and Decrement operators
    Iterator& operator++() {
      if (curr == nullptr)
      {
        // Head(), goto Front()
        // Front() == Tail() if List is empty.
        curr = owner->first;
      }
      else if (curr->next != NullPMemOffset)
      {
        // Not Back(), goto next
        curr = owner->address_of(curr->next);
      }
      else
      {
        // Back(), goto Tail()
        curr = nullptr;
      }
      return *this;
    }

    Iterator operator++(int) {
      Iterator old{*this};
      this->operator++();
      return old;
    }

    Iterator& operator--() {
      if (curr == nullptr)
      {
        // Tail(), goto Back()
        // Back() == Head() if List is empty.
        curr = owner->first;
      }
      else if (curr->prev != NullPMemOffset)
      {
        // Not Front(), goto prev
        curr = owner->address_of(curr->prev);
      }
      else
      {
        // Front(), goto Head()
        curr = nullptr;
      }
      return *this;
    }

    Iterator operator--(int) {
      Iterator old{*this};
      this->operator--();
      return old;
    }

    DLRecord& operator*() { return *curr; }

    DLRecord* operator->() { return curr; }

    PMemOffsetType Offset() const { return owner->offset_of(Address()); }

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
  GenericList(GenericList &&) = delete;
  GenericList& operator=(GenericList const&) = delete;
  GenericList& operator=(GenericList &&) = delete;
  ~GenericList() = default;

  // Initialize a List with pmem base address p_base, pre-allocated space,
  // Creation time, List name and id.
  void Init(char* p_base, SpaceEntry allocated, TimeStampType timestamp,
              StringView const key, CollectionIDType id) {
      collection_name_.assign(key.data(), key.size())
      collection_id_ = id;
      pmem_base = p_base;
      list_record = StringRecord::PersistStringRecord(
        pmem_base+allocated.offset, allocated.size, timestamp,
        RecordType::ListRecord, NullPMemOffset, key, ID2String(id));
  }

  // Restore a List with its ListRecord, first and last element and size
  // This function is used by GenericListBuilder to restore the List
  void Restore(char* p_base, StringRecord* list_rec, DLRecord* fi, DLRecord* la, size_t n)
  {
    auto key = list_rec->Key();
    collection_name_.assign(key.data(), key.size());
    collection_id_ = string2ID(list_rec->Value());
    pmem_base = p_base;
    list_record = list_rec;
    first = fi;
    last = la;
    sz = n;
  }

  Iterator Front() { return ++Head(); }

  Iterator Back() { return --Tail(); }

  Iterator Head() const { return Iterator{this}; }

  Iterator Tail() const { return Iterator{this}; }

  Iterator Erase(Iterator pos) {
    kvdk_assert(pos != Head(), "Cannot erase Head()");
    kvdk_assert(sz >= 1, "Cannot erase from empty List!");
    kvdk_assert(string2ID(pos->Key()) == ID(), "Erase from wrong List!");

    Iterator prev{pos};
    --prev;
    Iterator next{pos};
    ++next;

    if (sz == 1)
    {
      kvdk_assert(prev == Head() && next == Tail(), "Impossible!");
      first = nullptr;
      last = nullptr;
    }
    else if (prev == Head())
    {
      // Erase Front()
      kvdk_assert(next != Tail(), "");
      first = next.Address();
      _mm_stream_si64(&next->prev, NullPMemOffset);
      _mm_mfence();
    }
    else if (next == Tail())
    {
      // Erase Back()
      kvdk_assert(prev != Head(), "");
      last = prev.curr;
      _mm_stream_si64(&prev->next, next.Offset());
      _mm_mfence();
    }
    else
    {
      kvdk_assert(prev != Head() && next != Tail(), "");
      // Reverse procedure of emplace_between() between two elements
      _mm_stream_si64(&next->prev, prev.Offset());
      _mm_mfence();
      // If crash before prev->next = allocated.offset, 
      // the node will be repaired into List
      // If crash after that, the node will be deleted
      _mm_stream_si64(&prev->next, next.Offset());
      _mm_mfence();
    }
    --sz;
    return next;
  }

  void PopFront() { Erase(Front()); }

  void PopBack() { Erase(Back()); }

  void EmplaceBefore(SpaceEntry allocated, Iterator pos, TimeStampType timestamp,
                                StringView const key, StringView const value) {
    Iterator prev{pos};
    --prev;
    Iterator next{pos};
    emplace_between(allocated, prev, next, timestamp, key, value);
  }

  void EmplaceAfter(SpaceEntry allocated, Iterator pos, TimeStampType timestamp,
                               StringView const key, StringView const value) {
    Iterator prev{pos};
    Iterator next{pos};
    ++next;
    emplace_between(allocated, prev, next, timestamp, key, value);
  }

  void PushFront(SpaceEntry allocated, TimeStampType timestamp, StringView const key,
                               StringView const value) {
    emplace_between(allocated, Head(), Front(), timestamp, key, value);
  }

  void PushBack(TimeStampType timestamp, StringView const key,
                              StringView const value) {
    emplace_between(allocated, Back(), Tail(), timestamp, key, value);
  }

  void Replace(Iterator pos, TimeStampType timestamp,
                          StringView const key, StringView const value) {
    kvdk_assert(key == Collection::ExtractUserKey(pos->Key()), "Not match!");
    kvdk_assert(ID() == Collection::ExtractID(pos->Key()), "Not match!");
    Iterator prev{pos};
    --prev;
    Iterator next{pos};
    ++next;
    emplace_between(prev, next, timestamp, key, value);
  }

  LockType* Mutex()
  {
    return &mu;
  }

private:
  Iterator emplace_between(SpaceEntry allocated, Iterator prev, Iterator next,
                                 TimeStampType timestamp, StringView const key,
                                 StringView const value) {

    kvdk_assert(++Iterator{prev} == next || ++++Iterator{prev} == next, "Should only insert or replace");

    PMemOffsetType prev_off = (prev == Head()) ? NullPMemOffset : prev.Offset();
    PMemOffsetType next_off = (next == Tail()) ? NullPMemOffset : next.Offset();
    DLRecord* record = DLRecord::PersistDLRecord(
        address_of(allocated.offset), allocated.size, timestamp, DataType, NullPMemOffset,
        prev_off, next_off, InternalKey(key), value);

    if (sz == 0)
    {
      kvdk_assert(prev == Head() && next == Tail(), "Impossible!");
      first = record;
      last = record;
    }
    else if (prev == Head())
    {
      // PushFront()
      kvdk_assert(next != Tail(), "");
      _mm_stream_si64(&next->prev, allocated.offset);
      _mm_mfence();
      first = record;
    }
    else if (next == Tail())
    {
      // PushBack()
      kvdk_assert(prev != Head(), "");
      _mm_stream_si64(&prev->next, allocated.offset);
      _mm_mfence();
      last = record;
    }
    else
    {
      // Emplace between two elements on PMem
      kvdk_assert(prev != Head() && next != Tail(), "");
      _mm_stream_si64(&prev->next, allocated.offset);
      _mm_mfence();
      // If crash before prev->next = allocated.offset, 
      // newly emplaced node will be discarded
      // If crash after that, newly emplaced node will be repaired into List
      _mm_stream_si64(&next->prev, allocated.offset);
      _mm_mfence();
    }

    ++sz;
    return (next == Tail()) ? --next : ++prev;
  }

  DLRecord* address_of(PMemOffsetType off)
  {
    kvdk_assert(pmem_base != nullptr, "");
    kvdk_assert(off != NullPMemOffset, "Dereferencing Null!")
    return reinterpret_cast<DLRecord*>(pmem_base + off);
  }

  PMemOffsetType offset_of(DLRecord* rec)
  {
    kvdk_assert(pmem_base != nullptr, "");
    kvdk_assert(rec != nullptr, "Dereferencing nullptr!")
    return reinterpret_cast<char*>(rec) - pmem_base;
  }

  friend std::ostream& operator<<(std::ostream& out, GenericList const& list) {

    auto printRecord = [&](DLRecord* record) {
      auto internal_key = record->Key();
      out << "Type:\t" << to_hex(record->entry.meta.type) << "\t"
          << "Prev:\t" << to_hex(record->prev) << "\t"
          << "Offset:\t" << to_hex(reinterpret_cast<char*>(record) - list.pmem_base) << "\t"
          << "Next:\t" << to_hex(record->next) << "\t"
          << "Key: " << to_hex(Collection::ExtractID(internal_key))
          << Collection::ExtractUserKey(internal_key) << "\t"
          << "Value: " << record->Value() << "\n";
    };

    out << "Contents of List:\n";
    Iterator iter = list.Head();
    for (Iterator iter = list.Head(); iter != list.Tail(); iter++)
    {
        printRecord(iter.record);
    }
    printRecord(iter.record);
    return out;
  }
};

template <RecordType ListType, RecordType DataType>
class GenericListBuilder final {
  static constexpr size_t NMiddlePoints = 1024;

  char* pmem_base;
  using List = GenericList<ListType, DataType>;
  std::deque<std::unique_ptr<List>>* rebuilded_lists{nullptr};
  size_t n_worker{0};
  std::mutex mu;

  std::atomic_uint64_t mpoint_cnt;
  std::array<DLRecord*, NMiddlePoints> mpoints{};

  // There are 4 types of DLRecord
  // 0. Unique: prev == Null && next == Null
  // 1. First: prev == Null && next != Null
  // 2. Last: prev != Null && next == Null
  // 3. Middle: prev != Null && next != Null
  enum class RecordState
  {
    Unique,
    First,
    Last,
    Middle
  };

  // There are several types of failure during GenericList operations
  // 1. PushFront/PushBack/PopFront/PopBack Failure
  //    Interruption in Pop() after unlink before purge is same as
  //    Interruption in Push() after persist before link
  //    For uniformity with 1-elem Pop crash, which cannot be detected
  //    We always link the node to List
  // 2. ReplaceFront/ReplaceBack Failure
  //    Discard the unlinked node, 
  //    unlinked newer node treated as Replace failure
  //    unlinked older node treated as Replace success
  // 3. Replace Failure
  //    a. Node not linked from its prev and next purged directly
  //    b. Node not linked from prev but linked from next is saved for now
  //       Node not linked from next but linked from prev is linked from next
  //    After all nodes repaired, Node not linked from prev will not
  //    be linked to next and is purged
  // 4. Insertion/Erase Failure
  //    a. Node not linked from its prev and next purged directly
  //    b. Node not linked from prev but linked from next is saved for now
  //       Node not linked from next but linked from prev is linked from next
  //    After all node repaired, Node not linked from prev will be
  //    be linked to prev by repairing the other node
  
public:

  explicit GenericListBuilder() = default;
  void Init(char* pbase, std::deque<std::unique_ptr<List>>* lists, size_t num_worker)
  {
    kvdk_assert(lists != nullptr && lists->empty(), "");
    kvdk_assert(num_worker != 0, "");
    kvdk_assert(rebuilded_lists == nullptr, "Already initialized!");
    pmem_base = pbase;
    rebuilded_lists = lists;
    n_worker = num_worker;
  }

  void AddListRecord();


private:
  RecordState stateOf(DLRecord* elem)
  {
    if (elem->prev != NullPMemOffset && elem->next != NullPMemOffset)
    {
      return RecordState::Middle;
    }
    else if (elem->prev != NullPMemOffset)
    {
      return RecordState::Last;
    }
    else if (elem->next != NullPMemOffset)
    {
      return RecordState::First;
    }
    else
    {
      return RecordState::Unique;
    }
  }

  // Reservoir algorithm
  void addMiddleElem(DLRecord* elem)
  {
    kvdk_assert(elem->prev != NullPMemOffset && elem->next != NullPMemOffset, "Not middle point!");

    thread_local std::default_random_engine rengine{get_seed()};

    auto cnt = mpoint_cnt.fetch_add(1U);
    auto pos = cnt % NMiddlePoints;
    auto k = cnt / NMiddlePoints;
    // k-th point has posibility 1/(k+1) to replace previous point in reservoir
    if (std::bernoulli_distribution{1.0/(k+1)}(rengine))
    {
      mpoints.at(pos) = elem;
    }
  }

  void addFirstElem(DLRecord* elem)
  {

  }

  bool maybeFixUnique(DLRecord*)
  {
    return true;
  }

  bool maybeFixFirst(DLRecord* elem) 
  {
    if (address_of(elem->next)->prev == NullPMemOffset, "")
    {
      // Interrupted PushFront()/PopFront()
      _mm_stream_si64(address_of(elem->next)->prev, offset_of(elem));
      _mm_mfence();
    }
    return true;
  }

  bool maybeFixLast(DLRecord* elem) 
  {
    if (address_of(elem->prev)->next == NullPMemOffset, "")
    {
      // Interrupted PushBack()/PopBack()
      _mm_stream_si64(address_of(elem->prev)->next, offset_of(elem));
      _mm_mfence();
    }
    return true;
  }

  // When false is returned, the node is put in temporary pool
  // and processed after all restoration is done
  bool maybeFixMiddle(DLRecord* elem)
  {
    if (offset_of(elem) == address_of(elem->prev)->next)
    {
      if (offset_of(elem) == address_of(elem->next)->prev)
      {
        // Normal Middle
        return true;
      }
      else
      {
        // Interrupted Replace/Emplace, link into List
        _mm_stream_si64(&address_of(elem->next)->prev, offset_of(elem));
        _mm_mfence();
      }      
    }
    else
    {
      if (offset_of(elem) == address_of(elem->next)->prev)
      {
        // Interrupted Replace/Emplace
        // For interrupted Replace, the node will be unlinked
        // For interrupted Emplace, the node will be linked
        return false;
      }
      else
      {
        // Un-purged
        return false;
      }      
    }
  }

  DLRecord* address_of(PMemOffsetType off)
  {
    kvdk_assert(pmem_base != nullptr, "");
    kvdk_assert(off != NullPMemOffset, "Dereferencing Null!")
    return reinterpret_cast<DLRecord*>(pmem_base + off);
  }

  PMemOffsetType offset_of(DLRecord* rec)
  {
    kvdk_assert(pmem_base != nullptr, "");
    kvdk_assert(rec != nullptr, "Dereferencing nullptr!")
    return reinterpret_cast<char*>(rec) - pmem_base;
  }
};


}  // namespace KVDK_NAMESPACE
