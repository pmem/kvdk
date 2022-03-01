/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once


#include <atomic>
#include <stdexcept>

#include <immintrin.h>

#include <libpmem.h>

#include <libpmemobj++/string_view.hpp>

#include "kvdk/namespace.hpp"
#include "kvdk/collection.hpp"

#include "alias.hpp"
#include "macros.hpp"
#include "data_record.hpp"
#include "pmem_allocator/pmem_allocator.hpp"

namespace KVDK_NAMESPACE {
template <RecordType ListType, RecordType DataType>
class ListImpl2 final : public Collection {
 public:
  class Iterator {
   private:
    char* pmem_base{nullptr};
    DLRecord* curr{nullptr};
    friend class ListImpl2;

   public:
    Iterator() = default;
    Iterator(Iterator const&) = default;
    Iterator(Iterator &&) = default;
    Iterator& operator=(Iterator const&) = default;
    Iterator& operator=(Iterator &&) = default;

    /// Increment and Decrement operators
    Iterator& operator++() {
      curr = reinterpret_cast<DLRecord*>(pmem_base+curr->next);
      return *this;
    }

    Iterator operator++(int) {
      Iterator old{*this};
      this->operator++();
      return old;
    }

    Iterator& operator--() {
      curr = reinterpret_cast<DLRecord*>(pmem_base+curr->prev);
      return *this;
    }

    Iterator operator--(int) {
      Iterator old{*this};
      this->operator--();
      return old;
    }

    DLRecord& operator*() { return *curr; }

    DLRecord* operator->() { return curr; }

    friend bool operator==(Iterator lhs, Iterator rhs) { return (lhs.curr == rhs.curr); }

    friend bool operator!=(Iterator lhs, Iterator rhs) { return !(lhs == rhs); }

    PMemOffsetType offset() const {return reinterpret_cast<char*>(curr)-pmem_base;}

private:
    explicit Iterator(char* base, DLRecord* p) : pmem_base{base}, curr{p} {}

  };

 private:
  char* pmem_base;
  DLRecord* list_record{nullptr};
  std::atomic_uint64_t sz{0U};

  using LockType = SpinMutex;
  LockType spin;

  static constexpr PMemOffsetType NullPMemOffset = kNullPMemOffset;

 public:
  // Initialize a List with pmem base address p_base, pre-allocated space,
  // Creation time, List name and id.
  ListImpl2(char* p_base, SpaceEntry allocated, TimeStampType timestamp,
              StringView const key, CollectionIDType id)
      : pmem_base{p_base}
    {
    {
        list_record = DLRecord::PersistDLRecord(
            pmem_base+allocated.offset,
            allocated.size, timestamp, RecordType::QueueRecord,
            NullPMemOffset, NullPMemOffset, NullPMemOffset, key,
            ID2String(id));
    }
  }
  // Restore List from its record
  ListImpl2(char* p_base, DLRecord* lrecord) :
    pmem_base{p_base},
    list_record{lrecord}
  {
    {
      kvdk_assert(lrecord->entry.meta.type == ListType, "Invalid List record!");

      if (Front()->prev != Head().offset())
      {
        _mm_stream_si64(&Front(), Head().offset());
        _mm_mfence();
      }
      
      for (Iterator iter = Front(); iter != Tail(); iter++)
      {
        Iterator next{iter};
        next++;
        if (next->prev != iter.offset())
        {
          // Repair broken link by crash
          _mm_stream_si64(&next->prev, iter.offset());
          _mm_mfence();
        }

        sz.fetch_add(1U);
      }
    }
  }

  ListImpl2() = delete;
  ListImpl2(ListImpl2 const&) = delete;
  ListImpl2(ListImpl2 &&) = delete;
  ListImpl2& operator=(ListImpl2 const&) = delete;
  ListImpl2& operator=(ListImpl2 &&) = delete;
  ~ListImpl2() = default;

  Iterator Front() { return ++Head(); }

  Iterator Back() { return --Tail(); }

  Iterator Head() const { return Iterator{pmem_base, list_record}; }

  Iterator Tail() const { return Iterator{pmem_base, list_record}; }

  Iterator Erase(Iterator pos) {
    Iterator prev{pos};
    --prev;
    Iterator next{pos};
    ++next;

    _mm_stream_si64(&next->prev, prev.offset());
    _mm_mfence();
    _mm_stream_si64(&prev->next, next.offset());
    _mm_mfence();

    return next;
  }

  void PopFront() { Erase(Front()); }

  void PopBack() { Erase(Back()); }

  void EmplaceBefore(Iterator pos, TimeStampType timestamp,
                                StringView const key, StringView const value) {
    Iterator prev{pos};
    --prev;
    Iterator next{pos};
    emplace(prev, next, timestamp, key, value);
  }

  void EmplaceAfter(Iterator pos, TimeStampType timestamp,
                               StringView const key, StringView const value) {
    Iterator prev{pos};
    Iterator next{pos};
    ++next;
    emplace(prev, next, timestamp, key, value);
  }


  void EmplaceFront(TimeStampType timestamp, StringView const key,
                               StringView const value) {
    EmplaceAfter(Head(), timestamp, key, value);
  }

  void EmplaceBack(TimeStampType timestamp, StringView const key,
                              StringView const value) {
    EmplaceBefore(Tail(), timestamp, key, value);
  }

  void Replace(Iterator pos, TimeStampType timestamp,
                          StringView const key, StringView const value) {
    kvdk_assert(key == Collection::ExtractUserKey(pos->Key()), "Not match!");
    Iterator prev{pos};
    --prev;
    Iterator next{pos};
    ++next;
    emplace(prev, next, timestamp, key, value);
  }

  LockType* Mutex()
  {
    return &spin;
  }

private:
  Iterator emplace(SpaceEntry allocated, Iterator prev, Iterator next,
                                 TimeStampType timestamp, StringView const key,
                                 StringView const value) {

    kvdk_assert(++Iterator{prev} == next || ++++Iterator{prev} == next, "Should only insert or replace");

    DLRecord* record = DLRecord::PersistDLRecord(
        pmem_base+allocated.offset, allocated.size, timestamp, DataType, NullPMemOffset,
        prev.offset(), next.offset(), InternalKey(key), value);

    _mm_stream_si64(&prev->next, allocated.offset);
    _mm_mfence();
    _mm_stream_si64(&next->prev, allocated.offset);
    _mm_mfence();

    sz.fetch_add(1U);

    return Iterator{pmem_base, record};
  }

  friend std::ostream& operator<<(std::ostream& out, ListImpl2 const& list) {

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
        printRecord(iter.curr);
    }
    printRecord(iter.curr);
    return out;
  }

};

}  // namespace KVDK_NAMESPACE
