/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <algorithm>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iomanip>
#include <iostream>

#include <libpmem.h>
#include <libpmemobj++/string_view.hpp>

#include "kvdk/alias.hpp"
#include "kvdk/engine.hpp"
#include "kvdk/macros.h"

#include "hash_table.hpp"
#include "structures.hpp"
#include "utils.hpp"

#include <list>

namespace KVDK_NAMESPACE {
/// DLinkedList is a helper class to access PMem
/// DLinkedList guarantees that forward links are always valid
/// Backward links may be broken
/// if shutdown happens when new record is being emplaced.
/// DLinkedList does not deallocate records. Deallocation is done by caller
/// Locking is done by caller at HashTable
template <RecordType HeadType, RecordType TailType, RecordType DataType>
class DLinkedList {
public:
  /// It's up to caller to ensure that any
  /// iterator constructed belongs to the
  /// correct DlinkedList and to ensure that
  /// DlinkedList is valid when iterator
  /// accesses data on it.
  class iterator {
  private:
    friend class DLinkedList;

  private:
    /// PMem pointer to current Record
    /// Current position
    PMEMAllocator *pmem_allocator_ptr;
    DLRecord *current_pmmptr;

  private:
    /// It's up to caller to provide correct PMem pointer
    /// and PMemAllocator to construct a iterator
    explicit iterator(PMEMAllocator *pmem_allocator_p, DLRecord *curr)
        : pmem_allocator_ptr{pmem_allocator_p}, current_pmmptr{curr} {}

  public:
    iterator(iterator const &other) = default;

    /// Conversion to bool
    /// Returns true if the iterator is on some DlinkedList
    inline bool valid() const {
      if (!current_pmmptr) {
        return false;
      }
      switch (static_cast<RecordType>(current_pmmptr->entry.meta.type)) {
      case HeadType:
      case TailType:
      case DataType: {
        return true;
      }
      default: {
        return false;
      }
      }
    }

    inline operator bool() const { return valid(); }

    /// Increment and Decrement operators
    iterator &operator++() {
      current_pmmptr = getNextAddress();
      return *this;
    }

    iterator operator++(int) {
      iterator old{*this};
      this->operator++();
      return old;
    }

    iterator &operator--() {
      current_pmmptr = getPrevAddress();
      return *this;
    }

    iterator operator--(int) {
      iterator old{*this};
      this->operator--();
      return old;
    }

    DLRecord &operator*() { return *current_pmmptr; }

    DLRecord *operator->() { return current_pmmptr; }

    friend bool operator==(iterator lhs, iterator rhs) {
      return lhs.pmem_allocator_ptr == rhs.pmem_allocator_ptr &&
             lhs.current_pmmptr == rhs.current_pmmptr;
    }

    friend bool operator!=(iterator lhs, iterator rhs) { return !(lhs == rhs); }

  private:
    inline DLRecord *getNextAddress() const {
      return pmem_allocator_ptr->offset2addr_checked<DLRecord>(
          current_pmmptr->next);
    }

    inline DLRecord *getPrevAddress() const {
      return pmem_allocator_ptr->offset2addr_checked<DLRecord>(
          current_pmmptr->prev);
    }

  public:
    inline PMemOffsetType GetCurrentOffset() const {
      return pmem_allocator_ptr->addr2offset_checked(current_pmmptr);
    }

    inline DLRecord *GetCurrentAddress() const { return current_pmmptr; }
  };

private:
  /// Allocator for allocating space for new nodes,
  /// as well as for deallocating space to delete nodes
  PMEMAllocator *pmem_allocator_ptr;
  /// PMem pointer(pmp) to head node on PMem
  DLRecord *head_pmmptr;
  /// PMem pointer(pmp) to tail node on PMem
  DLRecord *tail_pmmptr;

  friend class UnorderedCollection;
  friend class UnorderedIterator;

  static constexpr PMemOffsetType NullPMemOffset = kNullPmemOffset;

public:
  /// Create DLinkedList and construct head and tail node on PMem.
  /// Caller supplied key and value are stored in head and tail nodes
  DLinkedList(PMEMAllocator *pmem_allocator_p, TimeStampType timestamp,
              StringView const key, StringView const value)
      : pmem_allocator_ptr{pmem_allocator_p}, head_pmmptr{nullptr},
        tail_pmmptr{nullptr} {
    {
      // head and tail can hold any key and value supplied by caller.
      auto head_space_entry = pmem_allocator_ptr->Allocate(
          sizeof(DLRecord) + key.size() + value.size());
      if (head_space_entry.size == 0) {
        throw std::bad_alloc{};
      }
      auto tail_space_entry = pmem_allocator_ptr->Allocate(
          sizeof(DLRecord) + key.size() + value.size());
      if (tail_space_entry.size == 0) {
        pmem_allocator_ptr->Free(head_space_entry);
        throw std::bad_alloc{};
      }

      PMemOffsetType head_offset = head_space_entry.space_entry.offset;
      PMemOffsetType tail_offset = tail_space_entry.space_entry.offset;

      // Persist tail first then head
      // If only tail is persisted then it can be deallocated by caller at
      // recovery
      tail_pmmptr = DLRecord::PersistDLRecord(
          pmem_allocator_ptr->offset2addr_checked(tail_offset),
          tail_space_entry.size, timestamp, TailType, head_offset,
          NullPMemOffset, key, value);
      head_pmmptr = DLRecord::PersistDLRecord(
          pmem_allocator_ptr->offset2addr_checked(head_offset),
          head_space_entry.size, timestamp, HeadType, NullPMemOffset,
          tail_offset, key, value);
    }
  }

  /// Create DLinkedList from existing head and tail node. Used for recovery.
  /// If from head to tail node is not forward linked, assertion fails.
  DLinkedList(PMEMAllocator *pmem_allocator_p, DLRecord *head_pmmptr,
              DLRecord *tail_pmmptr)
      : pmem_allocator_ptr{pmem_allocator_p}, head_pmmptr{head_pmmptr},
        tail_pmmptr{tail_pmmptr} {
#if DEBUG_LEVEL >= 0
    {
      kvdk_assert(head_pmmptr->entry.meta.type == HeadType,
                  "Cannot rebuild a DlinkedList from given PMem pointer "
                  "not pointing to a valid Head Record!");
      iterator curr{pmem_allocator_ptr, head_pmmptr};
      ++curr;

      while (true) {
        switch (static_cast<RecordType>(curr->entry.meta.type)) {
        case DataType: {
          iterator next{curr};
          ++next;
          iterator prev{curr};
          --prev;
          PMemOffsetType offset_curr = curr.GetCurrentOffset();
          kvdk_assert(next->prev == offset_curr,
                      "Found broken linkage when rebuilding DLinkedList!");
          kvdk_assert(prev->next == offset_curr,
                      "Found broken linkage when rebuilding DLinkedList!");
          ++curr;
          continue;
        }
        case TailType: {
          kvdk_assert(curr.GetCurrentAddress() == tail_pmmptr,
                      "Unmatched head and tail when rebuilding a DlinkedList!");
          return;
        }
        default: {
          kvdk_assert(false,
                      "Invalid Record met when rebuilding a DlinkedList!");
          std::abort();
        }
        }
      }
    }
#endif // DEBUG_LEVEL > 0
  }

  // head_pmmptr and tail_pmmptr points to persisted Record of Head and Tail on
  // PMem.
  // No need to delete anything
  ~DLinkedList() = default;

  iterator First() {
    iterator ret = Head();
    ++ret;
    kvdk_assert(ret->entry.meta.type == DataType,
                "Calling First on empty DlinkedList!");
    return ret;
  }

  iterator Last() {
    iterator ret = Tail();
    --ret;
    kvdk_assert(ret->entry.meta.type == DataType,
                "Calling Last on empty DlinkedList!");
    return ret;
  }

  iterator Head() const { return makeIterator(head_pmmptr); }

  iterator Tail() const { return makeIterator(tail_pmmptr); }

  // Connect prev and next of node addressed by pos,
  // logically delete this record by de-link it.
  // Return iterator at next node.
  // It's up to user to keep track of freed node
  inline iterator Erase(iterator pos) {
    iterator iter_prev{pos};
    --iter_prev;
    iterator iter_next{pos};
    ++iter_next;
    kvdk_assert(iter_prev && iter_next, "Invalid iterator in dlinked_list!");
    iter_prev->next = iter_next.GetCurrentOffset();
    pmem_persist(&iter_prev->next, sizeof(PMemOffsetType));
    iter_next->prev = iter_prev.GetCurrentOffset();
    pmem_persist(&iter_next->prev, sizeof(PMemOffsetType));

    return iter_next;
  }

  inline void PopFront() { Erase(First()); }

  inline void PopBack() { Erase(Last()); }

  inline iterator EmplaceFront(TimeStampType timestamp, StringView const key,
                               StringView const value) {
    return EmplaceAfter(Head(), timestamp, key, value);
  }

  inline iterator EmplaceBack(TimeStampType timestamp, StringView const key,
                              StringView const value) {
    return EmplaceBefore(Tail(), timestamp, key, value);
  }

  inline iterator EmplaceBefore(iterator pos, TimeStampType timestamp,
                                StringView const key, StringView const value) {
    iterator iter_prev{pos};
    --iter_prev;
    iterator iter_next{pos};
    return emplaceBetween(iter_prev, iter_next, timestamp, key, value);
  }

  inline iterator EmplaceAfter(iterator pos, TimeStampType timestamp,
                               StringView const key, StringView const value) {
    iterator iter_prev{pos};
    iterator iter_next{pos};
    ++iter_next;
    return emplaceBetween(iter_prev, iter_next, timestamp, key, value);
  }

  inline iterator Replace(iterator pos, TimeStampType timestamp,
                          StringView const key, StringView const value) {
    iterator iter_prev{pos};
    --iter_prev;
    iterator iter_next{pos};
    ++iter_next;
    return emplaceBetween(iter_prev, iter_next, timestamp, key, value);
  }

private:
  iterator makeIterator(DLRecord *pos) const {
    return iterator{pmem_allocator_ptr, pos};
  }

  void purgeAndFree(DLRecord *record_pmmptr) {
    record_pmmptr->Destroy();
    pmem_allocator_ptr->Free(
        SizedSpaceEntry(pmem_allocator_ptr->addr2offset_checked(record_pmmptr),
                        record_pmmptr->entry.header.record_size,
                        record_pmmptr->entry.meta.timestamp));
  }

  /// Emplace between iter_prev and iter_next, linkage not checked
  /// iter_prev and iter_next may be adjacent or separated
  /// If they are separated, node(s) between them are lost.
  /// It's up to caller to keep track of lost node(s).
  /// When fail to allocate space to emplace, throw bad_alloc
  /// If system fails, it is guaranteed the dlinked_list is in one of the
  /// following state:
  ///     1) Nothing emplaced
  ///     2) node emplaced but not linked
  ///     3) node emplaced and linked in the forward direction
  ///     4) node emplaced and linked in both directions
  /// Return iterator at newly emplace node
  inline iterator emplaceBetween(iterator iter_prev, iterator iter_next,
                                 TimeStampType timestamp, StringView const key,
                                 StringView const value) {
    kvdk_assert(iter_prev && iter_next, "Invalid iterator in dlinked_list!");

    auto space = pmem_allocator_ptr->Allocate(sizeof(DLRecord) + key.size() +
                                              value.size());
    if (space.size == 0) {
      throw std::bad_alloc{};
    }
    std::uint64_t offset = space.space_entry.offset;
    void *pmp = pmem_allocator_ptr->offset2addr_checked(offset);

    DLRecord *record = DLRecord::PersistDLRecord(
        pmp, space.size, timestamp, DataType, iter_prev.GetCurrentOffset(),
        iter_next.GetCurrentOffset(), key, value);

    iter_prev->next = offset;
    pmem_persist(&iter_prev->next, sizeof(PMemOffsetType));
    iter_next->prev = offset;
    pmem_persist(&iter_next->prev, sizeof(PMemOffsetType));

    return iterator{pmem_allocator_ptr, record};
  }

  /// Extract user-key from internal-key used by the collection.
  /// Internal key has 8-byte ID as prefix.
  /// User-key does not have that ID.
  inline static StringView extractKey(StringView internal_key) {
    constexpr size_t sz_id = 8;
    assert(sz_id <= internal_key.size() &&
           "internal_key does not has space for key");
    return StringView(internal_key.data() + sz_id, internal_key.size() - sz_id);
  }

  /// Extract ID from internal-key
  inline static std::uint64_t extractID(StringView internal_key) {
    CollectionIDType id;
    assert(sizeof(CollectionIDType) <= internal_key.size() &&
           "internal_key is smaller than the size of an id!");
    memcpy(&id, internal_key.data(), sizeof(CollectionIDType));
    return id;
  }

  /// Output DlinkedList to ostream for debugging purpose.
  friend std::ostream &operator<<(std::ostream &out, DLinkedList const &dlist) {
    out << "Contents of DlinkedList:\n";
    iterator iter = dlist.Head();
    iterator iter_end = dlist.Tail();
    while (iter != iter_end) {
      auto internal_key = iter->Key();
      out << "Type: " << to_hex(iter->entry.meta.type) << "\t"
          << "Offset: " << to_hex(iter.GetCurrentOffset()) << "\t"
          << "Prev: " << to_hex(iter->prev) << "\t"
          << "Next: " << to_hex(iter->next) << "\t"
          << "Key: " << to_hex(extractID(internal_key))
          << extractKey(internal_key) << "\t"
          << "Value: " << iter->Value() << "\n";
      ++iter;
    }
    auto internal_key = iter->Key();
    out << "Type: " << to_hex(iter->entry.meta.type) << "\t"
        << "Offset: " << to_hex(iter.GetCurrentOffset()) << "\t"
        << "Prev: " << to_hex(iter->prev) << "\t"
        << "Next: " << to_hex(iter->next) << "\t"
        << "Key: " << to_hex(extractID(internal_key))
        << extractKey(internal_key) << "\t"
        << "Value: " << iter->Value() << "\n";
    return out;
  }
};

} // namespace KVDK_NAMESPACE
