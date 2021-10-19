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

#include "hash_table.hpp"
#include "kvdk/engine.hpp"
#include "structures.hpp"
#include "utils.hpp"

#include <list>

#define hex_print(x)                                                           \
  std::hex << std::setfill('0') << std::setw(sizeof(decltype(x)) * 2) << x

namespace KVDK_NAMESPACE {
/// DListIterator does not pin DlinkedList
/// It's up to caller to ensure that any
/// DListIterator constructed belongs to the
/// correct DlinkedList and to ensure that
/// DlinkedList is valid when DListIterator
/// accesses data on it.
class DListIterator {
private:
  friend class DLinkedList;
  friend class UnorderedCollection;

private:
  /// PMem pointer to current Record
  PMEMAllocator *p_pmem_allocator_;
  /// Current position
  DLDataEntry *pmp_curr_;

public:
  /// It's up to caller to provide correct PMem pointer
  /// and PMemAllocator to construct a DListIterator
  explicit DListIterator(PMEMAllocator *p_pmem_allocator, DLDataEntry *curr)
      : p_pmem_allocator_{p_pmem_allocator}, pmp_curr_{curr} {}

  DListIterator(DListIterator const &other)
      : p_pmem_allocator_{other.p_pmem_allocator_}, pmp_curr_(other.pmp_curr_) {
  }

  /// Conversion to bool
  /// Returns true if the iterator is on some DlinkedList
  inline bool valid() const {
    if (!pmp_curr_) {
      return false;
    }
    switch (static_cast<DataEntryType>(pmp_curr_->type)) {
    case DataEntryType::DlistDataRecord:
    case DataEntryType::DlistHeadRecord:
    case DataEntryType::DlistTailRecord: {
      return true;
    }
    case DataEntryType::DlistRecord:
    default: {
      return false;
    }
    }
  }

  inline operator bool() const { return valid(); }

  /// Increment and Decrement operators
  DListIterator &operator++() {
    pmp_curr_ = getPmpNext();
    return *this;
  }

  DListIterator operator++(int) {
    DListIterator old{*this};
    this->operator++();
    return old;
  }

  DListIterator &operator--() {
    pmp_curr_ = getPmpPrev();
    return *this;
  }

  DListIterator operator--(int) {
    DListIterator old{*this};
    this->operator--();
    return old;
  }

  DLDataEntry &operator*() { return *pmp_curr_; }

  DLDataEntry *operator->() { return pmp_curr_; }

  friend bool operator==(DListIterator lhs, DListIterator rhs) {
    assert(lhs.p_pmem_allocator_ == rhs.p_pmem_allocator_);
    return lhs.pmp_curr_ == rhs.pmp_curr_;
  }

  friend bool operator!=(DListIterator lhs, DListIterator rhs) {
    return !(lhs == rhs);
  }

private:
  inline DLDataEntry *getPmpNext() const {
    return reinterpret_cast<DLDataEntry *>(
        p_pmem_allocator_->offset2addr_checked(pmp_curr_->next));
  }

  inline DLDataEntry *getPmpPrev() const {
    return reinterpret_cast<DLDataEntry *>(
        p_pmem_allocator_->offset2addr_checked(pmp_curr_->prev));
  }

public:
  inline std::uint64_t GetOffset() const {
    return p_pmem_allocator_->addr2offset_checked(pmp_curr_);
  }
};
} // namespace KVDK_NAMESPACE

namespace KVDK_NAMESPACE {
/// DLinkedList is a helper class to access PMem
/// DLinkedList guarantees that forward links are always valid
/// Backward links may be broken
/// if shutdown happens when new record is being emplaced.
/// DLinkedList does not deallocate records. Deallocation is done by caller
/// Locking is done by caller at HashTable
class DLinkedList {
private:
  /// Allocator for allocating space for new nodes,
  /// as well as for deallocating space to delete nodes
  PMEMAllocator *p_pmem_allocator_;
  /// PMem pointer(pmp) to head node on PMem
  DLDataEntry *pmp_head_;
  /// PMem pointer(pmp) to tail node on PMem
  DLDataEntry *pmp_tail_;

  friend class DListIterator;
  friend class UnorderedCollection;
  friend class UnorderedIterator;

  static constexpr std::uint64_t NullPMemOffset = kNullPmemOffset;

public:
  /// Create DLinkedList and construct head and tail node on PMem.
  /// Caller supplied key and value are stored in head and tail nodes
  DLinkedList(std::shared_ptr<PMEMAllocator> sp_pmem_allocator,
              std::uint64_t timestamp, pmem::obj::string_view const key,
              pmem::obj::string_view const value)
      : p_pmem_allocator_{sp_pmem_allocator.get()}, pmp_head_{nullptr},
        pmp_tail_{nullptr} {
    {
      // head and tail can hold any key and value supplied by caller.
      auto space_head = p_pmem_allocator_->Allocate(sizeof(DLDataEntry) +
                                                    key.size() + value.size());
      if (space_head.size == 0) {
        throw std::bad_alloc{};
      }
      auto space_tail = p_pmem_allocator_->Allocate(sizeof(DLDataEntry) +
                                                    key.size() + value.size());
      if (space_tail.size == 0) {
        p_pmem_allocator_->Free(space_head);
        throw std::bad_alloc{};
      }

      std::uint64_t offset_head = space_head.space_entry.offset;
      std::uint64_t offset_tail = space_tail.space_entry.offset;
      void *pmp_head = p_pmem_allocator_->offset2addr_checked(offset_head);
      void *pmp_tail = p_pmem_allocator_->offset2addr_checked(offset_tail);

      DLDataEntry entry_head; // Set up entry with meta
      {
        entry_head.timestamp = timestamp;
        entry_head.type = DataEntryType::DlistHeadRecord;
        entry_head.k_size = key.size();
        entry_head.v_size = value.size();

        // checksum can only be calculated with complete meta
        entry_head.header.b_size = space_head.size;
        entry_head.header.checksum = checkSum(entry_head, key, value);

        entry_head.prev = NullPMemOffset;
        entry_head.next = offset_tail;
      }

      DLDataEntry entry_tail; // Set up entry with meta
      {
        entry_tail.timestamp = timestamp;
        entry_tail.type = DataEntryType::DlistTailRecord;
        entry_tail.k_size = key.size();
        entry_tail.v_size = value.size();

        // checksum can only be calculated with complete meta
        entry_tail.header.b_size = space_head.size;
        entry_tail.header.checksum = checkSum(entry_tail, key, value);

        entry_tail.prev = offset_head;
        entry_tail.next = NullPMemOffset;
      }

      // Persist tail first then head
      // If only tail is persisted then it can be deallocated by caller at
      // recovery
      persistRecord(pmp_tail, entry_tail, key, value);
      persistRecord(pmp_head, entry_head, key, value);
      pmp_head_ = static_cast<DLDataEntry *>(pmp_head);
      pmp_tail_ = static_cast<DLDataEntry *>(pmp_tail);
    }
  }

  /// Create DLinkedList from existing head and tail node. Used for recovery.
  /// If from head to tail node is not forward linked, throw.
  DLinkedList(std::shared_ptr<PMEMAllocator> sp_pmem_allocator,
              DLDataEntry *pmp_head, DLDataEntry *pmp_tail)
      : p_pmem_allocator_{sp_pmem_allocator.get()}, pmp_head_{pmp_head},
        pmp_tail_{pmp_tail} {
#if DEBUG_LEVEL > 0
    {
      kvdk_assert(pmp_head->type == DataEntryType::DlistHeadRecord,
                  "Cannot rebuild a DlinkedList from given PMem pointer "
                  "not pointing to a DlistHeadRecord!");
      DListIterator curr{p_pmem_allocator_, pmp_head};
      ++curr;

      while (true) {
        switch (static_cast<DataEntryType>(curr->type)) {
        case DataEntryType::DlistDataRecord: {
          DListIterator next{curr};
          ++next;
          DListIterator prev{curr};
          --prev;
          std::uint64_t offset_curr = curr.GetOffset();
          kvdk_assert(next->prev == offset_curr,
                      "Found broken linkage when rebuilding DLinkedList!");
          kvdk_assert(prev->next == offset_curr,
                      "Found broken linkage when rebuilding DLinkedList!");
          ++curr;
          continue;
        }
        case DataEntryType::DlistTailRecord: {
          kvdk_assert(curr.pmp_curr_ == pmp_tail,
                      "Unmatched head and tail when rebuilding a DlinkedList!");
          return;
        }
        case DataEntryType::DlistHeadRecord:
        case DataEntryType::DlistRecord:
        default: {
          kvdk_assert(false,
                      "Invalid Record met when rebuilding a DlinkedList!");
        }
        }
      }
    }
#endif // DEBUG_LEVEL > 0
  }

  // pmp_head_ and pmp_tail_ points to persisted Record of Head and Tail on
  // PMem No need to delete anything
  ~DLinkedList() = default;

  // Not checked yet, may return Tail()
  DListIterator First() {
    DListIterator ret{p_pmem_allocator_, pmp_head_};
    assert(ret.valid());
    ++ret;
    return ret;
  }

  // Not checked yet, may return Head()
  DListIterator Last() {
    DListIterator ret{p_pmem_allocator_, pmp_tail_};
    assert(ret.valid());
    --ret;
    return ret;
  }

  DListIterator Head() { return DListIterator{p_pmem_allocator_, pmp_head_}; }

  DListIterator Tail() { return DListIterator{p_pmem_allocator_, pmp_tail_}; }

  /// Helper function to deallocate Record, called only by caller
  inline static void Deallocate(DListIterator iter) {
    // Necessary, as security measure.
    iter->type = DataEntryType::Padding;
    pmem_persist(iter.pmp_curr_, sizeof(DLDataEntry));

    iter.p_pmem_allocator_->Free(SizedSpaceEntry{
        iter.GetOffset(), iter->header.b_size, iter->timestamp});
  }

  /// Emplace between iter_prev and iter_next, linkage not checked
  /// When fail to Emplace, throw bad_alloc
  /// If system fails, it is guaranteed the dlinked_list is in one of the
  /// following state:
  ///     1) Nothing emplaced
  ///     2) entry emplaced but not linked
  ///     3) entry emplaced and linked in the forward direction
  ///     4) entry emplaced and linked in both directions
  inline DListIterator EmplaceBetween(
      DListIterator iter_prev, DListIterator iter_next,
      std::uint64_t timestamp, // Timestamp can only be supplied by caller
      pmem::obj::string_view const key, pmem::obj::string_view const value,
      DataEntryType type) {
    kvdk_assert(type == DataEntryType::DlistDataRecord,
                "Trying to emplace invalid Record!");
    kvdk_assert(iter_prev && iter_next, "Invalid iterator in dlinked_list!");

    auto space = p_pmem_allocator_->Allocate(sizeof(DLDataEntry) + key.size() +
                                             value.size());
    if (space.size == 0) {
      throw std::bad_alloc{};
    }
    std::uint64_t offset = space.space_entry.offset;
    void *pmp = p_pmem_allocator_->offset2addr_checked(offset);

    DLDataEntry entry; // Set up entry with meta
    {
      entry.timestamp = timestamp;
      entry.type = type;
      entry.k_size = key.size();
      entry.v_size = value.size();

      // checksum can only be calculated with complete meta
      entry.header.b_size = space.size;
      entry.header.checksum = checkSum(entry, key, value);

      entry.prev = iter_prev.GetOffset();
      entry.next = iter_next.GetOffset();
      // entry is now complete
    }

    persistRecord(pmp, entry, key, value);
    iter_prev->next = offset;
    pmem_persist(&iter_prev->next, sizeof(offset));
    iter_next->prev = offset;
    pmem_persist(&iter_next->prev, sizeof(offset));

    return DListIterator{p_pmem_allocator_, static_cast<DLDataEntry *>(pmp)};
  }

  // Connect prev and next of node addressed by iter_record_to_erase,
  // logically delete this record by de-link it.
  // Return iterator at next node.
  inline DListIterator Erase(DListIterator iter_record_to_erase) {
    DListIterator iter_prev{iter_record_to_erase};
    --iter_prev;
    DListIterator iter_next{iter_record_to_erase};
    ++iter_next;
    kvdk_assert(iter_prev && iter_next, "Invalid iterator in dlinked_list!");
    auto prev_offset = iter_prev.GetOffset();
    auto next_offset = iter_next.GetOffset();
    iter_prev->next = next_offset;
    pmem_persist(&iter_prev->next, sizeof(next_offset));
    iter_next->prev = prev_offset;
    pmem_persist(&iter_next->prev, sizeof(prev_offset));

    return iter_next;
  }

private:
  /// Persist a DLDataEntry.
  /// The caller must supply complete DLDataEntry, aka,
  /// a DLDataEntry with pre-calculated checksum
  inline static void persistRecord(
      void *pmp,
      DLDataEntry const &entry, // Complete DLDataEntry supplied by caller
      pmem::obj::string_view const key, pmem::obj::string_view const value) {
    // Persist key and value
    char *pmp_dest = static_cast<char *>(pmp);
    pmem_memcpy(pmp_dest, &entry, sizeof(DLDataEntry),
                PMEM_F_MEM_NOFLUSH | PMEM_F_MEM_NONTEMPORAL);
    pmp_dest += sizeof(DLDataEntry);
    pmem_memcpy(pmp_dest, key.data(), key.size(),
                PMEM_F_MEM_NOFLUSH | PMEM_F_MEM_NONTEMPORAL);
    pmp_dest += key.size();
    pmem_memcpy(pmp_dest, value.data(), value.size(),
                PMEM_F_MEM_NOFLUSH | PMEM_F_MEM_NONTEMPORAL);
    pmem_persist(pmp, sizeof(DLDataEntry) + key.size() + value.size());
  }

  /// Compute Checksum of the to-be-emplaced record
  /// with meta, key and value.
  inline static std::uint32_t checkSum(
      DLDataEntry const &entry, // Incomplete DLDataEntry, only meta is valid
      pmem::obj::string_view const key, pmem::obj::string_view const value) {
    std::uint32_t cs1 = get_checksum(
        reinterpret_cast<char const *>(&entry) + sizeof(decltype(entry.header)),
        sizeof(DataEntry) - sizeof(decltype(entry.header)));
    std::uint32_t cs2 = get_checksum(key.data(), key.size());
    std::uint32_t cs3 = get_checksum(value.data(), value.size());
    return cs1 + cs2 + cs3;
  }

  /// Extract user-key from internal-key used by the collection.
  /// Internal key has 8-byte ID as prefix.
  /// User-key does not have that ID.
  inline static pmem::obj::string_view
  extractKey(pmem::obj::string_view internal_key) {
    constexpr size_t sz_id = 8;
    assert(sz_id <= internal_key.size() &&
           "internal_key does not has space for key");
    return pmem::obj::string_view(internal_key.data() + sz_id,
                                  internal_key.size() - sz_id);
  }

  /// Extract ID from internal-key
  inline static std::uint64_t extractID(pmem::obj::string_view internal_key) {
    std::uint64_t id;
    assert(sizeof(decltype(id)) <= internal_key.size() &&
           "internal_key is smaller than the size of an id!");
    memcpy(&id, internal_key.data(), sizeof(decltype(id)));
    return id;
  }

  /// Output DlinkedList to ostream for debugging purpose.
  friend std::ostream &operator<<(std::ostream &out, DLinkedList const &dlist) {
    out << "Contents of DlinkedList:\n";
    DListIterator iter =
        DListIterator{dlist.p_pmem_allocator_, dlist.pmp_head_};
    DListIterator iter_end =
        DListIterator{dlist.p_pmem_allocator_, dlist.pmp_tail_};
    while (iter != iter_end) {
      auto internal_key = iter->Key();
      out << "Type: " << hex_print(iter->type) << "\t"
          << "Offset: " << hex_print(iter.GetOffset()) << "\t"
          << "Prev: " << hex_print(iter->prev) << "\t"
          << "Next: " << hex_print(iter->next) << "\t"
          << "Key: " << hex_print(extractID(internal_key))
          << extractKey(internal_key) << "\t"
          << "Value: " << iter->Value() << "\n";
      ++iter;
    }
    auto internal_key = iter->Key();
    out << "Type: " << hex_print(iter->type) << "\t"
        << "Offset: " << hex_print(iter.GetOffset()) << "\t"
        << "Prev: " << hex_print(iter->prev) << "\t"
        << "Next: " << hex_print(iter->next) << "\t"
        << "Key: " << hex_print(extractID(internal_key))
        << extractKey(internal_key) << "\t"
        << "Value: " << iter->Value() << "\n";
    return out;
  }
};

} // namespace KVDK_NAMESPACE
