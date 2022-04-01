/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>

#include "alias.hpp"
#include "dlinked_list.hpp"
#include "hash_table.hpp"
#include "kvdk/engine.hpp"
#include "kvdk/iterator.hpp"
#include "macros.hpp"
#include "structures.hpp"
#include "utils/utils.hpp"

namespace KVDK_NAMESPACE {

struct ModifyReturn {
  // Offset of newly emplaced Record for Emplace, otherwise set as FailOffset.
  PMemOffsetType offset_new;
  // Offset of old Record for Replace or Erase, otherwise set as FailOffset.
  PMemOffsetType offset_old;
  bool success;

  explicit ModifyReturn()
      : offset_new{FailOffset}, offset_old{FailOffset}, success{false} {}

  explicit ModifyReturn(PMemOffsetType offset_new_, PMemOffsetType offset_old_,
                        bool emplace_result)
      : offset_new{offset_new_},
        offset_old{offset_old_},
        success{emplace_result} {}

  ModifyReturn& operator=(ModifyReturn const& other) {
    offset_new = other.offset_new;
    offset_old = other.offset_old;
    success = other.success;
    return *this;
  }

  static constexpr PMemOffsetType FailOffset = kNullPMemOffset;
};
}  // namespace KVDK_NAMESPACE

namespace KVDK_NAMESPACE {
class UnorderedIterator;

/// UnorderedCollection is stored in DRAM, indexed by HashTable
/// A Record DlistRecord is stored in PMem,
/// whose key is the name of the UnorderedCollection
/// and value holds the ID of the Collection
/// prev and next pointer holds the head and tail of DLinkedList for recovery
/// At runtime, an object of UnorderedCollection is recovered from
/// the DlistRecord and then stored in HashTable.
/// The DlistRecord is for recovery only and never visited again
class UnorderedCollection final
    : public std::enable_shared_from_this<UnorderedCollection>,
      public Collection {
 private:
  using LockType = std::unique_lock<SpinMutex>;
  using LockPair = std::pair<LockType, LockType>;

  /// For locking, locking only
  HashTable* hash_table_ptr_;

  /// DlistRecord for recovering
  DLRecord* collection_record_ptr_;

  /// DLinkedList manages data on PMem, also hold a PMemAllocator
  using DLinkedListType =
      DLinkedList<RecordType::DlistHeadRecord, RecordType::DlistTailRecord,
                  RecordType::DlistDataRecord>;
  using iterator = DLinkedListType::iterator;
  DLinkedListType dlinked_list_;

  TimeStampType timestamp_;

  friend class UnorderedIterator;

 public:
  /// Create UnorderedCollection and persist it on PMem
  /// DlistHeadRecord and DlistTailRecord holds ID as key
  /// and empty string as value
  /// DlistRecord holds collection name as key
  /// and ID as value
  UnorderedCollection(HashTable* hash_table_ptr,
                      PMEMAllocator* pmem_allocator_ptr, std::string const name,
                      CollectionIDType id, TimeStampType timestamp);

  /// Recover UnorderedCollection from DLIST_RECORD
  UnorderedCollection(HashTable* hash_table_ptr,
                      PMEMAllocator* pmem_allocator_ptr,
                      DLRecord* collection_record);

  /// Emplace a Record into the Collection
  /// lock to emplaced node must been acquired before being passed in
  ModifyReturn Emplace(TimeStampType timestamp, StringView const key,
                       StringView const value, LockType const& lock);

  ModifyReturn Replace(DLRecord* pos, TimeStampType timestamp,
                       StringView const key, StringView const value,
                       LockType const& lock);

  ExpireTimeType GetExpireTime() const final {
    return collection_record_ptr_->GetExpireTime();
  }

  bool HasExpired() const final {
    return TimeUtils::CheckIsExpired(GetExpireTime());
  }

  Status SetExpireTime(ExpireTimeType time) final {
    collection_record_ptr_->PersistExpireTimeNT(time);
    return Status::Ok;
  }

  /// Erase given record
  /// Return new_offset as next record
  /// old_offset as erased record
  ModifyReturn Erase(DLRecord* pos, LockType const& lock);

  inline TimeStampType Timestamp() const { return timestamp_; };

  friend std::ostream& operator<<(std::ostream& out,
                                  UnorderedCollection const& col) {
    auto iter = col.collection_record_ptr_;
    out << "Name: " << col.Name() << "\t"
        << "ID: " << to_hex(col.ID()) << "\n";
    out << "Type: " << to_hex(iter->entry.meta.type) << "\t"
        << "Prev: " << to_hex(iter->prev) << "\t"
        << "Next: " << to_hex(iter->next) << "\t"
        << "Key: " << iter->Key() << "\t"
        << "Value: " << iter->Value() << "\n";
    out << col.dlinked_list_;
    return out;
  }

 private:
  inline bool lockPositions(iterator pos1, iterator pos2, LockType const& lock,
                            LockPair& lock_holder) {
    SpinMutex* spin = lock.mutex();
    SpinMutex* spin1 = getMutex(pos1->Key());
    SpinMutex* spin2 = getMutex(pos2->Key());

    kvdk_assert(lock.owns_lock(), "User supplied lock not acquired!");

    if (spin1 != spin) {
      lock_holder.first = LockType{*spin1, std::defer_lock};
      if (!lock_holder.first.try_lock()) return false;
    }
    if (spin2 != spin && spin2 != spin1) {
      lock_holder.second = LockType{*spin2, std::defer_lock};
      if (!lock_holder.second.try_lock()) return false;
    }
    return true;
  }

  inline static bool isAdjacent(iterator prev, iterator next) {
    iterator curr{prev};
    if (++curr != next) return false;
    if (--curr != prev) return false;
    return true;
  }

  inline bool isLinked(DLRecord* pos) {
    iterator curr = dlinked_list_.makeIterator(pos);
    iterator prev{curr};
    --prev;
    iterator next{curr};
    ++next;
    return (--next == curr) && (++prev == curr);
  }

  inline bool checkID(DLRecord* record_pmmptr) {
    if (!record_pmmptr || ExtractID(record_pmmptr->Key()) != ID()) return false;
    return true;
  }

  // Check if the Record is a valid record linked in current collection
  inline bool isValidRecord(DLRecord* record_pmmptr) {
    return checkID(record_pmmptr) &&
           (static_cast<RecordType>(record_pmmptr->entry.meta.type) ==
            RecordType::DlistDataRecord) &&
           isLinked(record_pmmptr);
  }

  inline SpinMutex* getMutex(StringView internal_key) {
    return hash_table_ptr_->GetHint(internal_key).spin;
  }

  inline iterator makeInternalIterator(DLRecord* pos) {
    return dlinked_list_.makeIterator(pos);
  }
};

}  // namespace KVDK_NAMESPACE

namespace KVDK_NAMESPACE {
class UnorderedIterator final : public Iterator {
 private:
  /// shared pointer to pin the UnorderedCollection
  std::shared_ptr<UnorderedCollection> collection_shrdptr;
  using DLinkedListType =
      DLinkedList<RecordType::DlistHeadRecord, RecordType::DlistTailRecord,
                  RecordType::DlistDataRecord>;
  using iterator = DLinkedListType::iterator;

  iterator internal_iterator;
  /// Whether the UnorderedIterator is at a DlistDataRecord
  bool valid;

  friend class UnorderedCollection;

 public:
  /// Construct UnorderedIterator of a given UnorderedCollection
  /// The Iterator is invalid now.
  /// Must SeekToFirst() or SeekToLast() before use.
  UnorderedIterator(std::shared_ptr<UnorderedCollection> sp_coll);

  /// UnorderedIterator currently does not support Seek to a key
  [[gnu::deprecated]] virtual void Seek(std::string const&) final {
    throw std::runtime_error{"UnorderedIterator does not support Seek()!"};
  }

  IteratorType Type() const final { return IteratorType::HashIterator; }

  /// Seek to First DlistDataRecord if exists,
  /// otherwise Valid() will return false.
  virtual void SeekToFirst() final {
    internal_iterator = collection_shrdptr->dlinked_list_.Head();
    internalNext();
  }

  /// Seek to Last DlistDataRecord if exists,
  /// otherwise Valid() will return false.
  virtual void SeekToLast() final {
    internal_iterator = collection_shrdptr->dlinked_list_.Tail();
    internalPrev();
  }

  /// Valid() is true only if the UnorderedIterator points to a DlistDataRecord.
  /// DlistHeadRecord, DlistTailRecord is considered
  /// invalid. User should always check Valid() before accessing data with Key()
  /// and Value() Iterating with Next() and Prev()
  inline virtual bool Valid() final { return valid; }

  /// Try proceeding to next DlistDataRecord.
  /// User should check Valid() before accessing data.
  /// Calling Next() on invalid UnorderedIterator will do nothing.
  /// This prevents any further misuses.
  virtual void Next() final {
    if (Valid()) {
      internalNext();
    }
    return;
  }

  /// Try proceeding to previous DlistDataRecord
  /// User should check Valid() before accessing data
  /// Calling Prev() on invalid UnorderedIterator will do nothing.
  /// This prevents any further misuses
  virtual void Prev() final override {
    if (Valid()) {
      internalPrev();
    }
    return;
  }

  /// return key in DlistDataRecord
  inline virtual std::string Key() override {
    kvdk_assert(Valid(), "Accessing data with invalid UnorderedIterator!");
    auto view_key = Collection::ExtractUserKey(internal_iterator->Key());
    return std::string(view_key.data(), view_key.size());
  }

  /// return value in DlistDataRecord
  /// throw runtime_error if !Valid()
  inline virtual std::string Value() override {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid UnorderedIterator!");
      return std::string{};
    }
    auto view_value = internal_iterator->Value();
    return std::string(view_value.data(), view_value.size());
  }

  virtual ~UnorderedIterator() = default;

 private:
  // Proceed to next DlistDataRecord, can start from
  // DlistHeadRecord, DlistDataRecord
  // If reached DlistTailRecord, valid_ is set to false and returns
  void internalNext();

  // Proceed to prev DlistDataRecord, can start from
  // DlistTailRecord, DlistDataRecord
  // If reached DlistHeadRecord, valid_ is set to false and returns
  void internalPrev();
};

}  // namespace KVDK_NAMESPACE
