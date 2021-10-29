/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <algorithm>

#include <cassert>
#include <cstdint>

#include "kvdk/engine.hpp"
#include "kvdk/iterator.hpp"
#include "kvdk/macros.h"

#include "dlinked_list.hpp"
#include "hash_table.hpp"
#include "structures.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {

struct EmplaceReturn {
  // Offset of newly emplaced Record
  PMemOffsetType offset_new;
  // Offset of old Record for Replace. Otherwise set as FailOffset
  PMemOffsetType offset_old;
  bool success;

  explicit EmplaceReturn()
      : offset_new{FailOffset}, offset_old{FailOffset}, success{false} {}

  explicit EmplaceReturn(PMemOffsetType offset_new_, PMemOffsetType offset_old_,
                         bool emplace_result)
      : offset_new{offset_new_}, offset_old{offset_old_}, success{
                                                              emplace_result} {}

  EmplaceReturn &operator=(EmplaceReturn const &other) {
    offset_new = other.offset_new;
    offset_old = other.offset_old;
    success = other.success;
    return *this;
  }

  static constexpr PMemOffsetType FailOffset = kNullPmemOffset;
};
} // namespace KVDK_NAMESPACE

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
    : public std::enable_shared_from_this<UnorderedCollection> {
private:
  using LockType = std::unique_lock<SpinMutex>;
  using LockPair = std::pair<LockType, LockType>;

  /// For locking, locking only
  static HashTable *hash_table_ptr;

  /// DlistRecord for recovering
  DLRecord *collection_record_ptr;

  /// DLinkedList manages data on PMem, also hold a PMemAllocator
  using DLinkedListType =
      DLinkedList<RecordType::DlistHeadRecord, RecordType::DlistTailRecord,
                  RecordType::DlistDataRecord>;
  using iterator = DLinkedListType::iterator;
  DLinkedListType dlinked_list;

  std::string collection_name;
  CollectionIDType collection_id;
  TimeStampType timestamp;

  friend class UnorderedIterator;

public:
  static void SetPMemAllocatorPtr(PMEMAllocator *ptr) {
    DLinkedListType::SetPMemAllocatorPtr(ptr);
  }

  static void ResetPMemAllocatorPtr() {
    DLinkedListType::ResetPMemAllocatorPtr();
  }

  static void SetHashTablePtr(HashTable *ptr) { hash_table_ptr = ptr; }

  static void ResetHashTablePtr() { hash_table_ptr = nullptr; }

  /// Create UnorderedCollection and persist it on PMem
  /// DlistHeadRecord and DlistTailRecord holds ID as key
  /// and empty string as value
  /// DlistRecord holds collection name as key
  /// and ID as value
  UnorderedCollection(std::string const name, CollectionIDType id,
                      TimeStampType timestamp);

  /// Recover UnorderedCollection from DLIST_RECORD
  UnorderedCollection(DLRecord *collection_record);

  /// Emplace a Record into the Collection
  /// lock to emplaced node must been acquired before being passed in
  EmplaceReturn Emplace(std::uint64_t timestamp, StringView const key,
                        StringView const value, LockType const &lock);

  EmplaceReturn Replace(DLRecord *pos, std::uint64_t timestamp,
                        StringView const key, StringView const value,
                        LockType const &lock);

  /// Erase given record
  /// Return new_offset as next record
  /// old_offset as erased record
  EmplaceReturn Erase(DLRecord *pos, LockType const &lock);

  /// Deallocate a Record given by caller.
  /// Emplace functions does not do deallocations.
  inline static void Deallocate(DLRecord *record_pmmptr) {
    DLinkedListType::Deallocate(iterator{record_pmmptr});
  }

  inline std::uint64_t ID() const { return collection_id; }

  inline std::string const &Name() const { return collection_name; }

  inline std::uint64_t Timestamp() const { return timestamp; };

  inline std::string GetInternalKey(StringView key) {
    return makeInternalKey(collection_id, key);
  }

  friend std::ostream &operator<<(std::ostream &out,
                                  UnorderedCollection const &col) {
    auto iter = col.collection_record_ptr;
    auto internal_key = iter->Key();
    out << "Name: " << col.Name() << "\t"
        << "ID: " << to_hex(col.ID()) << "\n";
    out << "Type: " << to_hex(iter->entry.meta.type) << "\t"
        << "Prev: " << to_hex(iter->prev) << "\t"
        << "Next: " << to_hex(iter->next) << "\t"
        << "Key: " << iter->Key() << "\t"
        << "Value: " << iter->Value() << "\n";
    out << col.dlinked_list;
    return out;
  }

private:
  inline static bool lockPositions(iterator pos1, iterator pos2,
                                   LockType const &lock,
                                   LockPair &lock_holder) {
    SpinMutex *spin = lock.mutex();
    SpinMutex *spin1 = getMutex(pos1->Key());
    SpinMutex *spin2 = getMutex(pos2->Key());

    kvdk_assert(lock.owns_lock(), "User supplied lock not acquired!")

        if (spin1 != spin) {
      lock_holder.first = LockType{*spin1, std::defer_lock};
      if (!lock_holder.first.try_lock())
        return false;
    }
    if (spin2 != spin && spin2 != spin1) {
      lock_holder.second = LockType{*spin2, std::defer_lock};
      if (!lock_holder.second.try_lock())
        return false;
    }
    return true;
  }

  inline static bool isAdjacent(iterator prev, iterator next) {
    iterator curr{prev};
    if (++curr != next)
      return false;
    if (--curr != prev)
      return false;
    return true;
  }

  inline static bool isLinked(DLRecord *pos) {
    iterator curr{pos};
    iterator prev{pos};
    --prev;
    iterator next{pos};
    ++next;
    return (--next == curr) && (++prev == curr);
  }

  inline bool checkID(DLRecord *record_pmmptr) {
    if (!record_pmmptr || extractID(record_pmmptr->Key()) != ID())
      return false;
    return true;
  }

  // Check if the Record is a valid record linked in current collection
  inline bool isValidRecord(DLRecord *record_pmmptr) {
    return checkID(record_pmmptr) &&
           (static_cast<RecordType>(record_pmmptr->entry.meta.type) ==
            RecordType::DlistDataRecord) &&
           isLinked(record_pmmptr);
  }

  inline static std::string makeInternalKey(std::uint64_t id, StringView key) {
    std::string internal_key{id2View(id)};
    internal_key += key;
    return internal_key;
  }

  inline std::string makeInternalKey(StringView key) {
    return makeInternalKey(collection_id, key);
  }

  inline static StringView extractKey(StringView internal_key) {
    constexpr size_t sz_id = sizeof(decltype(collection_id));
    // Allow empty string as key
    assert(sz_id <= internal_key.size() &&
           "internal_key does not has space for key");
    return StringView(internal_key.data() + sz_id, internal_key.size() - sz_id);
  }

  inline static std::uint64_t extractID(StringView internal_key) {
    std::uint64_t id;
    assert(sizeof(decltype(id)) <= internal_key.size() &&
           "internal_key is smaller than the size of an id!");
    memcpy(&id, internal_key.data(), sizeof(decltype(id)));
    return id;
  }

  inline static StringView id2View(std::uint64_t id) {
    // Thread local copy to prevent variable destruction
    thread_local uint64_t id_copy;
    id_copy = id;
    return StringView{reinterpret_cast<char *>(&id_copy),
                      sizeof(decltype(id_copy))};
  }

  inline static std::uint64_t view2ID(StringView view) {
    std::uint64_t id;
    assert(sizeof(decltype(id)) == view.size() &&
           "id_view does not match the size of an id!");
    memcpy(&id, view.data(), sizeof(decltype(id)));
    return id;
  }

  inline static SpinMutex *getMutex(StringView internal_key) {
    return hash_table_ptr->GetHint(internal_key).spin;
  }
};

} // namespace KVDK_NAMESPACE

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

  /// [Deprecated?]
  /// Construct UnorderedIterator of a certain UnorderedCollection
  /// pointing to a DLRecord belonging to this collection
  /// Runtime checking the type of this UnorderedIterator,
  /// which can be DlistDataRecord, DlistHeadRecord and
  /// DlistTailRecord ID is also checked. Checking failure results in throwing
  /// runtime_error. Valid() is true only if the iterator points to
  /// DlistDataRecord
  UnorderedIterator(std::shared_ptr<UnorderedCollection> sp_coll,
                    DLRecord *record_pmmptr);

  /// UnorderedIterator currently does not support Seek to a key
  virtual void Seek(std::string const &key) final override {
    throw std::runtime_error{"UnorderedIterator does not support Seek()!"};
  }

  /// Seek to First DlistDataRecord if exists,
  /// otherwise Valid() will return false.
  virtual void SeekToFirst() final override {
    internal_iterator = collection_shrdptr->dlinked_list.Head();
    internalNext();
  }

  /// Seek to Last DlistDataRecord if exists,
  /// otherwise Valid() will return false.
  virtual void SeekToLast() final override {
    internal_iterator = collection_shrdptr->dlinked_list.Tail();
    internalPrev();
  }

  /// Valid() is true only if the UnorderedIterator points to a DlistDataRecord.
  /// DlistHeadRecord, DlistTailRecord is considered
  /// invalid. User should always check Valid() before accessing data with Key()
  /// and Value() Iterating with Next() and Prev()
  inline virtual bool Valid() final override { return valid; }

  /// Try proceeding to next DlistDataRecord.
  /// User should check Valid() before accessing data.
  /// Calling Next() on invalid UnorderedIterator will do nothing.
  /// This prevents any further misuses.
  virtual void Next() final override {
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
    auto view_key = UnorderedCollection::extractKey(internal_iterator->Key());
    return std::string(view_key.data(), view_key.size());
  }

  /// return value in DlistDataRecord
  /// throw runtime_error if !Valid()
  inline virtual std::string Value() override {
    kvdk_assert(Valid(), "Accessing data with invalid UnorderedIterator!");
    auto view_value = internal_iterator->Value();
    return std::string(view_value.data(), view_value.size());
  }

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

} // namespace KVDK_NAMESPACE
