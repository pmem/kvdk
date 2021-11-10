/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cassert>
#include <cstdint>

#include <algorithm>

#include "kvdk/engine.hpp"
#include "kvdk/iterator.hpp"

#include "dlinked_list.hpp"
#include "macros.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "structures.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {

class Queue final {
private:
  using LockType = std::unique_lock<SpinMutex>;

  /// DlistRecord for recovering
  DLRecord *collection_record_ptr_;

  /// DLinkedList manages data on PMem, also hold a PMemAllocator
  using DLinkedListType =
      DLinkedList<RecordType::QueueHeadRecord, RecordType::QueueTailRecord,
                  RecordType::QueueDataRecord>;
  using iterator = DLinkedListType::iterator;
  DLinkedListType dlinked_list_;

  std::string collection_name_;
  CollectionIDType collection_id_;
  TimeStampType timestamp_;
  size_t sz_ = 0;

  SpinMutex queue_lock_;
  SpinMutex head_lock_;
  SpinMutex tail_lock_;

public:
  Queue(PMEMAllocator *pmem_allocator_ptr, std::string const name,
        CollectionIDType id, TimeStampType timestamp);

  Queue(PMEMAllocator *pmem_allocator_ptr, DLRecord *collection_record);

  void LPush(TimeStampType timestamp, StringView const value);

  void RPush(TimeStampType timestamp, StringView const value);

  bool LPop(std::string *value_got);

  bool RPop(std::string *value_got);

  inline CollectionIDType ID() const { return collection_id_; }

  inline std::string const &Name() const { return collection_name_; }

  inline TimeStampType Timestamp() const { return timestamp_; };

  inline std::string GetInternalKey(StringView key) {
    return makeInternalKey(collection_id_, key);
  }

private:
  inline static bool isAdjacent(iterator prev, iterator next) {
    iterator curr{prev};
    if (++curr != next)
      return false;
    if (--curr != prev)
      return false;
    return true;
  }

  inline bool isLinked(DLRecord *pos) {
    iterator curr = dlinked_list_.makeIterator(pos);
    iterator prev{curr};
    --prev;
    iterator next{curr};
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

  inline static std::string makeInternalKey(CollectionIDType id,
                                            StringView key) {
    std::string internal_key{id2View(id)};
    internal_key += key;
    return internal_key;
  }

  inline std::string makeInternalKey(StringView key) {
    return makeInternalKey(collection_id_, key);
  }

  inline static StringView extractKey(StringView internal_key) {
    constexpr size_t sz_id = sizeof(CollectionIDType);
    // Allow empty string as key
    assert(sz_id <= internal_key.size() &&
           "internal_key does not has space for key");
    return StringView(internal_key.data() + sz_id, internal_key.size() - sz_id);
  }

  inline static CollectionIDType extractID(StringView internal_key) {
    CollectionIDType id;
    assert(sizeof(CollectionIDType) <= internal_key.size() &&
           "internal_key is smaller than the size of an id!");
    memcpy(&id, internal_key.data(), sizeof(CollectionIDType));
    return id;
  }

  inline static StringView id2View(CollectionIDType id) {
    // Thread local copy to prevent variable destruction
    thread_local CollectionIDType id_copy;
    id_copy = id;
    return StringView{reinterpret_cast<char *>(&id_copy),
                      sizeof(CollectionIDType)};
  }

  inline static CollectionIDType view2ID(StringView view) {
    CollectionIDType id;
    assert(sizeof(CollectionIDType) == view.size() &&
           "id_view does not match the size of an id!");
    memcpy(&id, view.data(), sizeof(CollectionIDType));
    return id;
  }

  inline iterator makeInternalIterator(DLRecord *pos) {
    return dlinked_list_.makeIterator(pos);
  }
};

} // namespace KVDK_NAMESPACE
