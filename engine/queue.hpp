/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cassert>
#include <cstdint>

#include <algorithm>

#include "kvdk/engine.hpp"
#include "kvdk/iterator.hpp"

#include "collection.hpp"
#include "dlinked_list.hpp"
#include "macros.hpp"
#include "pmem_allocator/pmem_allocator.hpp"
#include "structures.hpp"
#include "utils.hpp"

namespace KVDK_NAMESPACE {

class Queue final : public Collection {
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

  TimestampType timestamp_;
  size_t sz_ = 0;

  SpinMutex queue_lock_;
  SpinMutex head_lock_;
  SpinMutex tail_lock_;

public:
  Queue(PMEMAllocator *pmem_allocator_ptr, std::string const name,
        CollectionIDType id, TimestampType timestamp);

  Queue(PMEMAllocator *pmem_allocator_ptr, DLRecord *collection_record);

  void PushFront(TimestampType timestamp, StringView const value);

  void PushBack(TimestampType timestamp, StringView const value);

  bool PopFront(std::string *value_got);

  bool PopBack(std::string *value_got);

  inline TimestampType Timestamp() const { return timestamp_; };

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
    if (!record_pmmptr || ExtractID(record_pmmptr->Key()) != ID())
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

  inline iterator makeInternalIterator(DLRecord *pos) {
    return dlinked_list_.makeIterator(pos);
  }
};

} // namespace KVDK_NAMESPACE
