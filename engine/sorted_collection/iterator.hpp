/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "../alias.hpp"
#include "skiplist.hpp"

namespace KVDK_NAMESPACE {

class KVEngine;

class SortedIterator : public Iterator {
 public:
  SortedIterator(Skiplist* skiplist, const PMEMAllocator* pmem_allocator,
                 const SnapshotImpl* snapshot, bool own_snapshot)
      : skiplist_(skiplist),
        pmem_allocator_(pmem_allocator),
        current_(nullptr),
        snapshot_(snapshot),
        own_snapshot_(own_snapshot) {}

  virtual ~SortedIterator() = default;

  virtual void Seek(const std::string& key) override {
    assert(skiplist_);
    Splice splice(skiplist_);
    skiplist_->Seek(key, &splice);
    current_ = splice.next_pmem_record;
    skipInvalidRecords(true);
  }

  virtual void SeekToFirst() override {
    uint64_t first = skiplist_->HeaderRecord()->next;
    current_ = pmem_allocator_->offset2addr<DLRecord>(first);
    skipInvalidRecords(true);
  }

  virtual void SeekToLast() override {
    uint64_t last = skiplist_->HeaderRecord()->prev;
    current_ = pmem_allocator_->offset2addr<DLRecord>(last);
    skipInvalidRecords(false);
  }

  virtual bool Valid() override {
    return (current_ != nullptr &&
            current_->GetRecordMark().record_type == RecordMark::RecordType::SortedElem);
  }

  virtual void Next() override {
    if (!Valid()) {
      return;
    }
    current_ = pmem_allocator_->offset2addr_checked<DLRecord>(current_->next);
    skipInvalidRecords(true);
  }

  virtual void Prev() override {
    if (!Valid()) {
      return;
    }
    current_ = (pmem_allocator_->offset2addr<DLRecord>(current_->prev));
    skipInvalidRecords(false);
  }

  virtual std::string Key() override {
    if (!Valid()) return "";
    return string_view_2_string(Skiplist::UserKey(current_));
  }

  virtual std::string Value() override {
    if (!Valid()) return "";
    return string_view_2_string(current_->Value());
  }

 private:
  friend KVEngine;
  DLRecord* findValidVersion(DLRecord* pmem_record) {
    DLRecord* curr = pmem_record;
    TimeStampType ts = snapshot_->GetTimestamp();
    while (curr != nullptr && curr->entry.meta.timestamp > ts) {
      curr = pmem_allocator_->offset2addr<DLRecord>(curr->old_version);
      kvdk_assert(curr == nullptr || curr->Validate(),
                  "Broken checkpoint: invalid older version sorted record");
      kvdk_assert(
          curr == nullptr || equal_string_view(curr->Key(), pmem_record->Key()),
          "Broken checkpoint: key of older version sorted data is "
          "not same as new "
          "version");
    }
    return curr;
  }

  // Move current_ to next/prev valid version data record
  void skipInvalidRecords(bool forward) {
    while (Valid()) {
      DLRecord* valid_version_record = findValidVersion(current_);
      if (valid_version_record == nullptr ||
          valid_version_record->entry.meta.mark.record_status ==
              RecordMark::RecordStatus::Outdated) {
        current_ =
            forward
                ? pmem_allocator_->offset2addr_checked<DLRecord>(current_->next)
                : pmem_allocator_->offset2addr_checked<DLRecord>(
                      current_->prev);
      } else {
        current_ = valid_version_record;
        break;
      }
    }
  }

  Skiplist* skiplist_;
  const PMEMAllocator* pmem_allocator_;
  DLRecord* current_;
  const SnapshotImpl* snapshot_;
  bool own_snapshot_;
};
}  // namespace KVDK_NAMESPACE