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
        snapshot_(snapshot),
        own_snapshot_(own_snapshot),
        dl_iter_(&skiplist->dl_list_, pmem_allocator, snapshot, own_snapshot) {}

  virtual ~SortedIterator() = default;

  virtual void Seek(const std::string& key) override {
    assert(skiplist_);
    Splice splice(skiplist_);
    skiplist_->Seek(key, &splice);
    dl_iter_.Locate(splice.next_pmem_record, true);
  }

  virtual void SeekToFirst() override { dl_iter_.SeekToFirst(); }

  virtual void SeekToLast() override { dl_iter_.SeekToLast(); }

  virtual bool Valid() override { return dl_iter_.Valid(); }

  virtual void Next() override { dl_iter_.Next(); }

  virtual void Prev() override { dl_iter_.Prev(); }

  virtual std::string Key() override {
    if (!Valid()) return "";
    return string_view_2_string(Skiplist::ExtractUserKey(dl_iter_.Key()));
  }

  virtual std::string Value() override {
    if (!Valid()) return "";
    return string_view_2_string(dl_iter_.Value());
  }

 private:
  friend KVEngine;

  Skiplist* skiplist_;
  const SnapshotImpl* snapshot_;
  bool own_snapshot_;
  DLListAccessIterator dl_iter_;
};
}  // namespace KVDK_NAMESPACE