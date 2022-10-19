/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#pragma once

#include "../version/version_controller.hpp"
#include "hash_list.hpp"
#include "kvdk/volatile/engine.hpp"
#include "kvdk/volatile/iterator.hpp"

namespace KVDK_NAMESPACE {
class KVEngine;

class HashIteratorImpl final : public HashIterator {
 public:
  HashIteratorImpl(HashList* hlist, const SnapshotImpl* snapshot,
                   bool own_snapshot)
      : hlist_(hlist),
        snapshot_(snapshot),
        own_snapshot_(own_snapshot),
        dl_iter_(&hlist->dl_list_, hlist->kv_allocator_, snapshot) {}
  void SeekToFirst() final { dl_iter_.SeekToFirst(); }

  void SeekToLast() final { dl_iter_.SeekToLast(); }

  bool Valid() const final { return dl_iter_.Valid(); }

  void Next() final { dl_iter_.Next(); }

  void Prev() final { dl_iter_.Prev(); }

  std::string Key() const final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid HashIterator!");
      return std::string{};
    }
    return string_view_2_string(Collection::ExtractUserKey(dl_iter_.Key()));
  }

  std::string Value() const final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid HashIterator!");
      return std::string{};
    }
    return string_view_2_string(dl_iter_.Value());
  }

  bool MatchKey(std::regex const& re) final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid HashIterator!");
      return false;
    }
    return std::regex_match(Key(), re);
  }

 private:
  friend KVEngine;

  HashList* hlist_;
  const SnapshotImpl* snapshot_;
  bool own_snapshot_;
  DLListDataIterator dl_iter_;
};
}  // namespace KVDK_NAMESPACE