/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#pragma once

#include "../version/version_controller.hpp"
#include "kvdk/volatile/engine.hpp"
#include "kvdk/volatile/iterator.hpp"
#include "list.hpp"

namespace KVDK_NAMESPACE {
class ListIteratorImpl final : public ListIterator {
 public:
  ListIteratorImpl(List* list, const SnapshotImpl* snapshot, bool own_snapshot)
      : list_(list),
        snapshot_(snapshot),
        own_snapshot_(own_snapshot),
        dl_iter_(&list->dl_list_, list->kv_allocator_, snapshot) {}

  void Seek(long index) final {
    if (index < 0) {
      SeekToLast();
      long cur = -1;
      while (cur-- > index && Valid()) {
        Prev();
      }
    } else {
      SeekToFirst();
      long cur = 0;
      while (cur++ < index && Valid()) {
        Next();
      }
    }
  }

  void SeekToFirst() final { dl_iter_.SeekToFirst(); }

  void SeekToLast() final { dl_iter_.SeekToLast(); }

  void SeekToFirst(StringView elem) final {
    SeekToFirst();
    Next(elem);
  }

  void SeekToLast(StringView elem) final {
    SeekToLast();
    Prev(elem);
  }

  bool Valid() const final { return dl_iter_.Valid(); }

  void Next() final { dl_iter_.Next(); }

  void Prev() final { dl_iter_.Prev(); }

  void Next(StringView elem) final {
    while (Valid()) {
      Next();
      if (!Valid() || equal_string_view(elem, dl_iter_.Value())) {
        break;
      }
    }
  }

  void Prev(StringView elem) final {
    while (Valid()) {
      Prev();
      if (!Valid() || equal_string_view(elem, dl_iter_.Value())) {
        break;
      }
    }
  }

  std::string Value() const final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid ListIterator!");
      return std::string{};
    }
    auto sw = dl_iter_.Value();
    return std::string{sw.data(), sw.size()};
  }

 private:
  friend KVEngine;

  List* list_;
  const SnapshotImpl* snapshot_;
  bool own_snapshot_;
  DLListDataIterator dl_iter_;
};
}  // namespace KVDK_NAMESPACE