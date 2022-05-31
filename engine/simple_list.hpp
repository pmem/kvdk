#pragma once

#include <mutex>
#include <stdexcept>

#include "generic_list.hpp"
#include "kvdk/iterator.hpp"

namespace KVDK_NAMESPACE {
using List = GenericList<RecordType::ListRecord, RecordType::ListElem>;
using ListBuilder =
    GenericListBuilder<RecordType::ListRecord, RecordType::ListElem>;

class ListIteratorImpl final : public ListIterator {
 public:
  void Seek(IndexType pos) final {
    if (list == nullptr) {
      return;
    }
    rep = list->Seek(pos);
  }

  void SeekToFirst() final {
    if (list == nullptr) {
      return;
    }
    rep = list->Front();
  }

  void SeekToLast() final {
    if (list == nullptr) {
      return;
    }
    rep = list->Back();
  }

  void Next() final {
    if (list == nullptr) {
      return;
    }
    ++rep;
  }

  void Prev() final {
    if (list == nullptr) {
      return;
    }
    --rep;
  }

  void SeekToFirst(StringView elem) final {
    if (list == nullptr) {
      return;
    }
    SeekToFirst();
    while (Valid() && elem != rep->Value()) {
      ++rep;
    }
  }

  void SeekToLast(StringView elem) final {
    if (list == nullptr) {
      return;
    }
    SeekToLast();
    while (Valid() && elem != rep->Value()) {
      --rep;
    }
  }

  void Next(StringView elem) final {
    if (list == nullptr) {
      return;
    }
    if (!Valid()) return;
    ++rep;
    while (Valid() && elem != rep->Value()) {
      ++rep;
    }
  }

  void Prev(StringView elem) final {
    if (list == nullptr) {
      return;
    }
    if (!Valid()) return;
    --rep;
    while (Valid() && elem != rep->Value()) {
      --rep;
    }
  }

  bool Valid() const final {
    // list->Head() == list->Tail()
    return (s == Status::Ok && rep != list->Tail());
  }

  Status CurrentStatus() const final { return s; }

  std::string Value() const final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid ListIterator!");
      return std::string{};
    }
    auto sw = rep->Value();
    return std::string{sw.data(), sw.size()};
  }

  ~ListIteratorImpl() final = default;

 public:
  ListIteratorImpl(List* l, Status status) : list{l}, rep{l}, s{status} {
    if (list != nullptr) {
      rep = l->Front();
      guard = list->AcquireLock();
    }
  }

  List::Iterator& Rep() { return rep; }

  List* Owner() const { return list; }

 private:
  List* list;
  List::Iterator rep;
  Status s;
  std::unique_lock<std::recursive_mutex> guard;
};

}  // namespace KVDK_NAMESPACE
