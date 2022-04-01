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
  void Seek(StringView elem) final {
    SeekToFirst();
    while (Valid() && elem != rep->Value()) {
      ++rep;
    }
  }

  void Seek(IndexType pos) final { rep = list->Seek(pos); }

  void SeekToFirst() final { rep = list->Front(); }

  void SeekToLast() final { rep = list->Back(); }

  bool Valid() const final {
    // list->Head() == list->Tail()
    return (rep != list->Tail());
  }

  void Next() final { ++rep; }

  void Prev() final { --rep; }

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
  ListIteratorImpl(List* l)
      : list{l}, rep{l->Front()}, guard{list->AcquireLock()} {
    kvdk_assert(list != nullptr, "");
  }

  List::Iterator& Rep() { return rep; }

  List* Owner() const { return list; }

 private:
  List* list;
  List::Iterator rep;
  std::unique_lock<std::recursive_mutex> guard;
};

}  // namespace KVDK_NAMESPACE
