#pragma once

#include "generic_list.hpp"
#include "kvdk/iterator.hpp"
#include "version/version_controller.hpp"

namespace KVDK_NAMESPACE {
using HashList = GenericList<RecordType::HashRecord, RecordType::HashElem>;
using HashListBuilder =
    GenericListBuilder<RecordType::HashRecord, RecordType::HashElem>;

class HashIteratorImpl final : public HashIterator {
 public:
  void SeekToFirst() final { rep = list->Front(); }

  void SeekToLast() final { rep = list->Back(); }

  bool Valid() const final {
    // list->Head() == list->Tail()
    return (rep != list->Tail());
  }

  void Next() final { ++rep; }

  void Prev() final { --rep; }

  std::string Key() const final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid ListIterator!");
      return std::string{};
    }
    auto sw = Collection::ExtractUserKey(rep->Key());
    return std::string{sw.data(), sw.size()};
  }

  std::string Value() const final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid ListIterator!");
      return std::string{};
    }
    auto sw = rep->Value();
    return std::string{sw.data(), sw.size()};
  }

  ~HashIteratorImpl() final = default;

 public:
  HashIteratorImpl(HashList* l, VersionController::GlobalToken&& token)
      : list{l}, rep{l->Front()}, token{std::move(token)} {
    kvdk_assert(list != nullptr, "");
  }

 private:
  HashList* list;
  HashList::Iterator rep;
  /// TODO: HashIterator should hold a global token and should not use token in
  /// thread cache.
  VersionController::GlobalToken token;
};

}  // namespace KVDK_NAMESPACE
