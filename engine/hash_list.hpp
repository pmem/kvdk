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

  bool MatchKey(std::regex const& re) final {
    return std::regex_match(Key(), re);
  }

  ~HashIteratorImpl() final = default;

 private:
  using AccessToken = std::shared_ptr<VersionController::GlobalSnapshotHolder>;

 public:
  HashIteratorImpl(HashList* l, AccessToken t)
      : list{l}, rep{l->Front()}, token{t} {
    kvdk_assert(list != nullptr, "");
  }

 private:
  HashList* list;
  HashList::Iterator rep;
  AccessToken token;
};

}  // namespace KVDK_NAMESPACE
