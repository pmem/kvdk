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

  bool Valid() const final {
    // list->Head() == list->Tail()
    return (s == Status::Ok && rep != list->Tail());
  }

  Status CurrentStatus() const final { return s; }

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

  std::string Key() const final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid HashIterator!");
      return std::string{};
    }
    auto sw = Collection::ExtractUserKey(rep->Key());
    return std::string{sw.data(), sw.size()};
  }

  std::string Value() const final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid HashIterator!");
      return std::string{};
    }
    auto sw = rep->Value();
    return std::string{sw.data(), sw.size()};
  }

  bool MatchKey(std::regex const& re) final {
    if (!Valid()) {
      kvdk_assert(false, "Accessing data with invalid HashIterator!");
      return false;
    }
    return std::regex_match(Key(), re);
  }

  ~HashIteratorImpl() final = default;

 private:
  using AccessToken = std::shared_ptr<VersionController::GlobalSnapshotHolder>;

 public:
  HashIteratorImpl(HashList* l, Status status, AccessToken t)
      : list{l}, rep{l}, s{status}, token{t} {
    if (list != nullptr) {
      rep = l->Front();
    }
  }

 private:
  HashList* list;
  HashList::Iterator rep;
  Status s;
  AccessToken token;
};

}  // namespace KVDK_NAMESPACE
