#pragma once

#include "dl_list.hpp"
#include "generic_list.hpp"
#include "hash_table.hpp"
#include "kvdk/iterator.hpp"
#include "version/version_controller.hpp"

namespace KVDK_NAMESPACE {

class HashList2 : public Collection {
 public:
  struct WriteResult {
    Status s = Status::Ok;
    DLRecord* existing_record = nullptr;
    DLRecord* write_record = nullptr;
    HashEntry* hash_entry_ptr = nullptr;
  };

  WriteResult Put(const StringView& key, const StringView& value,
                  TimeStampType timestamp);

  Status Get(const StringView& key, std::string* value);

  WriteResult Delete(const StringView& key, TimeStampType timestamp);

  Status PrepareWrite(HashWriteArgs& args);

  WriteResult Write(HashWriteArgs& args);

 private:
  DLList dl_list_;
};

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

  void Next() final {
    if (!Valid()) return;
    ++rep;
  }

  void Prev() final {
    if (!Valid()) return;
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
