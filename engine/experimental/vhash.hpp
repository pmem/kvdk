#pragma once

#include <functional>
#include <memory>
#include <string>

#include "../version/old_records_cleaner.hpp"
#include "hashptr_map.hpp"
#include "kvdk/iterator.hpp"
#include "vhash_kv.hpp"

namespace KVDK_NAMESPACE {
/// TODO: Support dynamically choose a allocator when creating VHash
/// Currently VHash allocate KVs by VHashKVBuilder,
/// which can bind to other allocators,
/// but VHashBuilder does not support custom allocator for
/// hashptr_map and name
class VHash {
 private:
  hashptr_map<StringView, VHashKV*, decltype(VHashKV::ExtractKey)> hpmap;
  VHashKVBuilder& kvb;
  std::atomic_int64_t sz{0LL};
  std::string name;
  // Number of iterators alive
  // This is a temporary workaround for iterator outliving VHash.
  /// TODO: Access token in KVDK for iterators
  std::atomic_uint64_t ref_cnt{0ULL};

 public:
  VHash(StringView n, VHashKVBuilder& b)
      : hpmap{4, VHashKV::ExtractKey}, kvb{b}, name{n.data(), n.size()} {}

  StringView Name() const { return name; }

  static StringView ExtractName(VHash* vhash) { return vhash->Name(); }

  size_t Size() const { return sz.load(); }

  bool Get(StringView key, StringView& value);

  void Put(StringView key, StringView value);

  void Delete(StringView key);

  // Cleanup is for cleaning up memory allocated by ModifyFunc.
  using ModifyFunc =
      std::function<ModifyOperation(StringView const*, StringView&, void*)>;
  using Cleanup = std::function<void(StringView)>;
  bool Modify(StringView key, ModifyFunc modify, void* cb_args,
              Cleanup cleanup);

  class Iterator : public VHashIterator {
   public:
    void SeekToFirst() final;
    void Next() final;
    bool Valid() const final;
    std::string Key() const final;
    std::string Value() const final;
    virtual ~Iterator() = default;

   private:
    friend VHash;
    using rep = typename decltype(hpmap)::iterator;
    VHash& owner;
    rep pos;
    Iterator(VHash& o, rep&& p) : owner{o}, pos{std::move(p)} {}
  };

  std::unique_ptr<Iterator> MakeIterator();

 private:
  friend class VHashBuilder;
  // Called by VHashBuilder::Delete() to Delete all VHashKVs inside it.
  void deleteAll();
};

class VHashBuilder : public IDeleter {
 private:
  OldRecordsCleaner& cleaner;

 public:
  VHashBuilder(OldRecordsCleaner& c);

  // Called by VHashGroup to create a VHash.
  VHash* NewVHash(StringView name, VHashKVBuilder& b);

  void Recycle(VHash* vhash);

  void Delete(void* vhash) final;
};

}  // namespace KVDK_NAMESPACE
