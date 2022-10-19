#pragma once

#include <string>

#include "../alias.hpp"
#include "hashptr_map.hpp"
#include "vhash.hpp"

namespace KVDK_NAMESPACE {

// A VHashGroup contains VHashes that share the same memory allocator for kvs.
/// TODO: Add hpmap_alloc to allocate memory for hashptr_maps.
class VHashGroup {
 private:
  IVolatileAllocator& kv_alloc;
  OldRecordsCleaner& cleaner;
  VHashKVBuilder kvb{kv_alloc, cleaner};
  VHashBuilder vhb{cleaner};

  hashptr_map<StringView, VHash*, decltype(VHash::ExtractName)> hpmap{
      4, VHash::ExtractName};
  std::atomic_int64_t sz{0LL};

 public:
  VHashGroup(IVolatileAllocator& a, OldRecordsCleaner& c)
      : kv_alloc{a}, cleaner{c} {}

  ~VHashGroup();

  bool Create(StringView name, size_t capacity);

  bool Destroy(StringView name);

  VHash* Get(StringView name);
};

}  // namespace KVDK_NAMESPACE
