#pragma once

#include <memory>
#include <string>

#include "hashptr_map.hpp"
#include "vhash_kv.hpp"
#include "kvdk/iterator.hpp"

namespace KVDK_NAMESPACE
{
/// TODO: Support dynamically choose a allocator when creating VHash
/// Currently VHash allocate KVs by VHashKVBuilder, 
/// which can bind to other allocators,
/// but VHashBuilder does not support custom allocator for 
/// hashptr_map and name
class VHash
{
private:
    hashptr_map<StringView, VHashKV*, decltype(VHashKV::ExtractKey)> hpmap;
    VHashKVBuilder& kvb;
    std::atomic_int64_t sz{0LL};
    std::string name;

public:
    VHash(StringView n, VHashKVBuilder& b) : hpmap{4, VHashKV::ExtractKey}, kvb{b}, name{n.data(), n.size()} {}

    StringView Name() const { return name; }

    // CopyTo should have signature of
    // void(*)(void* dst, StringView src)
    template<typename CopyTo>
    Status Get(StringView key, CopyTo copy, void* dst);

    Status Put(StringView key, StringView value);

    Status Delete(StringView key);

    // ModifyFunc should have signature of 
    // ModifyOperation(*)(StringView const* old_val, StringView& new_val, void* args)
    // Cleanup should have signature of
    // void(*)(StringView new_val)
    // for cleaning up memory allocated by ModifyFunc.
    template<typename ModifyFunc, typename Cleanup>
    Status Modify(StringView key, ModifyFunc modify, void* cb_args, Cleanup cleanup);

    // Called by VHashBuilder::PurgeVHash() to Delete all VHashKVs inside it.
    Status DeleteAll();
};


class VHashBuilder
{
private:
    OldRecordsCleaner& cleaner;
    VHashKVBuilder& kvb;

public:
    VHashBuilder(OldRecordsCleaner& c, VHashKVBuilder& b) : cleaner{c}, kvb{b} {}

    // Called by VHashGroup to create a VHash.
    VHash* NewVHash(StringView name);
    // Called by VHashGroup to "delete" a VHash. Delegate the deletion to cleaner.
    void DeleteVHash(VHash* vhash);
    // Called by cleaner to actually delete the VHash and recycle its memory.
    void PurgeVHash(VHash* vhash);
};

} // KVDK_NAMESPACE
