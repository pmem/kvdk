#include "hashptr_map.hpp"
#include "vhash.hpp"

#pragma once

#include <string>

#include "hashptr_map.hpp"
#include "vhash_kv.hpp"

namespace KVDK_NAMESPACE
{

class VHashGroup
{
private:
    hashptr_map<StringView, VHash*, decltype(VHash::Name)> hpmap;
    VHashKVBuilder& kvb;
    std::atomic_int64_t sz{0LL};

public:
    VHashGroup(VHashKVBuilder& b) : hpmap{4, VHash::Name}, kvb{b} {}

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
};

} // KVDK_NAMESPACE
