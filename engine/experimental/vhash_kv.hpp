#pragma once

#include <cstring>

#include "../alias.hpp"
#include "../macros.hpp"
#include "../allocator.hpp"

namespace KVDK_NAMESPACE
{

/// TODO: Add timestamp field for MVCC if necessary
class VHashKV
{
private:
    std::uint32_t key_sz;
    std::uint32_t value_sz;
    char data[];

public:
    VHashKV(StringView key, StringView value)
    {
        kvdk_assert(key.size() <= std::numeric_limits<std::uint32_t>::max(), "");
        kvdk_assert(value.size() <= std::numeric_limits<std::uint32_t>::max(), "");
        key_sz = static_cast<std::uint32_t>(key.size());
        value_sz = static_cast<std::uint32_t>(value.size());
        memcpy(data, key.data(), key.size());
        memcpy(data + key_sz, value.data(), value.size());
    }

    StringView Key() const
    {
        return StringView{data, key_sz};
    }

    StringView Value() const
    {
        return StringView{data+key_sz, value_sz};
    }

    static StringView ExtractKey(VHashKV* kvp)
    {
        return kvp->Key();
    }
};

class OldRecordsCleaner;

class VHashKVBuilder
{
private:
    IVolatileAllocator& alloc;
    OldRecordsCleaner& cleaner;

public:
    VHashKVBuilder(IVolatileAllocator& a, OldRecordsCleaner& c) : alloc{a}, cleaner{c} {}
    VHashKVBuilder(VHashKVBuilder const&) = delete;
    VHashKVBuilder(VHashKVBuilder&&) = default;

    // Called by VHash to create a KV.
    VHashKV* NewKV(StringView key, StringView value);
    // Called by VHash to "delete" a KV. Delegate the deletion to cleaner.
    void DeleteKV(VHashKV* kv);
    // Called by cleaner to actually delete the KV and recycle its memory.
    void PurgeKV(VHashKV* kv);
};



} // KVDK_NAMESPACE
