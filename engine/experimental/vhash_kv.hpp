#pragma once

#include <cstring>

#include "../alias.hpp"
#include "../macros.hpp"
#include "../allocator.hpp"
#include "../version/old_records_cleaner.hpp"

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

class VHashKVBuilder : public IDeleter
{
private:
    IVolatileAllocator& alloc;
    OldRecordsCleaner& cleaner;

public:
    VHashKVBuilder(IVolatileAllocator& a, OldRecordsCleaner& c);
    VHashKVBuilder(VHashKVBuilder const&) = delete;
    VHashKVBuilder(VHashKVBuilder&&) = default;

    VHashKV* NewKV(StringView key, StringView value);

    // Recycle VHashKV to OldRecordsCleaner for later deletion.
    void Recycle(VHashKV* kv);

    // Called by OldRecordsCleaner to delete KV.
    void Delete(void* kv) final;
};



} // KVDK_NAMESPACE
