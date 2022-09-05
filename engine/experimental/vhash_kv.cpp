#include "vhash_kv.hpp"
#include "../version/old_records_cleaner.hpp"

namespace KVDK_NAMESPACE
{

VHashKV* VHashKVBuilder::NewKV(StringView key, StringView value)
{
    void* dst = alloc.Allocate(sizeof(VHashKV) + key.size() + value.size());
    if (dst != nullptr) new(dst) VHashKV{key, value};
    return static_cast<VHashKV*>(dst);
}

void VHashKVBuilder::DeleteKV(VHashKV* kv)
{
    if (kv == nullptr) return;
    cleaner.DelayDelete(kv);
}

void VHashKVBuilder::PurgeKV(VHashKV* kv)
{
    kv->~VHashKV();
    alloc.Deallocate(kv, sizeof(VHashKV) + kv->Key().size() + kv->Value().size());
}

} // namespace KVDK_NAMESPACE
