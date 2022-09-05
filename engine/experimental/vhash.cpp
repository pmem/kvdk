#include "vhash.hpp"
#include "../alias.hpp"
#include "../macros.hpp"
#include "../version/old_records_cleaner.hpp"

namespace KVDK_NAMESPACE
{

template<typename CopyTo>
Status VHash::Get(StringView key, CopyTo copy, void* dst)
KVDK_TRY
{
    VHashKV* kvp = hpmap.lookup(key, hpmap.lockless);
    if (kvp == nullptr) return Status::NotFound;
    copy(dst, kvp->Value());
    return Status::Ok;
}
KVDK_HANDLE_EXCEPTIONS

Status VHash::Put(StringView key, StringView value)
KVDK_TRY
{
    VHashKV* new_kv = kvb.NewKV(key, value);
    VHashKV* old_kv = nullptr;
    {
        auto acc = hpmap.lookup(key, hpmap.acquire_lock);
        old_kv = acc.pointer();
        acc.set_pointer(new_kv);
    }
    if (old_kv == nullptr) sz.fetch_add(1LL);
    kvb.DeleteKV(old_kv);
    return Status::Ok;
}
KVDK_HANDLE_EXCEPTIONS

Status VHash::Delete(StringView key)
KVDK_TRY
{
    VHashKV* old_kv = nullptr;
    {
        auto acc = hpmap.lookup(key, hpmap.acquire_lock);
        old_kv = acc.pointer();
        acc.erase();
    }
    if (old_kv != nullptr) sz.fetch_sub(1LL);
    kvb.DeleteKV(old_kv);
    return Status::Ok;
}
KVDK_HANDLE_EXCEPTIONS

Status VHash::DeleteAll()
KVDK_TRY
{
    for(auto iter = hpmap.begin(); iter != hpmap.end(); ++iter)
    {
        VHashKV* old_kv = iter->pointer();
        iter->erase();
        kvb.DeleteKV(old_kv);
        sz.fetch_sub(1LL);
    }
    kvdk_assert(sz.load() == 0LL, "");
    return Status::Ok;
}
KVDK_HANDLE_EXCEPTIONS

template<typename ModifyFunc, typename Cleanup>
Status VHash::Modify(StringView key, ModifyFunc modify, void* cb_args, Cleanup cleanup)
KVDK_TRY
{
    VHashKV* old_kv = nullptr;
    ModifyOperation op;
    {
        auto acc = hpmap.lookup(key, hpmap.acquire_lock);
        old_kv = acc.pointer();
        StringView new_value;
        op = modify(old_kv ? old_kv->Value() : nullptr, &new_value, cb_args);
        switch (op)
        {
            case ModifyOperation::Write:
            {
                VHashKV* new_kv = kvb.NewKV(key, new_value);
                acc.set_pointer(new_kv);
                if (old_kv == nullptr) sz.fetch_add(1LL);
                kvb.DeleteKV(old_kv);
                break;
            }
            case ModifyOperation::Delete:
            {
                kvdk_assert(old_kv != nullptr, "Invalid callback!");
                acc.erase();
                sz.fetch_sub(1LL);
                kvb.DeleteKV(old_kv);
                break;
            }
            case ModifyOperation::Noop:
            case ModifyOperation::Abort:
            {
                break;
            }
        }
        cleanup(new_value);
    }
    return (op != ModifyOperation::Abort) ? Status::Ok : Status::Abort;
}
KVDK_HANDLE_EXCEPTIONS

void VHashKVBuilder::PurgeKV(VHashKV* kv)
{
    kv->~VHashKV();
    alloc.Deallocate(kv, sizeof(VHashKV) + kv->Key().size() + kv->Value().size());
}

VHash* VHashBuilder::NewVHash(StringView name)
{
    return new VHash{name, kvb};
}

void VHashBuilder::DeleteVHash(VHash* vhash)
{
    if (vhash == nullptr) return;
    cleaner.DelayDelete(vhash);
}

void VHashBuilder::PurgeVHash(VHash* vhash)
{
    auto iter = vhash->
}


} // namespace KVDK_NAMESPACE
