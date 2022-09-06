#include "vhash_group.hpp"
#include "../macros.hpp"

namespace KVDK_NAMESPACE
{

Status VHashGroup::Create(StringView name) 
KVDK_TRY
{
    auto acc = hpmap.lookup(name, hpmap.acquire_lock);
    if (acc.pointer() != nullptr) return Status::Existed;
    VHash* vhash = vhb.NewVHash(name, kvb);
    acc.set_pointer(vhash);
    return Status::Ok;
}
KVDK_HANDLE_EXCEPTIONS

Status VHashGroup::Destroy(StringView name)
KVDK_TRY
{
    auto acc = hpmap.lookup(name, hpmap.acquire_lock);
    VHash* vhash = acc.pointer();
    if (vhash == nullptr) return Status::NotFound;
    acc.erase();
    vhb.Recycle(vhash);
    return Status::Ok;
}
KVDK_HANDLE_EXCEPTIONS

Status VHashGroup::Get(StringView name, VHash** vhash)
KVDK_TRY
{
    *vhash = hpmap.lookup(name, hpmap.lockless);
    if (*vhash == nullptr) return Status::NotFound;
    else return Status::Ok;
}
KVDK_HANDLE_EXCEPTIONS

} // namespace KVDK_NAMESPACE
