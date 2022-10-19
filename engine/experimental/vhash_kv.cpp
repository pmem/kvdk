#include "vhash_kv.hpp"

#include "../version/old_records_cleaner.hpp"

namespace KVDK_NAMESPACE {

VHashKVBuilder::VHashKVBuilder(IVolatileAllocator& a, OldRecordsCleaner& c)
    : alloc{a}, cleaner{c} {
  c.RegisterDelayDeleter(*this);
}

VHashKV* VHashKVBuilder::NewKV(StringView key, StringView value) {
  void* dst = alloc.Allocate(sizeof(VHashKV) + key.size() + value.size());
  new (dst) VHashKV{key, value};
  return static_cast<VHashKV*>(dst);
}

void VHashKVBuilder::Recycle(VHashKV* kv) {
  if (kv == nullptr) return;
  cleaner.DelayDelete(*this, kv);
}

void VHashKVBuilder::Delete(void* obj) {
  VHashKV* kv = static_cast<VHashKV*>(obj);
  kv->~VHashKV();
  alloc.Deallocate(kv, sizeof(VHashKV) + kv->Key().size() + kv->Value().size());
}

}  // namespace KVDK_NAMESPACE
