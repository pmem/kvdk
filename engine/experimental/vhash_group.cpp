#include "vhash_group.hpp"

namespace KVDK_NAMESPACE {

VHashGroup::~VHashGroup() {
  for (auto iter = hpmap.begin(); iter != hpmap.end(); ++iter)
    delete iter->pointer();
}

bool VHashGroup::Create(StringView name, size_t capacity) {
  auto acc = hpmap.lookup(name, hpmap.acquire_lock);
  if (acc.pointer() != nullptr) return false;
  VHash* vhash = vhb.NewVHash(name, kvb, capacity);
  acc.set_pointer(vhash);
  return true;
}

bool VHashGroup::Destroy(StringView name) {
  auto acc = hpmap.lookup(name, hpmap.acquire_lock);
  VHash* vhash = acc.pointer();
  if (vhash == nullptr) return false;
  acc.erase();
  vhb.Recycle(vhash);
  return true;
}

VHash* VHashGroup::Get(StringView name) {
  return hpmap.lookup(name, hpmap.lockless);
}

}  // namespace KVDK_NAMESPACE
