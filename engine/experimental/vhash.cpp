#include "vhash.hpp"

namespace KVDK_NAMESPACE {

bool VHash::Get(StringView key, StringView& value) {
  VHashKV* kvp = hpmap.lookup(key, hpmap.lockless);
  if (kvp == nullptr) return false;
  value = kvp->Value();
  return true;
}

void VHash::Put(StringView key, StringView value) {
  VHashKV* new_kv = kvb.NewKV(key, value);
  VHashKV* old_kv = nullptr;
  {
    auto acc = hpmap.lookup(key, hpmap.acquire_lock);
    old_kv = acc.pointer();
    acc.set_pointer(new_kv);
  }
  if (old_kv == nullptr) sz.fetch_add(1LL);
  kvb.Recycle(old_kv);
}

void VHash::Delete(StringView key) {
  VHashKV* old_kv = nullptr;
  {
    auto acc = hpmap.lookup(key, hpmap.acquire_lock);
    old_kv = acc.pointer();
    acc.erase();
  }
  if (old_kv != nullptr) sz.fetch_sub(1LL);
  kvb.Recycle(old_kv);
}

void VHash::deleteAll() {
  kvdk_assert(ref_cnt.load() == 0UL, "Iterator outlives VHash!");
  for (auto iter = hpmap.begin(); iter != hpmap.end(); ++iter) {
    VHashKV* old_kv = iter->pointer();
    iter->erase();
    // It's safe to call Delete() instead of Recycle() here,
    // As deleteAll() is called by cleaner.
    kvb.Delete(old_kv);
    sz.fetch_sub(1LL);
  }
  kvdk_assert(sz.load() == 0LL, "");
}

bool VHash::Modify(StringView key, VHash::ModifyFunc modify, void* cb_args,
                   VHash::Cleanup cleanup) {
  VHashKV* old_kv = nullptr;
  ModifyOperation op;
  {
    auto acc = hpmap.lookup(key, hpmap.acquire_lock);
    old_kv = acc.pointer();
    StringView new_value;
    StringView old_value;
    old_value = old_kv ? old_kv->Value() : old_value;
    op = modify(old_kv ? &old_value : nullptr, new_value, cb_args);
    switch (op) {
      case ModifyOperation::Write: {
        VHashKV* new_kv = kvb.NewKV(key, new_value);
        acc.set_pointer(new_kv);
        if (old_kv == nullptr) sz.fetch_add(1LL);
        kvb.Recycle(old_kv);
        break;
      }
      case ModifyOperation::Delete: {
        kvdk_assert(old_kv != nullptr, "Invalid callback!");
        acc.erase();
        sz.fetch_sub(1LL);
        kvb.Recycle(old_kv);
        break;
      }
      case ModifyOperation::Noop:
      case ModifyOperation::Abort: {
        break;
      }
    }
    cleanup(new_value);
  }
  return (op != ModifyOperation::Abort);
}

void VHash::Iterator::SeekToFirst() { pos = owner.hpmap.begin(); }

void VHash::Iterator::Next() {
  if (Valid()) ++pos;
}

bool VHash::Iterator::Valid() const { return (pos != owner.hpmap.end()); }

std::string VHash::Iterator::Key() const {
  VHashKV* kv = pos->pointer();
  StringView key = kv->Key();
  return std::string(key.data(), key.size());
}

std::string VHash::Iterator::Value() const {
  VHashKV* kv = pos->pointer();
  StringView value = kv->Value();
  return std::string(value.data(), value.size());
}

std::unique_ptr<VHash::Iterator> VHash::MakeIterator() {
  ref_cnt.fetch_add(1UL);
  // Initialized to end() iterator without acquiring lock.
  return std::unique_ptr<Iterator>{new Iterator{*this, hpmap.end()}};
}

VHashBuilder::VHashBuilder(OldRecordsCleaner& c) : cleaner{c} {
  cleaner.RegisterDelayDeleter(*this);
}

VHash* VHashBuilder::NewVHash(StringView name, VHashKVBuilder& kvb) {
  return new VHash{name, kvb};
}

void VHashBuilder::Recycle(VHash* vhash) {
  if (vhash == nullptr) return;
  cleaner.DelayDelete(*this, vhash);
}

void VHashBuilder::Delete(void* vhash) {
  VHash* vh = static_cast<VHash*>(vhash);
  vh->deleteAll();
  delete vh;
}

}  // namespace KVDK_NAMESPACE
