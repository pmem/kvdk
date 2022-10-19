#include "kv_engine.hpp"
#include "macros.hpp"

namespace KVDK_NAMESPACE {

Status KVEngine::VHashCreate(StringView key, size_t capacity) KVDK_TRY {
  if (!checkKeySize(key)) return Status::InvalidDataSize;

  auto thread_access = AcquireAccessThread();
  return vhashes_.Create(key, capacity) ? Status::Ok : Status::Existed;
}
KVDK_HANDLE_EXCEPTIONS

Status KVEngine::VHashDestroy(StringView key) KVDK_TRY {
  if (!checkKeySize(key)) return Status::InvalidDataSize;

  auto thread_access = AcquireAccessThread();
  return vhashes_.Destroy(key) ? Status::Ok : Status::NotFound;
}
KVDK_HANDLE_EXCEPTIONS

Status KVEngine::VHashSize(StringView key, size_t* len) KVDK_TRY {
  if (!checkKeySize(key)) return Status::InvalidDataSize;

  auto thread_access = AcquireAccessThread();
  auto token = version_controller_.GetLocalSnapshotHolder();

  VHash* vhash = vhashes_.Get(key);
  if (vhash == nullptr) return Status::NotFound;
  *len = vhash->Size();
  return Status::Ok;
}
KVDK_HANDLE_EXCEPTIONS

Status KVEngine::VHashGet(StringView key, StringView field,
                          std::string* value) KVDK_TRY {
  if (!checkKeySize(key) || !checkKeySize(field))
    return Status::InvalidDataSize;

  auto thread_access = AcquireAccessThread();
  auto token = version_controller_.GetLocalSnapshotHolder();

  VHash* vhash = vhashes_.Get(key);
  if (vhash == nullptr) return Status::NotFound;

  StringView val;
  if (!vhash->Get(field, val)) return Status::NotFound;
  value->assign(val.data(), val.size());
  return Status::Ok;
}
KVDK_HANDLE_EXCEPTIONS

Status KVEngine::VHashPut(StringView key, StringView field,
                          StringView value) KVDK_TRY {
  if (!checkKeySize(key) || !checkKeySize(field) || !checkValueSize(value))
    return Status::InvalidDataSize;

  auto thread_access = AcquireAccessThread();
  auto token = version_controller_.GetLocalSnapshotHolder();

  VHash* vhash = vhashes_.Get(key);
  if (vhash == nullptr) return Status::NotFound;

  vhash->Put(field, value);
  return Status::Ok;
}
KVDK_HANDLE_EXCEPTIONS

Status KVEngine::VHashDelete(StringView key, StringView field) KVDK_TRY {
  if (!checkKeySize(key) || !checkKeySize(field))
    return Status::InvalidDataSize;

  auto thread_access = AcquireAccessThread();
  auto token = version_controller_.GetLocalSnapshotHolder();

  VHash* vhash = vhashes_.Get(key);
  if (vhash == nullptr) return Status::NotFound;
  vhash->Delete(field);
  return Status::Ok;
}
KVDK_HANDLE_EXCEPTIONS

Status KVEngine::VHashModify(StringView key, StringView field,
                             ModifyFunc modify_func, void* cb_args) KVDK_TRY {
  if (!checkKeySize(key) || !checkKeySize(field))
    return Status::InvalidDataSize;

  std::string old_value;
  std::string new_value;
  auto modify = [&](StringView const* old_val, StringView& new_val,
                    void* args) {
    if (old_val != nullptr) old_value.assign(old_val->data(), old_val->size());
    ModifyOperation op =
        modify_func(old_val ? &old_value : nullptr, &new_value, args);
    new_val = new_value;
    return op;
  };

  auto cleanup = [&](StringView) { return; };

  auto thread_access = AcquireAccessThread();
  auto token = version_controller_.GetLocalSnapshotHolder();
  VHash* vhash = vhashes_.Get(key);
  if (vhash == nullptr) return Status::NotFound;

  return (vhash->Modify(field, modify, cb_args, cleanup)) ? Status::Ok
                                                          : Status::Abort;
}
KVDK_HANDLE_EXCEPTIONS

std::unique_ptr<VHashIterator> KVEngine::VHashIteratorCreate(StringView key,
                                                             Status* s) try {
  Status sink;
  s = (s != nullptr) ? s : &sink;
  *s = Status::NotFound;

  if (!checkKeySize(key)) return nullptr;

  auto thread_access = AcquireAccessThread();
  /// TODO: iterator should hold an access token to keep VHash valid.
  auto token = version_controller_.GetLocalSnapshotHolder();

  VHash* vhash = vhashes_.Get(key);
  if (vhash == nullptr) return nullptr;

  *s = Status::Ok;
  return vhash->MakeIterator();
} catch (std::exception const& ex) {
  Status sink;
  s = (s != nullptr) ? s : &sink;
  *s = ExceptionToStatus(ex);
  return nullptr;
} catch (...) {
  Status sink;
  s = (s != nullptr) ? s : &sink;
  *s = Status::Abort;
  return nullptr;
}

}  // namespace KVDK_NAMESPACE
