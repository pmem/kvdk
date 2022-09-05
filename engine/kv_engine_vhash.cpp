#include "kv_engine.hpp"

namespace KVDK_NAMESPACE {

Status KVEngine::VHashCreate(StringView key) {
return Status::NotSupported;
}

Status KVEngine::VHashDestroy(StringView key) {
return Status::NotSupported;
}

Status KVEngine::VHashSize(StringView key, size_t* len) {
return Status::NotSupported;
}

Status KVEngine::VHashGet(StringView key, StringView field, std::string* value) {
return Status::NotSupported;
}

Status KVEngine::VHashPut(StringView key, StringView field, StringView value) {
return Status::NotSupported;
}

Status KVEngine::VHashDelete(StringView key, StringView field) {
return Status::NotSupported;
}

Status KVEngine::VHashModify(StringView key, StringView field, ModifyFunc modify_func,
                void* cb_args) {
return Status::NotSupported;
}

std::unique_ptr<VHashIterator>KVEngine::VHashIteratorCreate(StringView key, Status* s) {
return nullptr;
}

} // namespace KVDK_NAMESPACE
