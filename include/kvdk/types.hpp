#pragma once

#include <cinttypes>
#include <functional>
#include <string>

#include "libpmemobj++/string_view.hpp"
#include "types.h"

namespace KVDK_NAMESPACE {
using StringView = pmem::obj::string_view;
using CollectionIDType = std::uint64_t;
using IndexType = std::int64_t;

using UnixTimeType = std::int64_t;
using ExpireTimeType = UnixTimeType;
using TTLType = std::int64_t;

// Customized modify function used in Engine::Modify
//
// key: associated key of modify operation
// existing_value: existing value of key, or nullptr if key not exist
// new_value: result after modifying "existing_value". Caller is responsible to
// fill the result in "new_value" if return ModifyUpdate.
// args: customer args for modify function
//
// return ModifyUpdate indicates to update existing value to "new_value"
// return ModifyDelete indicates to delete the kv from engine
// return ModifyAbort indicates the existing kv should not be modified
enum class ModifyOperation : int {
  ModifyUpdate = KVDK_MODIFY_UPDATE,
  ModifyDelete = KVDK_MODIFY_DELETE,
  ModifyAbort = KVDK_MODIFY_ABORT,
};
using ModifyFunction = std::function<ModifyOperation(
    const StringView& key, const std::string* old_value, std::string* new_value,
    void* args)>;

constexpr ExpireTimeType kPersistTime = INT64_MAX;
constexpr TTLType kPersistTTL = INT64_MAX;
constexpr TTLType kInvalidTTL = 0;
constexpr uint32_t kMinPMemBlockSize = 32;
}  // namespace KVDK_NAMESPACE

#if !__cpp_lib_string_view
#include <bits/functional_hash.h>

#include <ios>

template <class CharT, class Traits>
std::basic_ostream<CharT, Traits>& operator<<(
    std::basic_ostream<CharT, Traits>& out,
    pmem::obj::basic_string_view<CharT, Traits> sv) {
  return std::__ostream_insert(out, sv.data(), sv.size());
}

namespace std {
template <>
struct hash<pmem::obj::string_view> {
  size_t operator()(pmem::obj::string_view const& sv) const noexcept {
    return _Hash_impl::hash(sv.data(), sv.size());
  }
};
}  // namespace std

#endif  // !__cpp_lib_string_view
