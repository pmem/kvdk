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

using Status = KVDKStatus;
using ValueType = KVDKValueType;

enum class ListPos : int {
  Front = KVDK_LIST_FRONT,
  Back = KVDK_LIST_BACK,
};

enum class ModifyOperation : int {
  Write = KVDK_MODIFY_WRITE,
  Delete = KVDK_MODIFY_DELETE,
  Abort = KVDK_MODIFY_ABORT,
  Noop = KVDK_MODIFY_NOOP
};

// Customized modify function used in Engine::Modify, indicate how to modify
// existing value
//
// Below is args of the function, "input" is passed by KVDK engine, and the
// function is responsible to fill outputs in "output" args.
//
// *(input) key: associated key of modify operation
// *(input) old_value: existing value of key, or nullptr if key not exist
// *(output) new_value: store new value after modifying "existing_value". Caller
// is responsible to fill the modify result here if the function returns
// ModifyOperation::Write.
// * args: customer args
//
// return ModifyOperation::Write indicates to update existing value to
// "new_value"
// return ModifyOperation::Delete indicates to delete the kv from engine
// return ModifyOperation::Abort indicates the existing kv should not be
// modified and abort the operation
using ModifyFunc = std::function<ModifyOperation(
    const std::string* old_value, std::string* new_value, void* args)>;

constexpr ExpireTimeType kPersistTime = INT64_MAX;
constexpr TTLType kPersistTTL = INT64_MAX;
constexpr TTLType kInvalidTTL = 0;
constexpr uint32_t kMinMemoryBlockSize = 32;
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
