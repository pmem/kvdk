#pragma once
#ifndef KVDKDEF_HPP
#define KVDKDEF_HPP

#include <cinttypes>
#include <string>

#include "libpmemobj++/string_view.hpp"

namespace KVDK_NAMESPACE {
using StringView = pmem::obj::string_view;
using CollectionIDType = std::uint64_t;
using IndexType = std::int64_t;

using UnixTimeType = std::int64_t;
using ExpireTimeType = UnixTimeType;
using TTLType = std::int64_t;

/// TODO: helper functions to convert between ExpireTime and TTL
constexpr ExpireTimeType kPersistTime = -1;
constexpr ExpireTimeType kInvalidExpireTime = -2;
constexpr TTLType kPersistTTL = -1;
constexpr TTLType kInvalidTTL = -2;
}  // namespace KVDK_NAMESPACE

#endif  // KVDKDEF_HPP

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
