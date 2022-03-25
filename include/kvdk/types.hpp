#pragma once
#ifndef KVDKDEF_HPP
#define KVDKDEF_HPP

#include <cinttypes>
#include <string>

#include "libpmemobj++/string_view.hpp"

#ifndef KVDK_NAMESPACE
#define KVDK_NAMESPACE kvdk
#endif

namespace KVDK_NAMESPACE {
using StringView = pmem::obj::string_view;
using CollectionIDType = std::uint64_t;

using UnixTimeType = std::int64_t;
using ExpiredTimeType = UnixTimeType;
using TTLTimeType = std::int64_t;

/// TODO: helper functions to convert between ExpireTime and TTL
constexpr ExpiredTimeType kPersistTime = -1;
constexpr ExpiredTimeType kInvalidExpireTime = -2;
constexpr TTLTimeType kPersistTTL = -1;
constexpr TTLTimeType kInvalidTTL = -2;
}  // namespace KVDK_NAMESPACE

#endif  // KVDKDEF_HPP