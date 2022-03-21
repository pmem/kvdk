/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cinttypes>
#include <functional>

#include "kvdk/namespace.hpp"
#include "libpmem.h"
#include "libpmemobj++/string_view.hpp"

namespace KVDK_NAMESPACE {
using StringView = pmem::obj::string_view;
using PMemOffsetType = std::uint64_t;
using TimeStampType = std::uint64_t;
using CollectionIDType = std::uint64_t;
using KeyHashType = std::uint64_t;
using ConfigFieldSizeType = std::uint32_t;
using ExpiredTimeType = std::int64_t;
using UnixTimeType = std::int64_t;
}  // namespace KVDK_NAMESPACE
