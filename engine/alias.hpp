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
using CompFunc =
    std::function<int(const StringView& src, const StringView& target)>;
}  // namespace KVDK_NAMESPACE
