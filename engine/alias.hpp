/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cinttypes>

#include "kvdk/namespace.hpp"
#include "libpmem.h"

namespace KVDK_NAMESPACE {
using StringView = pmem::obj::string_view;
using PMemOffsetType = std::uint64_t;
using TimestampType = std::uint64_t;
using CollectionIDType = std::uint64_t;
using KeyHashType = std::uint64_t;
} // namespace KVDK_NAMESPACE
