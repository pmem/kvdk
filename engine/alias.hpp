/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cinttypes>

#include "kvdk/types.hpp"

namespace KVDK_NAMESPACE {
// Internal types
using PMemOffsetType = std::uint64_t;
using TimeStampType = std::uint64_t;
using KeyHashType = std::uint64_t;
using ConfigFieldSizeType = std::uint32_t;
}  // namespace KVDK_NAMESPACE
