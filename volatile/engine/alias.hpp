/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cinttypes>

#include "kvdk/types.hpp"

namespace KVDK_NAMESPACE {
enum class WriteOp { Put, Delete };

// Internal types
using MemoryOffsetType = std::uint64_t;
using TimestampType = std::uint64_t;

const uint64_t kMaxCachedOldRecords = 1024;
}  // namespace KVDK_NAMESPACE
