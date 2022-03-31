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

/// TODO: move these constants to where they are relevant
constexpr uint64_t kMaxWriteBatchSize = (1 << 20);
// fsdax mode align to 2MB by default.
constexpr uint64_t kPMEMMapSizeUnit = (1 << 21);
// Select a record every 10000 into restored skiplist map for multi-thread
// restoring large skiplist.
constexpr uint64_t kRestoreSkiplistStride = 10000;
constexpr uint64_t kMaxCachedOldRecords = 10000;
constexpr size_t kLimitForegroundCleanOldRecords = 1;
}  // namespace KVDK_NAMESPACE
