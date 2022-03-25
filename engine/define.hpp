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
using TTLTimeType = std::int64_t;
using UnixTimeType = std::int64_t;

constexpr uint64_t kMaxWriteBatchSize = (1 << 20);
// fsdax mode align to 2MB by default.
constexpr uint64_t kPMEMMapSizeUnit = (1 << 21);
// Select a record every 10000 into restored skiplist map for multi-thread
// restoring large skiplist.
constexpr uint64_t kRestoreSkiplistStride = 10000;
constexpr uint64_t kMaxCachedOldRecords = 10000;
constexpr size_t kLimitForegroundCleanOldRecords = 1;
constexpr uint64_t kBackgroundAccessThread = 2;
constexpr int64_t kInvalidTTLTime = -2;
constexpr int64_t kPersistTime = -1;
}  // namespace KVDK_NAMESPACE
