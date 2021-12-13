/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "alias.hpp"
#include "kvdk/configs.hpp"

namespace KVDK_NAMESPACE {
constexpr TimeStampType kMaxTimestamp = UINT64_MAX;

struct SnapshotImpl : public Snapshot {
  explicit SnapshotImpl(const TimeStampType &t) : timestamp(t) {}

  TimeStampType GetTimestamp() override { return timestamp; }

  TimeStampType timestamp;
};

struct SnapshotSetter {
public:
  explicit SnapshotSetter(SnapshotImpl &snapshot, TimeStampType new_ts)
      : snapshot_(snapshot) {
    snapshot.timestamp = new_ts;
  }

  ~SnapshotSetter() { snapshot_.timestamp = kMaxTimestamp; }

private:
  SnapshotImpl &snapshot_;
};

struct Version {
  SnapshotImpl snapshot;

  Version *prev_version;
  Version *next_version;
};

struct VersionChain {};

} // namespace KVDK_NAMESPACE