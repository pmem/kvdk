/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "hash_table.hpp"
#include "structures.hpp"

namespace KVDK_NAMESPACE {

// Notice: this is on development
class HashList : public PersistentList {
public:
  uint64_t id() override { return id_; }

private:
  uint64_t id_;
};
} // namespace KVDK_NAMESPACE