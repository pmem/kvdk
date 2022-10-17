/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include "alias.hpp"
#include "kvdk/configs.hpp"

namespace KVDK_NAMESPACE {

// Immutable configs that should not be changed after engine created
struct ImmutableConfigs {
  // To indicate if this persisted configs valid
  uint64_t validation_flag;

  // The minimum allocation unit of PMem space
  uint32_t pmem_block_size;

  // The number of blocks in a PMem segment
  uint64_t pmem_segment_blocks;

  void AssignImmutableConfigs(Configs& configs) {
    configs.pmem_block_size = pmem_block_size;
    configs.pmem_segment_blocks = pmem_segment_blocks;
  }

  // Get persistent configs from "configs" and persist them to PMem
  // Notice: "this" should allocated on PMem
  void PersistImmutableConfigs(const Configs& configs) {
    pmem_block_size = configs.pmem_block_size;
    pmem_segment_blocks = configs.pmem_segment_blocks;
    pmem_persist(&pmem_block_size, sizeof(ImmutableConfigs) - 8);
    validation_flag = 1;
    pmem_persist(&validation_flag, 8);
  }

  bool Valid() { return validation_flag; }
};

}  // namespace KVDK_NAMESPACE