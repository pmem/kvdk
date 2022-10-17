/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include "utils.hpp"

#include <hwloc.h>

#include "../logger.hpp"

namespace KVDK_NAMESPACE {

int get_usable_pu(void) {
  hwloc_topology_t topology;
  hwloc_bitmap_t set;
  int err;

  /* create a topology */
  err = hwloc_topology_init(&topology);
  if (err < 0) {
    GlobalLogger.Error("Failed to initialize the topology\n");
    return err;
  }
  err = hwloc_topology_load(topology);
  if (err < 0) {
    GlobalLogger.Error("Failed to load the topology\n");
    hwloc_topology_destroy(topology);
    return err;
  }

  /* retrieve the entire set of available PUs */
  [[gnu::unused]] hwloc_const_bitmap_t cset_available =
      hwloc_topology_get_topology_cpuset(topology);

  /* retrieve the CPU binding of the current entire process */
  set = hwloc_bitmap_alloc();
  if (!set) {
    GlobalLogger.Error("Failed to allocate a bitmap\n");
    hwloc_topology_destroy(topology);
    return err;
  }
  err = hwloc_get_cpubind(topology, set, HWLOC_CPUBIND_PROCESS);
  if (err < 0) {
    GlobalLogger.Error("Failed to get cpu binding\n");
    hwloc_bitmap_free(set);
    hwloc_topology_destroy(topology);
    return err;
  }
  int pu = hwloc_bitmap_weight(set);
  hwloc_topology_destroy(topology);
  return pu;
}
}  // namespace KVDK_NAMESPACE
