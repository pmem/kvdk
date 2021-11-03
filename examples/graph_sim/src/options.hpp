/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <cstdint>

struct GraphOptions {
  // Max num of the edgelists element in a key/value.
  // Exists in the real workload, will split into two key.
  uint64_t max_edge_nums_per_value{2000};

  // Maybe add more options for the graph simulator.
};
