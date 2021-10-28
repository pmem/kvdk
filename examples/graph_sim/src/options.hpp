//
// Created by zhanghuigui on 2021/10/19.
//

#include <cstdint>

struct GraphOptions {
  // Max num of the edgelists element in a key/value.
  // Exists in the real workload, will split into two key.
  uint64_t max_edge_nums_per_value{2000};
};
