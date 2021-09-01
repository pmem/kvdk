/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <atomic>
#include <iostream>
#include <vector>

/* Create a string that contains 8 bytes from uint64_t. */
static inline std::string uint64_to_string(uint64_t &key) {
  return std::string(reinterpret_cast<const char *>(&key), 8);
}

inline void random_str(char *str, unsigned int size) {
  for (unsigned int i = 0; i < size; i++) {
    switch (rand() % 3) {
    case 0:
      str[i] = rand() % 10 + '0';
      break;
    case 1:
      str[i] = rand() % 26 + 'A';
      break;
    case 2:
      str[i] = rand() % 26 + 'a';
      break;
    default:
      break;
    }
  }
  str[size] = 0;
}