/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <atomic>
#include <iostream>
#include <vector>
#include <random>
#include <string>
#include <cassert>
#include <iomanip>

/* Create a string that contains 8 bytes from uint64_t. */
static inline std::string uint64_to_string(uint64_t &key) {
  return std::string(reinterpret_cast<const char *>(&key), 8);
}

static inline void random_str(char *str, unsigned int size) {
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

// Return a string of length len with random characters in ['a', 'z']
static inline std::string GetRandomString(size_t len)
{
  static std::default_random_engine re;
  std::string str;
  str.reserve(len);
  for (size_t i = 0; i < len; i++)
    str.push_back('a'+ re() % 26);
  return str;
}

// Return a string of length in [min_len, max_len] with random characters in ['a', 'z']
static inline std::string GetRandomString(size_t min_len, size_t max_len)
{
  static std::default_random_engine re;
  size_t len = 0;
  while (len < min_len)
    len = re() % (max_len + 1);
  return GetRandomString(len);  
}


static inline void LaunchNThreads(int n_thread, std::function<void(int tid)> func,
                    uint32_t id_start = 0) {
  std::vector<std::thread> ts;
  for (int i = id_start; i < id_start + n_thread; i++) {
    ts.emplace_back(std::thread(func, i));
  }
  for (auto &t : ts)
    t.join();
}

static void ShowProgress(std::ostream& os, int progress, int total, size_t len_bar = 50, char symbol_done = '#', char symbol_fill = '-')
{
  assert(0 <= progress && progress <= total);
  int step = total / len_bar;
  if (step == 0)
  {
    len_bar = total;
    step = 1;
  }

  os << "\r";
  os << std::setw(12) << std::right << progress;
  os <<  "/";
  os << std::setw(12) << std::left << total << "\t";
  os << "[";
  for (size_t i = 0; i < progress / step; i++)
    os << symbol_done;
  for (size_t i = 0; i < (total - progress) / step; i++)
    os << symbol_fill;
  os << "]";
  os << std::flush;

  if (progress == total)
    os << std::endl;
}