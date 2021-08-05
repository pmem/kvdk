/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <unistd.h>

#include "utils.hpp"
#include <chrono>
#include <cstdarg>
#include <mutex>

class Logger {
public:
  void Log(const char *format, ...);
  void Error(const char *format, ...);
  void Init(FILE *fp);
  void Exec(std::string cmd);

private:
  FILE *log_file_ = NULL;
  std::mutex mut_;

  std::chrono::time_point<std::chrono::system_clock> start_ts_;
};

#ifdef DO_LOG
extern Logger GlobalLogger;
#endif