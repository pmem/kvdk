/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#include <array>

#include "logger.hpp"

namespace KVDK_NAMESPACE {

void Logger::Log(const char *format, ...) {
#ifdef DO_LOG
  if (log_file_ != nullptr) {
    std::lock_guard<std::mutex> lg(mut_);
    auto now = std::chrono::system_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - start_ts_);
    fprintf(log_file_, "[LOG] time %ld ms: ", duration.count());
    va_list args;
    va_start(args, format);
    vfprintf(log_file_, format, args);
    va_end(args);
    fflush(log_file_);
    fsync(fileno(log_file_));
  }
#endif
}

void Logger::Error(const char *format, ...) {
  if (log_file_ != nullptr) {
    std::lock_guard<std::mutex> lg(mut_);
    auto now = std::chrono::system_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - start_ts_);
    fprintf(log_file_, "[ERROR] time %ld ms: ", duration.count());
    va_list args;
    va_start(args, format);
    vfprintf(log_file_, format, args);
    va_end(args);
    fflush(log_file_);
    fsync(fileno(log_file_));
  }
}

void Logger::Init(FILE *fp) {
  log_file_ = fp;
  start_ts_ = std::chrono::system_clock::now();
}

Logger GlobalLogger;

} // namespace KVDK_NAMESPACE