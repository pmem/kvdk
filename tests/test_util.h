/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <atomic>
#include <cassert>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

/* Create a string that contains 8 bytes from uint64_t. */
inline std::string uint64_to_string(uint64_t& key) {
  return std::string(reinterpret_cast<const char*>(&key), 8);
}

inline void random_str(char* str, unsigned int size) {
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
inline std::string GetRandomString(size_t len) {
  static std::default_random_engine re;
  std::string str;
  str.reserve(len);
  for (size_t i = 0; i < len; i++) str.push_back('a' + re() % 26);
  return str;
}

// Return a string of length in [min_len, max_len] with random characters in
// ['a', 'z']
inline std::string GetRandomString(size_t min_len, size_t max_len) {
  static std::default_random_engine re;
  size_t len = min_len + re() % (max_len - min_len + 1);
  return GetRandomString(len);
}

inline void LaunchNThreads(int n_thread, std::function<void(int tid)> func,
                           int id_start = 0) {
  std::vector<std::thread> ts;
  for (int i = id_start; i < id_start + n_thread; i++) {
    ts.emplace_back(std::thread(func, i));
  }
  for (auto& t : ts) t.join();
}

class ProgressBar {
 private:
  std::ostream& out_stream_;
  std::string tag_;
  size_t total_progress_;
  size_t current_progress_;
  size_t last_report_;
  size_t report_interval_;
  size_t bar_length_;
  size_t step_;
  bool enabled_;

  bool flush_newline_{false};

  static constexpr char symbol_done_{'#'};
  static constexpr char symbol_fill_{'-'};

 public:
  explicit ProgressBar(std::ostream& out, std::string tag,
                       size_t total_progress, size_t report_interval,
                       bool enabled = true, size_t bar_length = 50)
      : out_stream_{out},
        tag_{tag},
        total_progress_{total_progress},
        current_progress_{0},
        last_report_{0},
        report_interval_{report_interval},
        bar_length_{bar_length},
        step_{total_progress / bar_length},
        enabled_{enabled} {
    assert(total_progress_ > 0);
    assert(bar_length_ > 0);
    if (step_ == 0) {
      step_ = 1;
      bar_length_ = total_progress;
    }
    // Actual bar length may be 1 char longer than given bar length
    // 51 = 2048 / (2048 / 50). This prevents overflowing the bar
    bar_length_ = total_progress_ / step_;

    showProgress();
  }

  void Update(size_t current_progress) {
    assert(!flush_newline_ && "Trying to update a completed progress!");
    assert(current_progress_ < current_progress &&
           current_progress <= total_progress_);

    current_progress_ = current_progress;
    if (current_progress_ == total_progress_) {
      flush_newline_ = true;
    }

    if (last_report_ + report_interval_ <= current_progress_ ||
        current_progress_ == total_progress_) {
      showProgress();
      last_report_ = current_progress_;
    }
  }

  ~ProgressBar() {
    if (!flush_newline_) {
      flush_newline_ = true;
      showProgress();
    }
  }

 private:
  void showProgress() {
    if (!enabled_) return;

    assert(current_progress_ <= current_progress_);

    out_stream_ << "\r" << tag_ << std::setw(10) << std::right
                << current_progress_ << "/" << std::setw(10) << std::left
                << total_progress_ << "\t"
                << "[";

    {
      size_t n_step_done = current_progress_ / step_;
      for (size_t i = 0; i < n_step_done; i++) out_stream_ << symbol_done_;
      for (size_t i = 0; i < bar_length_ - n_step_done; i++)
        out_stream_ << symbol_fill_;
    }

    out_stream_ << "]" << std::flush;

    if (flush_newline_) out_stream_ << std::endl;
  }
};
