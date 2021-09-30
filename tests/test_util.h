/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#include <atomic>
#include <iostream>
#include <numeric>
#include <vector>
#include <random>
#include <string>
#include <cassert>
#include <iomanip>

/* Create a string that contains 8 bytes from uint64_t. */
inline std::string uint64_to_string(uint64_t &key) {
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

// Return a string of length len with random characters in ['a', 'z']
inline std::string GetRandomString(size_t len)
{
  static std::default_random_engine re;
  std::string str;
  str.reserve(len);
  for (size_t i = 0; i < len; i++)
    str.push_back('a'+ re() % 26);
  return str;
}

// Return a string of length in [min_len, max_len] with random characters in ['a', 'z']
inline std::string GetRandomString(size_t min_len, size_t max_len)
{
  static std::default_random_engine re;
  size_t len = min_len + re() % (max_len - min_len + 1);
  return GetRandomString(len);  
}


inline void LaunchNThreads(int n_thread, std::function<void(int tid)> func,
                    uint32_t id_start = 0) {
  std::vector<std::thread> ts;
  for (int i = id_start; i < id_start + n_thread; i++) {
    ts.emplace_back(std::thread(func, i));
  }
  for (auto &t : ts)
    t.join();
}

void ShowProgress(std::ostream& os, int progress, int total, size_t len_bar = 50, char symbol_done = '#', char symbol_fill = '-')
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

class ProgressBar
{
private:
  std::ostream& _out_stream_;
  std::string _tag_;
  int _total_progress_;   
  int _current_progress_;
  int _bar_length_;
  int _step_;

  bool _finished_{ false };

  static constexpr char _symbol_done_{'#'};
  static constexpr char _symbol_fill_{'-'};

public:
  explicit ProgressBar(std::ostream& out, std::string tag, int total_progress, int bar_length = 50) :
    _out_stream_{out},
    _tag_{tag},
    _total_progress_{total_progress},
    _current_progress_{0},
    _bar_length_{bar_length},
    _step_{total_progress/bar_length}
  {
    assert(_total_progress_ > 0);
    assert(_bar_length_ > 0);
    if (_step_ == 0)
    {
      _step_ = 1;
      _bar_length_ = total_progress;
    }

    showProgress();
  }

  void Update(int current_progress)
  {
    assert(!_finished_ && "Trying to update a completed progress!");
    assert(_current_progress_ < current_progress && current_progress <= _total_progress_);

    _current_progress_ = current_progress;
    if (_current_progress_ == _total_progress_)
      _finished_ = true;

    showProgress();
  }

  ~ProgressBar()
  {
    if(!_finished_)
    {
      _finished_ = true;
      showProgress();
    }
  }

private:
  void showProgress()
  {
    assert(0 <= _current_progress_ && _current_progress_ <= _total_progress_);

    _out_stream_ 
      << "\r"
      << _tag_
      << std::setw(10) << std::right << _current_progress_
      <<  "/"
      << std::setw(10) << std::left << _total_progress_ 
      << "\t"
      << "[";

      {
        int n_step_done = _current_progress_ / _step_;
        for (size_t i = 0; i < n_step_done; i++)
          _out_stream_ << _symbol_done_;
        for (size_t i = 0; i < _bar_length_ - n_step_done; i++)
          _out_stream_ << _symbol_fill_;
      }

    _out_stream_
      << "]"
      << std::flush;

    if (_finished_)
      _out_stream_ << std::endl;
  }
};