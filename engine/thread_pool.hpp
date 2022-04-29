/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace KVDK_NAMESPACE {

constexpr uint64_t kMaxThreadNum = 4;
constexpr uint64_t kMinThreadNum = 4;
constexpr uint64_t kMaxQueueNum = 512;

class ThreadPool {
 public:
  std::chrono::seconds timeout{10};

 public:
  // Temp means a thread is temporary, when it always waits task wthin
  // timeout(second), it will be recycled.
  enum class ThreadStatus { kDefault, kTemp };

  ThreadPool(int threads = kMinThreadNum) : stop_(false) {
    for (; thread_id_ < threads; ++thread_id_) {
      AddWorker(ThreadStatus::kDefault);
    }
  }

  template <class F>
  std::future<void> PushTaskQueue(const F& f) {
    auto task = std::make_shared<std::packaged_task<void(size_t)>>(f);

    if (workers_.size() < kMaxThreadNum && Busy()) {
      AddWorker(ThreadStatus::kTemp);
    }

    std::future<void> res = task->get_future();
    {
      std::unique_lock<std::mutex> lock(queue_mtx_);

      kvdk_assert(!stop_, "push task into stopped thread pool");
      if (stop_) {
        GlobalLogger.Error("push task into stopped thread pool");
        return res;
      }

      tasks_.emplace([task](size_t thread_id) { (*task)(thread_id); });
    }
    task_condition_.notify_one();
    return res;
  }

  void AddWorker(ThreadStatus thread_status) {
    std::thread new_thread([this, thread_status] {
      for (;;) {
        std::function<void(size_t)> task;
        {
          std::unique_lock<std::mutex> lock(this->queue_mtx_);
          idle_thread_num_++;
          if (thread_status == ThreadStatus::kDefault) {
            this->task_condition_.wait(
                lock, [this] { return this->stop_ || !this->tasks_.empty(); });
          } else {
            this->task_condition_.wait_for(lock, timeout, [this] {
              return this->stop_ || !this->tasks_.empty();
            });
            // time out
            if (!(this->stop_ || !this->tasks_.empty())) {
              break;
            }
          }

          if (this->stop_) break;

          task = std::move(this->tasks_.front());
          this->tasks_.pop();
          idle_thread_num_--;
        }
        // excute function
        task(this->thread_id_);
      }
    });

    if (new_thread.joinable()) {
      new_thread.detach();
    }

    workers_.emplace_back(std::move(new_thread));
  }

  void Stop() { shutDown(); }

  bool Busy() {
    return idle_thread_num_.load() == 0 && tasks_.size() >= kMaxQueueNum;
  }

  ~ThreadPool() {
    if (!stop_) {
      shutDown();
    }
  }

 private:
  void shutDown() {
    {
      std::unique_lock<std::mutex> lock(queue_mtx_);
      stop_ = true;
    }
    task_condition_.notify_all();
  }

 private:
  std::vector<std::thread> workers_;
  std::queue<std::function<void(size_t)>> tasks_;

  // synchronization
  std::mutex queue_mtx_;
  std::condition_variable task_condition_;
  bool stop_;
  std::atomic_int idle_thread_num_{0};
  int thread_id_{0};
};

}  // namespace KVDK_NAMESPACE
