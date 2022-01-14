#pragma one

#include <assert.h>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kvdk/namespace.hpp"

#if DEBUG_LEVEL > 0
namespace KVDK_NAMESPACE {

struct SyncPointPair {
  std::string predecessor;
  std::string successor;
};

struct SyncImpl {
  SyncImpl() : ready_(false) {}
  void LoadDependency(const std::vector<SyncPointPair> &dependencies) {
    std::lock_guard<std::mutex> lock(mutex_);
    successors_.clear();
    predecessors_.clear();
    cleared_points_.clear();
    for (const auto &dependency : dependencies) {
      successors_[dependency.predecessor].push_back(dependency.successor);
      predecessors_[dependency.successor].push_back(dependency.predecessor);
      point_table_.insert(dependency.successor);
      point_table_.insert(dependency.predecessor);
    }
    cv_.notify_all();
  }

  void EnableProcessing() { ready_ = true; }

  void DisableProcessing() { ready_ = false; }

  void Process(const std::string &point, void *func_arg) {
    if (!ready_) {
      return;
    }

    if (point_table_.find(point) == point_table_.end()) {
      return;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    while (!IsClearedAllPredecessors(point)) {
      cv_.wait(lock);
    }

    auto callback_pair = callbacks_.find(point);
    if (callback_pair != callbacks_.end()) {
      num_callbacks_running_++;
      mutex_.unlock();
      callback_pair->second(func_arg);
      mutex_.lock();
      num_callbacks_running_--;
    }
    cleared_points_.insert(point);

    cv_.notify_all();
  }

  void SetCallBack(const std::string &point,
                   const std::function<void(void *)> &callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    callbacks_[point] = callback;
    point_table_.insert(point);
  }

  void ClearAllCallBacks() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (num_callbacks_running_ > 0) {
      cv_.wait(lock);
    }
    callbacks_.clear();
  }

  bool IsClearedAllPredecessors(const std::string &point) {
    for (const std::string &predecessor : predecessors_[point]) {
      if (cleared_points_.find(predecessor) == cleared_points_.end())
        return false;
    }
    return true;
  }

  void ClearDependTrace() {
    std::lock_guard<std::mutex> lock(mutex_);
    cleared_points_.clear();
  }

  void Init() {
    std::lock_guard<std::mutex> lock(mutex_);
    cleared_points_.clear();
    successors_.clear();
    predecessors_.clear();
    callbacks_.clear();
    point_table_.clear();
    num_callbacks_running_ = 0;
  }

  virtual ~SyncImpl() {}

private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<bool> ready_;
  std::unordered_set<std::string> point_table_;
  int num_callbacks_running_ = 0;

  std::unordered_map<std::string, std::vector<std::string>> successors_;
  std::unordered_map<std::string, std::vector<std::string>> predecessors_;
  // sync points that have been passed through
  std::unordered_set<std::string> cleared_points_;
  std::unordered_map<std::string, std::function<void(void *)>> callbacks_;
};

} // namespace KVDK_NAMESPACE

#endif