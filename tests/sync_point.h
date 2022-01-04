#pragma once

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

#include "engine/hash_table.hpp"

namespace KVDK_NAMESPACE {
namespace TEST_UTILS {

struct SyncPointPair {
  std::string predecessor;
  std::string successor;
};

struct SyncImpl {
public:
  SyncImpl() : ready_(false) {}
  ~SyncImpl() {}

private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<bool> ready_;
  std::unordered_set<std::string> point_table_;

  std::unordered_map<std::string, std::vector<std::string>> successors_;
  std::unordered_map<std::string, std::vector<std::string>> predecessors_;
  // sync points that have been passed through
  std::unordered_set<std::string> cleared_points_;
  std::unordered_map<std::string, std::function<void(void *)>> callbacks_;

  void LoadDependency(const std::vector<SyncPointPair> &dependencies);

  void EnableProcessing() { ready_ = true; }

  void DisableProcessing() { ready_ = false; }
};

class SyncPoint {
public:
  static SyncPoint *GetInstance();
  SyncPoint(const SyncPoint &) = delete;
  SyncPoint &operator=(const SyncPoint &) = delete;
  ~SyncPoint();

  void Process(const std::string &point, void *cb_arg = nullptr);

  void LoadDependency(const std::vector<SyncPointPair> &dependencies);

private:
  SyncPoint();
  SyncImpl *sync_impl_;
};

#define TEST_SYNC_POINT(x)                                                     \
  KVDK_NAMESPACE::TEST_UTILS::SyncPoint::GetInstance()->Process(x)
#define TEST_SYNC_POINT_CALLBACK(x, y)                                         \
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->Process(x, y)
} // namespace TEST_UTILS
} // namespace KVDK_NAMESPACE
