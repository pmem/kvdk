/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#include <algorithm>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

#include "kvdk/engine.hpp"
#include "kvdk/namespace.hpp"

#include "../engine/alias.hpp"
#include "test_util.h"

using kvdk::StringView;

using KeyType = StringView;
using ValueType = StringView;
enum class KVState
{
  Existing,
  Deleted
};

struct ValueState
{
  KVState state;
  ValueType value;

  bool operator==(ValueState other)
  {
    return (state == other.state) && ((state == KVState::Deleted) || value == other.value);
  }
};

enum class EngineOperation
{
  Get,
  Set,
  Delete
};
struct EngineTask
{
  EngineOperation op;
  KeyType key;
  ValueType value;  // Empty for Delete, expected for Get
};

using GlobalEngineState = std::unordered_multimap<KeyType, ValueState>;
using ThreadLocalStagedStates = std::unordered_map<KeyType, ValueState>;
// A thread may exist after it has modify the engine but not update the staged state.
// This PendingState keeps track of that possible untracked change by StagedStates.
/// TODO: Actually use this mechanism to check engine after simulated abnormal exit.
using ThreadLocalPendingState = std::unique_ptr<std::pair<KeyType, ValueState>>;
using EngineTaskQueue = std::deque<EngineTask>;

// Contains functions to iterate through a collection and check its contents
// It's up to user to maintain an unordered_multimap between keys and values
// to keep track of the kv-pairs in a certain collection in the engine instance
namespace kvdk_testing {

// Commit changes from states to original. Clear() states.
static void CommitChanges(GlobalEngineState& original, std::vector<ThreadLocalStagedStates>& states)
{
  size_t done = 0;
  GlobalEngineState merged_parallel;
  ProgressBar pbar{std::cout, "", states.size(), 1, true};

  for (auto const& state : states)
  {
    for (auto const& s : state)
    {
      merged_parallel.emplace(s.first, s.second);
    }
    ++done;
    pbar.Update(done);
  }

  auto sz = states.size();
  states.clear();
  states.resize(sz);

  while (!merged_parallel.empty())
  {
    auto key = merged_parallel.begin()->first;
    original.erase(key);
    auto range = merged_parallel.equal_range(key);
    for (auto iter = range.first; iter != range.second; ++iter)
    {
      original.emplace(iter->first, iter->second);
    }
    merged_parallel.erase(key);
  }
}

// Check if a Key-ValueState is possible
static void CheckState(
    KeyType key, ValueState vstate,
    GlobalEngineState const &possible_state) {
  auto ranges = possible_state.equal_range(key);

  bool match = false;
  for (auto iter= ranges.first; iter != ranges.second; ++iter) {
    match = (match || (vstate == iter->second));
  }
  ASSERT_TRUE(match)
      << "Key and State supplied is not possible:\n"
      << "Key: " << key << "\n"
      << "State: " << (vstate.state == KVState::Deleted ? "Deleted" : "Existing") << "\n"
      << "Value: " << vstate.value << "\n";
}

template<typename Getter>
static void QueryExistingKey(Getter getter, KeyType key, ValueType value)
{
  kvdk::Status status;
  std::string value_got;
  status = getter(key, &value_got);
  ASSERT_EQ(status, kvdk::Status::Ok)
      << "Key cannot be queried with Get\n"
      << "Key: " << key << "\n";
  ASSERT_EQ(value, value_got)
      << "Value got does not match expected\n"
      << "Value got:\n"
      << value_got << "\n"
      << "Expected:\n"
      << value << "\n";
}

template<typename Getter>
static void QueryDeletedKey(Getter getter, KeyType key)
{
  kvdk::Status status;
  std::string value_got;
  status = getter(key, &value_got);
  ASSERT_EQ(status, kvdk::Status::NotFound)
      << "Found deleted key\n"
      << "Key: " << key << "\n";
}

template<typename Getter>
static void IteratePossibleState(Getter getter, GlobalEngineState const &possible_state, bool enable_progress_bar)
{
  GlobalEngineState possible_state_copy{possible_state};
  ProgressBar pbar{std::cout, "", possible_state.size(),
                                  1000, enable_progress_bar};

  kvdk::Status status;
  while (!possible_state_copy.empty())
  {
    auto key = possible_state_copy.begin()->first;
    std::string value_got;
    status = getter(key, &value_got);
    ASSERT_TRUE(status == kvdk::Status::NotFound || status == kvdk::Status::Ok);
    ValueState state = (status == kvdk::Status::Ok) ? ValueState{KVState::Existing, value_got} : ValueState{KVState::Deleted, ValueType{}};
    auto range = possible_state_copy.equal_range(key);
    bool match = false;
    for(auto iter = range.first; iter!=range.second; ++iter)
    {
      match = match || (state == iter->second);
    }
    ASSERT_TRUE(match) 
      << "Get returns a result not tracked by GlobalEngineState";
    possible_state_copy.erase(key);
    pbar.Update(possible_state.size() - possible_state_copy.size());
  }
  
}

enum class IteratingDirection
{
  Forward,
  Backward
};
template<typename Iterator, typename Getter>
// possible_kv_pairs is searched to try to find a match with iterated records
// possible_kv_pairs is copied because HashesIterateThrough erase entries to
// keep track of records
static void IterateThrough(
    Iterator iterator,
    Getter getter,
    GlobalEngineState const& possible_state,
    IteratingDirection direction,
    bool enable_progress_bar) {

  GlobalEngineState possible_state_copy{possible_state};

  // Iterating forward or backward.
  {
    ASSERT_TRUE(iterator != nullptr) << "Invalid Iterator";
    switch (direction)
    {
    case IteratingDirection::Forward:
      iterator->SeekToLast();
      std::cout << "[Testing] Iterating forward." << std::endl;
      break;
    case IteratingDirection::Backward:
      iterator->SeekToFirst();
      std::cout << "[Testing] Iterating forward." << std::endl;
      break;
    }

    ProgressBar pbar{std::cout, "", possible_state.size(),
                                   1000, enable_progress_bar};
    while (iterator->Valid()) 
    {
      auto key = iterator->Key();
      auto value = iterator->Value();

      QueryExistingKey(getter, key, value);
      CheckState(key, {KVState::Existing, value}, possible_state_copy);

      possible_state_copy.erase(key);
      pbar.Update(possible_state.size() - possible_state_copy.size());

      switch (direction)
      {
      case IteratingDirection::Forward:
        iterator->Next();
        break;
      case IteratingDirection::Backward:
        iterator->Prev();
        break;
      }
    }
    // Remaining kv-pairs in possible_kv_pairs are deleted kv-pairs
    {
      while(!possible_state_copy.empty()) {
        auto key = possible_state_copy.begin()->first;

        QueryDeletedKey(getter, key);
        CheckState(key, {KVState::Deleted, ValueType{}}, possible_state_copy);
        
        possible_state_copy.erase(key);
        pbar.Update(possible_state.size() - possible_state_copy.size());
      }
      ASSERT_TRUE(possible_state.empty())
          << "There should be no key left in possible_state_copy, "
          << "as they all should have been erased.\n";
    }
  }
}

template<typename Getter, typename Setter, typename Deleter>
void ExecuteTasks(Getter getter, Setter setter, Deleter deleter, EngineTaskQueue const& tasks,
  ThreadLocalStagedStates* done, ThreadLocalPendingState* pending, bool enable_progress_bar)
{
  kvdk::Status status;
  std::string value_got;
  size_t progress = 0;
  ProgressBar progress_bar{std::cout, "", tasks.size(), 100, enable_progress_bar};
  for(auto const& task: tasks)
  {
    switch (task.op)
    {
    case EngineOperation::Get:
    {
      status = getter(task.key, &value_got);
      ASSERT_EQ(status, kvdk::Status::Ok)
          << "Key cannot be queried with Get\n"
          << "Key: " << task.key << "\n";
      ASSERT_EQ(task.value, value_got)
          << "Value got does not match expected\n"
          << "Value got:\n"
          << value_got << "\n"
          << "Expected:\n"
          << task.value << "\n";
      break;
    }
    case EngineOperation::Set:
    {
      if (pending) 
      {
        ASSERT_EQ(*pending, nullptr);
        pending->reset(new std::pair<KeyType, ValueState>{task.key, {KVState::Existing, task.value}});
      }

      status = setter(task.key, task.value);
      ASSERT_EQ(status, kvdk::Status::Ok)
          << "Fail to set key\n"
          << "Key: " << task.key << "\n";

      if (done)
      {
      (*done)[task.key] = {KVState::Existing, task.value};
      }
      if (pending) 
      {
        pending->reset(nullptr);
      }
      break;
    }
    case EngineOperation::Delete:
    {
      if (pending) 
      {
        ASSERT_EQ(*pending, nullptr);
        pending->reset(new std::pair<KeyType, ValueState>{task.key, {KVState::Deleted, ValueType{}}});
      }
      

      status = deleter(task.key);
      ASSERT_EQ(status, kvdk::Status::Ok)
          << "Fail to delete key\n"
          << "Key: " << task.key << "\n";

      if (done)
      {
      (*done)[task.key] = {KVState::Deleted, ValueType{}};
      }
      if (pending) 
      {
        pending->reset(nullptr);
      }
      break;
    }
    }
    ++progress;
    progress_bar.Update(progress);
  }
}

EngineTaskQueue PrepareQueue(
                      std::vector<KeyType> const &keys,
                    std::vector<ValueType> const &values,
                    bool interleaved_set_delete)
{
  // ASSERT_EQ(keys.size(), values.size());
  EngineTaskQueue tasks(keys.size());
  for (size_t i = 0; i < tasks.size(); i++)
  {
    if (i % 2 == 0 || !interleaved_set_delete)
    {
      tasks[i] = {EngineOperation::Set, keys[i], values[i]};
    }
    else
    {
      tasks[i] = {EngineOperation::Delete, keys[i], ValueType{}};
    }
  }

  return tasks;
}


} // namespace kvdk_testing

/// Contains functions for putting batches of keys and values into a collection
/// in an engine instance.
namespace kvdk_testing {
class HGetter
{
  kvdk::Engine * engine;
  KeyType collection_name;
  public:
    HGetter(kvdk::Engine*e, KeyType cn) : engine{e}, collection_name{cn} {}
    kvdk::Status operator()(KeyType key, std::string* value_got)
    {
      return engine->HGet(collection_name, key, value_got);
    }
};
class HSetter
{
  kvdk::Engine * engine;
  KeyType collection_name;
  public:
    HSetter(kvdk::Engine*e, KeyType cn) : engine{e}, collection_name{cn} {}
    kvdk::Status operator()(KeyType key, ValueType value)
    {
      return engine->HSet(collection_name, key, value);
    }
};
class HDeleter
{
  kvdk::Engine * engine;
  KeyType collection_name;
  public:
    HDeleter(kvdk::Engine*e, KeyType cn) : engine{e}, collection_name{cn} {}
    kvdk::Status operator()(KeyType key)
    {
      return engine->HDelete(collection_name, key);
    }
};

class SGetter
{
  kvdk::Engine * engine;
  KeyType collection_name;
  public:
    SGetter(kvdk::Engine*e, KeyType cn) : engine{e}, collection_name{cn} {}
    kvdk::Status operator()(KeyType key, std::string* value_got)
    {
      return engine->HGet(collection_name, key, value_got);
    }
};
class SSetter
{
  kvdk::Engine * engine;
  KeyType collection_name;
  public:
    SSetter(kvdk::Engine*e, KeyType cn) : engine{e}, collection_name{cn} {}
    kvdk::Status operator()(KeyType key, ValueType value)
    {
      return engine->HSet(collection_name, key, value);
    }
};
class SDeleter
{
  kvdk::Engine * engine;
  KeyType collection_name;
  public:
    SDeleter(kvdk::Engine*e, KeyType cn) : engine{e}, collection_name{cn} {}
    kvdk::Status operator()(KeyType key)
    {
      return engine->HDelete(collection_name, key);
    }
};
class Getter
{
  kvdk::Engine * engine;
  public:
    Getter(kvdk::Engine*e) : engine{e} {}
    kvdk::Status operator()(KeyType key, std::string* value_got)
    {
      return engine->Get(key, value_got);
    }
};
class Setter
{
  kvdk::Engine * engine;
  public:
    Setter(kvdk::Engine*e) : engine{e} {}
    kvdk::Status operator()(KeyType key, ValueType value)
    {
      return engine->Set(key, value);
    }
};
class Deleter
{
  kvdk::Engine * engine;
  public:
    Deleter(kvdk::Engine*e) : engine{e} {}
    kvdk::Status operator()(KeyType key)
    {
      return engine->Delete(key);
    }
};

// Calling engine->HSet to put keys and values into collection named after
// collection_name.
static void AllHSet(kvdk::Engine *engine, std::string collection_name,
                    std::vector<KeyType> const &keys,
                    std::vector<ValueType> const &values,
                    ThreadLocalStagedStates* done,
                    ThreadLocalPendingState* pending,
                    bool enable_progress_bar) {

  HGetter getter{engine, collection_name};
  HSetter setter{engine, collection_name};
  HDeleter deleter{engine, collection_name};
  EngineTaskQueue tasks{PrepareQueue(keys, values, false)};
  ExecuteTasks(getter, setter, deleter, tasks, done, pending, enable_progress_bar);
}

// Calling engine->HSet to put keys and values into collection named after
// collection_name.
static void AllSSet(kvdk::Engine *engine, std::string collection_name,
                        std::vector<StringView> const &keys,
                        std::vector<StringView> const &values,
                    ThreadLocalStagedStates* done,
                    ThreadLocalPendingState* pending,
                        bool enable_progress_bar) {
  SGetter getter{engine, collection_name};
  SSetter setter{engine, collection_name};
  SDeleter deleter{engine, collection_name};
  EngineTaskQueue tasks{PrepareQueue(keys, values, false)};
  ExecuteTasks(getter, setter, deleter, tasks, done, pending, enable_progress_bar);
}

static void AllSet(kvdk::Engine *engine,
                        std::vector<StringView> const &keys,
                        std::vector<StringView> const &values,
                    ThreadLocalStagedStates* done,
                    ThreadLocalPendingState* pending,
                        bool enable_progress_bar) {
  Getter getter{engine};
  Setter setter{engine};
  Deleter deleter{engine};
  
  EngineTaskQueue tasks{PrepareQueue(keys, values, false)};
  ExecuteTasks(getter, setter, deleter, tasks, done, pending, enable_progress_bar);
}


// Calling engine->HSet to put evenly indexed keys and values into collection
// named after collection_name. Calling engine->HDelete to delete oddly indexed
// keys from collection named after collection_name.
static void EvenHSetOddHDelete(kvdk::Engine *engine,
                               std::string collection_name,
                               std::vector<StringView> const &keys,
                               std::vector<StringView> const &values,
                    ThreadLocalStagedStates* done,
                    ThreadLocalPendingState* pending,
                               bool enable_progress_bar) {

  HGetter getter{engine, collection_name};
  HSetter setter{engine, collection_name};
  HDeleter deleter{engine, collection_name};
  EngineTaskQueue tasks{PrepareQueue(keys, values, true)};
  ExecuteTasks(getter, setter, deleter, tasks, done, pending, enable_progress_bar);
}

// Calling engine->SSet to put evenly indexed keys and values into collection
// named after collection_name. Calling engine->SDelete to delete oddly indexed
// keys from collection named after collection_name.
static void EvenSSetOddSDelete(kvdk::Engine *engine,
                               std::string collection_name,
                               std::vector<StringView> const &keys,
                               std::vector<StringView> const &values,
                    ThreadLocalStagedStates* done,
                    ThreadLocalPendingState* pending,
                               bool enable_progress_bar) {
  SGetter getter{engine, collection_name};
  SSetter setter{engine, collection_name};
  SDeleter deleter{engine, collection_name};
  EngineTaskQueue tasks{PrepareQueue(keys, values, true)};
  ExecuteTasks(getter, setter, deleter, tasks, done, pending, enable_progress_bar);
}

static void EvenSetOddDelete(kvdk::Engine *engine,
                               std::vector<StringView> const &keys,
                               std::vector<StringView> const &values,
                    ThreadLocalStagedStates* done,
                    ThreadLocalPendingState* pending,
                               bool enable_progress_bar) {
  Getter getter{engine};
  Setter setter{engine};
  Deleter deleter{engine};
  EngineTaskQueue tasks{PrepareQueue(keys, values, true)};
  ExecuteTasks(getter, setter, deleter, tasks, done, pending, enable_progress_bar);
}

static void IterateThroughHashes(kvdk::Engine *engine,
                               std::string collection_name,
                    GlobalEngineState const& state,
                               bool enable_progress_bar) {
  HGetter getter{engine, collection_name};
  HSetter setter{engine, collection_name};
  HDeleter deleter{engine, collection_name};
  auto iterator = engine->NewUnorderedIterator(collection_name);
  IterateThrough(iterator, getter, state, IteratingDirection::Forward, enable_progress_bar);
  IterateThrough(iterator, getter, state, IteratingDirection::Backward, enable_progress_bar);
}
static void IterateThroughSortedSets(kvdk::Engine *engine,
                               std::string collection_name,
                    GlobalEngineState const& state,
                               bool enable_progress_bar) {
  SGetter getter{engine, collection_name};
  SSetter setter{engine, collection_name};
  SDeleter deleter{engine, collection_name};
  auto iterator = engine->NewSortedIterator(collection_name);
  IterateThrough(iterator, getter, state, IteratingDirection::Forward, enable_progress_bar);
  IterateThrough(iterator, getter, state, IteratingDirection::Backward, enable_progress_bar);
}

} // namespace kvdk_testing

class EngineTestBase : public testing::Test {
protected:
  kvdk::Engine *engine = nullptr;
  kvdk::Configs configs;
  kvdk::Status status;

  const std::string path_db{"/mnt/pmem0/kvdk_test_extensive"};

  /// The following parameters are used to configure the test.
  /// Override SetUpParameters to provide different parameters
  /// Default configure parameters
  bool do_populate_when_initialize;
  size_t sz_pmem_file;
  size_t n_hash_bucket;
  size_t sz_hash_bucket;
  size_t n_blocks_per_segment;
  size_t t_background_work_interval;

  /// Test specific parameters
  size_t n_thread;
  size_t n_kv_per_thread;
  // These parameters set the range of sizes of keys and values
  size_t sz_key_min;
  size_t sz_key_max;
  size_t sz_value_min;
  size_t sz_value_max;

  // Actual keys an values used by thread for insertion
  std::vector<std::vector<StringView>> grouped_keys;
  std::vector<std::vector<StringView>> grouped_values;

  std::unordered_map<std::string, GlobalEngineState> hashes_states;
  std::unordered_map<std::string, GlobalEngineState> sorted_states;
  GlobalEngineState string_states;

  std::unordered_map<std::string, std::vector<ThreadLocalStagedStates>> hashes_staged;
  std::unordered_map<std::string, std::vector<ThreadLocalStagedStates>> sorted_staged;
  std::vector<ThreadLocalStagedStates> string_staged;

  std::unordered_map<std::string, std::vector<ThreadLocalPendingState>> hashes_pending;
  std::unordered_map<std::string, std::vector<ThreadLocalPendingState>> sorted_pending;
  std::vector<ThreadLocalPendingState> string_pending;

private:
  std::vector<std::string> key_pool;
  std::vector<std::string> value_pool;
  std::default_random_engine rand{42};

protected:
  /// Other tests should overload this function to setup parameters
  virtual void SetUpParameters() = 0;

  virtual void SetUp() override {
    purgeDB();

    SetUpParameters();

    configs.populate_pmem_space = do_populate_when_initialize;
    configs.pmem_file_size = sz_pmem_file;
    configs.hash_bucket_num = n_hash_bucket;
    configs.hash_bucket_size = sz_hash_bucket;
    configs.pmem_segment_blocks = n_blocks_per_segment;
    configs.background_work_interval = t_background_work_interval;

    prepareKVPairs();

    status = kvdk::Engine::Open(path_db, &engine, configs, stderr);
    ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to open the KVDK instance";
  }

  virtual void TearDown() {
    delete engine;
    purgeDB();
  }

  void RebootDB() {
    delete engine;

    status = kvdk::Engine::Open(path_db, &engine, configs, stderr);
    ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to open the KVDK instance";
  }

  void ShuffleAllKeysValuesWithinThread() {
    for (size_t tid = 0; tid < n_thread; tid++) {
      shuffleKeys(tid);
      shuffleValues(tid);
    }
  }

  void HashesAllHSet(std::string const &collection_name) {
    auto ModifyEngine = [&](int tid) {
      if (tid == 0) {
        kvdk_testing::AllHSet(engine, collection_name, grouped_keys[tid],
                              grouped_values[tid], &hashes_staged[collection_name][tid], &hashes_pending[collection_name][tid], true);

      } else {
        kvdk_testing::AllHSet(engine, collection_name, grouped_keys[tid],
                              grouped_values[tid], &hashes_staged[collection_name][tid], &hashes_pending[collection_name][tid], false);
      }
    };

    std::cout << "[Testing] Execute HSet in " << collection_name << "."
              << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
    kvdk_testing::CommitChanges(hashes_states[collection_name], hashes_staged[collection_name]);
  }

  void HashesEvenHSetOddHDelete(std::string const &collection_name) {
    auto ModifyEngine = [&](int tid) {
      if (tid == 0) {
        kvdk_testing::EvenHSetOddHDelete(engine, collection_name, grouped_keys[tid],
                              grouped_values[tid], &hashes_staged[collection_name][tid], &hashes_pending[collection_name][tid], true);

      } else {
        kvdk_testing::EvenHSetOddHDelete(engine, collection_name, grouped_keys[tid],
                              grouped_values[tid], &hashes_staged[collection_name][tid], &hashes_pending[collection_name][tid], false);
      }
    };

    std::cout << "[Testing] Execute HSet in " << collection_name << "."
              << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
    kvdk_testing::CommitChanges(hashes_states[collection_name], hashes_staged[collection_name]);
  }

  void SortedSetsAllSSet(std::string const &collection_name) {
    auto ModifyEngine = [&](int tid) {
      if (tid == 0) {
        kvdk_testing::AllSSet(engine, collection_name, grouped_keys[tid],
                              grouped_values[tid], &sorted_staged[collection_name][tid], &sorted_pending[collection_name][tid], true);

      } else {
        kvdk_testing::AllSSet(engine, collection_name, grouped_keys[tid],
                              grouped_values[tid], &sorted_staged[collection_name][tid], &sorted_pending[collection_name][tid], false);
      }
    };

    std::cout << "[Testing] Execute HSet in " << collection_name << "."
              << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
    kvdk_testing::CommitChanges(sorted_states[collection_name], sorted_staged[collection_name]);
  }

  void SortedSetsEvenSSetOddSDelete(std::string const &collection_name) {
    auto ModifyEngine = [&](int tid) {
      if (tid == 0) {
        kvdk_testing::EvenSSetOddSDelete(engine, collection_name, grouped_keys[tid],
                              grouped_values[tid], &sorted_staged[collection_name][tid], &sorted_pending[collection_name][tid], true);

      } else {
        kvdk_testing::EvenSSetOddSDelete(engine, collection_name, grouped_keys[tid],
                              grouped_values[tid], &sorted_staged[collection_name][tid], &sorted_pending[collection_name][tid], false);
      }
    };

    std::cout << "[Testing] Execute HSet in " << collection_name << "."
              << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
    kvdk_testing::CommitChanges(sorted_states[collection_name], sorted_staged[collection_name]);
  }

  void CheckHashesCollection(std::string collection_name) {
    std::cout << "[Testing] Iterate through " << collection_name
              << " to check data." << std::endl;
    kvdk_testing::IterateThroughHashes(engine, collection_name, hashes_states[collection_name], true);
  }

  void CheckSortedSetsCollection(std::string collection_name) {
    std::cout << "[Testing] Iterate through " << collection_name
              << " to check data." << std::endl;
    kvdk_testing::IterateThroughSortedSets(engine, collection_name, sorted_states[collection_name], true);
  }

  void InitializeGlobalAnonymousCollection()
  {
    string_staged.resize(n_thread);
    string_pending.resize(n_thread);
  }
  void InitializeHashes(std::string const& collection_name)
  {
    hashes_states[collection_name];
    hashes_staged[collection_name].resize(n_thread);
    hashes_pending[collection_name].resize(n_thread);
  }
  void InitializeSorted(std::string const& collection_name)
  {
    sorted_states[collection_name];
    sorted_staged[collection_name].resize(n_thread);
    sorted_pending[collection_name].resize(n_thread);
  }

private:
  void purgeDB() {
    std::string cmd = "rm -rf " + path_db + "\n";
    [[gnu::unused]] int _sink = system(cmd.data());
  }

  void shuffleKeys(size_t tid) {
    std::shuffle(grouped_keys[tid].begin(), grouped_keys[tid].end(), rand);
  }

  void shuffleValues(size_t tid) {
    std::shuffle(grouped_values[tid].begin(), grouped_values[tid].end(), rand);
  }

  void prepareKVPairs() {
    key_pool.reserve(n_thread * n_kv_per_thread);
    value_pool.reserve(n_kv_per_thread);
    grouped_keys.resize(n_thread);
    grouped_values.resize(n_thread);

    for (size_t tid = 0; tid < n_thread; tid++) {
      grouped_keys[tid].reserve(n_kv_per_thread);
      grouped_values[tid].reserve(n_kv_per_thread);
    }

    std::cout << "[Testing] Generating string for keys and values" << std::endl;
    {
      ProgressBar progress_gen_kv{std::cout, "", n_kv_per_thread, true};
      for (size_t i = 0; i < n_kv_per_thread; i++) {
        value_pool.push_back(GetRandomString(sz_value_min, sz_value_max));
        for (size_t tid = 0; tid < n_thread; tid++) {
          key_pool.push_back(GetRandomString(sz_key_min, sz_key_max));
        }

        if ((i + 1) % 1000 == 0 || (i + 1) == n_kv_per_thread) {
          progress_gen_kv.Update(i + 1);
        }
      }
    }
    std::cout << "[Testing] Generating string_view for keys and values"
              << std::endl;
    {
      ProgressBar progress_gen_kv_view{std::cout, "", n_thread, true};
      for (size_t tid = 0; tid < n_thread; tid++) {
        for (size_t i = 0; i < n_kv_per_thread; i++) {
          grouped_keys[tid].emplace_back(key_pool[i * n_thread + tid]);
          grouped_values[tid].emplace_back(value_pool[i]);
        }
        progress_gen_kv_view.Update(tid + 1);
      }
    }
  }
};

class EngineStressTest : public EngineTestBase {
protected:
  virtual void SetUpParameters() override final {
    /// Default configure parameters
    do_populate_when_initialize = false;
    // 256GB PMem
    sz_pmem_file = (32ULL << 30);
    // Less buckets to increase hash collisions
    n_hash_bucket = (1ULL << 20);
    // Smaller buckets to increase hash collisions
    sz_hash_bucket = (3 + 1) * 16;
    n_blocks_per_segment = (1ULL << 10);
    t_background_work_interval = 1;

    /// Test specific parameters
    n_thread = 1;
    // 2M keys per thread, totaling about 100M records
    n_kv_per_thread = (2ULL << 10);
    // These parameters set the range of sizes of keys and values
    sz_key_min = 2;
    sz_key_max = 16;
    sz_value_min = 0;
    sz_value_max = 1024;
  }
  // Shared among EngineStressTest
  const size_t n_reboot = 3;
};

TEST_F(EngineStressTest, HashesHSetOnly) {
  std::string global_collection_name{"GlobalCollection"};

  InitializeHashes(global_collection_name);
  HashesAllHSet(global_collection_name);
  CheckHashesCollection(global_collection_name);

  std::cout << "[Testing] Close, reopen, iterate through engine for "
            << n_reboot << " times to test recovery." << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;
    RebootDB();
    CheckHashesCollection(global_collection_name);
    HashesAllHSet(global_collection_name);
    CheckHashesCollection(global_collection_name);
  }
}

TEST_F(EngineStressTest, HashesHSetAndHDelete) {
  std::string global_collection_name{"GlobalCollection"};

  InitializeHashes(global_collection_name);

  HashesEvenHSetOddHDelete(global_collection_name);

  std::cout << "[Testing] Iterate through collection to check data."
            << std::endl;
  CheckHashesCollection(global_collection_name);

  std::cout
      << "[Testing] Close, reopen, iterate through, update, iterate through "
         "engine for "
      << n_reboot << " times to test recovery and updating" << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    RebootDB();
    CheckHashesCollection(global_collection_name);

    ShuffleAllKeysValuesWithinThread();

    HashesEvenHSetOddHDelete(global_collection_name);
    CheckHashesCollection(global_collection_name);
  }
}

TEST_F(EngineStressTest, SortedSetsSSetOnly) {
  std::string global_collection_name{"GlobalCollection"};
  InitializeSorted(global_collection_name);

  kvdk::Collection *dummy;
  ASSERT_EQ(engine->CreateSortedCollection(global_collection_name,
                                           &dummy),
            kvdk::Status::Ok);
  SortedSetsAllSSet(global_collection_name);
  CheckSortedSetsCollection(global_collection_name);

  std::cout << "[Testing] Close, reopen, iterate through engine for "
            << n_reboot << " times to test recovery." << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    RebootDB();
    CheckSortedSetsCollection(global_collection_name);
    SortedSetsAllSSet(global_collection_name);
    CheckSortedSetsCollection(global_collection_name);
  }
}

TEST_F(EngineStressTest, SortedSetsSSetAndSDelete) {
  std::string global_collection_name{"GlobalCollection"};
  InitializeSorted(global_collection_name);
  
  kvdk::Collection *dummy;
  ASSERT_EQ(engine->CreateSortedCollection(global_collection_name,
                                           &dummy),
            kvdk::Status::Ok);

  SortedSetsEvenSSetOddSDelete(global_collection_name);

  std::cout << "[Testing] Iterate through collection to check data."
            << std::endl;
  CheckSortedSetsCollection(global_collection_name);

  std::cout
      << "[Testing] Close, reopen, iterate through, update, iterate through "
         "engine for "
      << n_reboot << " times to test recovery and updating" << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    RebootDB();
    CheckSortedSetsCollection(global_collection_name);

    ShuffleAllKeysValuesWithinThread();

    SortedSetsEvenSSetOddSDelete(global_collection_name);
    CheckSortedSetsCollection(global_collection_name);
  }
}

class EngineHotspotTest : public EngineTestBase {
private:
  virtual void SetUpParameters() override final {
    /// Default configure parameters
    do_populate_when_initialize = false;
    // 16GB PMem
    sz_pmem_file = (16ULL << 30);
    // Less buckets to increase hash collisions
    n_hash_bucket = (1ULL << 20);
    // Small buckets to increase hash collisions
    sz_hash_bucket = (3 + 1) * 16;
    n_blocks_per_segment = (1ULL << 20);
    t_background_work_interval = 1;

    /// Test specific parameters
    // Too many threads will make this test too slow
    n_thread = 4;
    // 1M keys per thread, totaling about 50M writes
    n_kv_per_thread = (1ULL << 20);
    // 0-sized key "" is a hotspot, which may reveal many defects
    // These parameters set the range of sizes of keys and values
    sz_key_min = 0;
    sz_key_max = 1;
    sz_value_min = 0;
    sz_value_max = 1024;
  }
};

TEST_F(EngineHotspotTest, HashesMultipleHotspot) {
  std::string global_collection_name{"GlobalHashesCollection"};
  InitializeHashes(global_collection_name);

  HashesEvenHSetOddHDelete(global_collection_name);
  std::cout << "[Testing] Iterate through collection to check data."
            << std::endl;
  CheckHashesCollection(global_collection_name);

  RebootDB();

  HashesEvenHSetOddHDelete(global_collection_name);
  std::cout << "[Testing] Iterate through collection to check data."
            << std::endl;
  CheckHashesCollection(global_collection_name);
}

TEST_F(EngineHotspotTest, SortedSetsMultipleHotspot) {
  std::string global_collection_name{"GlobalHashesCollection"};
    InitializeSorted(global_collection_name);


  kvdk::Collection *dummy;
  ASSERT_EQ(engine->CreateSortedCollection(global_collection_name,
                                           &dummy),
            kvdk::Status::Ok);

  SortedSetsEvenSSetOddSDelete(global_collection_name);
  std::cout << "[Testing] Iterate through collection to check data."
            << std::endl;
  CheckSortedSetsCollection(global_collection_name);

  RebootDB();

  SortedSetsEvenSSetOddSDelete(global_collection_name);
  std::cout << "[Testing] Iterate through collection to check data."
            << std::endl;
  CheckSortedSetsCollection(global_collection_name);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
