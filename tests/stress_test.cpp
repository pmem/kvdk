/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */
#include <gtest/gtest.h>

#include <algorithm>
#include <deque>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../engine/alias.hpp"
#include "../engine/kv_engine.hpp"
#include "kvdk/engine.hpp"
#include "test_util.h"

using kvdk::StringView;

namespace kvdk_testing {

using KeyType = StringView;
using ValueType = StringView;
using CollectionNameType = StringView;

// Operators are just wrappers of the engine and collection name
// They offer an universal interface for calling KVEngine APIs
class HashesOperator {
  kvdk::Engine*& engine;
  CollectionNameType collection_name;

 public:
  HashesOperator() = delete;
  HashesOperator(kvdk::Engine*& e, CollectionNameType cn)
      : engine{e}, collection_name{cn} {}
  kvdk::Status operator()(KeyType key, std::string* value_got) {
    return engine->HGet(collection_name, key, value_got);
  }
  kvdk::Status operator()(KeyType key, ValueType value) {
    return engine->HSet(collection_name, key, value);
  }
  kvdk::Status operator()(KeyType key) {
    return engine->HDelete(collection_name, key);
  }
};

class SortedOperator {
  kvdk::Engine*& engine;
  CollectionNameType collection_name;

 public:
  SortedOperator() = delete;
  SortedOperator(kvdk::Engine*& e, CollectionNameType cn)
      : engine{e}, collection_name{cn} {}
  kvdk::Status operator()(KeyType key, std::string* value_got) {
    return engine->SGet(collection_name, key, value_got);
  }
  kvdk::Status operator()(KeyType key, ValueType value) {
    return engine->SSet(collection_name, key, value);
  }
  kvdk::Status operator()(KeyType key) {
    return engine->SDelete(collection_name, key);
  }
};

class StringOperator {
  kvdk::Engine*& engine;
  CollectionNameType collection_name;

 public:
  StringOperator() = delete;
  // For convenince, introducing empty collection_name for global anonymous
  // collection
  StringOperator(kvdk::Engine*& e, CollectionNameType cn)
      : engine{e}, collection_name{} {
    if (cn != collection_name) throw;
  }
  kvdk::Status operator()(KeyType key, std::string* value_got) {
    return engine->Get(key, value_got);
  }
  kvdk::Status operator()(KeyType key, ValueType value) {
    return engine->Set(key, value);
  }
  kvdk::Status operator()(KeyType key) { return engine->Delete(key); }
};

enum class IteratingDirection { Forward, Backward };

// A ShadowKVEngine operates on one KVEngine collection,
// including the global anonymous collection(string).
// User should call EvenXSetOddXSet() first to modify KVEngine,
// then call UpdatePossibleStates() to update possible_state to
// keep track of the state of the KVEngine.
template <typename EngineOperator>
class ShadowKVEngine {
 public:
  struct StateAndValue {
    enum class State { Existing, Deleted } state;
    ValueType value;

    bool operator==(StateAndValue other) {
      bool match_state = (state == other.state);
      bool match_value =
          ((state == State::Existing) && (value == other.value)) ||
          (state == State::Deleted);
      return match_state && match_value;
    }
  };

  struct SingleOp {
    enum class OpType { Get, Set, Delete } op;
    KeyType key;
    ValueType value;  // Empty for Delete, expected for Get

    // For printing error message
    friend std::ostream& operator<<(std::ostream& out, SingleOp const& sop) {
      out << "Op: "
          << (sop.op == OpType::Get
                  ? "Get"
                  : (sop.op == OpType::Set ? "Set" : "Delete"))
          << "\n"
          << "Key: " << sop.key << "\n"
          << "Value: " << sop.value << "\n";
      return out;
    }
  };

  using OperationQueue = std::deque<SingleOp>;
  using PossibleStates = std::unordered_multimap<KeyType, StateAndValue>;
  using StagedChanges = std::unordered_map<KeyType, StateAndValue>;

 private:
  kvdk::Engine*& engine;
  CollectionNameType collection_name;
  EngineOperator engine_operator;
  size_t const n_thread;
  // A Key may have multiple possible StateAndValue.
  // possible_state keep track of these StateAndValues
  PossibleStates possible_state;
  std::vector<OperationQueue> task_queues;

 public:
  ShadowKVEngine() = delete;
  ShadowKVEngine(kvdk::Engine*& e, CollectionNameType cn, size_t nt)
      : engine{e},
        collection_name{cn},
        engine_operator{engine, collection_name},
        n_thread{nt},
        possible_state{},
        task_queues(n_thread) {}

  // Execute task_queues in ShadowKVEngine
  // Update possible_state
  /// TODO: make this private, put it in operateKVEngine(), which should
  /// run multiple threads.
  void UpdatePossibleStates() {
    std::cout << "[Testing] Updating Engine State" << std::endl;

    // Some keys are overwritten by operations in operateKVEngine(),
    // states and values before calling operateKVEngine() are
    // no longer possible.
    {
      ProgressBar pbar{std::cout, "", n_thread, 1, true};
      for (size_t tid = 0; tid < n_thread; tid++) {
        for (auto const& sop : task_queues[tid]) {
          possible_state.erase(sop.key);
        }
        pbar.Update(tid + 1);
      }
    }

    // Squash every task queue and merge into possible_state
    {
      ProgressBar pbar{std::cout, "", n_thread, 1, true};
      for (size_t tid = 0; tid < n_thread; tid++) {
        StagedChanges squashed_changes{task_queues[tid].size() * 2};
        for (auto const& sop : task_queues[tid]) {
          switch (sop.op) {
            case SingleOp::OpType::Get: {
              // Get will not change the state of any KV
              continue;
            }
            case SingleOp::OpType::Set: {
              squashed_changes[sop.key] =
                  StateAndValue{StateAndValue::State::Existing, sop.value};
              continue;
            }
            case SingleOp::OpType::Delete: {
              squashed_changes[sop.key] =
                  StateAndValue{StateAndValue::State::Deleted, ValueType{}};
              continue;
            }
          }
        }
        for (auto const& kvs : squashed_changes) {
          possible_state.emplace(kvs);
        }
        pbar.Update(tid + 1);
      }
    }
    task_queues.clear();
    task_queues.resize(n_thread);
  }

  // Modify KVEngine by Set
  void EvenXSetOddXSet(size_t tid, std::vector<KeyType> const& keys,
                       std::vector<ValueType> const& values) {
    task_queues[tid] = generateOperations(keys, values, false);
    operateKVEngine(tid, (tid == 0));
  }

  // Modify KVEngine by Set and Delete
  void EvenXSetOddXDelete(size_t tid, std::vector<KeyType> const& keys,
                          std::vector<ValueType> const& values) {
    task_queues[tid] = generateOperations(keys, values, true);
    operateKVEngine(tid, (tid == 0));
  }

  // Check KVEngine by iterating through it.
  // Iterated KVs are looked up in possible_state.
  void CheckIterator(kvdk::Iterator* iterator, IteratingDirection direction) {
    PossibleStates possible_state_copy{possible_state};

    // Iterating forward or backward.
    {
      ASSERT_TRUE(iterator != nullptr) << "Invalid Iterator";
      switch (direction) {
        case IteratingDirection::Forward: {
          std::cout << "[Testing] Iterating forward." << std::endl;
          iterator->SeekToFirst();
          break;
        }
        case IteratingDirection::Backward: {
          std::cout << "[Testing] Iterating backward." << std::endl;
          iterator->SeekToLast();
          break;
        }
      }

      ProgressBar pbar{std::cout, "", possible_state.size(), 1000, true};
      while (iterator->Valid()) {
        auto key = iterator->Key();
        auto value = iterator->Value();

        checkState(key, {StateAndValue::State::Existing, value});

        possible_state_copy.erase(key);
        pbar.Update(possible_state.size() - possible_state_copy.size());

        switch (direction) {
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
        while (!possible_state_copy.empty()) {
          auto key = possible_state_copy.begin()->first;

          checkState(key, {StateAndValue::State::Deleted, ValueType{}});

          possible_state_copy.erase(key);
          pbar.Update(possible_state.size() - possible_state_copy.size());
        }
      }
    }
  }

  // Check KVEngine by get every key in possible_state
  // and check its value and state.
  void CheckGetter() {
    kvdk::Status status;
    std::string value_got;
    PossibleStates possible_state_copy{possible_state};
    {
      std::cout << "[Testing] Checking by Get" << std::endl;
      ProgressBar pbar{std::cout, "", possible_state.size(), 1000, true};
      while (!possible_state_copy.empty()) {
        auto key = possible_state_copy.begin()->first;

        status = engine_operator(key, &value_got);
        switch (status) {
          case kvdk::Status::Ok: {
            checkState(key, {StateAndValue::State::Existing, value_got});
            break;
          }
          case kvdk::Status::NotFound: {
            checkState(key, {StateAndValue::State::Deleted, ValueType{}});
            break;
          }
          default: {
            ASSERT_TRUE(false) << "Invalid kvdk status in CheckGetter.";
            break;
          }
        }

        possible_state_copy.erase(key);
        pbar.Update(possible_state.size() - possible_state_copy.size());
      }
    }
  }

 private:
  // Excecute task_queues in KVEngine by calling EngineOperator
  // ShadowKVEngine remains unchanged
  void operateKVEngine(size_t tid, bool enable_progress_bar) {
    OperationQueue const& tasks = task_queues[tid];

    kvdk::Status status;
    std::string value_got;
    size_t progress = 0;
    ProgressBar progress_bar{std::cout, "", tasks.size(), 100,
                             enable_progress_bar};
    /// TODO: Catch kill point and clean up tasks
    for (auto const& task : tasks) {
      switch (task.op) {
        case SingleOp::OpType::Get: {
          status = engine_operator(task.key, &value_got);
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
        case SingleOp::OpType::Set: {
          status = engine_operator(task.key, task.value);
          ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to set key\n"
                                              << "Key: " << task.key << "\n";
          break;
        }
        case SingleOp::OpType::Delete: {
          status = engine_operator(task.key);
          ASSERT_EQ(status, kvdk::Status::Ok) << "Fail to delete key\n"
                                              << "Key: " << task.key << "\n";
          break;
        }
      }
      ++progress;
      progress_bar.Update(progress);
    }
  }

  // Check whether a key and corresponding state is in possible_state
  void checkState(KeyType key, StateAndValue vstate) {
    auto ranges = possible_state.equal_range(key);

    bool match = false;
    for (auto iter = ranges.first; iter != ranges.second; ++iter) {
      match = (match || (vstate == iter->second));
    }
    ASSERT_TRUE(match) << "Key and State supplied is not possible:\n"
                       << "Supplied Key, State and Value is:\n"
                       << "Key: " << key << "\n"
                       << "State: "
                       << (vstate.state == StateAndValue::State::Deleted
                               ? "Deleted"
                               : "Existing")
                       << "\n"
                       << "Value: " << vstate.value << "\n";
  }

  static OperationQueue generateOperations(std::vector<KeyType> const& keys,
                                           std::vector<ValueType> const& values,
                                           bool interleaved_set_delete) {
    OperationQueue queue(keys.size());
    for (size_t i = 0; i < queue.size(); i++) {
      if (i % 2 == 0 || !interleaved_set_delete) {
        queue[i] = {SingleOp::OpType::Set, keys[i], values[i]};
      } else {
        queue[i] = {SingleOp::OpType::Delete, keys[i], ValueType{}};
      }
    }
    return queue;
  }
};
}  // namespace kvdk_testing

class EngineTestBase : public testing::Test {
 protected:
  kvdk::Engine* engine = nullptr;
  kvdk::Configs configs;
  kvdk::Status status;

  const std::string path_db{"/mnt/pmem0/kvdk_stress_test_" +
                            std::to_string(__rdtsc())};

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

  using ShadowHashes =
      kvdk_testing::ShadowKVEngine<kvdk_testing::HashesOperator>;
  using ShadowSorted =
      kvdk_testing::ShadowKVEngine<kvdk_testing::SortedOperator>;
  using ShadowString =
      kvdk_testing::ShadowKVEngine<kvdk_testing::StringOperator>;

  std::unordered_map<std::string, std::unique_ptr<ShadowHashes>>
      shadow_hashes_engines;
  std::unordered_map<std::string, std::unique_ptr<ShadowSorted>>
      shadow_sorted_engines;
  std::unique_ptr<ShadowString> shadow_string_engine;

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

  void HashesAllHSet(std::string const& collection_name) {
    ShuffleAllKeysValuesWithinThread();
    auto ModifyEngine = [&](int tid) {
      shadow_hashes_engines[collection_name]->EvenXSetOddXSet(
          tid, grouped_keys[tid], grouped_values[tid]);
    };

    std::cout << "[Testing] Execute HashesAllHSet in " << collection_name << "."
              << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
    shadow_hashes_engines[collection_name]->UpdatePossibleStates();
  }

  void HashesEvenHSetOddHDelete(std::string const& collection_name) {
    ShuffleAllKeysValuesWithinThread();
    auto ModifyEngine = [&](int tid) {
      shadow_hashes_engines[collection_name]->EvenXSetOddXDelete(
          tid, grouped_keys[tid], grouped_values[tid]);
    };

    std::cout << "[Testing] Execute HashesEvenHSetOddHDelete in "
              << collection_name << "." << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
    shadow_hashes_engines[collection_name]->UpdatePossibleStates();
  }

  void SortedSetsAllSSet(std::string const& collection_name) {
    ShuffleAllKeysValuesWithinThread();
    auto ModifyEngine = [&](int tid) {
      shadow_sorted_engines[collection_name]->EvenXSetOddXSet(
          tid, grouped_keys[tid], grouped_values[tid]);
    };

    std::cout << "[Testing] Execute SortedSetsAllSSet in " << collection_name
              << "." << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
    shadow_sorted_engines[collection_name]->UpdatePossibleStates();
  }

  void SortedSetsEvenSSetOddSDelete(std::string const& collection_name) {
    ShuffleAllKeysValuesWithinThread();
    auto ModifyEngine = [&](int tid) {
      shadow_sorted_engines[collection_name]->EvenXSetOddXDelete(
          tid, grouped_keys[tid], grouped_values[tid]);
    };

    std::cout << "[Testing] Execute SortedSetsEvenSSetOddSDelete in "
              << collection_name << "." << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
    shadow_sorted_engines[collection_name]->UpdatePossibleStates();
  }

  void StringAllSet() {
    ShuffleAllKeysValuesWithinThread();
    auto ModifyEngine = [&](int tid) {
      shadow_string_engine->EvenXSetOddXSet(tid, grouped_keys[tid],
                                            grouped_values[tid]);
    };

    std::cout << "[Testing] Execute StringAllSet " << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
    shadow_string_engine->UpdatePossibleStates();
  }

  void StringEvenSetOddDelete() {
    ShuffleAllKeysValuesWithinThread();
    auto ModifyEngine = [&](int tid) {
      shadow_string_engine->EvenXSetOddXDelete(tid, grouped_keys[tid],
                                               grouped_values[tid]);
    };

    std::cout << "[Testing] Execute StringEvenSetOddDelete " << std::endl;
    LaunchNThreads(n_thread, ModifyEngine);
    shadow_string_engine->UpdatePossibleStates();
  }

  void CheckHashesCollection(std::string collection_name) {
    std::cout << "[Testing] Checking Hashes Collection: " << collection_name
              << std::endl;
    shadow_hashes_engines[collection_name]->CheckGetter();
    shadow_hashes_engines[collection_name]->CheckIterator(
        engine->NewUnorderedIterator(collection_name).get(),
        kvdk_testing::IteratingDirection::Forward);
    shadow_hashes_engines[collection_name]->CheckIterator(
        engine->NewUnorderedIterator(collection_name).get(),
        kvdk_testing::IteratingDirection::Backward);
  }

  void CheckSortedSetsCollection(std::string collection_name) {
    std::cout << "[Testing] Checking Sorted Collection: " << collection_name
              << std::endl;
    shadow_sorted_engines[collection_name]->CheckGetter();
    auto iter = engine->NewSortedIterator(collection_name);
    shadow_sorted_engines[collection_name]->CheckIterator(
        iter, kvdk_testing::IteratingDirection::Forward);
    shadow_sorted_engines[collection_name]->CheckIterator(
        iter, kvdk_testing::IteratingDirection::Backward);
    engine->ReleaseSortedIterator(iter);
  }

  void CheckStrings() {
    std::cout << "[Testing] Checking strings." << std::endl;
    shadow_string_engine->CheckGetter();
  }

  void InitializeStrings() {
    shadow_string_engine.reset(
        new ShadowString{engine, kvdk_testing::CollectionNameType{}, n_thread});
  }

  void InitializeHashes(std::string const& collection_name) {
    shadow_hashes_engines[collection_name].reset(
        new ShadowHashes{engine, collection_name, n_thread});
  }

  void InitializeSorted(std::string const& collection_name) {
    shadow_sorted_engines[collection_name].reset(
        new ShadowSorted{engine, collection_name, n_thread});
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
    // 64GB PMem
    sz_pmem_file = (64ULL << 30);
    // Less buckets to increase hash collisions
    n_hash_bucket = (1ULL << 20);
    // Smaller buckets to increase hash collisions
    sz_hash_bucket = (3 + 1) * 16;
    n_blocks_per_segment = (1ULL << 10);
    t_background_work_interval = 1;

    /// Test specific parameters
    n_thread = 32;
    // 1M keys per thread, totaling about 32M(actually less) records
    n_kv_per_thread = (1ULL << 20);
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
  std::string global_collection_name{"HashesCollection"};
  InitializeHashes(global_collection_name);

  std::cout << "[Testing] Modify, check, reboot and check engine for "
            << n_reboot << " times." << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    HashesAllHSet(global_collection_name);
    CheckHashesCollection(global_collection_name);

    RebootDB();
    CheckHashesCollection(global_collection_name);
  }
}

TEST_F(EngineStressTest, HashesHSetAndHDelete) {
  std::string global_collection_name{"HashesCollection"};
  InitializeHashes(global_collection_name);

  std::cout << "[Testing] Modify, check, reboot and check engine for "
            << n_reboot << " times." << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    HashesEvenHSetOddHDelete(global_collection_name);
    CheckHashesCollection(global_collection_name);

    RebootDB();
    CheckHashesCollection(global_collection_name);
  }
}

TEST_F(EngineStressTest, SortedSetsSSetOnly) {
  std::string global_collection_name{"SortedCollection"};
  InitializeSorted(global_collection_name);

  ASSERT_EQ(engine->CreateSortedCollection(global_collection_name),
            kvdk::Status::Ok);

  std::cout << "[Testing] Modify, check, reboot and check engine for "
            << n_reboot << " times." << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    SortedSetsAllSSet(global_collection_name);
    CheckSortedSetsCollection(global_collection_name);

    RebootDB();
    CheckSortedSetsCollection(global_collection_name);
  }
}

TEST_F(EngineStressTest, SortedSetsSSetAndSDelete) {
  std::string global_collection_name{"SortedCollection"};
  InitializeSorted(global_collection_name);

  ASSERT_EQ(engine->CreateSortedCollection(global_collection_name),
            kvdk::Status::Ok);

  std::cout << "[Testing] Modify, check, reboot and check engine for "
            << n_reboot << " times." << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    SortedSetsEvenSSetOddSDelete(global_collection_name);
    CheckSortedSetsCollection(global_collection_name);

    RebootDB();
    CheckSortedSetsCollection(global_collection_name);
  }
}

TEST_F(EngineStressTest, StringSetOnly) {
  InitializeStrings();

  std::cout << "[Testing] Modify, check, reboot and check engine for "
            << n_reboot << " times." << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    StringAllSet();
    CheckStrings();

    RebootDB();
    CheckStrings();
  }
}

TEST_F(EngineStressTest, StringSetAndDelete) {
  InitializeStrings();

  std::cout << "[Testing] Modify, check, reboot and check engine for "
            << n_reboot << " times." << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    StringEvenSetOddDelete();
    CheckStrings();

    RebootDB();
    CheckStrings();
  }
}

class EngineHotspotTest : public EngineTestBase {
 protected:
  virtual void SetUpParameters() override final {
    /// Default configure parameters
    do_populate_when_initialize = false;
    // 64GB PMem
    sz_pmem_file = (64ULL << 30);
    // Less buckets to increase hash collisions
    n_hash_bucket = (1ULL << 20);
    // Small buckets to increase hash collisions
    sz_hash_bucket = (3 + 1) * 16;
    n_blocks_per_segment = (1ULL << 20);
    t_background_work_interval = 1;

    /// Test specific parameters
    // Too many threads will make this test too slow
    n_thread = 4;
    // 1M keys per thread
    n_kv_per_thread = (1ULL << 20);
    // 0-sized key "" is a hotspot, which may reveal many defects
    // These parameters set the range of sizes of keys and values
    sz_key_min = 0;
    sz_key_max = 8;
    sz_value_min = 0;
    sz_value_max = 128;
  }

  size_t n_repeat = 10;
  size_t n_reboot = 3;
};

TEST_F(EngineHotspotTest, HashesMultipleHotspot) {
  std::string global_collection_name{"HashesCollection"};
  InitializeHashes(global_collection_name);

  std::cout << "[Testing] Modify, check, reboot and check engine for "
            << n_reboot << " times." << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    for (size_t i = 0; i < n_repeat; i++) {
      HashesAllHSet(global_collection_name);
      CheckHashesCollection(global_collection_name);
      HashesEvenHSetOddHDelete(global_collection_name);
      CheckHashesCollection(global_collection_name);
    }
    RebootDB();
    CheckHashesCollection(global_collection_name);
  }
}

TEST_F(EngineHotspotTest, SortedSetsMultipleHotspot) {
  std::string global_collection_name{"SortedCollection"};
  InitializeSorted(global_collection_name);

  ASSERT_EQ(engine->CreateSortedCollection(global_collection_name),
            kvdk::Status::Ok);

  std::cout << "[Testing] Modify, check, reboot and check engine for "
            << n_reboot << " times." << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    for (size_t i = 0; i < n_repeat; i++) {
      SortedSetsAllSSet(global_collection_name);
      CheckSortedSetsCollection(global_collection_name);
      SortedSetsEvenSSetOddSDelete(global_collection_name);
      CheckSortedSetsCollection(global_collection_name);
    }
    RebootDB();
    CheckSortedSetsCollection(global_collection_name);
  }
}

TEST_F(EngineHotspotTest, StringMultipleHotspot) {
  InitializeStrings();

  std::cout << "[Testing] Modify, check, reboot and check engine for "
            << n_reboot << " times." << std::endl;
  for (size_t i = 0; i < n_reboot; i++) {
    std::cout << "[Testing] Repeat: " << i + 1 << std::endl;

    for (size_t i = 0; i < n_repeat; i++) {
      StringAllSet();
      CheckStrings();
      StringEvenSetOddDelete();
      CheckStrings();
    }
    RebootDB();
    CheckStrings();
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
