/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <cassert>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "c_api_test.hpp"

struct FetchAddArgs {
  size_t old_val;
  size_t new_val;
  size_t n;
};

int FetchAdd(char const* old_val_data, size_t old_val_len, char** new_val_data,
             size_t* new_val_len, void* args) {
  FetchAddArgs* fa_args = (FetchAddArgs*)args;
  if (old_val_data != nullptr) {
    assert(sizeof(size_t) == old_val_len);
    fa_args->old_val = *(size_t*)old_val_data;
  } else {
    fa_args->old_val = 0;
  }
  fa_args->new_val = fa_args->old_val + fa_args->n;
  *new_val_len = sizeof(size_t);
  *new_val_data = (char*)&fa_args->new_val;
  return KVDK_MODIFY_WRITE;
}

TEST_F(EngineCAPITestBase, Hash) {
  size_t num_threads = 16;
  size_t count = 1000;

  std::string key{"Hash"};
  ASSERT_EQ(KVDKHashCreate(engine, key.data(), key.size()), KVDKStatus::Ok);
  using umap = std::unordered_map<std::string, std::string>;
  std::vector<umap> local_copies(num_threads);
  std::mutex mu;

  auto HPut = [&](size_t tid) {
    umap& local_copy = local_copies[tid];
    for (size_t j = 0; j < count; j++) {
      std::string field{std::to_string(tid) + "_" + GetRandomString(10)};
      std::string value{GetRandomString(120)};
      ASSERT_EQ(KVDKHashPut(engine, key.data(), key.size(), field.data(),
                            field.size(), value.data(), value.size()),
                KVDKStatus::Ok);
      local_copy[field] = value;
    }
  };

  auto HGet = [&](size_t tid) {
    umap const& local_copy = local_copies[tid];
    for (auto const& kv : local_copy) {
      char* resp_data;
      size_t resp_len;
      ASSERT_EQ(KVDKHashGet(engine, key.data(), key.size(), kv.first.data(),
                            kv.first.size(), &resp_data, &resp_len),
                KVDKStatus::Ok);
      ASSERT_EQ(std::string(resp_data, resp_len), kv.second);
      free(resp_data);
    }
  };

  auto HDelete = [&](size_t tid) {
    umap& local_copy = local_copies[tid];
    std::string sink;
    for (size_t i = 0; i < count / 2; i++) {
      auto iter = local_copy.begin();
      char* resp_data;
      size_t resp_len;
      ASSERT_EQ(KVDKHashDelete(engine, key.data(), key.size(),
                               iter->first.data(), iter->first.size()),
                KVDKStatus::Ok);
      ASSERT_EQ(KVDKHashGet(engine, key.data(), key.size(), iter->first.data(),
                            iter->first.size(), &resp_data, &resp_len),
                KVDKStatus::NotFound);
      local_copy.erase(iter);
    }
  };

  auto HashLength = [&](size_t) {
    size_t len = 0;
    ASSERT_EQ(KVDKHashLength(engine, key.data(), key.size(), &len),
              KVDKStatus::Ok);
    size_t cnt = 0;
    for (size_t tid = 0; tid < num_threads; tid++) {
      cnt += local_copies[tid].size();
    }
    ASSERT_EQ(len, cnt);
  };

  auto HashIterate = [&](size_t tid) {
    umap combined;
    for (size_t tid = 0; tid < num_threads; tid++) {
      umap const& local_copy = local_copies[tid];
      for (auto const& kv : local_copy) {
        combined[kv.first] = kv.second;
      }
    }

    KVDKHashIterator* iter =
        KVDKHashIteratorCreate(engine, key.data(), key.size());

    ASSERT_NE(iter, nullptr);
    size_t cnt = 0;
    for (KVDKHashIteratorSeekToFirst(iter); KVDKHashIteratorIsValid(iter);
         KVDKHashIteratorNext(iter)) {
      ++cnt;
      char* field_data;
      size_t field_len;
      char* value_data;
      size_t value_len;
      KVDKHashIteratorGetKey(iter, &field_data, &field_len);
      KVDKHashIteratorGetValue(iter, &value_data, &value_len);
      ASSERT_EQ(combined[std::string(field_data, field_len)],
                std::string(value_data, value_len));
      free(field_data);
      free(value_data);
    }
    ASSERT_EQ(cnt, combined.size());

    cnt = 0;
    for (KVDKHashIteratorSeekToLast(iter); KVDKHashIteratorIsValid(iter);
         KVDKHashIteratorPrev(iter)) {
      ++cnt;
      char* field_data;
      size_t field_len;
      char* value_data;
      size_t value_len;
      KVDKHashIteratorGetKey(iter, &field_data, &field_len);
      KVDKHashIteratorGetValue(iter, &value_data, &value_len);
      ASSERT_EQ(combined[std::string(field_data, field_len)],
                std::string(value_data, value_len));
      free(field_data);
      free(value_data);
    }
    ASSERT_EQ(cnt, combined.size());

    std::string re_str1{".*"};
    std::string re_str2{std::to_string(tid) + "_.*"};
    KVDKRegex* re1 = KVDKRegexCreate(re_str1.data(), re_str1.size());
    KVDKRegex* re2 = KVDKRegexCreate(re_str2.data(), re_str2.size());
    size_t match_cnt1 = 0;
    size_t match_cnt2 = 0;
    for (KVDKHashIteratorSeekToFirst(iter); KVDKHashIteratorIsValid(iter);
         KVDKHashIteratorNext(iter)) {
      match_cnt1 += KVDKHashIteratorMatchKey(iter, re1);
      match_cnt2 += KVDKHashIteratorMatchKey(iter, re2);
    }
    ASSERT_EQ(match_cnt1, combined.size());
    ASSERT_EQ(match_cnt2, local_copies[tid].size());
    KVDKRegexDestroy(re2);
    KVDKRegexDestroy(re1);

    KVDKHashIteratorDestroy(iter);
  };

  std::string counter{"counter"};
  auto HashModify = [&](size_t) {
    FetchAddArgs args;
    args.n = 1;
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(KVDKHashModify(engine, key.data(), key.size(), counter.data(),
                               counter.size(), FetchAdd, &args, NULL),
                KVDKStatus::Ok);
    }
  };

  for (size_t i = 0; i < 3; i++) {
    RebootDB();
    LaunchNThreads(num_threads, HPut);
    LaunchNThreads(num_threads, HGet);
    LaunchNThreads(num_threads, HDelete);
    LaunchNThreads(num_threads, HashIterate);
    LaunchNThreads(num_threads, HashLength);
    LaunchNThreads(num_threads, HPut);
    LaunchNThreads(num_threads, HGet);
    LaunchNThreads(num_threads, HDelete);
    LaunchNThreads(num_threads, HashIterate);
    LaunchNThreads(num_threads, HashLength);
  }
  LaunchNThreads(num_threads, HashModify);
  char* resp_data;
  size_t resp_len;
  ASSERT_EQ(KVDKHashGet(engine, key.data(), key.size(), counter.data(),
                        counter.size(), &resp_data, &resp_len),
            KVDKStatus::Ok);
  ASSERT_EQ(resp_len, sizeof(size_t));
  ASSERT_EQ(*(size_t*)resp_data, num_threads * count);
  free(resp_data);
}
