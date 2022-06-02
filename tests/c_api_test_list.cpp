/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <list>
#include <string>
#include <vector>

#include "c_api_test.hpp"

void ConcatStrings(char const* elem_data, size_t elem_len, void* arg) {
  std::string* buffer = static_cast<std::string*>(arg);
  buffer->append(elem_data, elem_len);
  buffer->append(1, '\n');
}

TEST_F(EngineCAPITestBase, List) {
  size_t num_threads = 16;
  size_t count = 1000;

  std::vector<std::vector<std::string>> elems_vec(num_threads);
  std::vector<std::string> key_vec(num_threads);
  for (size_t i = 0; i < num_threads; i++) {
    key_vec[i] = "List_" + std::to_string(i);
    ASSERT_EQ(KVDKListCreate(engine, key_vec[i].data(), key_vec[i].size()),
              KVDKStatus::Ok);
    for (size_t j = 0; j < count; j++) {
      elems_vec[i].push_back(std::to_string(i) + "_" + std::to_string(j));
    }
  }

  std::vector<std::list<std::string>> list_copy_vec(num_threads);

  auto ListIteratorGetValue = [&](KVDKListIterator* iter) {
    char* value;
    size_t sz;
    // Read the value at the ListIterator
    KVDKListIteratorGetValue(iter, &value, &sz);
    std::string ret{value, sz};
    free(value);
    return ret;
  };

  auto ListPopFront = [&](std::string const& key) {
    char* value;
    size_t sz;
    KVDKStatus s =
        KVDKListPopFront(engine, key.data(), key.size(), &value, &sz);
    std::string ret{value, sz};
    free(value);
    return std::make_pair(s, ret);
  };

  auto ListPopBack = [&](std::string const& key) {
    char* value;
    size_t sz;
    KVDKStatus s = KVDKListPopBack(engine, key.data(), key.size(), &value, &sz);
    std::string ret{value, sz};
    free(value);
    return std::make_pair(s, ret);
  };

  auto LPush = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto const& elems = elems_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    size_t sz;
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(KVDKListPushFront(engine, key.data(), key.size(),
                                  elems[j].data(), elems[j].size()),
                KVDKStatus::Ok);
      list_copy.push_front(elems[j]);
      ASSERT_EQ(KVDKListLength(engine, key.data(), key.size(), &sz),
                KVDKStatus::Ok);
      ASSERT_EQ(sz, list_copy.size());
    }
  };

  auto RPush = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto const& elems = elems_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    size_t sz;
    for (size_t j = 0; j < count; j++) {
      ASSERT_EQ(KVDKListPushBack(engine, key.data(), key.size(),
                                 elems[j].data(), elems[j].size()),
                KVDKStatus::Ok);
      list_copy.push_back(elems[j]);
      ASSERT_EQ(KVDKListLength(engine, key.data(), key.size(), &sz),
                KVDKStatus::Ok);
      ASSERT_EQ(sz, list_copy.size());
    }
  };

  auto LPop = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    size_t len;
    for (size_t j = 0; j < count; j++) {
      if (list_copy.empty()) {
        break;
      }
      ASSERT_EQ(std::make_pair(KVDKStatus::Ok, list_copy.front()),
                ListPopFront(key));
      list_copy.pop_front();
      ASSERT_TRUE((KVDKListLength(engine, key.data(), key.size(), &len) ==
                       KVDKStatus::NotFound &&
                   list_copy.empty()) ||
                  len == list_copy.size());
    }
  };

  auto RPop = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    size_t len;
    for (size_t j = 0; j < count; j++) {
      if (list_copy.empty()) {
        break;
      }
      ASSERT_EQ(std::make_pair(KVDKStatus::Ok, list_copy.back()),
                ListPopBack(key));
      list_copy.pop_back();
      ASSERT_TRUE((KVDKListLength(engine, key.data(), key.size(), &len) ==
                       KVDKStatus::NotFound &&
                   list_copy.empty()) ||
                  len == list_copy.size());
    }
  };

  auto ListIterate = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];

    KVDKListIterator* iter =
        KVDKListIteratorCreate(engine, key.data(), key.size(), NULL);
    if (iter != nullptr) {
      KVDKListIteratorSeekPos(iter, 0);
      for (auto iter2 = list_copy.begin(); iter2 != list_copy.end(); iter2++) {
        ASSERT_TRUE(KVDKListIteratorIsValid(iter));
        ASSERT_EQ(ListIteratorGetValue(iter), *iter2);
        KVDKListIteratorNext(iter);
      }

      KVDKListIteratorSeekPos(iter, -1);
      for (auto iter2 = list_copy.rbegin(); iter2 != list_copy.rend();
           iter2++) {
        ASSERT_TRUE(KVDKListIteratorIsValid(iter));
        ASSERT_EQ(ListIteratorGetValue(iter), *iter2);
        KVDKListIteratorPrev(iter);
      }
    }
    KVDKListIteratorDestroy(iter);
  };

  auto ConvertParams = [](std::vector<std::string> const& elems) {
    std::vector<char const*> elems_data;
    std::vector<size_t> elems_len;
    for (auto const& elem : elems) {
      elems_data.push_back(elem.data());
      elems_len.push_back(elem.size());
    }
    return std::make_pair(elems_data, elems_len);
  };

  auto ListInsertPutRemove = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    size_t len;
    std::string elem;
    size_t const insert_pos = 5;

    ASSERT_EQ(KVDKListLength(engine, key.data(), key.size(), &len),
              KVDKStatus::Ok);
    ASSERT_GT(len, insert_pos);

    KVDKListIterator* iter =
        KVDKListIteratorCreate(engine, key.data(), key.size(), NULL);
    ASSERT_NE(iter, nullptr);

    KVDKListIteratorSeekPos(iter, insert_pos);
    auto iter2 = std::next(list_copy.begin(), insert_pos);
    ASSERT_EQ(ListIteratorGetValue(iter), *iter2);

    elem = *iter2 + "_before";
    ASSERT_EQ(KVDKListInsertBefore(engine, iter, elem.data(), elem.size()),
              KVDKStatus::Ok);
    iter2 = list_copy.insert(iter2, elem);
    ASSERT_EQ(ListIteratorGetValue(iter), *iter2);

    KVDKListIteratorPrev(iter);
    KVDKListIteratorPrev(iter);
    ----iter2;
    ASSERT_EQ(ListIteratorGetValue(iter), *iter2);
    elem = *iter2 + "_new";
    ASSERT_EQ(KVDKListReplace(engine, iter, elem.data(), elem.size()),
              KVDKStatus::Ok);
    *iter2 = elem;
    ASSERT_EQ(ListIteratorGetValue(iter), *iter2);

    KVDKListIteratorPrev(iter);
    KVDKListIteratorPrev(iter);
    ----iter2;
    ASSERT_EQ(ListIteratorGetValue(iter), *iter2);
    ASSERT_EQ(KVDKListErase(engine, iter), KVDKStatus::Ok);
    iter2 = list_copy.erase(iter2);
    ASSERT_EQ(ListIteratorGetValue(iter), *iter2);

    KVDKListIteratorDestroy(iter);
  };

  auto LBatchPush = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto const& elems = elems_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    for (size_t j = 0; j < count; j++) {
      list_copy.push_front(elems[j]);
    }
    auto param = ConvertParams(elems);
    ASSERT_EQ(KVDKListBatchPushFront(engine, key.data(), key.size(),
                                     param.first.data(), param.second.data(),
                                     elems.size()),
              KVDKStatus::Ok);
    size_t sz;
    ASSERT_EQ(KVDKListLength(engine, key.data(), key.size(), &sz),
              KVDKStatus::Ok);
    ASSERT_EQ(sz, list_copy.size());
  };

  auto RBatchPush = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto const& elems = elems_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    for (size_t j = 0; j < count; j++) {
      list_copy.push_back(elems[j]);
    }
    auto param = ConvertParams(elems);
    ASSERT_EQ(KVDKListBatchPushBack(engine, key.data(), key.size(),
                                    param.first.data(), param.second.data(),
                                    elems.size()),
              KVDKStatus::Ok);
    size_t sz;
    ASSERT_EQ(KVDKListLength(engine, key.data(), key.size(), &sz),
              KVDKStatus::Ok);
    ASSERT_EQ(sz, list_copy.size());
  };

  auto LBatchPop = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    std::string buffer1;
    std::string buffer2;
    ASSERT_EQ(KVDKListBatchPopFront(engine, key.data(), key.size(), count,
                                    ConcatStrings, &buffer1),
              KVDKStatus::Ok);
    for (size_t j = 0; j < count && !list_copy.empty(); j++) {
      ConcatStrings(list_copy.front().data(), list_copy.front().size(),
                    &buffer2);
      list_copy.pop_front();
    }
    ASSERT_EQ(buffer1, buffer2);
    size_t sz;
    ASSERT_EQ(KVDKListLength(engine, key.data(), key.size(), &sz),
              KVDKStatus::Ok);
    ASSERT_EQ(sz, list_copy.size());
  };

  auto RBatchPop = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    std::string buffer1;
    std::string buffer2;
    ASSERT_EQ(KVDKListBatchPopBack(engine, key.data(), key.size(), count,
                                   ConcatStrings, &buffer1),
              KVDKStatus::Ok);
    for (size_t j = 0; j < count && !list_copy.empty(); j++) {
      ConcatStrings(list_copy.back().data(), list_copy.back().size(), &buffer2);
      list_copy.pop_back();
    }
    ASSERT_EQ(buffer1, buffer2);
    size_t sz;
    ASSERT_EQ(KVDKListLength(engine, key.data(), key.size(), &sz),
              KVDKStatus::Ok);
    ASSERT_EQ(sz, list_copy.size());
  };

  auto RPushLPop = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];

    ASSERT_FALSE(list_copy.empty());

    auto elem_copy = list_copy.front();
    list_copy.push_back(elem_copy);
    list_copy.pop_front();

    char* elem_data;
    size_t elem_len;
    ASSERT_EQ(KVDKListMove(engine, key.data(), key.size(), 0, key.data(),
                           key.size(), -1, &elem_data, &elem_len),
              KVDKStatus::Ok);
    ASSERT_EQ(std::string(elem_data, elem_len), elem_copy);
    free(elem_data);

    size_t sz;
    ASSERT_EQ(KVDKListLength(engine, key.data(), key.size(), &sz),
              KVDKStatus::Ok);
    ASSERT_EQ(sz, list_copy.size());
  };

  for (size_t i = 0; i < 3; i++) {
    LaunchNThreads(num_threads, LPop);
    LaunchNThreads(num_threads, RPop);
    LaunchNThreads(num_threads, LPush);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, RPush);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, LBatchPush);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, RBatchPush);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, LBatchPop);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, RBatchPop);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, LPop);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, RPop);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, RPush);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, LPush);
    LaunchNThreads(num_threads, ListIterate);
    for (size_t j = 0; j < 100; j++) {
      LaunchNThreads(num_threads, ListInsertPutRemove);
      LaunchNThreads(num_threads, ListIterate);
      LaunchNThreads(num_threads, RPushLPop);
      LaunchNThreads(num_threads, ListIterate);
    }
    RebootDB();
  }
}
