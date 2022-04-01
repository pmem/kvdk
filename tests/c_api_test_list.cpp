#include <list>
#include <string>
#include <vector>

#include "c_api_test.hpp"

TEST_F(EngineCAPITestBase, List) {
  size_t num_threads = 16;
  size_t count = 1000;

  std::vector<std::vector<std::string>> elems_vec(num_threads);
  std::vector<std::string> key_vec(num_threads);
  for (size_t i = 0; i < num_threads; i++) {
    key_vec[i] = "List_" + std::to_string(i);
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
        KVDKListIteratorCreate(engine, key.data(), key.size());
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

  auto ListInsertSetRemove = [&](size_t tid) {
    auto const& key = key_vec[tid];
    auto& list_copy = list_copy_vec[tid];
    size_t len;
    std::string elem;
    size_t const insert_pos = 5;

    ASSERT_EQ(KVDKListLength(engine, key.data(), key.size(), &len),
              KVDKStatus::Ok);
    ASSERT_GT(len, insert_pos);

    KVDKListIterator* iter =
        KVDKListIteratorCreate(engine, key.data(), key.size());
    ASSERT_NE(iter, nullptr);

    KVDKListIteratorSeekPos(iter, insert_pos);
    auto iter2 = std::next(list_copy.begin(), insert_pos);
    ASSERT_EQ(ListIteratorGetValue(iter), *iter2);

    elem = *iter2 + "_before";
    ASSERT_EQ(KVDKListInsert(engine, iter, elem.data(), elem.size()),
              KVDKStatus::Ok);
    iter2 = list_copy.insert(iter2, elem);
    ASSERT_EQ(ListIteratorGetValue(iter), *iter2);

    KVDKListIteratorPrev(iter);
    KVDKListIteratorPrev(iter);
    ----iter2;
    ASSERT_EQ(ListIteratorGetValue(iter), *iter2);
    elem = *iter2 + "_new";
    ASSERT_EQ(KVDKListSet(engine, iter, elem.data(), elem.size()),
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

  for (size_t i = 0; i < 3; i++) {
    LaunchNThreads(num_threads, LPush);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, RPush);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, LPop);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, RPop);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, RPush);
    LaunchNThreads(num_threads, ListIterate);
    LaunchNThreads(num_threads, LPush);
    for (size_t j = 0; j < 100; j++) {
      LaunchNThreads(num_threads, ListInsertSetRemove);
      LaunchNThreads(num_threads, ListIterate);
    }
    RebootDB();
  }
}
