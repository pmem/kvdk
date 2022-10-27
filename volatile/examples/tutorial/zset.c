/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

// This is a example of single-thread redis zset implementation with kvdk c api

#include "assert.h"
#include "kvdk/volatile/engine.h"
#include "malloc.h"
#include "string.h"

#define NUM_MEMBERS 7

int StrCompare(const char* a, size_t a_len, const char* b, size_t b_len) {
  int n = (a_len < b_len) ? a_len : b_len;
  int cmp = memcmp(a, b, n);
  if (cmp == 0) {
    if (a_len < b_len)
      cmp = -1;
    else if (a_len > b_len)
      cmp = 1;
  }
  return cmp;
}

const char* comp_name = "score_comp";
int ScoreCmp(const char* score_key_a, size_t a_len, const char* score_key_b,
             size_t b_len) {
  assert(a_len >= sizeof(int64_t));
  assert(b_len >= sizeof(int64_t));
  int cmp = (*(int64_t*)score_key_a) - (*(int64_t*)score_key_b);
  return cmp == 0 ? StrCompare(
                        score_key_a + sizeof(int64_t), a_len - sizeof(int64_t),
                        score_key_b + sizeof(int64_t), b_len - sizeof(int64_t))
                  : cmp;
}

// Store score key in sorted collection to index score->member
void EncodeScoreKey(int64_t score, const char* member, size_t member_len,
                    char** score_key, size_t* score_key_len) {
  *score_key_len = sizeof(int64_t) + member_len;
  *score_key = (char*)malloc(*score_key_len);
  memcpy(*score_key, &score, sizeof(int64_t));
  memcpy(*score_key + sizeof(int64_t), member, member_len);
}

// Store member with string type to index member->score
void EncodeStringKey(const char* collection, size_t collection_len,
                     const char* member, size_t member_len, char** string_key,
                     size_t* string_key_len) {
  *string_key_len = collection_len + member_len;
  *string_key = (char*)malloc(*string_key_len);
  memcpy(*string_key, collection, collection_len);
  memcpy(*string_key + collection_len, member, member_len);
}

// notice: we set member as a view of score key (which means no ownership)
void DecodeScoreKey(char* score_key, size_t score_key_len, char** member,
                    size_t* member_len, int64_t* score) {
  assert(score_key_len > sizeof(int64_t));
  memcpy(score, score_key, sizeof(int64_t));
  *member = score_key + sizeof(int64_t);
  *member_len = score_key_len - sizeof(int64_t);
}

void PrintMemberScore(size_t index, char* member, size_t member_len,
                      int64_t score) {
  char member_c_str[member_len + 1];
  memcpy(member_c_str, member, member_len);
  member_c_str[member_len] = '\0';
  printf("(%lu)\"%s\"\n", 2 * index + 1, member_c_str);
  printf("(%lu)\"%ld\"\n", 2 * index + 2, score);
}

KVDKStatus KVDKZAdd(KVDKEngine* engine, const char* collection,
                    size_t collection_len, int64_t score, const char* member,
                    size_t member_len, KVDKWriteOptions* write_option) {
  printf("ZADD %s %ld %s\n", collection, score, member);

  char* score_key;
  char* string_key;
  size_t score_key_len;
  size_t string_key_len;
  EncodeScoreKey(score, member, member_len, &score_key, &score_key_len);
  EncodeStringKey(collection, collection_len, member, member_len, &string_key,
                  &string_key_len);

  KVDKStatus s = KVDKSortedPut(engine, collection, collection_len, score_key,
                               score_key_len, "", 0);
  if (s == NotFound) {
    KVDKSortedCollectionConfigs* s_config = KVDKCreateSortedCollectionConfigs();
    KVDKSetSortedCollectionConfigs(
        s_config, comp_name, strlen(comp_name),
        0 /*we do not need hash index for score part*/);
    s = KVDKSortedCreate(engine, collection, collection_len, s_config);
    if (s == Ok) {
      s = KVDKSortedPut(engine, collection, collection_len, score_key,
                        score_key_len, "", 0);
    }
    KVDKDestroySortedCollectionConfigs(s_config);
  }

  if (s == Ok) {
    s = KVDKPut(engine, string_key, string_key_len, (char*)&score,
                sizeof(int64_t), write_option);
  }

  free(score_key);
  free(string_key);

  return s;
}

KVDKStatus KVDKZPopMin(KVDKEngine* engine, const char* collection,
                       size_t collection_len, size_t n) {
  printf("ZPOPMIN %s %lu\n", collection, n);

  KVDKStatus s;
  KVDKSortedIterator* iter =
      KVDKSortedIteratorCreate(engine, collection, collection_len, NULL, NULL);
  if (iter == NULL) {
    return Ok;
  }
  size_t cnt = 0;

  for (KVDKSortedIteratorSeekToFirst(iter);
       KVDKSortedIteratorValid(iter) && n > 0; KVDKSortedIteratorNext(iter)) {
    char* score_key;
    char* string_key;
    char* member;
    size_t score_key_len;
    size_t string_key_len;
    size_t member_len;
    int64_t score;
    KVDKSortedIteratorKey(iter, &score_key, &score_key_len);
    DecodeScoreKey(score_key, score_key_len, &member, &member_len, &score);
    EncodeStringKey(collection, collection_len, member, member_len, &string_key,
                    &string_key_len);
    s = KVDKSortedDelete(engine, collection, collection_len, score_key,
                         score_key_len);
    if (s == Ok) {
      s = KVDKDelete(engine, string_key, string_key_len);
    }

    if (s == Ok) {
      // do anything with poped key, like print
      PrintMemberScore(cnt++, member, member_len, score);
      (void)member;
    }

    free(score_key);
    free(string_key);
    if (s != Ok || cnt == n) {
      break;
    }
  }
  KVDKSortedIteratorDestroy(engine, iter);
  return s;
}

KVDKStatus KVDKZPopMax(KVDKEngine* engine, const char* collection,
                       size_t collection_len, size_t n) {
  printf("ZPOPMAX %s %lu\n", collection, n);

  KVDKStatus s;
  KVDKSortedIterator* iter =
      KVDKSortedIteratorCreate(engine, collection, collection_len, NULL, NULL);
  if (iter == NULL) {
    return Ok;
  }
  size_t cnt = 0;

  for (KVDKSortedIteratorSeekToLast(iter);
       KVDKSortedIteratorValid(iter) && n > 0; KVDKSortedIteratorPrev(iter)) {
    char* score_key;
    char* string_key;
    char* member;
    size_t score_key_len;
    size_t string_key_len;
    size_t member_len;
    int64_t score;
    KVDKSortedIteratorKey(iter, &score_key, &score_key_len);
    DecodeScoreKey(score_key, score_key_len, &member, &member_len, &score);
    EncodeStringKey(collection, collection_len, member, member_len, &string_key,
                    &string_key_len);
    s = KVDKSortedDelete(engine, collection, collection_len, score_key,
                         score_key_len);
    if (s == Ok) {
      s = KVDKDelete(engine, string_key, string_key_len);
    }

    if (s == Ok) {
      // do anything with poped key, like print
      PrintMemberScore(cnt++, member, member_len, score);
      (void)member;
    }

    free(score_key);
    free(string_key);
    if (s != Ok || cnt == n) {
      break;
    }
  }
  KVDKSortedIteratorDestroy(engine, iter);
  return s;
}

KVDKStatus KVDKZRange(KVDKEngine* engine, const char* collection,
                      size_t collection_len, int64_t min_score,
                      int64_t max_score) {
  printf("ZRANGE %s %ld %ld\n", collection, min_score, max_score);

  KVDKSortedIterator* iter =
      KVDKSortedIteratorCreate(engine, collection, collection_len, NULL, NULL);
  if (iter == NULL) {
    return Ok;
  }
  size_t cnt = 0;

  for (KVDKSortedIteratorSeek(iter, (char*)&min_score, sizeof(int64_t));
       KVDKSortedIteratorValid(iter); KVDKSortedIteratorNext(iter)) {
    char* score_key;
    size_t score_key_len;
    KVDKSortedIteratorKey(iter, &score_key, &score_key_len);
    char* member;
    size_t member_len;
    int64_t score;
    DecodeScoreKey(score_key, score_key_len, &member, &member_len, &score);
    if (score <= max_score) {
      // do any thing with in-range key, like print;
      PrintMemberScore(cnt++, member, member_len, score);
      free(score_key);
    } else {
      free(score_key);
      break;
    }
  }
  KVDKSortedIteratorDestroy(engine, iter);
  return Ok;
}

void ZSetTest(KVDKEngine* engine) {
  const char* zset_name = "zset";
  const size_t zset_name_len = strlen(zset_name);
  char* members[NUM_MEMBERS] = {"one",  "two",          "three", "four",
                                "five", "another_five", "six"};
  int64_t scores[NUM_MEMBERS] = {1, 2, 3, 4, 5, 5, 6};
  KVDKWriteOptions* write_option = KVDKCreateWriteOptions();
  for (size_t i = 0; i < NUM_MEMBERS; i++) {
    KVDKStatus s = KVDKZAdd(engine, zset_name, zset_name_len, scores[i],
                            members[i], strlen(members[i]), write_option);
    assert(s == Ok);
  }

  assert(KVDKZRange(engine, zset_name, zset_name_len, 1, 6) == Ok);
  assert(KVDKZPopMax(engine, zset_name, zset_name_len, 2) == Ok);
  assert(KVDKZPopMin(engine, zset_name, zset_name_len, 2) == Ok);
  assert(KVDKZRange(engine, zset_name, zset_name_len, 1, 6) == Ok);
  KVDKDestroyWriteOptions(write_option);
}

int main() {
  KVDKConfigs* kvdk_configs = KVDKCreateConfigs();
  KVDKSetConfigs(kvdk_configs, 48, 1ull << 10, 1 << 4);
  const char* engine_path = "/mnt/pmem0/kvdk_zset_example";
  KVDKEngine* kvdk_engine;
  KVDKStatus s = KVDKOpen(engine_path, kvdk_configs, stdout, &kvdk_engine);
  assert(s == Ok);
  KVDKRegisterCompFunc(kvdk_engine, comp_name, strlen(comp_name), ScoreCmp);
  ZSetTest(kvdk_engine);

  KVDKDestroyConfigs(kvdk_configs);
  KVDKCloseEngine(kvdk_engine);
  return 0;
}