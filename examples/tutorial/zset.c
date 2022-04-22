// Single thread example of redis zset with KVDK c api

#include "kvdk/engine.h"
#include "malloc.h"

// Store score key in sorted collection to index score->member
void encodeScoreKey(int64_t score, const char* member, size_t member_len, char** score_key, size_t* score_key_len){
    *score_key_len = sizeof(int64_t)+member_len;
    *score_key = (char*)malloc(*score_key_len);
    memcpy(*score_key, &score,sizeof(int64_t));
    memcpy(*score_key+sizeof(int64_t), member, member_len);
}

// Store member with string type to index member->score
void encodeStringKey(const char* collection, size_t collection_len, const char* member, size_t member_len, char** string_key, size_t* string_key_len){
    *string_key_len = collection_len + member_len;
    *string_key = (char*)malloc(*string_key_len);
    memcpy(*string_key, collection, collection_len);
    memcpy(*string_key+collection_len, member, member_len);
}

// notice: we set member as a view of score key (which means no ownership)
void decodeScoreKey(const char* score_key, size_t score_key_len, char** member, size_t* member_len, int64_t* score){
    assert(score_key_len>sizeof(int64_t));
    memcpy(score, score_key, sizeof(int64_t));
    *member = score_key + sizeof(int64_t);
    *member_len = score_key_len - sizeof(int64_t);
}

KVDKStatus KVDKZAdd(KVDKEngine* engine, const char* collection,
                    size_t collection_len, int64_t score, const char* member,
                    size_t member_len, KVDKWriteOptions* write_option) {
    char* score_key;
    char* string_key;
    size_t score_key_len;
    size_t string_key_len;
  encodeScoreKey(score, member, member_len, &score_key, &score_key_len);
  encodeStringKey(collection, collection_len, member, member_len, &string_key, &string_key_len);

  KVDKStatus s = KVDKSortedSet(engine, collection, collection_len,
                               score_key, score_key_len, "", 0);
  if (s == NotFound) {
    KVDKSortedCollectionConfigs* s_config = KVDKCreateSortedCollectionConfigs();
    s = KVDKCreateSortedCollection(engine, collection, collection_len,
                                   s_config);
    if (s == Ok) {
      s = KVDKSortedSet(engine, collection, collection_len, score_key,
                        score_key_len, "", 0);
    }
  }

  if (s == Ok) {
    s = KVDKSet(engine, string_key, string_key_len, (char*)&score,
                sizeof(int64_t), write_option);
  }

  free(score_key);
  free(string_key);
  return s;
}

KVDKStatus KVDKZPopMin(KVDKEngine* engine, const char* collection,
                      size_t collection_len, size_t n) {
  KVDKStatus s;
  KVDKIterator* iter =
      KVDKCreateSortedIterator(engine, collection, collection_len, NULL);
  if (iter == NULL) {
    return Ok;
  }

  for (KVDKIterSeekToFirst(iter); KVDKIterValid(iter) && n > 0;
       KVDKIterNext(iter)) {
    char* score_key;
    char* string_key;
    size_t score_key_len;
    size_t string_key_len;
    KVDKIterKey(iter, &score_key, &score_key_len);
    encodeStringKey(collection, collection_len, score_key + sizeof(int64_t) /* maybe use decode function */, score_key_len - sizeof(int64_t), &string_key, &string_key_len);
    s = KVDKSortedDelete(engine, collection, collection_len, score_key, score_key_len);
    if(s == Ok){
        s = KVDKDelete(engine,string_key, string_key_len);
    }
    free(score_key);
    free(string_key);
    if (s != Ok) {
      return s;
    }
  }
  return Ok;
}

KVDKStatus KVDKZPopMax(KVDKEngine* engine, const char* collection,
                      size_t collection_len, size_t n){
                          KVDKStatus s;
  KVDKIterator* iter =
      KVDKCreateSortedIterator(engine, collection, collection_len, NULL);
  if (iter == NULL) {
    return Ok;
  }

  for (KVDKIterSeekToLast(iter); KVDKIterValid(iter) && n > 0;
       KVDKIterPrev(iter)) {
    char* score_key;
    char* string_key;
    char* member;
    size_t score_key_len;
    size_t string_key_len;
    size_t member_len;
    int64_t score;
    KVDKIterKey(iter, &score_key, &score_key_len);
    decodeScoreKey(score_key, score_key_len, &member, &member_len, &score);
    encodeStringKey(collection, collection_len, member, member_len, &string_key, &string_key_len);
    s = KVDKSortedDelete(engine, collection, collection_len, score_key, score_key_len);
    if(s == Ok){
        s = KVDKDelete(engine,string_key, string_key_len);
    }

    if(s == Ok){
        // do anything with poped key, like print
        (void)member;
    }

    free(score_key);
    free(string_key);
    if (s != Ok) {
      return s;
    }
  }
  return Ok;  
}

KVDKStatus KVDKZRange(KVDKEngine* engine, const char* collection,
                      size_t collection_len, int64_t min_score, int64_t max_score){
      KVDKIterator* iter =
      KVDKCreateSortedIterator(engine, collection, collection_len, NULL);
  if (iter == NULL) {
    return Ok;
  }

for (KVDKIterSeek(iter, &min_score, sizeof(int64_t)); KVDKIterValid(iter);
    KVDKIterNext(iter)) {
    char* score_key;
    size_t score_key_len;
    KVDKIterKey(iter, &score_key, &score_key_len);
    char* member;
    size_t member_len;
    int64_t score;
    decodeScoreKey(score_key, score_key_len, &member, &member_len, &score);
    if(score < max_score){
        // do any thing with in-range key, like print;
        (void)member;
        free(score_key);
    } else {
        free(score_key);
        break;
    }
  }

  return Ok;
}

int main() {

}