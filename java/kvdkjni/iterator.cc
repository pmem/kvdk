/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <assert.h>

#include "include/io_pmem_kvdk_Iterator.h"
#include "kvdkjni/kvdkjni.h"

/*
 * Class:     io_pmem_kvdk_Iterator
 * Method:    closeInternal
 * Signature: (J)V
 */
void Java_io_pmem_kvdk_Iterator_closeInternal(JNIEnv*, jobject,
                                              jlong iterator_handle,
                                              jlong engine_handle) {
  auto* iterator = reinterpret_cast<KVDK_NAMESPACE::Iterator*>(iterator_handle);
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(engine_handle);
  assert(iterator != nullptr);
  assert(engine != nullptr);

  engine->SortedIteratorRelease(iterator);
}

/*
 * Class:     io_pmem_kvdk_Iterator
 * Method:    seek
 * Signature: (J[BII)V
 */
void Java_io_pmem_kvdk_Iterator_seek(JNIEnv* env, jobject, jlong handle,
                                     jbyteArray key, jint key_off,
                                     jint key_len) {
  auto* iterator = reinterpret_cast<KVDK_NAMESPACE::Iterator*>(handle);

  jbyte* key_bytes = new jbyte[key_len];
  env->GetByteArrayRegion(key, key_off, key_len, key_bytes);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key_bytes;
    return;
  }

  iterator->Seek(std::string(reinterpret_cast<char*>(key_bytes), key_len));

  delete[] key_bytes;
}

/*
 * Class:     io_pmem_kvdk_Iterator
 * Method:    seekToFirst
 * Signature: (J)V
 */
void Java_io_pmem_kvdk_Iterator_seekToFirst(JNIEnv* env, jobject,
                                            jlong handle) {
  auto* iterator = reinterpret_cast<KVDK_NAMESPACE::Iterator*>(handle);
  iterator->SeekToFirst();
}

/*
 * Class:     io_pmem_kvdk_Iterator
 * Method:    seekToLast
 * Signature: (J)V
 */
void Java_io_pmem_kvdk_Iterator_seekToLast(JNIEnv* env, jobject, jlong handle) {
  auto* iterator = reinterpret_cast<KVDK_NAMESPACE::Iterator*>(handle);
  iterator->SeekToLast();
}

/*
 * Class:     io_pmem_kvdk_Iterator
 * Method:    isValid
 * Signature: (J)Z
 */
jboolean Java_io_pmem_kvdk_Iterator_isValid(JNIEnv* env, jobject,
                                            jlong handle) {
  auto* iterator = reinterpret_cast<KVDK_NAMESPACE::Iterator*>(handle);
  return iterator->Valid();
}

/*
 * Class:     io_pmem_kvdk_Iterator
 * Method:    next
 * Signature: (J)V
 */
void Java_io_pmem_kvdk_Iterator_next(JNIEnv* env, jobject, jlong handle) {
  auto* iterator = reinterpret_cast<KVDK_NAMESPACE::Iterator*>(handle);
  iterator->Next();
}

/*
 * Class:     io_pmem_kvdk_Iterator
 * Method:    prev
 * Signature: (J)V
 */
void Java_io_pmem_kvdk_Iterator_prev(JNIEnv* env, jobject, jlong handle) {
  auto* iterator = reinterpret_cast<KVDK_NAMESPACE::Iterator*>(handle);
  iterator->Prev();
}

/*
 * Class:     io_pmem_kvdk_Iterator
 * Method:    key
 * Signature: (J)[B
 */
jbyteArray Java_io_pmem_kvdk_Iterator_key(JNIEnv* env, jobject, jlong handle) {
  auto* iterator = reinterpret_cast<KVDK_NAMESPACE::Iterator*>(handle);

  std::string key = iterator->Key();
  jbyteArray ret = KVDK_NAMESPACE::JniUtil::createJavaByteArray(
      env, key.c_str(), key.size());
  return ret;
}

/*
 * Class:     io_pmem_kvdk_Iterator
 * Method:    value
 * Signature: (J)[B
 */
jbyteArray Java_io_pmem_kvdk_Iterator_value(JNIEnv* env, jobject,
                                            jlong handle) {
  auto* iterator = reinterpret_cast<KVDK_NAMESPACE::Iterator*>(handle);

  std::string value = iterator->Value();
  jbyteArray ret = KVDK_NAMESPACE::JniUtil::createJavaByteArray(
      env, value.c_str(), value.size());
  return ret;
}
