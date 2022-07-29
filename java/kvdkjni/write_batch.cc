/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <assert.h>

#include "include/io_pmem_kvdk_WriteBatch.h"
#include "kvdkjni/kvdkjni.h"

/*
 * Class:     io_pmem_kvdk_WriteBatch
 * Method:    closeInternal
 * Signature: (J)V
 */
void Java_io_pmem_kvdk_WriteBatch_closeInternal(JNIEnv*, jobject,
                                                jlong batch_handle) {
  auto* batch = reinterpret_cast<KVDK_NAMESPACE::WriteBatch*>(batch_handle);
  assert(batch != nullptr);
  delete batch;
}

/*
 * Class:     io_pmem_kvdk_WriteBatch
 * Method:    stringPut
 * Signature: (J[BII[BII)V
 */
void Java_io_pmem_kvdk_WriteBatch_stringPut(JNIEnv* env, jobject,
                                            jlong batch_handle, jbyteArray key,
                                            jint key_off, jint key_len,
                                            jbyteArray value, jint value_off,
                                            jint value_len) {
  auto* batch = reinterpret_cast<KVDK_NAMESPACE::WriteBatch*>(batch_handle);

  jbyte* key_bytes = new jbyte[key_len];
  env->GetByteArrayRegion(key, key_off, key_len, key_bytes);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key_bytes;
    return;
  }

  jbyte* value_bytes = new jbyte[value_len];
  env->GetByteArrayRegion(value, value_off, value_len, value_bytes);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key_bytes;
    delete[] value_bytes;
    return;
  }

  batch->StringPut(
      std::string(reinterpret_cast<char*>(key_bytes), key_len),
      std::string(reinterpret_cast<char*>(value_bytes), value_len));

  delete[] key_bytes;
  delete[] value_bytes;
}

/*
 * Class:     io_pmem_kvdk_WriteBatch
 * Method:    stringDelete
 * Signature: (J[BII)V
 */
void Java_io_pmem_kvdk_WriteBatch_stringDelete(JNIEnv* env, jobject,
                                               jlong batch_handle,
                                               jbyteArray key, jint key_off,
                                               jint key_len) {
  auto* batch = reinterpret_cast<KVDK_NAMESPACE::WriteBatch*>(batch_handle);

  jbyte* key_bytes = new jbyte[key_len];
  env->GetByteArrayRegion(key, key_off, key_len, key_bytes);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key_bytes;
    return;
  }

  batch->StringDelete(std::string(reinterpret_cast<char*>(key_bytes), key_len));

  delete[] key_bytes;
}

/*
 * Class:     io_pmem_kvdk_WriteBatch
 * Method:    sortedPut
 * Signature: (JJI[BII[BII)V
 */
void Java_io_pmem_kvdk_WriteBatch_sortedPut(JNIEnv* env, jobject,
                                            jlong batch_handle,
                                            jlong name_handle, jint name_len,
                                            jbyteArray key, jint key_off,
                                            jint key_len, jbyteArray value,
                                            jint value_off, jint value_len) {
  auto* batch = reinterpret_cast<KVDK_NAMESPACE::WriteBatch*>(batch_handle);
  auto* name_chars = reinterpret_cast<char*>(name_handle);

  jbyte* key_bytes = new jbyte[key_len];
  env->GetByteArrayRegion(key, key_off, key_len, key_bytes);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key_bytes;
    return;
  }

  jbyte* value_bytes = new jbyte[value_len];
  env->GetByteArrayRegion(value, value_off, value_len, value_bytes);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key_bytes;
    delete[] value_bytes;
    return;
  }

  batch->SortedPut(
      std::string(name_chars, name_len),
      std::string(reinterpret_cast<char*>(key_bytes), key_len),
      std::string(reinterpret_cast<char*>(value_bytes), value_len));

  delete[] key_bytes;
  delete[] value_bytes;
}

/*
 * Class:     io_pmem_kvdk_WriteBatch
 * Method:    sortedDelete
 * Signature: (JJI[BII)V
 */
void Java_io_pmem_kvdk_WriteBatch_sortedDelete(JNIEnv* env, jobject,
                                               jlong batch_handle,
                                               jlong name_handle, jint name_len,
                                               jbyteArray key, jint key_off,
                                               jint key_len) {
  auto* batch = reinterpret_cast<KVDK_NAMESPACE::WriteBatch*>(batch_handle);
  auto* name_chars = reinterpret_cast<char*>(name_handle);

  jbyte* key_bytes = new jbyte[key_len];
  env->GetByteArrayRegion(key, key_off, key_len, key_bytes);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key_bytes;
    return;
  }

  batch->SortedDelete(std::string(name_chars, name_len),
                      std::string(reinterpret_cast<char*>(key_bytes), key_len));

  delete[] key_bytes;
}

/*
 * Class:     io_pmem_kvdk_WriteBatch
 * Method:    clear
 * Signature: (J)V
 */
void Java_io_pmem_kvdk_WriteBatch_clear(JNIEnv*, jobject, jlong batch_handle) {
  auto* batch = reinterpret_cast<KVDK_NAMESPACE::WriteBatch*>(batch_handle);
  batch->Clear();
}

/*
 * Class:     io_pmem_kvdk_WriteBatch
 * Method:    size
 * Signature: (J)J
 */
jlong Java_io_pmem_kvdk_WriteBatch_size(JNIEnv*, jobject, jlong batch_handle) {
  auto* batch = reinterpret_cast<KVDK_NAMESPACE::WriteBatch*>(batch_handle);
  return batch->Size();
}