/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <assert.h>

#include "include/io_pmem_kvdk_Engine.h"
#include "kvdkjni/kvdkjni.h"

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    open
 * Signature: (Ljava/lang/String;J)V
 */
jlong Java_io_pmem_kvdk_Engine_open(JNIEnv* env, jclass, jstring jengine_path,
                                    jlong jcfg_handle) {
  const char* engine_path_chars = env->GetStringUTFChars(jengine_path, nullptr);
  if (engine_path_chars == nullptr) {
    // exception thrown: OutOfMemoryError
    return 0;
  }
  std::string engine_path_str = engine_path_chars;

  auto* cfg = reinterpret_cast<KVDK_NAMESPACE::Configs*>(jcfg_handle);
  KVDK_NAMESPACE::Engine* engine = nullptr;
  KVDK_NAMESPACE::Status s =
      KVDK_NAMESPACE::Engine::Open(engine_path_str, &engine, *cfg, stdout);

  env->ReleaseStringUTFChars(jengine_path, engine_path_chars);

  if (s == KVDK_NAMESPACE::Status::Ok) {
    return GET_CPLUSPLUS_POINTER(engine);
  } else {
    KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
    return 0;
  }
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    put
 * Signature: (J[BII[BIIJZ)V
 */
void Java_io_pmem_kvdk_Engine_put(JNIEnv* env, jobject, jlong handle,
                                  jbyteArray key, jint key_off, jint key_len,
                                  jbyteArray value, jint value_off,
                                  jint value_len, jlong ttl_in_millis,
                                  jboolean update_ttl_if_existed) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(handle);

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

  KVDK_NAMESPACE::WriteOptions write_options;
  write_options.ttl_time = ttl_in_millis;
  write_options.update_ttl = update_ttl_if_existed;

  auto s =
      engine->Put(std::string(reinterpret_cast<char*>(key_bytes), key_len),
                  std::string(reinterpret_cast<char*>(value_bytes), value_len),
                  write_options);

  delete[] key_bytes;
  delete[] value_bytes;

  if (s != KVDK_NAMESPACE::Status::Ok) {
    KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    expire
 * Signature: (J[BIIJ)V
 */
void Java_io_pmem_kvdk_Engine_expire__J_3BIIJ(JNIEnv* env, jobject,
                                              jlong handle, jbyteArray key,
                                              jint key_off, jint key_len,
                                              jlong ttl_in_millis) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(handle);

  jbyte* key_bytes = new jbyte[key_len];
  env->GetByteArrayRegion(key, key_off, key_len, key_bytes);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key_bytes;
    return;
  }

  auto s = engine->Expire(
      std::string(reinterpret_cast<char*>(key_bytes), key_len), ttl_in_millis);

  delete[] key_bytes;

  if (s != KVDK_NAMESPACE::Status::Ok) {
    KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    expire
 * Signature: (JJIJ)V
 */
void Java_io_pmem_kvdk_Engine_expire__JJIJ(JNIEnv* env, jobject,
                                           jlong engine_handle,
                                           jlong name_handle, jint name_len,
                                           jlong ttl_in_millis) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(engine_handle);
  auto* name_chars = reinterpret_cast<char*>(name_handle);

  auto s = engine->Expire(std::string(name_chars, name_len), ttl_in_millis);

  if (s != KVDK_NAMESPACE::Status::Ok) {
    KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    get
 * Signature: (J[BII)[B
 */
jbyteArray Java_io_pmem_kvdk_Engine_get(JNIEnv* env, jobject, jlong handle,
                                        jbyteArray key, jint key_off,
                                        jint key_len) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(handle);

  jbyte* key_bytes = new jbyte[key_len];
  env->GetByteArrayRegion(key, key_off, key_len, key_bytes);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key_bytes;
    return nullptr;
  }

  std::string value;
  auto s = engine->Get(std::string(reinterpret_cast<char*>(key_bytes), key_len),
                       &value);

  delete[] key_bytes;

  if (s == KVDK_NAMESPACE::Status::NotFound) {
    return nullptr;
  }

  if (s == KVDK_NAMESPACE::Status::Ok) {
    jbyteArray ret = KVDK_NAMESPACE::JniUtil::createJavaByteArray(
        env, value.c_str(), value.size());
    return ret;
  }

  KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
  return nullptr;
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    delete
 * Signature: (J[BII)V
 */
void Java_io_pmem_kvdk_Engine_delete(JNIEnv* env, jobject, jlong handle,
                                     jbyteArray key, jint key_off,
                                     jint key_len) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(handle);

  jbyte* key_bytes = new jbyte[key_len];
  env->GetByteArrayRegion(key, key_off, key_len, key_bytes);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key_bytes;
    return;
  }

  auto s =
      engine->Delete(std::string(reinterpret_cast<char*>(key_bytes), key_len));

  delete[] key_bytes;

  if (s != KVDK_NAMESPACE::Status::Ok) {
    KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    sortedCreate
 * Signature: (JJI)V
 */
void Java_io_pmem_kvdk_Engine_sortedCreate(JNIEnv* env, jobject,
                                           jlong engine_handle,
                                           jlong name_handle, jint name_len) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(engine_handle);
  auto* name_chars = reinterpret_cast<char*>(name_handle);

  auto s = engine->SortedCreate(std::string(name_chars, name_len));

  if (s != KVDK_NAMESPACE::Status::Ok) {
    KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    sortedDestroy
 * Signature: (JJI)V
 */
void Java_io_pmem_kvdk_Engine_sortedDestroy(JNIEnv* env, jobject,
                                            jlong engine_handle,
                                            jlong name_handle, jint name_len) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(engine_handle);
  auto* name_chars = reinterpret_cast<char*>(name_handle);

  auto s = engine->SortedDestroy(std::string(name_chars, name_len));

  if (s != KVDK_NAMESPACE::Status::Ok) {
    KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    sortedSize
 * Signature: (JJI)J
 */
jlong Java_io_pmem_kvdk_Engine_sortedSize(JNIEnv* env, jobject,
                                          jlong engine_handle,
                                          jlong name_handle, jint name_len) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(engine_handle);
  auto* name_chars = reinterpret_cast<char*>(name_handle);

  size_t size = 0;
  auto s = engine->SortedSize(std::string(name_chars, name_len), &size);

  if (s != KVDK_NAMESPACE::Status::Ok) {
    KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
  }

  return size;
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    sortedPut
 * Signature: (JJI[BII[BII)V
 */
void Java_io_pmem_kvdk_Engine_sortedPut(JNIEnv* env, jobject,
                                        jlong engine_handle, jlong name_handle,
                                        jint name_len, jbyteArray key,
                                        jint key_off, jint key_len,
                                        jbyteArray value, jint value_off,
                                        jint value_len) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(engine_handle);
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

  auto s = engine->SortedPut(
      std::string(name_chars, name_len),
      std::string(reinterpret_cast<char*>(key_bytes), key_len),
      std::string(reinterpret_cast<char*>(value_bytes), value_len));

  delete[] key_bytes;
  delete[] value_bytes;

  if (s != KVDK_NAMESPACE::Status::Ok) {
    KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    sortedGet
 * Signature: (JJI[BII)[B
 */
jbyteArray Java_io_pmem_kvdk_Engine_sortedGet(JNIEnv* env, jobject,
                                              jlong engine_handle,
                                              jlong name_handle, jint name_len,
                                              jbyteArray key, jint key_off,
                                              jint key_len) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(engine_handle);
  auto* name_chars = reinterpret_cast<char*>(name_handle);

  jbyte* key_bytes = new jbyte[key_len];
  env->GetByteArrayRegion(key, key_off, key_len, key_bytes);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key_bytes;
    return nullptr;
  }

  std::string value;
  auto s = engine->SortedGet(
      std::string(name_chars, name_len),
      std::string(reinterpret_cast<char*>(key_bytes), key_len), &value);

  delete[] key_bytes;

  if (s == KVDK_NAMESPACE::Status::NotFound) {
    return nullptr;
  }

  if (s == KVDK_NAMESPACE::Status::Ok) {
    jbyteArray ret = KVDK_NAMESPACE::JniUtil::createJavaByteArray(
        env, value.c_str(), value.size());
    return ret;
  }

  KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
  return nullptr;
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    sortedDelete
 * Signature: (JJI[BII)V
 */
void Java_io_pmem_kvdk_Engine_sortedDelete(JNIEnv* env, jobject,
                                           jlong engine_handle,
                                           jlong name_handle, jint name_len,
                                           jbyteArray key, jint key_off,
                                           jint key_len) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(engine_handle);
  auto* name_chars = reinterpret_cast<char*>(name_handle);

  jbyte* key_bytes = new jbyte[key_len];
  env->GetByteArrayRegion(key, key_off, key_len, key_bytes);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] key_bytes;
    return;
  }

  auto s = engine->SortedDelete(
      std::string(name_chars, name_len),
      std::string(reinterpret_cast<char*>(key_bytes), key_len));

  delete[] key_bytes;

  if (s != KVDK_NAMESPACE::Status::Ok) {
    KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    newSortedIterator
 * Signature: (JJI)J
 */
jlong Java_io_pmem_kvdk_Engine_newSortedIterator(JNIEnv* env, jobject,
                                                 jlong engine_handle,
                                                 jlong name_handle,
                                                 jint name_len) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(engine_handle);
  auto* name_chars = reinterpret_cast<char*>(name_handle);

  KVDK_NAMESPACE::Status s;
  KVDK_NAMESPACE::Iterator* iter =
      engine->NewSortedIterator(std::string(name_chars, name_len), nullptr, &s);

  if (s == KVDK_NAMESPACE::Status::Ok) {
    return GET_CPLUSPLUS_POINTER(iter);
  } else {
    KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
    return 0;
  }
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    closeInternal
 * Signature: (J)V
 */
void Java_io_pmem_kvdk_Engine_closeInternal(JNIEnv*, jobject, jlong handle) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(handle);
  assert(engine != nullptr);
  delete engine;
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    writeBatchCreate
 * Signature: (J)J
 */
jlong Java_io_pmem_kvdk_Engine_writeBatchCreate(JNIEnv*, jobject,
                                                jlong handle) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(handle);
  std::unique_ptr<KVDK_NAMESPACE::WriteBatch> ptr = engine->WriteBatchCreate();
  return GET_CPLUSPLUS_POINTER(ptr.release());
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    batchWrite
 * Signature: (JJ)V
 */
void Java_io_pmem_kvdk_Engine_batchWrite(JNIEnv* env, jobject,
                                         jlong engine_handle,
                                         jlong batch_handle) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(engine_handle);
  auto* batch = reinterpret_cast<KVDK_NAMESPACE::WriteBatch*>(batch_handle);
  std::unique_ptr<KVDK_NAMESPACE::WriteBatch> ptr(batch);
  auto s = engine->BatchWrite(ptr);
  ptr.release();

  if (s != KVDK_NAMESPACE::Status::Ok) {
    KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     io_pmem_kvdk_Engine
 * Method:    releaseAccessThread
 * Signature: (JJ)V
 */
void Java_io_pmem_kvdk_Engine_releaseAccessThread(JNIEnv* env, jobject,
                                                  jlong engine_handle) {
  auto* engine = reinterpret_cast<KVDK_NAMESPACE::Engine*>(engine_handle);
  engine->ReleaseAccessThread();
}
