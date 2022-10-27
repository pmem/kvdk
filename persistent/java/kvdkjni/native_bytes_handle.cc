/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <assert.h>

#include "include/io_pmem_kvdk_NativeBytesHandle.h"
#include "kvdkjni/kvdkjni.h"

/*
 * Class:     io_pmem_kvdk_NativeBytesHandle
 * Method:    newNativeBytes
 * Signature: ([B)J
 */
jlong Java_io_pmem_kvdk_NativeBytesHandle_newNativeBytes(JNIEnv* env, jclass,
                                                         jbyteArray bytes) {
  int len = env->GetArrayLength(bytes);
  jbyte* b = new jbyte[len];
  env->GetByteArrayRegion(bytes, 0, len, b);
  if (env->ExceptionCheck()) {
    // exception thrown: ArrayIndexOutOfBoundsException
    delete[] b;
    return 0;
  }

  return GET_CPLUSPLUS_POINTER(b);
}

/*
 * Class:     io_pmem_kvdk_NativeBytesHandle
 * Method:    closeInternal
 * Signature: (J)V
 */
void Java_io_pmem_kvdk_NativeBytesHandle_closeInternal(JNIEnv*, jobject,
                                                       jlong handle) {
  auto* b = reinterpret_cast<jbyte*>(handle);
  assert(b != nullptr);
  delete[] b;
}
