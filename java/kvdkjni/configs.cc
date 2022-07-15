/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <assert.h>

#include "include/io_pmem_kvdk_Configs.h"
#include "kvdkjni/kvdkjni.h"


/**
 * Class:     io_pmem_kvdk_Configs
 * Method:    newConfigs
 * Signature: ()J
 */
jlong Java_io_pmem_kvdk_Configs_newConfigs(
    JNIEnv*, jclass) {
  auto* cfg = new kvdk::Configs();
  return GET_CPLUSPLUS_POINTER(cfg);
}

/**
 * Class:     io_pmem_kvdk_Configs
 * Method:    setPMemFileSize
 * Signature: (JJ)V
 */
void Java_io_pmem_kvdk_Configs_setPMemFileSize(
  JNIEnv*, jobject, jlong handle, jlong size) {
  reinterpret_cast<kvdk::Configs *>(handle)->pmem_file_size = size;
}

/**
 * Class:     io_pmem_kvdk_Configs
 * Method:    setPMemSegmentBlocks
 * Signature: (JJ)V
 */
void Java_io_pmem_kvdk_Configs_setPMemSegmentBlocks(
  JNIEnv*, jobject, jlong handle, jlong blocks) {
  reinterpret_cast<kvdk::Configs *>(handle)->pmem_segment_blocks = blocks;
}

/**
 * Class:     io_pmem_kvdk_Configs
 * Method:    setHashBucketNum
 * Signature: (JJ)V
 */
void Java_io_pmem_kvdk_Configs_setHashBucketNum(
  JNIEnv*, jobject, jlong handle, jlong num) {
  reinterpret_cast<kvdk::Configs *>(handle)->hash_bucket_num = num;
}

/**
 * Class:     io_pmem_kvdk_Configs
 * Method:    closeInternal
 * Signature: (J)V
 */
void Java_io_pmem_kvdk_Configs_closeInternal(
    JNIEnv*, jobject, jlong handle) {
  auto* cfg = reinterpret_cast<kvdk::Configs *>(handle);
  assert(cfg != nullptr);
  delete cfg;
}
