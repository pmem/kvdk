/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#include <assert.h>

#include "include/io_pmem_kvdk_Configs.h"
#include "kvdkjni/kvdkjni.h"

/*
 * Class:     io_pmem_kvdk_Configs
 * Method:    newConfigs
 * Signature: ()J
 */
jlong Java_io_pmem_kvdk_Configs_newConfigs(JNIEnv*, jclass) {
  auto* cfg = new KVDK_NAMESPACE::Configs();
  return GET_CPLUSPLUS_POINTER(cfg);
}

/*
 * Class:     io_pmem_kvdk_Configs
 * Method:    setMaxAccessThreads
 * Signature: (JJ)V
 */
void Java_io_pmem_kvdk_Configs_setMaxAccessThreads(JNIEnv*, jobject,
                                                   jlong handle, jlong num) {
  reinterpret_cast<KVDK_NAMESPACE::Configs*>(handle)->max_access_threads = num;
}

/*
 * Class:     io_pmem_kvdk_Configs
 * Method:    setPMemFileSize
 * Signature: (JJ)V
 */
void Java_io_pmem_kvdk_Configs_setPMemFileSize(JNIEnv*, jobject, jlong handle,
                                               jlong size) {
  reinterpret_cast<KVDK_NAMESPACE::Configs*>(handle)->pmem_file_size = size;
}

/*
 * Class:     io_pmem_kvdk_Configs
 * Method:    setPMemSegmentBlocks
 * Signature: (JJ)V
 */
void Java_io_pmem_kvdk_Configs_setPMemSegmentBlocks(JNIEnv*, jobject,
                                                    jlong handle,
                                                    jlong blocks) {
  reinterpret_cast<KVDK_NAMESPACE::Configs*>(handle)->pmem_segment_blocks =
      blocks;
}

/*
 * Class:     io_pmem_kvdk_Configs
 * Method:    setHashBucketNum
 * Signature: (JJ)V
 */
void Java_io_pmem_kvdk_Configs_setHashBucketNum(JNIEnv*, jobject, jlong handle,
                                                jlong num) {
  reinterpret_cast<KVDK_NAMESPACE::Configs*>(handle)->hash_bucket_num = num;
}

/*
 * Class:     io_pmem_kvdk_Configs
 * Method:    setPopulatePMemSpace
 * Signature: (JZ)V
 */
void Java_io_pmem_kvdk_Configs_setPopulatePMemSpace(JNIEnv*, jobject,
                                                    jlong handle,
                                                    jboolean populate) {
  reinterpret_cast<KVDK_NAMESPACE::Configs*>(handle)->populate_pmem_space =
      populate;
}

/*
 * Class:     io_pmem_kvdk_Configs
 * Method:    setDestMemoryNodes
 * Signature: (JLjava/lang/String;)V
 */
void Java_io_pmem_kvdk_Configs_setDestMemoryNodes(JNIEnv* env, jobject,
                                                  jlong handle,
                                                  jstring jnodes) {
  const char* nodes_chars = env->GetStringUTFChars(jnodes, nullptr);
  if (nodes_chars == nullptr) {
    // exception thrown: OutOfMemoryError
    return;
  }
  std::string nodes_str = nodes_chars;
  reinterpret_cast<KVDK_NAMESPACE::Configs*>(handle)->dest_memory_nodes =
      nodes_str;
  env->ReleaseStringUTFChars(jnodes, nodes_chars);
}

/*
 * Class:     io_pmem_kvdk_Configs
 * Method:    setOptLargeSortedCollectionRecovery
 * Signature: (JZ)V
 */
void Java_io_pmem_kvdk_Configs_setOptLargeSortedCollectionRecovery(
    JNIEnv*, jobject, jlong handle, jboolean opt) {
  reinterpret_cast<KVDK_NAMESPACE::Configs*>(handle)
      ->opt_large_sorted_collection_recovery = opt;
}

/*
 * Class:     io_pmem_kvdk_Configs
 * Method:    setUseDevDaxMode
 * Signature: (JZ)V
 */
void Java_io_pmem_kvdk_Configs_setUseDevDaxMode(JNIEnv*, jobject, jlong handle,
                                                jboolean use) {
  reinterpret_cast<KVDK_NAMESPACE::Configs*>(handle)->use_devdax_mode = use;
}

/*
 * Class:     io_pmem_kvdk_Configs
 * Method:    setCleanThreads
 * Signature: (JJ)V
 */
void Java_io_pmem_kvdk_Configs_setCleanThreads(JNIEnv*, jobject, jlong handle,
                                               jlong num_threds) {
  reinterpret_cast<KVDK_NAMESPACE::Configs*>(handle)->clean_threads =
      num_threds;
}

/*
 * Class:     io_pmem_kvdk_Configs
 * Method:    closeInternal
 * Signature: (J)V
 */
void Java_io_pmem_kvdk_Configs_closeInternal(JNIEnv*, jobject, jlong handle) {
  auto* cfg = reinterpret_cast<KVDK_NAMESPACE::Configs*>(handle);
  assert(cfg != nullptr);
  delete cfg;
}
