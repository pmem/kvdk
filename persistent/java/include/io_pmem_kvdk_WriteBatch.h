/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class io_pmem_kvdk_WriteBatch */

#ifndef _Included_io_pmem_kvdk_WriteBatch
#define _Included_io_pmem_kvdk_WriteBatch
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     io_pmem_kvdk_WriteBatch
 * Method:    closeInternal
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_io_pmem_kvdk_WriteBatch_closeInternal(JNIEnv*,
                                                                  jobject,
                                                                  jlong);

/*
 * Class:     io_pmem_kvdk_WriteBatch
 * Method:    stringPut
 * Signature: (J[BII[BII)V
 */
JNIEXPORT void JNICALL Java_io_pmem_kvdk_WriteBatch_stringPut(
    JNIEnv*, jobject, jlong, jbyteArray, jint, jint, jbyteArray, jint, jint);

/*
 * Class:     io_pmem_kvdk_WriteBatch
 * Method:    stringDelete
 * Signature: (J[BII)V
 */
JNIEXPORT void JNICALL Java_io_pmem_kvdk_WriteBatch_stringDelete(JNIEnv*,
                                                                 jobject, jlong,
                                                                 jbyteArray,
                                                                 jint, jint);

/*
 * Class:     io_pmem_kvdk_WriteBatch
 * Method:    sortedPut
 * Signature: (JJI[BII[BII)V
 */
JNIEXPORT void JNICALL Java_io_pmem_kvdk_WriteBatch_sortedPut(
    JNIEnv*, jobject, jlong, jlong, jint, jbyteArray, jint, jint, jbyteArray,
    jint, jint);

/*
 * Class:     io_pmem_kvdk_WriteBatch
 * Method:    sortedDelete
 * Signature: (JJI[BII)V
 */
JNIEXPORT void JNICALL Java_io_pmem_kvdk_WriteBatch_sortedDelete(
    JNIEnv*, jobject, jlong, jlong, jint, jbyteArray, jint, jint);

/*
 * Class:     io_pmem_kvdk_WriteBatch
 * Method:    clear
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_io_pmem_kvdk_WriteBatch_clear(JNIEnv*, jobject,
                                                          jlong);

/*
 * Class:     io_pmem_kvdk_WriteBatch
 * Method:    size
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_io_pmem_kvdk_WriteBatch_size(JNIEnv*, jobject,
                                                          jlong);

#ifdef __cplusplus
}
#endif
#endif
