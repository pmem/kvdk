/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021-2022 Intel Corporation
 */

#ifndef JAVA_KVDKJNI_KVDKJNI_H_
#define JAVA_KVDKJNI_KVDKJNI_H_

#include <jni.h>
#include <stdio.h>

#include <iostream>

#include "kvdk/volatile/engine.hpp"
#include "kvdkjni/cplusplus_to_java_convert.h"

namespace KVDK_NAMESPACE {

// Helper class to get Java class by name from C++.
class JavaClass {
 public:
  /*
   * Gets and initializes a Java Class
   *
   * @param env A pointer to the Java environment
   * @param jclazz_name The fully qualified JNI name of the Java Class
   *     e.g. "java/lang/String"
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env, const char* jclazz_name) {
    jclass jclazz = env->FindClass(jclazz_name);
    assert(jclazz != nullptr);
    return jclazz;
  }
};

// The portal class for io.pmem.kvdk.Status
class StatusJni : public JavaClass {
 public:
  /*
   * Get the Java Class io.pmem.kvdk.Status
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "io/pmem/kvdk/Status");
  }

  /*
   * Create a new Java io.pmem.kvdk.Status object with the same properties as
   * the provided C++ KVDK_NAMESPACE::Status object
   *
   * @param env A pointer to the Java environment
   * @param status The KVDK_NAMESPACE::Status object
   *
   * @return A reference to a Java io.pmem.kvdk.Status object, or nullptr
   *     if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const Status& status) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(B)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jstatus = env->NewObject(jclazz, mid, toJavaStatusCode(status));
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    return jstatus;
  }

  static jobject construct(JNIEnv* env, const Status* status) {
    return construct(env, *status);
  }

  // Returns the equivalent io.pmem.kvdk.Status.Code for the provided
  // C++ KVDK_NAMESPACE::Status enum
  static jbyte toJavaStatusCode(const KVDK_NAMESPACE::Status& status) {
    switch (status) {
      case KVDK_NAMESPACE::Status::Ok:
        return 0x0;
      case KVDK_NAMESPACE::Status::NotFound:
        return 0x1;
      case KVDK_NAMESPACE::Status::Outdated:
        return 0x2;
      case KVDK_NAMESPACE::Status::WrongType:
        return 0x3;
      case KVDK_NAMESPACE::Status::Existed:
        return 0x4;
      case KVDK_NAMESPACE::Status::OperationFail:
        return 0x5;
      case KVDK_NAMESPACE::Status::OutOfRange:
        return 0x6;
      case KVDK_NAMESPACE::Status::MemoryOverflow:
        return 0x7;
      case KVDK_NAMESPACE::Status::NotSupported:
        return 0x8;
      case KVDK_NAMESPACE::Status::InvalidBatchSize:
        return 0x9;
      case KVDK_NAMESPACE::Status::InvalidDataSize:
        return 0xA;
      case KVDK_NAMESPACE::Status::InvalidArgument:
        return 0xB;
      case KVDK_NAMESPACE::Status::IOError:
        return 0xC;
      case KVDK_NAMESPACE::Status::InvalidConfiguration:
        return 0xD;
      case KVDK_NAMESPACE::Status::Fail:
        return 0xE;
      case KVDK_NAMESPACE::Status::Abort:
        return 0xF;
      default:
        return 0x7F;  // undefined
    }
  }
};

// Java Exception template
template <class DERIVED>
class JavaException : public JavaClass {
 public:
  /*
   * Create and throw a java exception with the provided message
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg) {
    jclass jclazz = DERIVED::getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "JavaException::ThrowNew - Error: unexpected exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    const jint rs = env->ThrowNew(jclazz, msg.c_str());
    if (rs != JNI_OK) {
      // exception could not be thrown
      std::cerr << "JavaException::ThrowNew - Fatal: could not throw exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    return true;
  }
};

// The portal class for io.pmem.kvdk.KVDKException
class KVDKExceptionJni : public JavaException<KVDKExceptionJni> {
 public:
  /*
   * Get the Java Class io.pmem.kvdk.KVDKException
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaException::getJClass(env, "io/pmem/kvdk/KVDKException");
  }

  /*
   * Create and throw a Java KVDKException with the provided message
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg) {
    return JavaException::ThrowNew(env, msg);
  }

  /*
   * Create and throw a Java KVDKException with the provided status
   *
   * If s == Status::Ok, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const Status& s) {
    if (s == Status::Ok) {
      return false;
    }

    // get the KVDKException class
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr
          << "KVDKExceptionJni::ThrowNew/class - Error: unexpected exception!"
          << std::endl;
      return env->ExceptionCheck();
    }

    // get the constructor of io.pmem.kvdk.KVDKException
    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(Lio/pmem/kvdk/Status;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      std::cerr
          << "KVDKExceptionJni::ThrowNew/cstr - Error: unexpected exception!"
          << std::endl;
      return env->ExceptionCheck();
    }

    // get the Java status object
    jobject jstatus = StatusJni::construct(env, s);
    if (jstatus == nullptr) {
      // exception occcurred
      std::cerr << "KVDKExceptionJni::ThrowNew/StatusJni - Error: unexpected "
                   "exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    // construct the KVDKException
    jthrowable kvdk_exception =
        reinterpret_cast<jthrowable>(env->NewObject(jclazz, mid, jstatus));
    if (env->ExceptionCheck()) {
      if (jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if (kvdk_exception != nullptr) {
        env->DeleteLocalRef(kvdk_exception);
      }
      std::cerr << "KVDKExceptionJni::ThrowNew/NewObject - Error: unexpected "
                   "exception!"
                << std::endl;
      return true;
    }

    // throw the KVDKException
    const jint rs = env->Throw(kvdk_exception);
    if (rs != JNI_OK) {
      // exception could not be thrown
      std::cerr
          << "KVDKExceptionJni::ThrowNew - Fatal: could not throw exception!"
          << std::endl;
      if (jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if (kvdk_exception != nullptr) {
        env->DeleteLocalRef(kvdk_exception);
      }
      return env->ExceptionCheck();
    }

    if (jstatus != nullptr) {
      env->DeleteLocalRef(jstatus);
    }
    if (kvdk_exception != nullptr) {
      env->DeleteLocalRef(kvdk_exception);
    }

    return true;
  }
};

class JniUtil {
 public:
  static jbyteArray createJavaByteArray(JNIEnv* env, const char* bytes,
                                        const size_t size) {
    // Limitation for java array size is vm specific
    // In general it cannot exceed Integer.MAX_VALUE (2^31 - 1)
    // Current HotSpot VM limitation for array size is Integer.MAX_VALUE - 5
    // (2^31 - 1 - 5) It means that the next call to env->NewByteArray can still
    // end with OutOfMemoryError("Requested array size exceeds VM limit") coming
    // from VM
    static const size_t MAX_JARRAY_SIZE = (static_cast<size_t>(1)) << 31;
    if (size > MAX_JARRAY_SIZE) {
      KVDK_NAMESPACE::KVDKExceptionJni::ThrowNew(
          env, "Requested array size exceeds VM limit");
      return nullptr;
    }

    const jsize jlen = static_cast<jsize>(size);
    jbyteArray jbytes = env->NewByteArray(jlen);
    if (jbytes == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    env->SetByteArrayRegion(
        jbytes, 0, jlen,
        const_cast<jbyte*>(reinterpret_cast<const jbyte*>(bytes)));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jbytes);
      return nullptr;
    }

    return jbytes;
  }
};

}  // namespace KVDK_NAMESPACE

#endif  // JAVA_KVDKJNI_KVDKJNI_H_
