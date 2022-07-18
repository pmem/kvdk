cmake_minimum_required(VERSION 3.4)

if(${CMAKE_VERSION} VERSION_LESS "3.11.4")
    message("Please consider switching to CMake 3.11.4 or newer")
endif()

set(CMAKE_JAVA_COMPILE_FLAGS -source 7)

set(JNI_NATIVE_SOURCES
  kvdkjni/configs.cc
  kvdkjni/engine.cc
  kvdkjni/iterator.cc
  kvdkjni/native_bytes_handle.cc
)

set(JAVA_MAIN_SOURCES
  src/main/java/io/pmem/kvdk/AbstractNativeReference.java
  src/main/java/io/pmem/kvdk/Configs.java
  src/main/java/io/pmem/kvdk/Engine.java
  src/main/java/io/pmem/kvdk/Iterator.java
  src/main/java/io/pmem/kvdk/Status.java
  src/main/java/io/pmem/kvdk/KVDKException.java
  src/main/java/io/pmem/kvdk/KVDKObject.java
  src/main/java/io/pmem/kvdk/NativeBytesHandle.java
  src/main/java/io/pmem/kvdk/NativeLibraryLoader.java
)

set(JAVA_TEST_SOURCES
  src/test/java/io/pmem/kvdk/ConfigsTest.java
  src/test/java/io/pmem/kvdk/EngineTest.java
)

include(FindJava)
include(UseJava)
find_package(JNI)
include(ExternalProject)

include_directories(${JNI_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR}/java)

set(MVN_LOCAL ~/.m2/repository)
set(MVN_CENTRAL https://repo1.maven.org/maven2)

set(JAVA_TEST_LIBDIR ${PROJECT_SOURCE_DIR}/java/test-libs)
set(JAVA_TMP_JAR ${JAVA_TEST_LIBDIR}/tmp.jar)

set(JAVA_JUNIT_JAR_SOURCE /junit/junit/4.12/junit-4.12.jar)
set(JAVA_JUNIT_JAR ${JAVA_TEST_LIBDIR}/junit-4.12.jar)
set(JAVA_JUNIT_JAR_CHECKSUM_256 59721f0805e223d84b90677887d9ff567dc534d7c502ca903c0c2b17f05c116a)

set(JAVA_TESTCLASSPATH ${JAVA_JUNIT_JAR})

set(JNI_OUTPUT_DIR ${PROJECT_SOURCE_DIR}/java/include)
file(MAKE_DIRECTORY ${JNI_OUTPUT_DIR})

if(${Java_VERSION_MINOR} VERSION_LESS_EQUAL "7" AND ${Java_VERSION_MAJOR} STREQUAL "1")
  message(FATAL_ERROR "Detected Java 7 or older (${Java_VERSION_STRING}), minimum required version in now Java 8")
endif()

# Download dependencies for Java unit tests
if(NOT EXISTS ${JAVA_TEST_LIBDIR})
  file(MAKE_DIRECTORY ${JAVA_TEST_LIBDIR})
endif()

if(NOT EXISTS ${JAVA_JUNIT_JAR})
  if(EXISTS ${MVN_LOCAL}${JAVA_JUNIT_JAR_SOURCE})
    message("Copying from ${MVN_LOCAL}${JAVA_JUNIT_JAR_SOURCE} to ${JAVA_JUNIT_JAR}")
    file(COPY ${MVN_LOCAL}${JAVA_JUNIT_JAR_SOURCE} DESTINATION ${JAVA_TEST_LIBDIR})
  else()
    message("Downloading ${MVN_CENTRAL}${JAVA_JUNIT_JAR_SOURCE} to ${JAVA_TMP_JAR}")
    execute_process(COMMAND curl --fail --insecure --location ${MVN_CENTRAL}${JAVA_JUNIT_JAR_SOURCE}
      RESULT_VARIABLE error_code
      OUTPUT_VARIABLE error_message
      OUTPUT_FILE ${JAVA_TMP_JAR})

    if(NOT error_code EQUAL 0)
      message(FATAL_ERROR "Failed downloading ${MVN_CENTRAL}${JAVA_JUNIT_JAR_SOURCE} to ${JAVA_JUNIT_JAR}: ${error_message}")
    endif()
    file(RENAME ${JAVA_TMP_JAR} ${JAVA_JUNIT_JAR})
  endif()
endif()

# Verify checksum of downloaded files
file(SHA256 ${JAVA_JUNIT_JAR} TEMP_FILE_CHECKSUM)
if (NOT ${JAVA_JUNIT_JAR_CHECKSUM_256} STREQUAL ${TEMP_FILE_CHECKSUM})
  message(FATAL_ERROR "Checksum mismatch, file: ${JAVA_JUNIT_JAR}, expected: ${JAVA_JUNIT_JAR_CHECKSUM_256}, is: ${TEMP_FILE_CHECKSUM}")
endif()

if(${Java_VERSION_MAJOR} VERSION_GREATER_EQUAL "10" AND ${CMAKE_VERSION} VERSION_LESS "3.11.4")
  # Java 10 and newer don't have javah, but the alternative GENERATE_NATIVE_HEADERS requires CMake 3.11.4 or newer
  message(FATAL_ERROR "Detected Java 10 or newer (${Java_VERSION_STRING}), to build with CMake please upgrade CMake to 3.11.4 or newer")

elseif(${CMAKE_VERSION} VERSION_LESS "3.11.4")
  # Old CMake
  message("Using an old CMAKE (${CMAKE_VERSION}) - JNI headers generated in separate step")
  add_jar(
      kvdkjni_classes
      SOURCES
      ${JAVA_MAIN_SOURCES}
      ${JAVA_TEST_SOURCES}
      INCLUDE_JARS ${JAVA_TESTCLASSPATH}
  )

else ()
  # Java 1.8 or newer prepare the JAR...
  message("Preparing Jar for JDK ${Java_VERSION_STRING}")
  add_jar(
      kvdkjni_classes
      SOURCES
      ${JAVA_MAIN_SOURCES}
      ${JAVA_TEST_SOURCES}
      INCLUDE_JARS ${JAVA_TESTCLASSPATH}
      GENERATE_NATIVE_HEADERS kvdkjni_headers DESTINATION ${JNI_OUTPUT_DIR}
  )

endif()

if(${CMAKE_VERSION} VERSION_LESS "3.11.4")
  # Old CMake ONLY generate JNI headers, otherwise JNI is handled in add_jar step above
  message("Preparing JNI headers for old CMake (${CMAKE_VERSION})")
  set(NATIVE_JAVA_CLASSES
          io.pmem.kvdk.Configs
  )

  create_javah(
    TARGET kvdkjni_headers
    CLASSES ${NATIVE_JAVA_CLASSES}
    CLASSPATH kvdkjni_classes ${JAVA_TESTCLASSPATH}
    OUTPUT_DIR ${JNI_OUTPUT_DIR}
  )
endif()

set(KVDKJNI_SHARED_LIB kvdkjni${ARTIFACT_SUFFIX})
add_library(${KVDKJNI_SHARED_LIB} SHARED ${JNI_NATIVE_SOURCES})
add_dependencies(${KVDKJNI_SHARED_LIB} kvdkjni_headers)
target_link_libraries(${KVDKJNI_SHARED_LIB} ${KVDK_STATIC_LIB})

if(NOT EXISTS ${PROJECT_SOURCE_DIR}/java/target)
  file(MAKE_DIRECTORY ${PROJECT_SOURCE_DIR}/java/target)
endif()
set(KVDKJNI_SHARED_LIB_COPY_PATH ${PROJECT_SOURCE_DIR}/java/target/${CMAKE_SHARED_LIBRARY_PREFIX}${KVDKJNI_SHARED_LIB}${CMAKE_SHARED_LIBRARY_SUFFIX})

add_custom_command(TARGET ${KVDKJNI_SHARED_LIB} POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy
        $<TARGET_FILE:${KVDKJNI_SHARED_LIB}>
        ${KVDKJNI_SHARED_LIB_COPY_PATH}
    COMMENT "Copying JNI library file to: ${KVDKJNI_SHARED_LIB_COPY_PATH}"
)