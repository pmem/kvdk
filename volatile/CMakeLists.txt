cmake_minimum_required(VERSION 3.12.4)

project(KVDK VERSION 1.0
        DESCRIPTION "A fast persistent KV engine for Persistent Memory"
        LANGUAGES CXX C)

set(KVDK_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
include(${KVDK_ROOT_DIR}/cmake/functions.cmake)
include(GNUInstallDirs)

set(CMAKE_CXX_STANDARD 11)

option(COVERAGE "code coverage" OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx512f -mrdseed -mrdrnd -mclwb -mclflushopt")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")

if (CMAKE_BUILD_TYPE STREQUAL "Release")
elseif (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
elseif (CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
elseif (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -fsanitize=address -fsanitize-address-use-after-scope -fno-omit-frame-pointer")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -fsanitize=address -fsanitize-address-use-after-scope -fno-omit-frame-pointer")
elseif (CMAKE_BUILD_TYPE STREQUAL "FastDebug")
    set(CMAKE_CXX_FLAGS "-Wall -Wextra -g -O2 -march=native")
    set(CMAKE_C_FLAGS "-Wall -Wextra -g -O2 -march=native")
else ()
    message(FATAL_ERROR "Invalid build type!")
endif ()

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_definitions(KVDK_DEBUG_LEVEL=0)
else ()
    add_compile_definitions(KVDK_DEBUG_LEVEL=1)
    add_compile_definitions(KVDK_ENABLE_CRASHPOINT)
endif ()

# code coverage
if (COVERAGE)
    if(NOT ${CMAKE_BUILD_TYPE} MATCHES Debug)
        message(WARNING "Code coverage results with an optimised (non-Debug) build may be misleading" )
    endif()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage -fprofile-arcs -ftest-coverage")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -fprofile-arcs -ftest-coverage")
endif()

set (KVDK_SHARED_LIB_LINK_LIBS pthread gflags hwloc atomic)
set (KVDK_STATIC_LIB_LINK_LIBS "")

# jemalloc
option(WITH_JEMALLOC "Build with jemalloc to support memory management with jemalloc" ON)
if (WITH_JEMALLOC)
    message(STATUS "jemalloc support is enabled")
    # libnuma static library, to support memory binding on numa nodes
    add_subdirectory(extern/numactl-cmake)

    list(APPEND KVDK_SHARED_LIB_LINK_LIBS kvdk_extern_lib::numa)
    list(APPEND KVDK_STATIC_LIB_LINK_LIBS kvdk_extern_lib::numa)

    # jemalloc
    add_subdirectory(extern/jemalloc-cmake)
    add_compile_definitions(KVDK_WITH_JEMALLOC)

    list(APPEND KVDK_SHARED_LIB_LINK_LIBS kvdk_extern_lib::jemalloc)
    list(APPEND KVDK_STATIC_LIB_LINK_LIBS kvdk_extern_lib::jemalloc)
else ()
    message(STATUS "jemalloc support is disabled")
endif()

# source files
set(SOURCES
        engine/c/kvdk_basic_op.cpp
        engine/c/kvdk_batch.cpp
        engine/c/kvdk_hash.cpp
        engine/c/kvdk_list.cpp
        engine/c/kvdk_sorted.cpp
        engine/c/kvdk_string.cpp
        engine/utils/utils.cpp
        engine/utils/sync_point.cpp
        engine/engine.cpp
        engine/kv_engine.cpp
        engine/kv_engine_cleaner.cpp
        engine/kv_engine_hash.cpp
        engine/kv_engine_list.cpp
        engine/kv_engine_sorted.cpp
        engine/kv_engine_string.cpp
        engine/logger.cpp
        engine/hash_table.cpp
        engine/sorted_collection/skiplist.cpp
        engine/sorted_collection/rebuilder.cpp
        engine/hash_collection/hash_list.cpp
        engine/list_collection/list.cpp
        engine/write_batch_impl.cpp
        engine/dram_allocator.cpp
        engine/thread_manager.cpp
        engine/data_record.cpp
        engine/dl_list.cpp
        engine/version/old_records_cleaner.cpp
    )

# .so library
add_library(engine SHARED ${SOURCES})
target_include_directories(engine PUBLIC ./include ./extern)
target_link_libraries(engine PUBLIC ${KVDK_SHARED_LIB_LINK_LIBS})

configure_file(kvdk.pc.in kvdk.pc @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/kvdk.pc
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

set(KVDK_PUBLIC_HEADERS
    ${PROJECT_SOURCE_DIR}/include/kvdk/comparator.hpp
    ${PROJECT_SOURCE_DIR}/include/kvdk/configs.hpp
    ${PROJECT_SOURCE_DIR}/include/kvdk/engine.h
    ${PROJECT_SOURCE_DIR}/include/kvdk/engine.hpp
    ${PROJECT_SOURCE_DIR}/include/kvdk/iterator.hpp
    ${PROJECT_SOURCE_DIR}/include/kvdk/write_batch.hpp
    ${PROJECT_SOURCE_DIR}/extern/libpmemobj++/string_view.hpp
)

set_target_properties(engine PROPERTIES PUBLIC_HEADER "${KVDK_PUBLIC_HEADERS}")

install(TARGETS engine
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kvdk
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

# executables
add_executable(bench benchmark/bench.cpp)
target_link_libraries(bench PUBLIC engine)
target_include_directories(bench PUBLIC ./include ./extern ./)

option(BUILD_TESTING "Build the tests" ON)
if (BUILD_TESTING)
    add_subdirectory(${CMAKE_SOURCE_DIR}/extern/gtest)
    add_subdirectory(tests)
endif ()


option(BUILD_TUTORIAL "Build the tutorial" ON)
add_subdirectory(examples/tutorial)

option(BUILD_GRAPH_SIMULATOR "Build the graph simulator" ON)

add_custom_target(checkers ALL)
add_custom_target(cppstyle)
add_custom_target(cppformat)

find_program(CLANG_FORMAT NAMES clang-format-9 clang-format-9.0 clang-format)
set(CLANG_FORMAT_MINIMUM_REQUIRED "9.0")
if (CLANG_FORMAT)
    get_program_version_major_minor(${CLANG_FORMAT} CLANG_FORMAT_VERSION)
    message(STATUS "Found clang-format: ${CLANG_FORMAT} (version: ${CLANG_FORMAT_VERSION})")
endif ()

if (CHECK_CPP_STYLE)
    if (CLANG_FORMAT)
        if (NOT (CLANG_FORMAT_VERSION VERSION_GREATER_EQUAL CLANG_FORMAT_MINIMUM_REQUIRED))
            message(FATAL_ERROR "minimum required clang-format version is ${CCLANG_FORMAT_MINIMUM_REQUIRED}")
        endif ()
    else ()
        message(FATAL_ERROR "CHECK_CPP_STYLE=ON, but clang-format not found (required version: ${CLANG_FORMAT_MINIMUM_REQUIRED})")
    endif ()

    add_dependencies(checkers cppstyle)
endif ()

add_cppstyle(src ${CMAKE_CURRENT_SOURCE_DIR}/engine/*.c*
        ${CMAKE_CURRENT_SOURCE_DIR}/engine/*.h*
        ${CMAKE_CURRENT_SOURCE_DIR}/engine/utils/*.c*
        ${CMAKE_CURRENT_SOURCE_DIR}/engine/utils/*.h*
        ${CMAKE_CURRENT_SOURCE_DIR}/examples/tutorial/*.c*
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/*.c*
        ${CMAKE_CURRENT_SOURCE_DIR}/include/kvdk/*.h*
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.c*
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.h*)

option(WITH_JNI "Build with JNI" OFF)
if(WITH_JNI OR JNI)
    message(STATUS "JNI library is enabled")
    set(KVDK_SHARED_LIB engine)
    set(KVDK_STATIC_LIB engine-static)

    include(FindPackageHandleStandardArgs)

    add_library(${KVDK_STATIC_LIB} STATIC ${SOURCES})
    target_include_directories(${KVDK_STATIC_LIB} PUBLIC ./include ./extern)
    target_link_libraries(${KVDK_STATIC_LIB} PRIVATE ${KVDK_STATIC_LIB_LINK_LIBS} ${KVDK_SHARED_LIB_LINK_LIBS})

    set_property(TARGET ${KVDK_STATIC_LIB} PROPERTY POSITION_INDEPENDENT_CODE ON)

    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/java)

    add_cppstyle(jni_src
        ${CMAKE_CURRENT_SOURCE_DIR}/java/kvdkjni/*.c*
        ${CMAKE_CURRENT_SOURCE_DIR}/java/kvdkjni/*.h*)
else()
    message(STATUS "JNI library is disabled")
endif()
