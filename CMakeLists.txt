cmake_minimum_required(VERSION 3.12.4)

project(KVDK VERSION 1.0
        DESCRIPTION "A fast persistent KV engine for Persistent Memory"
        LANGUAGES CXX C)

set(KVDK_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})
include(${KVDK_ROOT_DIR}/cmake/functions.cmake)
include(GNUInstallDirs)

set(CMAKE_CXX_STANDARD 11)
option(DEBUG_CHECK "check connection" OFF)
option(COVERAGE "code coverage" OFF)

if(DEBUG_CHECK)
    add_compile_definitions(DEBUG_CHECK)
endif()

set(KVDK_COMPILE_SHARED_FLAGS "-mavx -mavx2 -mavx512f -mrdrnd -mrdseed")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${KVDK_COMPILE_SHARED_FLAGS}")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${KVDK_COMPILE_SHARED_FLAGS}")

if (CMAKE_BUILD_TYPE STREQUAL "Release")
elseif (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
elseif (CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
elseif (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope -fno-omit-frame-pointer")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fsanitize-address-use-after-scope -fno-omit-frame-pointer")
elseif (CMAKE_BUILD_TYPE STREQUAL "FastDebug")
    set(CMAKE_CXX_FLAGS "${KVDK_COMPILE_SHARED_FLAGS} -g -O2")
    set(CMAKE_C_FLAGS "${KVDK_COMPILE_SHARED_FLAGS} -g -O2")
else ()
    message(FATAL_ERROR "Invalid build type!")
endif ()

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_definitions(DEBUG_LEVEL=0)
else ()
    add_compile_definitions(DEBUG_LEVEL=1)
endif()

# code coverage
if (COVERAGE)
    if(NOT ${CMAKE_BUILD_TYPE} MATCHES Debug)
        message(WARNING "Code coverage results with an optimised (non-Debug) build may be misleading" )
    endif()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage -fprofile-arcs -ftest-coverage")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -fprofile-arcs -ftest-coverage")
endif()

# source files
set(SOURCES
        engine/engine.cpp
        engine/kv_engine.cpp
        engine/logger.cpp
        engine/hash_table.cpp
        engine/skiplist.cpp
        engine/write_batch.cpp
        engine/dram_allocator.cpp
        engine/pmem_allocator/pmem_allocator.cpp
        engine/thread_manager.cpp
        engine/utils.cpp
        engine/pmem_allocator/free_list.cpp
        engine/c.cc
        engine/unordered_collection.cpp
        engine/data_record.cpp
        engine/queue.cpp
    )


# .a library
add_library(engine SHARED ${SOURCES})
target_include_directories(engine PUBLIC ./include ./extern)
target_link_libraries(engine PUBLIC pthread pmem gflags hwloc)

configure_file(kvdk.pc.in kvdk.pc @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/kvdk.pc
	DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

set_target_properties(
engine PROPERTIES PUBLIC_HEADER 
"\
include/kvdk/configs.hpp;\
include/kvdk/engine.h;\
include/kvdk/engine.hpp;\
include/kvdk/iterator.hpp;\
include/kvdk/namespace.hpp;\
include/kvdk/status.h;\
include/kvdk/status.hpp;\
include/kvdk/write_batch.hpp")


install(TARGETS engine
		PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/kvdk
		LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
		RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

# executables
add_executable(bench benchmark/bench.cpp)
target_link_libraries(bench PUBLIC engine)

option(BUILD_TESTING "Build the tests" ON)
if (BUILD_TESTING)
    set(TEST_SOURCES tests/tests.cpp)
    enable_testing()
    include(GoogleTest)
    add_subdirectory(extern/gtest)
    add_executable(dbtest ${TEST_SOURCES})
    target_link_libraries(dbtest PUBLIC engine gtest gmock gtest_main)
    set(TEST_SOURCES2 tests/stress_test.cpp)
    add_executable(dbstress_test ${TEST_SOURCES2})
    target_link_libraries(dbstress_test PUBLIC engine gtest gmock gtest_main)
endif ()

option(BUILD_TUTORIAL "Build the tutorial" ON)
add_subdirectory(examples/tutorial)

option(BUILD_GRAPH_SIMULATOR "Build the graph simulator" ON)
add_subdirectory(examples/graph_sim)

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
        ${CMAKE_CURRENT_SOURCE_DIR}/engine/pmem_allocator/*.h*
        ${CMAKE_CURRENT_SOURCE_DIR}/engine/pmem_allocator/*.c*
        ${CMAKE_CURRENT_SOURCE_DIR}/examples/tutorial/*.c*
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/*.c*
        ${CMAKE_CURRENT_SOURCE_DIR}/include/kvdk/*.h*
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.c*
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.h*)