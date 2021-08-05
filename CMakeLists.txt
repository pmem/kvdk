cmake_minimum_required(VERSION 3.1)

project(PMEM_HASH VERSION 1.0
        DESCRIPTION "A fast persistent KV engine for Persistent Memory"
        LANGUAGES CXX)

set_property(GLOBAL PROPERTY CXX_STANDARD 14)
set(OPT "-msse4 -mavx -mavx2 -O2 -fno-omit-frame-pointer -DNDEBUG -momit-leaf-frame-pointer -g")

set(CMAKE_CXX_FLAGS ${OPT})

# source files
set(SOURCES
        engine/db.cpp
        engine/kv_engine.cpp
        engine/engine_impl.cpp
        engine/logger.cpp
        engine/hash_table.cpp
        engine/skiplist.cpp)

add_subdirectory(extern/safestringlib)

# .a library
add_library(engine SHARED ${SOURCES})
target_include_directories(engine PUBLIC ./include ./extern)
target_link_libraries(engine PUBLIC pthread pmem gflags safestring_shared)

# executables
add_executable(bench benchmark/bench.cpp)
target_link_libraries(bench PUBLIC engine safestring_shared)

option(BUILD_TESTING "Build the tests" ON)
if (BUILD_TESTING)
    set(TEST_SOURCES tests/tests.cpp)
    enable_testing()
    include(GoogleTest)
    add_subdirectory(extern/gtest)
    add_executable(dbtest tests/tests.cpp)
    target_link_libraries(dbtest PUBLIC engine gtest gmock gtest_main)
endif ()
