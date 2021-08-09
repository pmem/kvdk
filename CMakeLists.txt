cmake_minimum_required(VERSION 3.1)

project(KVDK VERSION 1.0
        DESCRIPTION "A fast persistent KV engine for Persistent Memory"
        LANGUAGES CXX)

set_property(GLOBAL PROPERTY CXX_STANDARD 14)
set(OPT "-msse4 -mavx -mavx2 -O2 -fno-omit-frame-pointer -momit-leaf-frame-pointer")

if (CMAKE_BUILD_TYPE STREQUAL "Release")
    set(FLAGS "-g -DNDEBUG ${OPT}")
else ()
    set(FLAGS "-g ")
endif ()

set(CMAKE_CXX_FLAGS "${FLAGS}")

# source files
set(SOURCES
        engine/engine.cpp
        engine/kv_engine.cpp
        engine/logger.cpp
        engine/hash_table.cpp
        engine/skiplist.cpp
        engine/write_batch.cpp
        engine/hash_list.cpp
        engine/dram_allocator.cpp
        engine/pmem_allocator.cpp
        engine/thread_manager.cpp)


# .a library
add_library(engine SHARED ${SOURCES})
target_include_directories(engine PUBLIC ./include ./extern)
target_link_libraries(engine PUBLIC pthread pmem gflags)

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
endif ()

option(BUILD_TUTORIAL "Build the tutorial" ON)
if (BUILD_TUTORIAL)
    set(TUTORIAL_SOURCES examples/tutorial.cpp)
    add_executable(dbtutorial ${TUTORIAL_SOURCES})
    target_link_libraries(dbtutorial PUBLIC engine)
endif ()