if (NOT BUILD_GRAPH_SIMULATOR)
    return()
endif()

set(ROCKSDB_INCLUDE_DIR)
set(ROCKSDB_LIBRARY )
message(${CMAKE_CURRENT_SOURCE_DIR})
option(BUILD_ROCKSDB "build with rocksdb" OFF)
if (BUILD_ROCKSDB)
    # Build the rocksdb and add the include / library / defines
    execute_process(COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/scripts/build_rocksdb.sh)
    set(ROCKSDB_INCLUDE_DIR ${PROJECT_BINARY_DIR}/rocksdb/include)
    set(ROCKSDB_LIBRARY ${PROJECT_BINARY_DIR}/rocksdb/build/librocksdb.so)
    add_definitions(-DBUILD_ROCKSDB)
    set(CMAKE_CXX_STANDARD 17)
endif()

file(GLOB sources
        src/kv_engines/kvdk.cpp
        src/kv_engines/rocksdb.cpp
        src/kv_engines/KVEngine.hpp
        src/kv_engines/engine_factory.hpp
        src/graph_algorithm/top_n.hpp
        src/graph_impl.cpp
        src/graph_impl.hpp
        src/options.hpp
        src/coding.hpp)

add_library(graph SHARED ${sources})
target_include_directories(graph PUBLIC ./src ./src/kv_engines ${ROCKSDB_INCLUDE_DIR} )
target_link_libraries(graph PUBLIC engine pthread pmem gflags hwloc ${ROCKSDB_LIBRARY})

add_executable(graph_bench bench/graph_bench.cpp)
target_link_libraries(graph_bench PUBLIC graph engine)

if (NOT BUILD_TESTING)
    return()
endif()
set(TEST_SOURCES src/graph_impl_test.cpp)
enable_testing()
include(GoogleTest)
add_executable(graph_impl_test ${TEST_SOURCES})
target_link_libraries(graph_impl_test PUBLIC graph engine gtest gmock gtest_main)
