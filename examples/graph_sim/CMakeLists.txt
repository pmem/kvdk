if (NOT BUILD_GRAPH_SIMULATOR)
    return()
endif()

enable_testing()

file(GLOB sources
        src/kv_engines/KVEngine.cpp
        src/kv_engines/KVEngine.hpp
        src/kv_engines/engine_factory.hpp
        src/graph_algorithm/top_n.hpp
        src/graph_impl.cpp
        src/graph_impl.hpp
        src/options.hpp
        src/coding.hpp)

add_library(graph SHARED ${sources})
target_include_directories(graph PUBLIC ./src ./src/kv_engines )
target_link_libraries(graph PUBLIC engine pthread pmem gflags hwloc)

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
