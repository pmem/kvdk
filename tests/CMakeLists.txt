# For kvdk api general test
set(TEST_SOURCES tests.cpp)
add_executable(dbtest ${TEST_SOURCES})
target_link_libraries(dbtest PUBLIC engine gtest gmock gtest_main)

# For stress tests
set(TEST_SOURCES2 stress_test.cpp)
add_executable(dbstress_test ${TEST_SOURCES2})
target_link_libraries(dbstress_test PUBLIC engine gtest gmock gtest_main)

message( ${PROJECT_SOURCE_DIR})
# For pmem allocator performance bench
add_executable(dbbench_pmem_allocator pmem_allocator_bench.cpp)
target_include_directories(dbbench_pmem_allocator 
    PRIVATE 
    ${PROJECT_SOURCE_DIR}/engine 
    ${PROJECT_SOURCE_DIR}/include
    )
target_link_libraries(dbbench_pmem_allocator PUBLIC engine)

