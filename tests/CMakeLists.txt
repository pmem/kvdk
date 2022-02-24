# For kvdk api general test
set(TEST_SOURCES tests.cpp)
add_executable(dbtest ${TEST_SOURCES})
target_link_libraries(dbtest PUBLIC engine gtest gtest_main)

# For stress tests
set(TEST_SOURCES2 stress_test.cpp)
add_executable(dbstress_test ${TEST_SOURCES2})
target_link_libraries(dbstress_test PUBLIC engine gtest gtest_main)

# For pmem allocator module tests
set(TEST_PMEM_ALLOC test_pmem_allocator.cpp)
add_executable(dbtest_pmem_allocator ${TEST_PMEM_ALLOC})
target_include_directories(dbtest_pmem_allocator 
    PRIVATE 
    ${PROJECT_SOURCE_DIR}/engine 
    ${PROJECT_SOURCE_DIR}/include
    )
target_link_libraries(dbtest_pmem_allocator PUBLIC engine gtest gtest_main)

# For pmem allocator bench
add_executable(dbbench_pmem_allocator pmem_allocator_bench.cpp)
target_include_directories(dbbench_pmem_allocator 
    PRIVATE 
    ${PROJECT_SOURCE_DIR}/engine 
    ${PROJECT_SOURCE_DIR}/include
    )
target_link_libraries(dbbench_pmem_allocator PUBLIC engine gtest gtest_main)
