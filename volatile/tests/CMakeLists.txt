# For kvdk api general test
set(TEST_SOURCES tests.cpp)
add_executable(dbtest ${TEST_SOURCES})
target_link_libraries(dbtest PUBLIC engine gtest gtest_main)

# C API test
add_executable(c_api_test 
               c_api_test_list.cpp
               c_api_test_hash.cpp
               )
target_link_libraries(c_api_test PUBLIC engine gtest gtest_main)

# For stress tests
set(TEST_SOURCES2 stress_test.cpp)
add_executable(dbstress_test ${TEST_SOURCES2})
target_link_libraries(dbstress_test PUBLIC engine gtest gtest_main)
