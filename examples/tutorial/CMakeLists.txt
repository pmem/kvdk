if (NOT BUILD_TUTORIAL)
    return()
endif()

enable_testing()

file(GLOB sources 
    ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/*.c)

foreach(src ${sources})
    file(RELATIVE_PATH src_rel_path ${CMAKE_CURRENT_SOURCE_DIR} ${src}) 
    string(REPLACE ".cpp" "" example_name ${src_rel_path})
    string(REPLACE ".c" "" example_name ${example_name})  
    string(REGEX REPLACE "[/\\.]" "_" example_name ${example_name})
    add_executable(${example_name} ${src})
    target_link_libraries(${example_name} PUBLIC engine)
endforeach()
