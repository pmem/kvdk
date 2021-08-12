# SPDX-License-Identifier: BSD-3-Clause
# Copyright 2018-2021, Intel Corporation

#
# functions.cmake - helper functions for top-level CMakeLists.txt
#

# Sets ${ret} to version of program specified by ${name} in major.minor format
function(get_program_version_major_minor name ret)
    execute_process(COMMAND ${name} --version
        OUTPUT_VARIABLE cmd_ret
        ERROR_QUIET)
    STRING(REGEX MATCH "([0-9]+.)([0-9]+)" VERSION ${cmd_ret})
    SET(${ret} ${VERSION} PARENT_SCOPE)
endfunction()

# Generates cppstyle-$name and cppformat-$name targets and attaches them
# as dependencies of global "cppformat" target.
# cppstyle-$name target verifies C++ style of files in current source dir.
# cppformat-$name target reformats files in current source dir.
# If more arguments are used, then they are used as files to be checked
# instead.
# ${name} must be unique.
function(add_cppstyle name)
	if(NOT CLANG_FORMAT OR NOT (CLANG_FORMAT_VERSION VERSION_GREATER_EQUAL CLANG_FORMAT_REQUIRED))
		return()
	endif()

	if(${ARGC} EQUAL 1)
		add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/cppstyle-${name}-status
			DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
				${CMAKE_CURRENT_SOURCE_DIR}/*.hpp
			COMMAND ${PERL_EXECUTABLE}
				${KVDK_ROOT_DIR}/scripts/cppstyle
				${CLANG_FORMAT}
				check
				${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
				${CMAKE_CURRENT_SOURCE_DIR}/*.hpp
			COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/cppstyle-${name}-status
			)

		add_custom_target(cppformat-${name}
			COMMAND ${PERL_EXECUTABLE}
				${KVDK_ROOT_DIR}/scripts/cppstyle
				${CLANG_FORMAT}
				format
				${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
				${CMAKE_CURRENT_SOURCE_DIR}/*.hpp
			)
	else()
		add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/cppstyle-${name}-status
			DEPENDS ${ARGN}
			COMMAND ${PERL_EXECUTABLE}
				${KVDK_ROOT_DIR}/scripts/cppstyle
				${CLANG_FORMAT}
				check
				${ARGN}
			COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/cppstyle-${name}-status
			)

		add_custom_target(cppformat-${name}
			COMMAND ${PERL_EXECUTABLE}
				${KVDK_ROOT_DIR}/scripts/cppstyle
				${CLANG_FORMAT}
				format
				${ARGN}
			)
	endif()

	add_custom_target(cppstyle-${name}
			DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/cppstyle-${name}-status)

	add_dependencies(cppstyle cppstyle-${name})
	add_dependencies(cppformat cppformat-${name})
endfunction()
