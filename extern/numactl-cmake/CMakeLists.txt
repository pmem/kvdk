if (NOT CMAKE_SYSTEM_NAME MATCHES "Linux")
    message (FATAL_ERROR "Not supported for non-linux environment")
endif()

message (STATUS "Configure to build libnuma")

set (LIBNUMA_SOURCE_DIR "${PROJECT_SOURCE_DIR}/extern/numactl")

set (SRCS
    "${LIBNUMA_SOURCE_DIR}/libnuma.c"
    "${LIBNUMA_SOURCE_DIR}/syscall.c"
    "${LIBNUMA_SOURCE_DIR}/distance.c"
    "${LIBNUMA_SOURCE_DIR}/affinity.c"
    "${LIBNUMA_SOURCE_DIR}/sysfs.c"
    "${LIBNUMA_SOURCE_DIR}/rtnetlink.c"
)

add_library(_numa ${SRCS})

target_include_directories(_numa PRIVATE include)
target_include_directories(_numa SYSTEM PUBLIC "${LIBNUMA_SOURCE_DIR}")

set_target_properties(_numa PROPERTIES COMPILE_FLAGS "-w -fPIC")

# disable SYMVER, it make shared libraries failed to link
add_custom_target(
    _disable_symver
    COMMAND sed -i "'25s/.*/#define SYMVER(a,b)/'" ${LIBNUMA_SOURCE_DIR}/util.h
)

add_dependencies(_numa _disable_symver)

add_custom_command(
    TARGET _numa POST_BUILD
    COMMAND sed -i "'25s/.*/#define SYMVER(a,b) __asm__ (\".symver \" a \",\" b);/'" ${LIBNUMA_SOURCE_DIR}/util.h
)

add_library(kvdk_extern_lib::numa ALIAS _numa)
