if (NOT CMAKE_SYSTEM_NAME MATCHES "Linux")
    message (FATAL_ERROR "Not supported for non-linux environment")
endif()

message (STATUS "Configure to build jemalloc")

set (JEMALLOC_CONFIG_MALLOC_CONF "percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:5000,dirty_decay_ms:5000")

set (JEMALLOC_CONFIG_MALLOC_CONF_OVERRIDE "" CACHE STRING "Change default configuration string of jemalloc" )
if (JEMALLOC_CONFIG_MALLOC_CONF_OVERRIDE)
    set (JEMALLOC_CONFIG_MALLOC_CONF "${JEMALLOC_CONFIG_MALLOC_CONF_OVERRIDE}")
endif()
message (STATUS "jemalloc malloc_conf: ${JEMALLOC_CONFIG_MALLOC_CONF}")

set (JEMALLOC_SOURCE_DIR "${PROJECT_SOURCE_DIR}/extern/jemalloc")

set (SRCS
    "${JEMALLOC_SOURCE_DIR}/src/arena.c"
    "${JEMALLOC_SOURCE_DIR}/src/background_thread.c"
    "${JEMALLOC_SOURCE_DIR}/src/base.c"
    "${JEMALLOC_SOURCE_DIR}/src/bin.c"
    "${JEMALLOC_SOURCE_DIR}/src/bin_info.c"
    "${JEMALLOC_SOURCE_DIR}/src/bitmap.c"
    "${JEMALLOC_SOURCE_DIR}/src/buf_writer.c"
    "${JEMALLOC_SOURCE_DIR}/src/cache_bin.c"
    "${JEMALLOC_SOURCE_DIR}/src/ckh.c"
    "${JEMALLOC_SOURCE_DIR}/src/counter.c"
    "${JEMALLOC_SOURCE_DIR}/src/ctl.c"
    "${JEMALLOC_SOURCE_DIR}/src/decay.c"
    "${JEMALLOC_SOURCE_DIR}/src/div.c"
    "${JEMALLOC_SOURCE_DIR}/src/ecache.c"
    "${JEMALLOC_SOURCE_DIR}/src/edata.c"
    "${JEMALLOC_SOURCE_DIR}/src/edata_cache.c"
    "${JEMALLOC_SOURCE_DIR}/src/ehooks.c"
    "${JEMALLOC_SOURCE_DIR}/src/emap.c"
    "${JEMALLOC_SOURCE_DIR}/src/eset.c"
    "${JEMALLOC_SOURCE_DIR}/src/exp_grow.c"
    "${JEMALLOC_SOURCE_DIR}/src/extent.c"
    "${JEMALLOC_SOURCE_DIR}/src/extent_dss.c"
    "${JEMALLOC_SOURCE_DIR}/src/extent_mmap.c"
    "${JEMALLOC_SOURCE_DIR}/src/fxp.c"
    "${JEMALLOC_SOURCE_DIR}/src/hook.c"
    "${JEMALLOC_SOURCE_DIR}/src/hpa.c"
    "${JEMALLOC_SOURCE_DIR}/src/hpa_hooks.c"
    "${JEMALLOC_SOURCE_DIR}/src/hpdata.c"
    "${JEMALLOC_SOURCE_DIR}/src/inspect.c"
    "${JEMALLOC_SOURCE_DIR}/src/jemalloc.c"
    "${JEMALLOC_SOURCE_DIR}/src/large.c"
    "${JEMALLOC_SOURCE_DIR}/src/log.c"
    "${JEMALLOC_SOURCE_DIR}/src/malloc_io.c"
    "${JEMALLOC_SOURCE_DIR}/src/mutex.c"
    "${JEMALLOC_SOURCE_DIR}/src/nstime.c"
    "${JEMALLOC_SOURCE_DIR}/src/pa.c"
    "${JEMALLOC_SOURCE_DIR}/src/pac.c"
    "${JEMALLOC_SOURCE_DIR}/src/pa_extra.c"
    "${JEMALLOC_SOURCE_DIR}/src/pages.c"
    "${JEMALLOC_SOURCE_DIR}/src/pai.c"
    "${JEMALLOC_SOURCE_DIR}/src/peak_event.c"
    "${JEMALLOC_SOURCE_DIR}/src/prof.c"
    "${JEMALLOC_SOURCE_DIR}/src/prof_data.c"
    "${JEMALLOC_SOURCE_DIR}/src/prof_log.c"
    "${JEMALLOC_SOURCE_DIR}/src/prof_recent.c"
    "${JEMALLOC_SOURCE_DIR}/src/prof_stats.c"
    "${JEMALLOC_SOURCE_DIR}/src/prof_sys.c"
    "${JEMALLOC_SOURCE_DIR}/src/psset.c"
    "${JEMALLOC_SOURCE_DIR}/src/rtree.c"
    "${JEMALLOC_SOURCE_DIR}/src/safety_check.c"
    "${JEMALLOC_SOURCE_DIR}/src/san_bump.c"
    "${JEMALLOC_SOURCE_DIR}/src/san.c"
    "${JEMALLOC_SOURCE_DIR}/src/sc.c"
    "${JEMALLOC_SOURCE_DIR}/src/sec.c"
    "${JEMALLOC_SOURCE_DIR}/src/stats.c"
    "${JEMALLOC_SOURCE_DIR}/src/sz.c"
    "${JEMALLOC_SOURCE_DIR}/src/tcache.c"
    "${JEMALLOC_SOURCE_DIR}/src/test_hooks.c"
    "${JEMALLOC_SOURCE_DIR}/src/thread_event.c"
    "${JEMALLOC_SOURCE_DIR}/src/ticker.c"
    "${JEMALLOC_SOURCE_DIR}/src/tsd.c"
    "${JEMALLOC_SOURCE_DIR}/src/witness.c"
)

add_library(_jemalloc ${SRCS})

target_include_directories(_jemalloc SYSTEM PUBLIC include)
target_include_directories(_jemalloc SYSTEM PRIVATE "jemalloc/internal")
target_include_directories(_jemalloc SYSTEM PRIVATE "${JEMALLOC_SOURCE_DIR}/include")

configure_file(include/jemalloc/internal/jemalloc_internal_defs.h.in
    include/jemalloc/internal/jemalloc_internal_defs.h)
target_include_directories(_jemalloc SYSTEM PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}/include/jemalloc/internal")

target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_NO_PRIVATE_NAMESPACE)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_definitions(_jemalloc PRIVATE
        # -DJEMALLOC_DEBUG=1 # Disable debug due to possible assertion failures
        -DJEMALLOC_LOG=1)
endif ()

target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_PROF=1)

# for RTLD_NEXT
target_compile_options(_jemalloc PRIVATE -D_GNU_SOURCE)

set_target_properties(_jemalloc PROPERTIES COMPILE_FLAGS "-w -fPIC")

add_library(kvdk_extern_lib::jemalloc ALIAS _jemalloc)
