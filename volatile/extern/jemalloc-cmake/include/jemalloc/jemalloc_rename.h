/*
 * Name mangling for public symbols is controlled by --with-mangling and
 * --with-jemalloc-prefix.  With default settings the je_ prefix is stripped by
 * these macro definitions.
 */
#ifndef JEMALLOC_NO_RENAME
#  define je_aligned_alloc je_kvdk_aligned_alloc
#  define je_calloc je_kvdk_calloc
#  define je_dallocx je_kvdk_dallocx
#  define je_free je_kvdk_free
#  define je_mallctl je_kvdk_mallctl
#  define je_mallctlbymib je_kvdk_mallctlbymib
#  define je_mallctlnametomib je_kvdk_mallctlnametomib
#  define je_malloc je_kvdk_malloc
#  define je_malloc_conf je_kvdk_malloc_conf
#  define je_malloc_conf_2_conf_harder je_kvdk_malloc_conf_2_conf_harder
#  define je_malloc_message je_kvdk_malloc_message
#  define je_malloc_stats_print je_kvdk_malloc_stats_print
#  define je_malloc_usable_size je_kvdk_malloc_usable_size
#  define je_mallocx je_kvdk_mallocx
#  define je_smallocx_36366f3c4c741723369853c923e56999716398fc je_kvdk_smallocx_36366f3c4c741723369853c923e56999716398fc
#  define je_nallocx je_kvdk_nallocx
#  define je_posix_memalign je_kvdk_posix_memalign
#  define je_rallocx je_kvdk_rallocx
#  define je_realloc je_kvdk_realloc
#  define je_sallocx je_kvdk_sallocx
#  define je_sdallocx je_kvdk_sdallocx
#  define je_xallocx je_kvdk_xallocx
#  define je_memalign je_kvdk_memalign
#  define je_valloc je_kvdk_valloc
#  define je_pvalloc je_kvdk_pvalloc
#endif
