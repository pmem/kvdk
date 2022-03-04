#pragma once
#ifndef KVDK_PMEM_OBJECT
#define KVDK_PMEM_OBJECT

#include <cstdint>

namespace KVDK_NAMESPACE
{
    // To Persist a PMemObject instance on PMem,
    // 1. size, checksum and MutableField mut is first persisted,
    // 2. ImmutableField immut is persisted.
    // If system crashes before step 2, checksum will fail
    // 
    template<typename MutableField, typename ImmutableField>
    struct PMemObject
    {

        // size space occupied by PMemObject, may be larger than actual size.
        std::uint32_t size;
        // checksum to validify ImmutableField immut
        std::uint32_t checksum;
        // MutableField must be modified atomically and persisted by user
        MutableField mut;
        // ImmutableField is validated on recovery by checksum
        ImmutableField immut;
    };

} // KVDK_NAMESPACE

#endif // KVDK_PMEM_OBJECT