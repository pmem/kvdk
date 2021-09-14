#include "unordered_collection.hpp"

namespace KVDK_NAMESPACE 
{
    UnorderedIterator UnorderedCollection::First()
    {
        UnorderedIterator iter{ shared_from_this() };
        iter.SeekToFirst();
        return iter;
    }

    UnorderedIterator UnorderedCollection::Last()
    {
        UnorderedIterator iter{ shared_from_this() };
        iter.SeekToLast();
        return iter;
    }

    UnorderedIterator UnorderedCollection::EmplaceBefore
    (
        DLDataEntry* pmp,
        std::uint64_t timestamp,    // Timestamp can only be supplied by caller
        pmem::obj::string_view const key,
        pmem::obj::string_view const value,
        DataEntryType type,
        SpinMutex* spin             // spin in Slot containing HashEntry to pmp
    )
    {
        assert(spin);
        // Validifying PMem address by constructing UnorderedIterator
        UnorderedIterator iter{ shared_from_this(), pmp };
        DLinkedList::DlistIterator iter_prev{ iter._iterator_internal_ }; --iter_prev;
        UniqueLockTriplet<SpinMutex> locks{ _MakeUniqueLockTriplet2Nodes_(iter_prev, spin) };
        locks.LockAll();
        _sp_dlinked_list_->EmplaceBefore(iter._iterator_internal_, timestamp, key, value);
    }

    UnorderedIterator UnorderedCollection::EmplaceAfter
    (
        DLDataEntry* pmp,
        std::uint64_t timestamp,    // Timestamp can only be supplied by caller
        pmem::obj::string_view const key,
        pmem::obj::string_view const value,
        DataEntryType type,
        SpinMutex* spin             // spin in Slot containing HashEntry to pmp
    )
    {
        assert(spin);
        // Validifying PMem address by constructing UnorderedIterator
        UnorderedIterator iter{ shared_from_this(), pmp };
        UniqueLockTriplet<SpinMutex> locks{ _MakeUniqueLockTriplet2Nodes_(iter._iterator_internal_, spin) };
        locks.LockAll();
        _sp_dlinked_list_->EmplaceAfter(iter._iterator_internal_, timestamp, key, value);
    }

    UnorderedIterator UnorderedCollection::SwapEmplace
    (
        DLDataEntry* pmp,
        std::uint64_t timestamp,    // Timestamp can only be supplied by caller
        pmem::obj::string_view const key,
        pmem::obj::string_view const value,
        DataEntryType type,
        SpinMutex* spin             // spin in Slot containing HashEntry to pmp
    )
    {
        // Validifying PMem address by constructing UnorderedIterator
        UnorderedIterator iter{ shared_from_this(), pmp };
        UniqueLockTriplet<SpinMutex> locks{ _MakeUniqueLockTriplet3Nodes_(iter._iterator_internal_, spin) };
        locks.LockAll();
        _sp_dlinked_list_->SwapEmplace(iter._iterator_internal_, timestamp, key, value, type);            
    }
}

