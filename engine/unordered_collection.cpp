#include "unordered_collection.hpp"

namespace KVDK_NAMESPACE 
{
    UnorderedCollection::UnorderedCollection
    (
        std::shared_ptr<PMEMAllocator> sp_pmem_allocator,
        std::shared_ptr<HashTable> sp_hash_table,
        std::string const& name,
        std::uint64_t id,
        std::uint64_t timestamp
    )
    try :
        _sp_hash_table_{ sp_hash_table },
        _pmp_dlist_record_{ nullptr },
        _dlinked_list_{ sp_pmem_allocator, timestamp, _ID2View_(id), pmem::obj::string_view{""} },
        _name_{ name },
        _id_{ id }
    {
    {
        auto space_list_record = _dlinked_list_._sp_pmem_allocator_->Allocate(sizeof(DLDataEntry) + _name_.size() + sizeof(decltype(_id_)));
        if (space_list_record.size == 0)
        {
            DLinkedList::Deallocate(_dlinked_list_.Head());
            DLinkedList::Deallocate(_dlinked_list_.Tail());
            _dlinked_list_._pmp_head_ = nullptr;
            _dlinked_list_._pmp_tail_ = nullptr;
            throw std::bad_alloc{};
        }
        std::uint64_t offset_list_record = space_list_record.space_entry.offset;
        void* pmp_list_record = _dlinked_list_._sp_pmem_allocator_->offset2addr(offset_list_record);
        DLDataEntry entry_list_record;  // Set up entry with meta
        {
            entry_list_record.timestamp = timestamp;
            entry_list_record.type = DataEntryType::DlistRecord;
            entry_list_record.k_size = _name_.size();
            entry_list_record.v_size = sizeof(decltype(_id_));

            // checksum can only be calculated with complete meta
            entry_list_record.header.b_size = space_list_record.size;
            entry_list_record.header.checksum = DLinkedList::_CheckSum_(entry_list_record, _name_, _ID2View_(_id_));

            entry_list_record.prev = _dlinked_list_.Head()._GetOffset_();
            entry_list_record.next = _dlinked_list_.Tail()._GetOffset_();
        }
        DLinkedList::_PersistRecord_(pmp_list_record, entry_list_record, _name_, _ID2View_(_id_));
        _pmp_dlist_record_ = static_cast<DLDataEntry*>(pmp_list_record);
    }
    }
    catch (std::bad_alloc const& ex)
    {
        std::cerr << ex.what() << std::endl;
        std::cerr << "Fail to create UnorderedCollection object!" << std::endl;
        throw;
    }

    UnorderedCollection::UnorderedCollection
    (
        std::shared_ptr<PMEMAllocator> sp_pmem_allocator,
        std::shared_ptr<HashTable> sp_hash_table,
        DLDataEntry* pmp_dlist_record
    ) : 
        _sp_hash_table_{ sp_hash_table },
        _pmp_dlist_record_{ pmp_dlist_record },
        _dlinked_list_
        { 
            sp_pmem_allocator,
            _GetPmpPrev_(pmp_dlist_record),
            _GetPmpNext_(pmp_dlist_record)
        },
        _name_{ pmp_dlist_record->Key() },
        _id_{ _View2ID_(pmp_dlist_record->Value()) }
    {
    }
    
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
        SpinMutex* spin             // spin in Slot containing HashEntry to new node
    )
    {
        if (spin == nullptr)
        {
            throw std::runtime_error{"Must provide a spin to lock new node in UnorderedCollection::EmplaceBefore()"};
        }
        
        // Validifying PMem address by constructing UnorderedIterator
        UnorderedIterator iter{ shared_from_this(), pmp };
        DlistIterator iter_prev{ iter._iterator_internal_ }; --iter_prev;
        UniqueLockTriplet<SpinMutex> locks{ _MakeUniqueLockTriplet2Nodes_(iter_prev, spin) };
        locks.LockAll();
        _dlinked_list_.EmplaceBefore(iter._iterator_internal_, timestamp, key, value, type);
    }

    UnorderedIterator UnorderedCollection::EmplaceAfter
    (
        DLDataEntry* pmp,
        std::uint64_t timestamp,    // Timestamp can only be supplied by caller
        pmem::obj::string_view const key,
        pmem::obj::string_view const value,
        DataEntryType type,
        SpinMutex* spin             // spin in Slot containing HashEntry to new node
    )
    {
        if (spin == nullptr)
        {
            throw std::runtime_error{"Must provide a spin to lock new node in UnorderedCollection::EmplaceBefore()"};
        }

        // Validifying PMem address by constructing UnorderedIterator
        UnorderedIterator iter{ shared_from_this(), pmp };
        UniqueLockTriplet<SpinMutex> locks{ _MakeUniqueLockTriplet2Nodes_(iter._iterator_internal_, spin) };
        locks.LockAll();
        _dlinked_list_.EmplaceAfter(iter._iterator_internal_, timestamp, key, value, type);
    }

    UnorderedIterator UnorderedCollection::EmplaceFront
    (
        std::uint64_t timestamp,    // Timestamp can only be supplied by caller
        pmem::obj::string_view const key,
        pmem::obj::string_view const value,
        DataEntryType type,
        SpinMutex* spin             // spin in Slot containing HashEntry to new node
    )
    {
        if (spin == nullptr)
        {
            throw std::runtime_error{"Must provide a spin to lock new node in UnorderedCollection::EmplaceFront()"};
        }

        DlistIterator iter = _dlinked_list_.Head();
        UniqueLockTriplet<SpinMutex> locks{ _MakeUniqueLockTriplet2Nodes_(iter, spin) };
        locks.LockAll();
        _dlinked_list_.EmplaceFront(timestamp, key, value, type);
    }

    UnorderedIterator UnorderedCollection::EmplaceBack
    (
        std::uint64_t timestamp,    // Timestamp can only be supplied by caller
        pmem::obj::string_view const key,
        pmem::obj::string_view const value,
        DataEntryType type,
        SpinMutex* spin             // spin in Slot containing HashEntry to new node
    )
    {
        if (spin == nullptr)
        {
            throw std::runtime_error{"Must provide a spin to lock new node in UnorderedCollection::EmplaceBack()"};
        }

        DlistIterator iter = _dlinked_list_.Tail();
        UniqueLockTriplet<SpinMutex> locks{ _MakeUniqueLockTriplet2Nodes_(iter, spin) };
        locks.LockAll();
        _dlinked_list_.EmplaceBack(timestamp, key, value, type);
    }

    UnorderedIterator UnorderedCollection::SwapEmplace
    (
        DLDataEntry* pmp,
        std::uint64_t timestamp,    // Timestamp can only be supplied by caller
        pmem::obj::string_view const key,
        pmem::obj::string_view const value,
        DataEntryType type,
        SpinMutex* spin             // spin in Slot containing HashEntry to pmp(same Slot as new node)
    )
    {
        // Validifying PMem address by constructing UnorderedIterator
        UnorderedIterator iter{ shared_from_this(), pmp };
        UniqueLockTriplet<SpinMutex> locks{ _MakeUniqueLockTriplet3Nodes_(iter._iterator_internal_, spin) };
        locks.LockAll();
        _dlinked_list_.SwapEmplace(iter._iterator_internal_, timestamp, key, value, type);            
    }

    UniqueLockTriplet<SpinMutex> UnorderedCollection::_MakeUniqueLockTriplet3Nodes_(DlistIterator iter_mid, SpinMutex* spin_mid)
    {
        DlistIterator iter_prev{ iter_mid }; --iter_prev;
        DlistIterator iter_next{ iter_mid }; ++iter_next;

        SpinMutex* p_spin_1 = _sp_hash_table_->GetHint(iter_prev->Key()).spin;
        SpinMutex* p_spin_2 = spin_mid ? spin_mid : _sp_hash_table_->GetHint(iter_mid->Key()).spin;
        SpinMutex* p_spin_3 = _sp_hash_table_->GetHint(iter_next->Key()).spin;

        UniqueLockTriplet<SpinMutex> unique_lock_triplet
        {
            *p_spin_1,
            *p_spin_2,
            *p_spin_3,
            std::defer_lock
        };
        return unique_lock_triplet;
    }

    UniqueLockTriplet<SpinMutex> UnorderedCollection::_MakeUniqueLockTriplet2Nodes_(DlistIterator iter_prev, SpinMutex* spin_new)
    {
        DlistIterator iter_next{ iter_prev }; ++iter_next;

        SpinMutex* p_spin_1 = _sp_hash_table_->GetHint(iter_prev->Key()).spin;
        SpinMutex* p_spin_2 = spin_new;
        SpinMutex* p_spin_3 = _sp_hash_table_->GetHint(iter_next->Key()).spin;

        UniqueLockTriplet<SpinMutex> unique_lock_triplet
        {
            *p_spin_1,
            *p_spin_2,
            *p_spin_3,
            std::defer_lock
        };
        return unique_lock_triplet;
    }

}

namespace KVDK_NAMESPACE
{
    UnorderedIterator::UnorderedIterator(std::shared_ptr<UnorderedCollection> sp_coll) :
        _sp_coll_{ sp_coll },
        _iterator_internal_{ sp_coll->_dlinked_list_.Head() },
        _valid_{ false }
    {
    }

    UnorderedIterator::UnorderedIterator(std::shared_ptr<UnorderedCollection> sp_coll, DLDataEntry* pmp) :
        _sp_coll_{ sp_coll },
        _iterator_internal_{ _sp_coll_->_dlinked_list_._sp_pmem_allocator_, pmp },
        _valid_{ false }
    {
        if (!pmp)
        {
            goto UNORDEREDITERATOR_CONSTRUCTION_WITH_PMP_FAILURE;
        }       
        bool is_pmp_valid;
        switch (static_cast<DataEntryType>(pmp->type))
        {
        case DataEntryType::DlistHeadRecord:
        case DataEntryType::DlistTailRecord:
        case DataEntryType::DlistDataRecord:
        case DataEntryType::DlistDeleteRecord:
        {
            is_pmp_valid = true;
            break;
        }
        case DataEntryType::DlistRecord:
        default:
        {
            goto UNORDEREDITERATOR_CONSTRUCTION_WITH_PMP_FAILURE;
        }
        }
        is_pmp_valid = is_pmp_valid && _CheckID_(pmp);

        if(is_pmp_valid)
        {
            _valid_ = (pmp->type == DataEntryType::DlistDataRecord);
            return;
        }
    UNORDEREDITERATOR_CONSTRUCTION_WITH_PMP_FAILURE:
        throw std::runtime_error{ "PMem pointer does not point to a valid Record belonging to the UnorderedCollection" };
    }

    void UnorderedIterator::_Next_()
    {
        if(!_iterator_internal_.valid())
        {
            goto UNORDERED_ITERATOR_NEXT_FAILRURE;
        }
        switch (static_cast<DataEntryType>(_iterator_internal_->type))
        {
        case DataEntryType::DlistHeadRecord:
        case DataEntryType::DlistDataRecord:
        case DataEntryType::DlistDeleteRecord:
        {
            break;
        }
        case DataEntryType::DlistRecord:
        case DataEntryType::DlistTailRecord:        
        default:
        {
            goto UNORDERED_ITERATOR_NEXT_FAILRURE;
        }
        }

        ++_iterator_internal_;
        while (_iterator_internal_.valid())
        {
            _valid_ = false;
            switch (_iterator_internal_->type)
            {
            case DataEntryType::DlistDataRecord:
            {
                _valid_ = true;
                return;
            }
            case DataEntryType::DlistDeleteRecord:
            {
                _valid_ = false;
                ++_iterator_internal_;
                continue;
            }
            case DataEntryType::DlistTailRecord:
            {
                _valid_ = false;
                return;
            }
            case DataEntryType::DlistHeadRecord:
            case DataEntryType::DlistRecord:
            default:
            {
                goto UNORDERED_ITERATOR_NEXT_FAILRURE;
            }
            }          
        }
    UNORDERED_ITERATOR_NEXT_FAILRURE:
        throw std::runtime_error{ "UnorderedIterator::_Next_() fails!" };
    }

    void UnorderedIterator::_Prev_()
    {
        if(!_iterator_internal_.valid())
        {
            goto UNOERDERED_ITERATOR_PREV_FAILURE;
        }
        switch (static_cast<DataEntryType>(_iterator_internal_->type))
        {
        case DataEntryType::DlistTailRecord:        
        case DataEntryType::DlistDataRecord:
        case DataEntryType::DlistDeleteRecord:
        {
            break;
        }
        case DataEntryType::DlistHeadRecord:
        case DataEntryType::DlistRecord:
        default:
        {
            goto UNOERDERED_ITERATOR_PREV_FAILURE;
        }
        }

        ++_iterator_internal_;
        while (_iterator_internal_.valid())
        {
            _valid_ = false;
            switch (_iterator_internal_->type)
            {
            case DataEntryType::DlistDataRecord:
            {
                _valid_ = true;
                return;
            }
            case DataEntryType::DlistDeleteRecord:
            {
                _valid_ = false;
                --_iterator_internal_;
                continue;
            }
            case DataEntryType::DlistHeadRecord:
            {
                _valid_ = false;
                return;
            }
            case DataEntryType::DlistTailRecord:
            case DataEntryType::DlistRecord:
            default:
            {
                goto UNOERDERED_ITERATOR_PREV_FAILURE;
            }
            }          
        }
    UNOERDERED_ITERATOR_PREV_FAILURE:
        throw std::runtime_error{ "UnorderedCollection::DlistIterator::_Prev_() fails!" };
    }

}
