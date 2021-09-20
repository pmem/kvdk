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
        _id_{ id },
        _time_stamp_{ timestamp }
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
        throw ex;
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
        _id_{ _View2ID_(pmp_dlist_record->Value()) },
        _time_stamp_{ pmp_dlist_record->timestamp }
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

    EmplaceReturn UnorderedCollection::EmplaceBefore
    (
        DLDataEntry* pmp,
        std::uint64_t timestamp,  
        pmem::obj::string_view const key,
        pmem::obj::string_view const value,
        DataEntryType type,
        std::unique_lock<SpinMutex> const& lock
    )
    {
        _CheckUserSuppliedPmp_(pmp);
        DlistIterator iter_prev{_dlinked_list_._sp_pmem_allocator_, pmp}; --iter_prev;
        DlistIterator iter_next{_dlinked_list_._sp_pmem_allocator_, pmp};
        EmplaceReturn ret = _EmplaceBetween_(iter_prev._pmp_curr_, iter_next._pmp_curr_, timestamp, key, value, type, lock);
        return ret;
    }

    EmplaceReturn UnorderedCollection::EmplaceAfter
    (
        DLDataEntry* pmp,
        std::uint64_t timestamp,   
        pmem::obj::string_view const key,
        pmem::obj::string_view const value,
        DataEntryType type,
        std::unique_lock<SpinMutex> const& lock
    )
    {
        _CheckUserSuppliedPmp_(pmp);
        DlistIterator iter_prev{_dlinked_list_._sp_pmem_allocator_, pmp};
        DlistIterator iter_next{_dlinked_list_._sp_pmem_allocator_, pmp}; ++iter_next;

        EmplaceReturn ret = _EmplaceBetween_(iter_prev._pmp_curr_, iter_next._pmp_curr_, timestamp, key, value, type, lock);
        return ret;
    }

    EmplaceReturn UnorderedCollection::EmplaceFront
    (
        std::uint64_t timestamp, 
        pmem::obj::string_view const key,
        pmem::obj::string_view const value,
        DataEntryType type,
        std::unique_lock<SpinMutex> const& lock
    )
    {
        DlistIterator iter_prev{_dlinked_list_.Head()};
        DlistIterator iter_next{_dlinked_list_.Head()}; ++iter_next;

        EmplaceReturn ret = _EmplaceBetween_(iter_prev._pmp_curr_, iter_next._pmp_curr_, timestamp, key, value, type, lock);
        return ret;
    }

    EmplaceReturn UnorderedCollection::EmplaceBack
    (
        std::uint64_t timestamp, 
        pmem::obj::string_view const key,
        pmem::obj::string_view const value,
        DataEntryType type,
        std::unique_lock<SpinMutex> const& lock
    )
    {
        DlistIterator iter_prev{_dlinked_list_.Tail()}; --iter_prev;
        DlistIterator iter_next{_dlinked_list_.Tail()}; 

        EmplaceReturn ret = _EmplaceBetween_(iter_prev._pmp_curr_, iter_next._pmp_curr_, timestamp, key, value, type, lock);
        return ret;
    }

    /// key is also checked to match old key
    EmplaceReturn UnorderedCollection::SwapEmplace
    (
        DLDataEntry* pmp,
        std::uint64_t timestamp, 
        pmem::obj::string_view const key,
        pmem::obj::string_view const value,
        DataEntryType type,
        std::unique_lock<SpinMutex> const& lock
    )
    {
        _CheckUserSuppliedPmp_(pmp);
        DlistIterator iter_prev{_dlinked_list_._sp_pmem_allocator_, pmp}; --iter_prev;
        DlistIterator iter_next{_dlinked_list_._sp_pmem_allocator_, pmp}; ++iter_next;

        EmplaceReturn ret = _EmplaceBetween_(iter_prev._pmp_curr_, iter_next._pmp_curr_, timestamp, key, value, type, lock, true);
        ret.offset_old = _dlinked_list_._sp_pmem_allocator_->addr2offset(pmp);
        return ret;
    }

    EmplaceReturn UnorderedCollection::_EmplaceBetween_
    (
        DLDataEntry* pmp_prev,
        DLDataEntry* pmp_next,
        std::uint64_t timestamp, 
        pmem::obj::string_view const key,
        pmem::obj::string_view const value,
        DataEntryType type,
        std::unique_lock<SpinMutex> const& lock,    // lock to prev or next or newly inserted, passed in and out.
        bool is_swap_emplace                  
    )
    {
        _CheckLock_(lock);
        _CheckEmplaceType_(type);
        
        DlistIterator iter_prev{ _dlinked_list_._sp_pmem_allocator_, pmp_prev };
        DlistIterator iter_next{ _dlinked_list_._sp_pmem_allocator_, pmp_next };        

        // This locks may be invalidified after other threads insert another node!
        std::string internal_key = GetInternalKey(key);
        SpinMutex* spin = lock.mutex();
        SpinMutex* spin1 = _GetMutex_(iter_prev->Key());
        SpinMutex* spin2 = _GetMutex_(internal_key);
        SpinMutex* spin3 = _GetMutex_(iter_next->Key());
        
        std::unique_lock<SpinMutex> lock1;
        std::unique_lock<SpinMutex> lock2;
        std::unique_lock<SpinMutex> lock3;

        if (spin1 != spin)
        {
            lock1 = std::unique_lock<SpinMutex>{*spin1, std::try_to_lock};
            if (!lock1.owns_lock())
            {
                return EmplaceReturn{};
            }   
        }
        if (spin2 != spin && spin2 != spin1)
        {
            lock2 = std::unique_lock<SpinMutex>{*spin2, std::try_to_lock};
            if (!lock2.owns_lock())
            {
                return EmplaceReturn{};
            }   
        }
        if (spin3 != spin && spin3 != spin1 && spin3 != spin2)
        {
            lock3 = std::unique_lock<SpinMutex>{*spin3, std::try_to_lock};
            if (!lock3.owns_lock())
            {
                return EmplaceReturn{};
            }    
        }

        if (!is_swap_emplace)
        {
            bool has_other_thread_modified = false;
            DlistIterator iter_prev_copy{iter_prev};
            DlistIterator iter_next_copy(iter_next);
            has_other_thread_modified = has_other_thread_modified || (++iter_prev_copy != iter_next);
            has_other_thread_modified = has_other_thread_modified || (--iter_next_copy != iter_prev);
            if (has_other_thread_modified)
            {
                return EmplaceReturn{};
            }            
        }
        else
        {
            bool has_other_thread_modified = false;
            DlistIterator iter_prev_copy{iter_prev};
            DlistIterator iter_next_copy(iter_next);
            has_other_thread_modified = has_other_thread_modified || (++++iter_prev_copy != iter_next);
            has_other_thread_modified = has_other_thread_modified || (----iter_next_copy != iter_prev);
            if (has_other_thread_modified)
            {
                return EmplaceReturn{};
            }            
        }
        DlistIterator iter = _dlinked_list_._EmplaceBetween_(iter_prev, iter_next, timestamp, internal_key, value, type);
        
        return EmplaceReturn{iter._GetOffset_(), EmplaceReturn::FailOffset, true};          
    }

    // UniqueLockTriplet<SpinMutex> UnorderedCollection::_MakeUniqueLockTriplet3Nodes_(DlistIterator iter_mid, SpinMutex* spin_mid)
    // {
    //     DlistIterator iter_prev{ iter_mid }; --iter_prev;
    //     DlistIterator iter_next{ iter_mid }; ++iter_next;

    //     SpinMutex* p_spin_1 = _sp_hash_table_->GetHint(iter_prev->Key()).spin;
    //     SpinMutex* p_spin_2 = spin_mid ? spin_mid : _sp_hash_table_->GetHint(iter_mid->Key()).spin;
    //     SpinMutex* p_spin_3 = _sp_hash_table_->GetHint(iter_next->Key()).spin;

    //     UniqueLockTriplet<SpinMutex> unique_lock_triplet
    //     {
    //         *p_spin_1,
    //         *p_spin_2,
    //         *p_spin_3,
    //         std::defer_lock
    //     };
    //     return unique_lock_triplet;
    // }

    // UniqueLockTriplet<SpinMutex> UnorderedCollection::_MakeUniqueLockTriplet2Nodes_(DlistIterator iter_prev, SpinMutex* spin_new)
    // {
    //     DlistIterator iter_next{ iter_prev }; ++iter_next;

    //     SpinMutex* p_spin_1 = _sp_hash_table_->GetHint(iter_prev->Key()).spin;
    //     SpinMutex* p_spin_2 = spin_new;
    //     SpinMutex* p_spin_3 = _sp_hash_table_->GetHint(iter_next->Key()).spin;

    //     UniqueLockTriplet<SpinMutex> unique_lock_triplet
    //     {
    //         *p_spin_1,
    //         *p_spin_2,
    //         *p_spin_3,
    //         std::defer_lock
    //     };
    //     return unique_lock_triplet;
    // }

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
            throw std::runtime_error{"Explicit Constructor of UnorderedIterator does not accept nullptr!"};
        }
        _sp_coll_->_CheckUserSuppliedPmp_(pmp);
        _valid_ = (pmp->type == DataEntryType::DlistDataRecord);
        return;
    }

    void UnorderedIterator::_Next_()
    {
        if(!_iterator_internal_.valid())
        {
            goto FATAL_FAILURE;
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
            goto FATAL_FAILURE;
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
                goto FATAL_FAILURE;
            }
            }          
        }
    FATAL_FAILURE:
        throw std::runtime_error{ "UnorderedIterator::_Next_() fails!" };
    }

    void UnorderedIterator::_Prev_()
    {
        if(!_iterator_internal_.valid())
        {
            goto FATAL_FAILURE;
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
            goto FATAL_FAILURE;
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
                goto FATAL_FAILURE;
            }
            }          
        }
    FATAL_FAILURE:
        throw std::runtime_error{ "UnorderedCollection::DlistIterator::_Prev_() fails!" };
    }

}
