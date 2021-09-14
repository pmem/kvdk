/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2021 Intel Corporation
 */

#pragma once

#include <algorithm>
#include <assert.h>
#include <cstdint>

#include "hash_table.hpp"
#include "kvdk/engine.hpp"
#include "structures.hpp"
#include "utils.hpp"
#include "dlinked_list.hpp"

namespace KVDK_NAMESPACE 
{
    // Triplet of unique_kock. Owns all or none of three locks.
    // The locks may duplicate. Duplicate locks are ignored.
    // e.g. if third and first lock are same, third lock is ignored.
    // Locks with same address are considered duplicate.
    template<typename Lock>
    class UniqueLockTriplet
    {
    private:
        // Whether the lock has be acquired
        bool _acquired_first_;
        bool _acquired_second_;
        bool _acquired_third_;
        // If second or third lock duplicte with others
        // Duplicate locks are ignored
        bool _dup_second_;
        bool _dup_third_;
        // Actual locks
        std::unique_lock<Lock> _first_;
        std::unique_lock<Lock> _second_;
        std::unique_lock<Lock> _third_;

    public:
        // Empty UniqueLockTriplet. Needs to swap with other UniqueLockTriplet to gain actual Lock.
        UniqueLockTriplet() = default;

        UniqueLockTriplet(Lock& first, Lock& second, Lock& third, std::defer_lock_t) :
            _acquired_first_{ false },
            _acquired_second_{ false },
            _acquired_third_{ false },
            _dup_second_{ false },
            _dup_third_{ false }
        {
            _first_ = std::unique_lock<Lock>{ first, std::defer_lock };
            if (&second != &first)
            {
                _second_ = std::unique_lock<Lock>{ second, std::defer_lock };
            }
            else
            {
                _dup_second_ = true;
            }
            if (&third != &first && &third != &second)
            {
                _third_ = std::unique_lock<Lock>{ _dummy_third_, std::defer_lock };
            }
            else
            {
                _dup_third_ = true;
            }
        }

        UniqueLockTriplet(UniqueLockTriplet&& other) :
            _acquired_first_{ other._acquired_first_ },
            _acquired_second_{ other._acquired_second_ },
            _acquired_third_{ other._acquired_third_ },
            _dup_second_{ other._dup_second_ },
            _dup_third_{ other._dup_third_ },
            _first_{ std::move(other._first_) },
            _second_{ std::move(other._second_) },
            _third_{ std::move(other._third_) }
        {
        }

        ~UniqueLockTriplet()
        {
            if (OwnsLocks())
            {
                Unlock();
            }            
        }

        Swap(UniqueLockTriplet& other)
        {
            std::swap(_acquired_first_, other._acquired_first_);
            std::swap(_acquired_second_, other._acquired_second_);
            std::swap(_acquired_third_, other._acquired_third_);
            std::swap(_dup_second_, other._dup_second_);
            std::swap(_dup_third_, other._dup_third_);
            _first_.swap(other._first_);
            _second_.swap(other._second_);
            _third_.swap(other._third_);
        }

        bool TryLockAll()
        {
            if (_AcquiredNone_() && _OwnsNone_())
            {
                _acquired_first_ = _first_.try_lock();
                _acquired_second_ = _dup_second_ || _second_.try_lock();
                _acquired_third_ = _dup_third_ || _third_.try_lock();
                if (_AcquiredAll_())
                {
                    return true;
                }
                else
                {
                    if (_acquired_first_) 
                    {
                        _first_.unlock();
                        _acquired_first_ = false;
                    }
                    if (!_dup_second_ && _acquired_second_) 
                    {
                        _second_.unlock();
                        _acquired_second_ = false;
                    }
                    if (!_dup_third_ && _acquired_third_) 
                    {
                        _third_.unlock();
                        _acquired_third_ = false;
                    }
                    return false;
                }
            }
            else
            {
                throw std::runtime_error{"Invalid internal state of UniqueLockTriplet, already owns lock but TryLockAll() called!"};
            }
        }

        void LockAll()
        {
            while(!TryLockAll())
            {
                // Is this necessary?
                _mm_pause();
            }
        }

        void Unlock()
        {
            if (_AcquiredAll_() && _OwnsAll_())
            {
                _first_.unlock();
                _acquired_first_ = false;
                _second_.unlock();
                _acquired_second_ = false;
                _third_.unlock();
                _acquired_third_ = false;
            }
            else if (_AcquiredNone_() && _OwnsNone_())
            {
                throw std::runtime_error{"Trying to Unlock locks not acquired yet in UniqueLockTriplet!"};
            }
            else
            {
                throw std::runtime_error{"Invalid internal state of UniqueLockTriplet, not all or none locks acquired when calling Unlock()."};
            }
        }

        bool OwnsLocks()
        {
            if (_AcquiredAll_() && _OwnsAll_())
            {
                return true;
            }
            else if (_AcquiredNone_() && _OwnsNone_())
            {
                return false;
            }
            else
            {
                throw std::runtime_error{"Invalid internal state of UniqueLockTriplet, not all or none locks acquired when calling OwnsLocks()."};
            }
        }

    private:
        bool _OwnsAll_()
        {
            bool owns = true;
            owns = owns && _first_.owns_lock();
            owns = owns && (_dup_second_ || _second_.owns_lock());
            owns = owns && (_dup_third_ || _third_.owns_lock());
            return owns;
        }

        bool _OwnsNone_()
        {
            bool owns = false;
            owns = owns || !_first_.owns_lock();
            owns = owns || (!_dup_second_ && !_second_.owns_lock());
            owns = owns || (!_dup_third_ && !_third_.owns_lock());
            return owns;
        }

        bool _AcquiredAll_()
        {
            return _acquired_first_ && _acquired_second_ && _acquired_third_;
        }

        bool _AcquiredNone_()
        {
            return !_acquired_first_ && !_acquired_second_ && !_acquired_third_;
        }

    };

    /// UnorderedCollection is stored in DRAM, indexed by HashTable
    /// A Record DlistRecord is stored in PMem,
    /// whose key is the name of the UnorderedCollection
    /// and value holds the id of the Collection 
    /// prev and next pointer holds the head and tail of DLinkedList for recovery
    class UnorderedCollection : public PersistentList, public std::enable_shared_from_this<UnorderedCollection>
    {
    private:
        /// For allocation and mapping
        std::shared_ptr<PMEMAllocator> _sp_pmem_allocator_;
        /// For locking, locking only
        std::shared_ptr<HashTable> _sp_hash_table_;

        DLDataEntry* _pmp_dlist_record_;
        std::shared_ptr<DLinkedList> _sp_dlinked_list_;
        std::string _name_;
        std::uint64_t _id_;

        friend class UnorderedIterator;

    public:
        /// Create UnorderedCollection and persist it on PMem
        UnorderedCollection 
        (
            std::shared_ptr<PMEMAllocator> sp_pmem_allocator,
            std::shared_ptr<HashTable> sp_hash_table,
            std::string const& name,
            std::uint64_t id,
            std::uint64_t timestamp = 0ULL
        )
        try :
            _sp_pmem_allocator_{ sp_pmem_allocator },
            _sp_hash_table_{ sp_hash_table },
            _pmp_dlist_record_{ nullptr },
            _sp_dlinked_list_{ std::make_shared<DLinkedList>(sp_pmem_allocator, timestamp, name, _ID2View_(id)) },
            _name_{ name },
            _id_{ id }
        {
            auto space_list_record = _sp_pmem_allocator_->Allocate(sizeof(DLDataEntry) + _name_.size() + sizeof(decltype(_id_)));
            if (space_list_record.size == 0)
            {
                DLinkedList::Deallocate(_sp_dlinked_list_->Head());
                DLinkedList::Deallocate(_sp_dlinked_list_->Tail());
                _sp_dlinked_list_->_pmp_head_ = nullptr;
                _sp_dlinked_list_->_pmp_tail_ = nullptr;
                throw std::bad_alloc{};
            }
            std::uint64_t offset_list_record = space_list_record.space_entry.offset;
            void* pmp_list_record = _sp_pmem_allocator_->offset2addr(offset_list_record);
            DLDataEntry entry_list_record;  // Set up entry with meta
            {
                entry_list_record.timestamp = timestamp;
                entry_list_record.type = DataEntryType::DlistRecord;
                entry_list_record.k_size = _name_.size();
                entry_list_record.v_size = sizeof(decltype(_id_));

                // checksum can only be calculated with complete meta
                entry_list_record.header.b_size = space_list_record.size;
                entry_list_record.header.checksum = DLinkedList::_CheckSum_(entry_list_record, _name_, _ID2View_(_id_));

                entry_list_record.prev = _sp_dlinked_list_->Head()._GetOffset_();
                entry_list_record.next = _sp_dlinked_list_->Tail()._GetOffset_();
            }
            DLinkedList::_PersistRecord_(pmp_list_record, entry_list_record, _name_, _ID2View_(_id_));
        }
        catch (std::bad_alloc const& ex)
        {
            std::cerr << ex.what() << std::endl;
            std::cerr << "Fail to create UnorderedCollection object!" << std::endl;
            throw;
        }

        /// Recover UnorderedCollection from DLIST_RECORD
        UnorderedCollection
        (
            std::shared_ptr<PMEMAllocator> sp_pmem_allocator,
            std::shared_ptr<HashTable> sp_hash_table,
            DLDataEntry* pmp_dlist_record
        ) : 
            _sp_pmem_allocator_{ sp_pmem_allocator },
            _sp_hash_table_{ sp_hash_table },
            _sp_dlinked_list_
            { 
                std::make_shared<DLinkedList>
                (
                    _sp_pmem_allocator_->offset2addr(pmp_dlist_record->prev),
                    _sp_pmem_allocator_->offset2addr(pmp_dlist_record->next),
                    _sp_pmem_allocator_
                )
            },
            _name_{ pmp_dlist_record->Key() },
            _id_{ _View2ID_(pmp_dlist_record->Value()) }
        {
        }

        UnorderedIterator First()
        {
            UnorderedIterator iter{ shared_from_this() };
            iter.SeekToFirst();
            return iter;
        }

        UnorderedIterator Last()
        {
            UnorderedIterator iter{ shared_from_this() };
            iter.SeekToLast();
            return iter;
        }

        UnorderedIterator EmplaceBefore
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
            DLinkedList::Iterator iter_prev{ iter._iterator_internal_ }; --iter_prev;
            UniqueLockTriplet<SpinMutex> locks{ _MakeUniqueLockTriplet2Nodes_(iter_prev, spin) };
            locks.LockAll();
            _sp_dlinked_list_->EmplaceBefore(iter._iterator_internal_, timestamp, key, value);
        }

        UnorderedIterator EmplaceAfter
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

        UnorderedIterator SwapEmplace
        (
            DLDataEntry* pmp,
            std::uint64_t timestamp,    // Timestamp can only be supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value,
            DataEntryType type,
            SpinMutex* spin = nullptr   // spin in Slot containing HashEntry to pmp
        )
        {
            // Validifying PMem address by constructing UnorderedIterator
            UnorderedIterator iter{ shared_from_this(), pmp };
            UniqueLockTriplet<SpinMutex> locks{ _MakeUniqueLockTriplet3Nodes_(iter._iterator_internal_, spin) };
            locks.LockAll();
            _sp_dlinked_list_->SwapEmplace(iter._iterator_internal_, timestamp, key, value, type);            
        }

        uint64_t id() override { return _id_; }

        std::string const& name() { return _name_; }

    private:
        inline static pmem::obj::string_view _ExtractKey_(pmem::obj::string_view internal_key)
        {
            constexpr size_t sz_id = sizeof(decltype(_id_));
            assert(sz_id < internal_key.size() && "internal_key does not has space for key");
            return pmem::obj::string_view(internal_key.data() + sz_id, internal_key.size() + sz_id);
        }

        inline static std::uint64_t _ExtractID_(pmem::obj::string_view internal_key)
        {
            std::uint64_t id;
            assert(sizeof(decltype(id)) <= internal_key.size() && "internal_key is smaller than the size of an id!");
            memcpy(&id, internal_key.data(), sizeof(decltype(id)));
            return id;
        }

        inline static std::string _MakeInternalKey_(std::uint64_t id, pmem::obj::string_view key)
        {
            return std::string{_ID2View_(id)} + std::string{ key };
        }

        inline static pmem::obj::string_view _ID2View_(std::uint64_t id)
        {
            // Thread local copy to prevent variable destruction
            thread_local uint64_t id_copy = id;
            return pmem::obj::string_view{ reinterpret_cast<char*>(&id_copy), sizeof(decltype(id_copy)) };
        }

        inline static std::uint64_t _View2ID_(pmem::obj::string_view view)
        {
            std::uint64_t id;
            assert(sizeof(decltype(id)) == view.size() && "id_view does not match the size of an id!");
            memcpy(&id, view.data(), sizeof(decltype(id)));
            return id;
        }

        /// Make UniqueLockTriplet<SpinMutex> to lock adjacent three nodes, not locked yet.
        /// Also accepts UnorderedIterator by implicit casting
        UniqueLockTriplet<SpinMutex> _MakeUniqueLockTriplet3Nodes_(DLinkedList::Iterator iter_mid, SpinMutex* spin_mid = nullptr)
        {
            DLinkedList::Iterator iter_prev{ iter_mid }; --iter_prev;
            DLinkedList::Iterator iter_next{ iter_mid }; ++iter_next;

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

        /// Make UniqueLockTriplet<SpinMutex> to lock adjacent two nodes between which the new node is to be emplaced
        /// Also locks the slot for new node
        UniqueLockTriplet<SpinMutex> _MakeUniqueLockTriplet2Nodes_(DLinkedList::Iterator iter_prev, SpinMutex* spin_new)
        {
            DLinkedList::Iterator iter_next{ iter_prev }; ++iter_next;

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
    
    };

    class UnorderedIterator : public Iterator 
    {
    private:
        /// shared pointer to pin the UnorderedCollection
        std::shared_ptr<UnorderedCollection> _sp_coll_;
        DLinkedList::Iterator _iterator_internal_;
        bool _valid_;

        friend class UnorderedCollection;

    public:
        /// Construct UnorderedIterator and SeekToFirst().
        UnorderedIterator(std::shared_ptr<UnorderedCollection> sp_coll) :
            _sp_coll_{ sp_coll },
            _iterator_internal_{ nullptr },
            _valid_{ false }
        {
        }

        UnorderedIterator(std::shared_ptr<UnorderedCollection> sp_coll, DLDataEntry* pmp) :
            _sp_coll_{ sp_coll },
            _iterator_internal_{ _sp_coll_->_sp_dlinked_list_, pmp },
            _valid_{ false }
        {
            bool valid_pmp = false;
            DataEntryType type_pmp = static_cast<DataEntryType>(pmp->type);
            valid_pmp = valid_pmp || (type_pmp == DataEntryType::DlistHeadRecord);
            valid_pmp = valid_pmp || (type_pmp == DataEntryType::DlistTailRecord);
            valid_pmp = valid_pmp || ((type_pmp == DataEntryType::DlistDataRecord || type_pmp == DataEntryType::DlistDeleteRecord) && UnorderedCollection::_ExtractID_(pmp->Key()) == sp_coll->id());
            if(valid_pmp)
            {
                _valid_ = (pmp->type == DataEntryType::DlistDataRecord);
                return;
            }
            throw std::runtime_error{ "PMem pointer does not point to a valid Record belonging to the UnorderedCollection" };
        }

        virtual void Seek(std::string const& key)
        {
            throw std::runtime_error{ "Seek() not implemented for UnorderedIterator!" };
        }

        virtual void SeekToFirst() override
        {
            _iterator_internal_ = _sp_coll_->_sp_dlinked_list_->Head();
            _Next_();
        }

        virtual void SeekToLast() override
        {
            _iterator_internal_ = _sp_coll_->_sp_dlinked_list_->Tail();
            _Prev_();
        }

        virtual bool Valid() override
        {
            return _valid_;
        }

        virtual bool Next() override 
        {
            _Next_();
            return _valid_;
        }

        virtual bool Prev() override
        {
            _Prev_();
            return _valid_;
        }

        virtual std::string Key() override 
        {
            if (!Valid())
            {
                return "";
            }
            auto view_key = UnorderedCollection::_ExtractKey_(_iterator_internal_->Key());
            return std::string(view_key.data(), view_key.size());
        }

        virtual std::string Value() override 
        {
            if (!Valid())
            {
                return "";
            }
            auto view_value = _iterator_internal_->Value();
            return std::string(view_value.data(), view_value.size());
        }


    private:
        // Precede to next DLIST_DATA_RECORD, can start from head
        void _Next_()
        {
            assert(_iterator_internal_.valid());
            assert(_iterator_internal_->type != DataEntryType::DlistTailRecord);
            ++_iterator_internal_;
            while (_iterator_internal_.valid())
            {
                _valid_ = false;
                switch (_iterator_internal_->type)
                {
                case DataEntryType::DlistDataRecord:
                    _valid_ = true;
                    return;
                case DataEntryType::DlistDeleteRecord:
                    ++_iterator_internal_;
                    continue;
                case DataEntryType::DlistTailRecord:
                    return;
                default:
                    break;
                }
                throw std::runtime_error{ "UnorderedCollection::Iterator::_Next_() fails!" };
            }
        }

        void _Prev_()
        {
            assert(_iterator_internal_.valid());
            assert(_iterator_internal_->type != DataEntryType::DlistHeadRecord);
            --_iterator_internal_;
            while (_iterator_internal_.valid())
            {
                _valid_ = false;
                switch (_iterator_internal_->type)
                {
                case DataEntryType::DlistDataRecord:
                    _valid_ = true;
                    return;
                case DataEntryType::DlistDeleteRecord:
                    --_iterator_internal_;
                    continue;
                case DataEntryType::DlistHeadRecord:
                    return;
                default:
                    break;
                }
                throw std::runtime_error{ "UnorderedCollection::Iterator::_Prev_() fails!" };
            }
        }

    };
} // namespace KVDK_NAMESPACE
