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
#include "kvdk/iterator.hpp"

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
                _third_ = std::unique_lock<Lock>{ third, std::defer_lock };
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

        UniqueLockTriplet& Swap(UniqueLockTriplet& other)
        {
            std::swap(_acquired_first_, other._acquired_first_);
            std::swap(_acquired_second_, other._acquired_second_);
            std::swap(_acquired_third_, other._acquired_third_);
            std::swap(_dup_second_, other._dup_second_);
            std::swap(_dup_third_, other._dup_third_);
            _first_.swap(other._first_);
            _second_.swap(other._second_);
            _third_.swap(other._third_);
            return *this;
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

    class UnorderedIterator;

    /// UnorderedCollection is stored in DRAM, indexed by HashTable
    /// A Record DlistRecord is stored in PMem,
    /// whose key is the name of the UnorderedCollection
    /// and value holds the ID of the Collection 
    /// prev and next pointer holds the head and tail of DLinkedList for recovery
    /// At runtime, an object of UnorderedCollection is recovered from
    /// the DlistRecord and then stored in HashTable.
    /// The DlistRecord is for recovery only and never visited again
    class UnorderedCollection : public PersistentList, public std::enable_shared_from_this<UnorderedCollection>
    {
    private:
        /// For allocation and mapping
        std::shared_ptr<PMEMAllocator> _sp_pmem_allocator_;
        /// For locking, locking only
        std::shared_ptr<HashTable> _sp_hash_table_;

        /// DlistRecord for recovering
        DLDataEntry* _pmp_dlist_record_;
        std::shared_ptr<DLinkedList> _sp_dlinked_list_;
        
        std::string _name_;
        std::uint64_t _id_;

        friend class UnorderedIterator;

    public:
        /// Create UnorderedCollection and persist it on PMem
        /// DlistHeadRecord and DlistTailRecord holds ID as key
        /// and empty string as value
        /// DlistRecord holds collection name as key
        /// and ID as value
        UnorderedCollection
        (
            std::shared_ptr<PMEMAllocator> sp_pmem_allocator,
            std::shared_ptr<HashTable> sp_hash_table,
            std::string const& name,
            std::uint64_t id,
            std::uint64_t timestamp = 0ULL
        );

        /// Recover UnorderedCollection from DLIST_RECORD
        UnorderedCollection
        (
            std::shared_ptr<PMEMAllocator> sp_pmem_allocator,
            std::shared_ptr<HashTable> sp_hash_table,
            DLDataEntry* pmp_dlist_record
        );

        /// Create UnorderedIterator and SeekToFirst()
        UnorderedIterator First();

        /// Create UnorderedIterator and SeekToLast()
        UnorderedIterator Last();

        /// EmplaceBefore a DlistRecord before pmp
        /// Runtime checking is done
        UnorderedIterator EmplaceBefore
        (
            DLDataEntry* pmp,
            std::uint64_t timestamp,    // Timestamp can only be supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value,
            DataEntryType type,
            SpinMutex* spin             // spin in Slot containing HashEntry to new node
        );

        UnorderedIterator EmplaceAfter
        (
            DLDataEntry* pmp,
            std::uint64_t timestamp,    // Timestamp can only be supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value,
            DataEntryType type,
            SpinMutex* spin             // spin in Slot containing HashEntry to new node
        );

        UnorderedIterator SwapEmplace
        (
            DLDataEntry* pmp,
            std::uint64_t timestamp,    // Timestamp can only be supplied by caller
            pmem::obj::string_view const key,
            pmem::obj::string_view const value,
            DataEntryType type,
            SpinMutex* spin = nullptr   // spin in Slot containing HashEntry to pmp(same Slot as new node)
        );

        inline uint64_t id() override { return _id_; }

        inline std::string const& name() { return _name_; }

        inline std::string GetInternalKey(pmem::obj::string_view key)
        {
            return _MakeInternalKey_(_id_, key);
        }

    private:
        inline static std::string _MakeInternalKey_(std::uint64_t id, pmem::obj::string_view key)
        {
            return std::string{_ID2View_(id)} + std::string{ key };
        }

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
        UniqueLockTriplet<SpinMutex> _MakeUniqueLockTriplet3Nodes_(DLinkedList::DlistIterator iter_mid, SpinMutex* spin_mid = nullptr);

        /// Make UniqueLockTriplet<SpinMutex> to lock adjacent two nodes between which the new node is to be emplaced
        /// Also locks the slot for new node
        UniqueLockTriplet<SpinMutex> _MakeUniqueLockTriplet2Nodes_(DLinkedList::DlistIterator iter_prev, SpinMutex* spin_new);

        inline DLDataEntry* _GetPmpPrev_(DLDataEntry* pmp)
        {
            return reinterpret_cast<DLDataEntry*>(_sp_pmem_allocator_->offset2addr(pmp->prev));
        }
    
        inline DLDataEntry* _GetPmpNext_(DLDataEntry* pmp)
        {
            return reinterpret_cast<DLDataEntry*>(_sp_pmem_allocator_->offset2addr(pmp->next));
        }
    
    };

    class UnorderedIterator final : public Iterator 
    {
    private:
        /// shared pointer to pin the UnorderedCollection
        std::shared_ptr<UnorderedCollection> _sp_coll_;
        /// DlistIterator does not ignore DlistDeleteRecord
        DLinkedList::DlistIterator _iterator_internal_;
        /// Whether the UnorderedIterator is at a DlistDataRecord
        bool _valid_;

        friend class UnorderedCollection;

    public:
        /// Construct UnorderedIterator of a certain UnorderedCollection
        /// The Iterator is invalid now.
        /// Must SeekToFirst() or SeekToLast() before use.
        UnorderedIterator(std::shared_ptr<UnorderedCollection> sp_coll);

        /// Construct UnorderedIterator of a certain UnorderedCollection
        /// pointing to a DLDataEntry belonging to this collection
        /// Runtime checking the type of this UnorderedIterator,
        /// which can be DlistDataRecord, DlistDeleteRecord, DlistHeadRecord and DlistTailRecord
        /// ID is also checked. Checking failure results in throwing runtime_error
        /// Valid() is true only if the iterator points to DlistDataRecord
        UnorderedIterator(std::shared_ptr<UnorderedCollection> sp_coll, DLDataEntry* pmp);

        /// UnorderedIterator currently does not support Seek to a key
        /// throw runtime_error directly
        virtual void Seek(std::string const& key) final override
        {
            throw std::runtime_error{ "Seek() not implemented for UnorderedIterator!" };
        }

        /// Seek to First DlistDataRecord if exists,
        /// otherwise Valid() will return false.
        virtual void SeekToFirst() final override
        {
            _iterator_internal_ = _sp_coll_->_sp_dlinked_list_->Head();
            _Next_();
        }

        /// Seek to Last DlistDataRecord if exists,
        /// otherwise Valid() will return false.
        virtual void SeekToLast() final override
        {
            _iterator_internal_ = _sp_coll_->_sp_dlinked_list_->Tail();
            _Prev_();
        }

        /// Valid() is true only if the UnorderedIterator points to a DlistDataRecord.
        /// DlistHeadRecord, DlistTailRecord and DlistDeleteRecord is considered invalid.
        /// User should always check Valid() before
        /// accessing data with Key() and Value()
        /// Iterating with Next() and Prev()
        inline virtual bool Valid() final override
        {
            return _valid_;
        }

        /// Try proceeding to next DlistDataRecord.
        /// User should check Valid() before accessing data.
        /// Calling Next() on invalid UnorderedIterator will do nothing.
        /// This prevents any further mistakes
        virtual void Next() final override 
        {
            if (Valid())
            {
                _Next_();
            }
            return;
        }

        /// Try proceeding to previous DlistDataRecord
        /// User should check Valid() before accessing data
        /// Calling Prev() on invalid UnorderedIterator will do nothing.
        /// This prevents any further mistakes
        virtual void Prev() final override
        {
            if (Valid())
            {
                _Prev_();
            }
            return;
        }

        /// return key in DlistDataRecord
        /// throw runtime_error if !Valid()
        inline virtual std::string Key() override 
        {
            if (!Valid())
            {
                throw std::runtime_error{"Accessing data with invalid UnorderedIterator!"};
            }
            auto view_key = UnorderedCollection::_ExtractKey_(_iterator_internal_->Key());
            return std::string(view_key.data(), view_key.size());
        }

        /// return value in DlistDataRecord
        /// throw runtime_error if !Valid()
        inline virtual std::string Value() override 
        {
            if (!Valid())
            {
                throw std::runtime_error{"Accessing data with invalid UnorderedIterator!"};
            }
            auto view_value = _iterator_internal_->Value();
            return std::string(view_value.data(), view_value.size());
        }


    private:
        // Proceed to next DlistDataRecord, can start from 
        // DlistHeadRecord, DlistDataRecord or DlistDeleteRecord
        // If reached DlistTailRecord, _valid_ is set to false and returns
        void _Next_();

        // Proceed to prev DlistDataRecord, can start from
        // DlistTailRecord, DlistDataRecord or DlistDeleteRecord
        // If reached DlistHeadRecord, _valid_ is set to false and returns
        void _Prev_();

        /// Treat pmp as PMem pointer to a
        /// DlistHeadRecord, DlistTailRecord, DlistDataRecord, DlistDeleteRecord
        /// Access ID and check whether pmp belongs to current UnorderedCollection
        inline bool _CheckID_(DLDataEntry* pmp)
        {
            return (UnorderedCollection::_ExtractID_(pmp->Key()) == _sp_coll_->id());
        }

    };
    
} // namespace KVDK_NAMESPACE

