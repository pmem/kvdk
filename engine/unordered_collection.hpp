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
    template<typename Lock>
    class UniqueLockTriplet
    {
    private:
        std::unique_lock<Lock> _first_;
        std::unique_lock<Lock> _second_;
        std::unique_lock<Lock> _third_;

    public:
        UniqueLockTriplet(Lock& first, Lock& second, Lock& third, std::defer_lock_t) :
            _first_ { first, std::defer_lock },
            _second_ { second, std::defer_lock },
            _third_ { third, std::defer_lock },
        {
        }

        UniqueLockTriplet(UniqueLockTriplet&& other) :
            _first_{ std::move(other._first_) },
            _second_{ std::move(other._second_) },
            _third_{ std::move(other._third_) }
        {
        }

        bool try_lock_three()
        {
            bool s1 = _first_.try_lock();
            bool s2 = _second_.try_lock();
            bool s3 = _third_.try_lock();
            if (s1 && s2 && s3)
            {
                return true;
            }
            else
            {
                if (s1) _first_.unlock();
                if (s2) _second_.unlock();
                if (s3) _third_.unlock();
                return false;
            }
        }

        void lock_three()
        {
            _first_.lock();
            _second_.lock();
            _third_.lock();
        }

        void unlock()
        {
            assert(_first_.owns_lock() && "Trying to unlock a lock not acquired!");
            assert(_second_.owns_lock() && "Trying to unlock a lock not acquired!");
            assert(_third_.owns_lock() && "Trying to unlock a lock not acquired!");

            _first_.unlock();
            _second_.unlock();
            _third_.unlock();
        }

        bool owns_lock()
        {
            bool owns_first = _first_.owns_lock();
            bool owns_second = _second_.owns_lock();
            bool owns_third = _third_.owns_lock();
            bool owns_all = owns_first && owns_second && owns_third;
            bool owns_none = !owns_first && !owns_second && !owns_third;
            assert((owns_all || owns_none) && "UniqueLockTriplet invalid state!");
            return owns_all;
        }
    };

    /// UnorderedCollection is stored in DRAM, indexed by HashTable
    /// A Record DlistRecord is stored in PMem,
    /// whose key is the name of the UnorderedCollection
    /// and value holds the id of the Collection 
    /// prev and next pointer holds the head and tail of DLinkedList for recovery
    class UnorderedCollection : public PersistentList 
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
            _sp_dlinked_list_{ std::make_shared<DLinkedList>(sp_pmem_allocator, timestamp) },
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
                entry_list_record.header.checksum = DLinkedList::_check_sum_(entry_list_record, _name_, _id_to_view_(_id_));

                entry_list_record.prev = _sp_dlinked_list_->Head()._get_offset_();
                entry_list_record.next = _sp_dlinked_list_->Tail()._get_offset_();
            }
            DLinkedList::_persist_record_(pmp_list_record, entry_list_record, _name_, _id_to_view_(_id_));
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
            _id_{ _view_to_id_(pmp_dlist_record->Value()) }
        {
        }

        uint64_t id() override { return _id_; }

        std::string const& name() { return _name_; }

    private:
        inline static pmem::obj::string_view _extract_key_(pmem::obj::string_view internal_key)
        {
            constexpr size_t sz_id = sizeof(decltype(_id_));
            assert(sz_id < internal_key.size() && "internal_key does not has space for key");
            return pmem::obj::string_view(internal_key.data() + sz_id, internal_key.size() + sz_id);
        }

        inline static std::uint64_t _extract_id_(pmem::obj::string_view internal_key)
        {
            std::uint64_t id;
            assert(sizeof(decltype(id)) <= internal_key.size() && "internal_key is smaller than the size of an id!");
            memcpy(&id, internal_key.data(), sizeof(decltype(id)));
            return id;
        }

        inline static std::string _make_internal_key_(std::uint64_t id, pmem::obj::string_view key)
        {
            return std::string{_id_to_view_(id)} + std::string{ key };
        }

        /// reference to const id to prevent local variable destruction, which will invalidify string_view
        inline static pmem::obj::string_view _id_to_view_(std::uint64_t id)
        {
            return pmem::obj::string_view{ reinterpret_cast<char*>(&id), sizeof(decltype(id)) };
        }

        inline static std::uint64_t _view_to_id_(pmem::obj::string_view view)
        {
            std::uint64_t id;
            assert(sizeof(decltype(id)) == view.size() && "id_view does not match the size of an id!");
            memcpy(&id, view.data(), sizeof(decltype(id)));
            return id;
        }

        /// Make UniqueLockTriplet<SpinMutex> to lock adjacent three nodes, not locked yet.
        /// Also accepts UnorderedIterator by implicit casting
        UniqueLockTriplet<SpinMutex> _make_unique_lock_triplet_(DLinkedList::Iterator iter_mid, SpinMutex* spin_mid = nullptr)
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
    
    };

    class UnorderedIterator : public Iterator 
    {
    private:
        /// shared pointer to pin the UnorderedCollection
        std::shared_ptr<UnorderedCollection> _sp_coll_;
        DLinkedList::Iterator _iterator_internal_;
        bool _valid_;

    public:
        /// Construct UnorderedIterator and SeekToFirst().
        UnorderedIterator(std::shared_ptr<UnorderedCollection> sp_coll) :
            _sp_coll_{ sp_coll },
            _iterator_internal_{ nullptr },
            _valid_{ false }
        {
            SeekToFirst();
        }

        UnorderedIterator(std::shared_ptr<UnorderedCollection> sp_coll, DLDataEntry* pmp) :
            _sp_coll_{ sp_coll },
            _iterator_internal_{ _sp_coll_->_sp_dlinked_list_, pmp },
            _valid_{ false }
        {
            _valid_ = false;
            throw;
        }

        virtual void SeekToFirst() override
        {
            _iterator_internal_ = _sp_coll_->_sp_dlinked_list_->Head();
            _next_();
        }

        virtual void SeekToLast() override
        {
            _iterator_internal_ = _sp_coll_->_sp_dlinked_list_->Tail();
            _prev_();
        }

        virtual bool Valid() override
        {
            return _valid_;
        }

        virtual bool Next() override 
        {
            _next_();
            return _valid_;
        }

        virtual bool Prev() override
        {
            _prev_();
            return _valid_;
        }

        virtual std::string Key() override 
        {
            if (!Valid())
            {
                return "";
            }
            auto view_key = UnorderedCollection::_extract_key_(_iterator_internal_->Key());
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

        operator DLinkedList::Iterator() const
        {
            return _iterator_internal_;
        }

    private:
        // Precede to next DLIST_DATA_RECORD, can start from head
        void _next_()
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
                throw std::runtime_error{ "UnorderedCollection::Iterator::_next_() fails!" };
            }
        }

        void _prev_()
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
                throw std::runtime_error{ "UnorderedCollection::Iterator::_prev_() fails!" };
            }
        }

    };
} // namespace KVDK_NAMESPACE
