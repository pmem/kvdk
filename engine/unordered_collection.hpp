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
        bool try_lock_three(Lock first, Lock second, Lock third)
        {
            bool s1 = _first_.try_lock(first);
            bool s2 = _second_.try_lock(second);
            bool s3 = _third_.try_lock(third);
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

        void lock_three(Lock first, Lock second, Lock third)
        {
            _first_.lock(first);
            _second_.lock(second);
            _third_.lock(third);
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
    /// A Record DLIST_RECORD is stored in PMem,
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
            _sp_dlinked_list_{ std::make_shared(sp_pmem_allocator, timestamp) },
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
                throw std::bad_alloc{ "Fail to allocate space for UnorderedCollection" };
            }
            std::uint64_t offset_list_record = space_list_record.space_entry.offset;
            void* pmp_list_record = _sp_pmem_allocator_->offset2addr(offset_list_record);
            DLDataEntry entry_list_record;  // Set up entry with meta
            {
                entry_list_record.timestamp = timestamp;
                entry_list_record.type = DATA_ENTRY_TYPE::DLIST_RECORD;
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
                std::make_shared
                (
                    _sp_pmem_allocator_->offset2addr(pmp_dlist_record->prev),
                    _sp_pmem_allocator_->offset2addr(pmp_dlist_record->next),
                    _sp_pmem_allocator_
                )
            },
            _name_{ pmp_dlist_record->Key() },
            _id_{ pmp_dlist_record->Value() }
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
            return _id_to_string_(id) + key;
        }

        /// reference to const id to prevent local variable destruction, which will invalidify string_view
        inline static pmem::obj::string_view _id_to_view_(std::uint64_t const& id)
        {
            return pmem::obj::string_view{ &id, sizeof(decltype(id)) };
        }

        inline static std::uint64_t _view_to_id_(pmem::obj::string_view view)
        {
            std::uint64_t id;
            assert(sizeof(decltype(id)) == view.size() && "id_view does not match the size of an id!");
            memcpy(&id, view.data(), sizeof(decltype(id)));
            return id;
        }

        /// Try lock three adjacent nodes.
        /// Check UniqueLockTriplet<SpinMutex>::owns_lock() for success or not.
        /// Also accepts UnorderedIterator by implicit casting
        UniqueLockTriplet<SpinMutex> _try_lock_three_(DLinkedList::Iterator iter_mid)
        {
            DLinkedList::Iterator iter_prev{ iter_mid }; --iter_prev;
            DLinkedList::Iterator iter_next{ iter_mid }; ++iter_next;

            auto str_key_prev = iter_prev->Key();
            auto str_key_mid = iter_mid->Key();
            auto str_key_next = iter_next->Key();

            UniqueLockTriplet<SpinMutex> unique_lock_triplet;

            unique_lock_triplet.try_lock_three
            (
                *_sp_hash_table_->GetHint(str_key_prev).spin,
                *_sp_hash_table_->GetHint(str_key_mid).spin,
                *_sp_hash_table_->GetHint(str_key_next).spin,
            );
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
            _valid_ = UnorderedCollection::_extract_id_(pmp->Key())
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
            return _next_();
        }

        virtual bool Prev() override
        {
            return _prev_();
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
            assert(_iterator_internal_->type != DATA_ENTRY_TYPE::DLIST_TAIL_RECORD);
            ++_iterator_internal_;
            while (_iterator_internal_.valid())
            {
                _valid_ = false;
                switch (_iterator_internal_->type)
                {
                case DATA_ENTRY_TYPE::DLIST_DATA_RECORD:
                    _valid_ = true;
                    return;
                case DATA_ENTRY_TYPE::DLIST_DELETE_RECORD:
                    ++_iterator_internal_;
                    continue;
                case DATA_ENTRY_TYPE::DLIST_TAIL_RECORD:
                    return;
                default:
                    break;
                }
                break;
            }
            throw std::runtime_error{ "UnorderedCollection::Iterator::_next_() fails!" };
        }

        void _prev_()
        {
            assert(_iterator_internal_.valid());
            assert(_iterator_internal_->type != DATA_ENTRY_TYPE::DLIST_HEAD_RECORD);
            --_iterator_internal_;
            while (_iterator_internal_.valid())
            {
                _valid_ = false;
                switch (_iterator_internal_->type)
                {
                case DATA_ENTRY_TYPE::DLIST_DATA_RECORD:
                    _valid_ = true;
                    return;
                case DATA_ENTRY_TYPE::DLIST_DELETE_RECORD:
                    --_iterator_internal_;
                    continue;
                case DATA_ENTRY_TYPE::DLIST_HEAD_RECORD:
                    return;
                default:
                    break;
                }
                break;
            }
            throw std::runtime_error{ "UnorderedCollection::Iterator::_prev_() fails!" };
        }

    };
} // namespace KVDK_NAMESPACE
