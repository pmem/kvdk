#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>

#include <atomic>
#include <bitset>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

#include <immintrin.h>
#include <x86intrin.h>

#include "../macros.hpp"
#define dynamic_assert kvdk_assert

namespace KVDK_NAMESPACE
{

// Internal helpers
namespace
{
// Templates to convert between a pointer and its compact representation
// If we use a cache-backed pool allocator, or a allocator with reference count,
// which defines it's own pointer class, we need these converters
// to convert the pointer and std::uint64_t.
template<typename T, typename Alloc, typename Pointer = typename std::allocator_traits<Alloc>::pointer>
struct cvt_helper : public std::enable_if<
    std::is_convertible<decltype(std::declval<Alloc>().to_pointer(std::declval<std::uint64_t>())), Pointer>::value && 
    std::is_convertible<decltype(std::declval<Alloc>().to_rep(std::declval<Pointer>())), std::uint64_t>::value, T>::type {};

template<typename Alloc, typename Pointer = typename std::allocator_traits<Alloc>::pointer, typename = void>
Pointer to_pointer(std::uint64_t rep, Alloc const&)
{
    return reinterpret_cast<Pointer>(rep);
}

template<typename Alloc, typename Pointer = typename std::allocator_traits<Alloc>::pointer, typename = void>
std::uint64_t to_rep(Pointer ptr, Alloc const&)
{
    return reinterpret_cast<std::uint64_t>(ptr);
}

// template<typename Alloc, typename Pointer = typename std::allocator_traits<Alloc>::pointer>
// auto to_pointer(std::uint64_t rep, Alloc const& alloc) -> typename cvt_helper<Pointer, Alloc>::type
// {
//     return alloc.to_pointer(rep);
// }

// template<typename Alloc, typename Pointer = typename std::allocator_traits<Alloc>::pointer>
// auto to_rep(Pointer const& ptr, Alloc const& alloc) -> typename cvt_helper<std::uint64_t, Alloc>::type
// {
//     return alloc.to_rep(ptr);
// }

std::uint64_t reverse_bits(std::uint64_t u64)
{
    u64 = (u64 & 0xFFFFFFFF00000000) >> 32 | (u64 & 0x00000000FFFFFFFF) << 32;
    u64 = (u64 & 0xFFFF0000FFFF0000) >> 16 | (u64 & 0x0000FFFF0000FFFF) << 16;
    u64 = (u64 & 0xFF00FF00FF00FF00) >> 8 | (u64 & 0x00FF00FF00FF00FF) << 8;
    u64 = (u64 & 0xF0F0F0F0F0F0F0F0) >> 4 | (u64 & 0x0F0F0F0F0F0F0F0F) << 4;
    u64 = (u64 & 0xCCCCCCCCCCCCCCCC) >> 2 | (u64 & 0x3333333333333333) << 2;
    u64 = (u64 & 0xAAAAAAAAAAAAAAAA) >> 1 | (u64 & 0x5555555555555555) << 1;
    return u64;
}

void mm_pause()
{
    constexpr size_t NPause = 32;
    for (size_t i = 0; i < NPause; i++)
    {
        _mm_pause();
    }
}

template <typename Func>
struct maybe_add_pointer
    : public std::conditional<std::is_function<Func>::value, typename std::add_pointer<Func>::type, Func>
{
};

} // namespace

// A hashptr_map consists of multiple buckets.
// Each Bucket is a single linked list of node
// Each node consists of 1 control_field and field_cnt - 1 storage_field.
template<typename Key, typename Pointer, typename KeyExtract, typename HashFunc = std::hash<Key>, typename KeyEqual = std::equal_to<Key>, size_t field_cnt = 8, size_t embed_cnt = 4, size_t max_scale = 32, bool PML5 = false, typename Allocator = std::allocator<void*>>
class hashptr_map
{
  private:
    static_assert(field_cnt == 8 || field_cnt == 16, "");
    static_assert(((embed_cnt - 1) & embed_cnt) == 0, "embed_cnt must be power of 2");
    static_assert(max_scale <= 64, "");

    using Hash = std::uint64_t;
    static_assert(std::is_convertible<decltype(std::declval<HashFunc>()(std::declval<Key const&>())), Hash>::value, "");
  
    using field_rep = std::uint64_t;
    using control_rep = field_rep;
    using storage_rep = field_rep;
    using hash_rep = std::uint64_t;
    using tag_type = typename std::conditional<PML5, std::uint8_t, std::uint16_t>::type;
    using mask_type = std::uint16_t;

    class node;
    using node_alloc = typename std::allocator_traits<Allocator>::template rebind_alloc<node>;
    using node_alloc_traits = typename std::allocator_traits<Allocator>::template rebind_traits<node>;
    using node_pointer = typename node_alloc_traits::pointer;

    // constants for bit manipulation in nodes
    // A node consists of multiple 64-bit fields, one control field and several storage fields.
    // The control field holds the pointer to next node, and bits for lock and rehash.
    // A storage field holds a user pointer to some data, a tag extracted from hash value and a bit for rehash.
    static_assert(sizeof(field_rep) == sizeof(void*), "64-bit system required");
    static_assert(sizeof(Hash) == 8, "64-bit hash required");

    // Actual bits used by a pointer in a 4-level or 5-level paging system
    static constexpr size_t pointer_bits = PML5 ? 56 : 48;

    // Bits [63:56]/[63:48] are reused by a node to store meta information
    static constexpr field_rep meta_mask = ((~0UL >> pointer_bits) << pointer_bits);

    // Bits [55:0]/[47:0] are untouched
    static constexpr field_rep ptr_mask = ~meta_mask;

    // Bit 63 is used as a mark for rehash
    static constexpr field_rep mark_bit = (0x1UL << 63);

    // For the storage unit,
    // bits [62:56]/[62:48] are used to store a tag.
    // The tag consists high 7/15 bits inside a hash value
    static constexpr storage_rep tag_mask = (meta_mask & ~mark_bit);

    // For the control unit of a node,
    // bits [62:56]/[62:48] are used as a read-write lock.
    static constexpr control_rep lock_mask = (meta_mask & ~mark_bit);
    static constexpr control_rep slock_val = (0x1UL << pointer_bits);
    static constexpr control_rep elock_bit = (0x1UL << 62);

    class tagged_pointer
    {
    private:
        storage_rep u64{0};

    public:
        explicit tagged_pointer(nullptr_t) {}

        explicit tagged_pointer(tag_type tag, Pointer p) : u64{reinterpret_cast<storage_rep>(p)}
        {
            dynamic_assert(!(u64 & meta_mask), "Upper bits in Pointer not zeros!");
            u64 |= (static_cast<storage_rep>(tag) << pointer_bits);
            dynamic_assert(!(u64 & mark_bit), "Invalid tag!");
        }

        explicit tagged_pointer(storage_rep rep) : u64{rep} 
        {
            u64 &= ~mark_bit;
        }

        tagged_pointer(tagged_pointer const&) = default;
        tagged_pointer(tagged_pointer&&) = default;

        tag_type tag() const
        {
            return static_cast<tag_type>((u64 & tag_mask) >> pointer_bits);
        }

        Pointer pointer() const
        {
            return reinterpret_cast<Pointer>(u64 & ptr_mask);
        }

        storage_rep rep() const
        {
            return u64;
        }
    };

    class alignas(64) node
    {
    private:
        std::atomic<control_rep> meta{};
        std::array<std::atomic<storage_rep>, field_cnt - 1> data{};

    public:
        node() : node{false} {}

        explicit node(bool mark)
        {
            if (!mark) return;
            meta_mark_flip();             
            for (size_t i = 0; i < data.size(); i++)
                entry_mark_flip(i);
        }
    
        // Access Entrys by indexing
        void set_entry(size_t index, tagged_pointer tp)
        {
            dynamic_assert((data.at(index).load() & ~mark_bit) == 0, "Entry already set!");
            
            storage_rep rep = tp.rep();
            rep |= data.at(index).load() & mark_bit;
            data.at(index).store(rep);
        }

        void set_entry(size_t index, tag_type tag, Pointer p)
        {
            set_entry(index, tagged_pointer{tag, p});
        }

        void set_entry(size_t index, Pointer p)
        {
            dynamic_assert((data.at(index).load() & ptr_mask) != 0, "Entry not set yet!");
            storage_rep rep = tagged_pointer{0, p}.rep();
            rep |= data.at(index).load() & meta_mask;
            data.at(index).store(rep);
        }

        void erase_entry(size_t index)
        {
            dynamic_assert((data.at(index).load() & ptr_mask) != 0, "Entry not set yet!");
            storage_rep rep = tagged_pointer{nullptr}.rep();
            rep |= data.at(index).load() & mark_bit;
            data.at(index).store(rep);
        }

        tagged_pointer load_entry(size_t index)
        {
            return tagged_pointer{data.at(index).load()};
        }

        void entry_mark_flip(size_t index)
        {
            data.at(index).fetch_xor(mark_bit);
        }

        bool entry_mark(size_t index)
        {
            return data.at(index).load() & mark_bit;
        }

        // Every node can be locked, 
        // but only the first node in a bucket is locked
        // for exclusive/shared access of the bucket
        bool try_lock()
        {
            control_rep old = meta.load();
            if ((old & elock_bit) || !meta.compare_exchange_strong(old, old | elock_bit))
                return false;
            return true;
        }

        void lock()
        {
            while (!try_lock())
            {
                mm_pause();
            }
        }

        void unlock()
        {
            dynamic_assert((meta.load() & lock_mask) == elock_bit, "Unlock a lock not locked yet!");
            meta.fetch_xor(elock_bit);
        }

        bool try_lock_shared()
        {
            field_rep old = meta.load();
            if ((old & elock_bit) || (old & lock_mask) + slock_val >= mark_bit || !meta.compare_exchange_strong(old, old + slock_val))
            {
                return false;
            }
            return true;
        }

        void lock_shared()
        {
            while (!try_lock_shared())
            {
                mm_pause();
            }
        }

        void unlock_shared()
        {
            dynamic_assert((meta.load() & lock_mask) != 0 && !(meta.load() & elock_bit), "Unlock a lock yet locked!");
            meta.fetch_sub(slock_val);
        }

        void meta_mark_flip()
        {
            meta.fetch_xor(mark_bit);
        }

        bool meta_mark()
        {
            return meta.load() & mark_bit;
        }

        node_pointer next_node(node_alloc const& alloc) const
        {
            control_rep u64 = meta.load();
            u64 &= ptr_mask;
            return to_pointer(u64, alloc);
        }

        node_pointer append_new_node(node_alloc& alloc)
        {          
            node_pointer p_new_node = node_alloc_traits::allocate(alloc, 1);
            node_alloc_traits::construct(alloc, p_new_node, meta_mark());
            control_rep rep = to_rep(p_new_node, alloc);

            dynamic_assert(next_node(alloc) == nullptr, "Not last node!");
            dynamic_assert(!(rep & meta_mask), "");

            meta.fetch_or(rep);
            return p_new_node;
        }

        static void remove_appended_nodes(node_pointer node, node_alloc& alloc)
        {
            node_pointer next = node->next_node(alloc);
            if (next == nullptr) return;

            remove_appended_nodes(next, alloc);
            node->meta.fetch_and(meta_mask);
            node_alloc_traits::destroy(alloc, next);
            node_alloc_traits::deallocate(alloc, next, 1);
        }

        // Unlink, destroy and deallocate next node if it is empty
        static bool remove_empty_nodes(node_pointer node, node_alloc& alloc)
        {
            node_pointer next = node->next_node(alloc);

            if (next == nullptr) return true;
            if (!remove_empty_nodes(next, alloc)) return false;

            for (size_t i = 0; i < field_cnt - 1; i++)
                if (next->load_entry(i).pointer() != nullptr)
                    return false;

            remove_appended_nodes(node, alloc);
            return true;
        }

        template<bool pml5, typename std::enable_if<!pml5, bool>::type = true>
        mask_type match_tag_impl(tag_type tag) const
        {
            static_assert(sizeof(node) / sizeof(__m512i) == 1 || sizeof(node) / sizeof(__m512i) == 2, "");
            union {
                __m128i m128;
                std::array<std::uint64_t, 2> mask{};
            } results{};

            // 0b1000 0000
            constexpr std::uint64_t high_mask = 0x8080808080808080;
            // 0b0100 0000
            constexpr std::uint64_t low_mask = 0x4040404040404040;
            __m512i low_tag = _mm512_set1_epi8(static_cast<char>(tag));
            __m512i high_tag = _mm512_set1_epi8(static_cast<char>(tag >> 8));
            __m512i high_tag2 = _mm512_set1_epi8(static_cast<char>(tag >> 8) | (mark_bit >> 56));

            {
                __m512i m512_0 = _mm512_load_si512(&meta);
                results.mask[0] = (_mm512_mask_cmpeq_epi8_mask(low_mask, low_tag, m512_0) |
                                _mm512_mask_cmpeq_epi8_mask(high_mask, high_tag, m512_0) |
                                _mm512_mask_cmpeq_epi8_mask(high_mask, high_tag2, m512_0));
            }
            if (sizeof(node) / sizeof(__m512i) == 2)
            {
                __m512i m512_1 = _mm512_load_si512(&data[sizeof(__m512i) / sizeof(Pointer) - 1]);
                results.mask[1] = (_mm512_mask_cmpeq_epi8_mask(low_mask, low_tag, m512_1) |
                                _mm512_mask_cmpeq_epi8_mask(high_mask, high_tag, m512_1) |
                                _mm512_mask_cmpeq_epi8_mask(high_mask, high_tag2, m512_1));
            }
            // 0b1100 0000
            constexpr char compress_mask = static_cast<char>(0xC0);
            mask_type compressed = _mm_cmpeq_epi8_mask(results.m128, _mm_set1_epi8(compress_mask));

            return (compressed & match_nonempty());
        }

        template<bool pml5, typename std::enable_if<pml5, bool>::type = true>
        mask_type match_tag_impl(tag_type tag) const
        {
            /// TODO: test this on a machine with real five-level paging
            static_assert(sizeof(node) / sizeof(__m512i) == 1 || sizeof(node) / sizeof(__m512i) == 2, "");
            union {
                __m128i m128;
                std::array<std::uint64_t, 2> mask{};
            } results{};

            // 0b1000 0000
            constexpr std::uint64_t mask = 0x8080808080808080;
            __m512i my_tag = _mm512_set1_epi8(static_cast<char>(tag));
            __m512i my_tag2 = _mm512_set1_epi8(static_cast<char>(tag) | mark_bit);
            {
                __m512i m512_0 = _mm512_load_si512(&meta);
                results.mask[0] = _mm512_mask_cmpeq_epi8_mask(mask, my_tag, m512_0) |
                                    _mm512_mask_cmpeq_epi8_mask(mask, my_tag2, m512_0);
            }
            if (sizeof(node) / sizeof(__m512i) == 2)
            {
                __m512i m512_1 = _mm512_load_si512(&data[sizeof(__m512i) / sizeof(Pointer) - 1]);
                results.mask[1] = _mm512_mask_cmpeq_epi8_mask(mask, my_tag, m512_1) |
                                    _mm512_mask_cmpeq_epi8_mask(mask, my_tag2, m512_1);
            }
            // 0b1000 0000
            constexpr char compress_mask = static_cast<char>(0xB0);
            mask_type compressed = _mm_cmpeq_epi8_mask(results.m128, _mm_set1_epi8(compress_mask));

            return (compressed & match_nonempty());
        }

        mask_type match_tag(tag_type tag) const
        {
            return match_tag_impl<PML5>(tag);
        }

        mask_type match_empty() const
        {
            static_assert(sizeof(node) / sizeof(__m512i) == 1 || sizeof(node) / sizeof(__m512i) == 2, "");
            std::array<mask_type, 2> empty{};

            __m512i m512_0 = _mm512_load_si512(&meta);
            empty[0] = _mm512_cmpeq_epu64_mask(_mm512_set1_epi64(0L), m512_0) | 
                        _mm512_cmpeq_epu64_mask(_mm512_set1_epi64(mark_bit), m512_0);
            if (sizeof(node) / sizeof(__m512i) == 2)
            {
                __m512i m512_1 = _mm512_load_si512(&data[sizeof(__m512i) / sizeof(Pointer) - 1]);
                empty[1] = _mm512_cmpeq_epu64_mask(_mm512_set1_epi64(0L), m512_1) |
                            _mm512_cmpeq_epu64_mask(_mm512_set1_epi64(mark_bit), m512_1);
            }

            return ((empty[0] | (empty[1] << 8)) & 0xFFFE);
        }

        mask_type match_nonempty() const
        {
            return (~match_empty() & 0xFFFE);
        }

        static size_t consume_mask(mask_type& match_result)
        {
            size_t tz = __tzcnt_u16(match_result);
            match_result ^= (0x0001 << tz);
            return tz - 1;
        }

        // for debugging
        friend std::ostream& operator<<(std::ostream& out, node const& node)
        {
            out << std::bitset<64>(node.meta.load()) << "\n";
            for (size_t i = 0; i < field_cnt - 1; i++)
            {
                out << std::bitset<sizeof(tag_type)>(node.data[i].tag()) << "\t" << node.data[i].pointer() << "\n";
            }
            return out;
        }

      private:
        /// TODO: use this helper function to improve readibility
        static char match_epi8_in_epi64(__m512i m512, std::uint8_t u8, size_t bias)
        {
            dynamic_assert(bias <= 7, "");
            union 
            {
                __m128i m128;
                std::array<std::uint64_t, 2> u64;
            } result;
            long long mask = (0x0101010101010101 << bias);
            result.u64[0] = _mm512_mask_cmpeq_epi8_mask(mask, m512, _mm512_set1_epi8(u8));
            return _mm_cmpgt_epi8_mask(result.m128, _mm_set1_epi8(0)); 
        }
    };

    static_assert(sizeof(node) == field_cnt * sizeof(void*), "");
    static_assert(sizeof(node) % 64 == 0, "");
    static_assert(alignof(node) % 64 == 0, "");

    struct entry_pointer
    {
        node_pointer np{nullptr};
        size_t idx{field_cnt-1};

        entry_pointer() = default;
        entry_pointer(node_pointer p, size_t i) : np{p}, idx{i} {}
        static entry_pointer next(entry_pointer ep, node_alloc const& a)
        {
            dynamic_assert(ep.np != nullptr && ep.idx < field_cnt - 1, "");
            ++ep.idx;
            if (ep.idx < field_cnt - 1) return ep;
            ep.np = ep.np->next_node(a);
            ep.idx = (ep.np != nullptr) ? 0 : field_cnt - 1;
            return ep;
        }
    };

    struct bucket_tuple
    {
        std::uint64_t old_svar;
        size_t b_cnt;
        node_pointer bucket1;
        node_pointer bucket2;
        node_pointer my_bucket;

        std::unique_lock<node> guard1;
        std::unique_lock<node> guard2;

        bucket_tuple() = default;
        bucket_tuple(std::uint64_t s, size_t cnt, node_pointer b1, node_pointer b2, node_pointer mb, std::unique_lock<node>&& g1 = std::unique_lock<node>{}, std::unique_lock<node>&& g2 = std::unique_lock<node>{}) :
            old_svar{s},
            b_cnt{cnt},
            bucket1{b1},
            bucket2{b2},
            my_bucket{mb},
            guard1{std::move(g1)},
            guard2{std::move(g2)} {}
        bucket_tuple(bucket_tuple const&) = delete;
        bucket_tuple(bucket_tuple&&) = default;
        bucket_tuple& operator=(bucket_tuple const&) = delete;
        bucket_tuple& operator=(bucket_tuple&&) = default;
    };

    class accessor
    {
      private:
        friend hashptr_map;
        friend class iterator;

        // help end relocation
        hashptr_map* map;

        // buckets that the key may fall in
        bucket_tuple tuple;

        // Location of looked-up key
        entry_pointer loc{};

      public:
        explicit accessor(hashptr_map& m) : map{&m} {}
        accessor(accessor const&) = delete;
        accessor& operator=(accessor const&) = delete;
        accessor(accessor&&) = default;
        accessor& operator=(accessor&&) = default;
        ~accessor()
        {
            {
                std::unique_lock<node> g1{};
                std::unique_lock<node> g2{};
                tuple.guard2.swap(g2);
                tuple.guard1.swap(g1);            
            }
            map->help_end_relocation();
        }

        Pointer pointer() const
        {
            return (loc.np == nullptr) ? nullptr : loc.np->load_entry(loc.idx).pointer();
        }

        void set_pointer(Pointer p)
        {
            if (p == nullptr)
            {
                erase();
            }
            else if (loc.np == nullptr)
            {
                // Since relocation may have been done by lookup(),
                // we must guarantee Pointer p is inserted to the correct bucket.
                Hash h = map->hasher(map->extract(p));
                loc = map->allocate_empty_entry(tuple.my_bucket);
                loc.np->set_entry(loc.idx, map->make_tag(h), p);
            }
            else
            {
                Pointer old_p = loc.np->load_entry(loc.idx).pointer();
                dynamic_assert(map->equal(map->extract(p), map->extract(old_p)), "");
                loc.np->set_entry(loc.idx, p);
            }
        }

        void erase()
        {
            if (loc.np != nullptr) loc.np->erase_entry(loc.idx);
        }
    
      private:
        explicit accessor(hashptr_map& m, bucket_tuple&& t, entry_pointer l) : 
            map{&m},
            tuple{std::move(t)},
            loc{l} {}
    };

  public:
    class lockless_t {};
    static constexpr lockless_t lockless{};
    class acquire_lock_t {};
    static constexpr acquire_lock_t acquire_lock{};

    // We iterate through hashmap by 
    class iterator
    {
    public:
        iterator() = delete;
        iterator(iterator const&) = delete;
        iterator& operator=(iterator const&) = delete;
        iterator(iterator&&) = default;
        iterator& operator=(iterator&&) = default;

        iterator& operator++()
        {
            acc.loc = entry_pointer::next(acc.loc, acc.map->alloc);
            while (true)
            {
                acc.loc = skip_visited(acc.loc);
                // Found unvisited entry
                if (acc.loc.np != nullptr) return *this;
                if (acc.tuple.my_bucket == acc.tuple.bucket1)
                {
                    acc.tuple.my_bucket = acc.tuple.bucket2;
                    acc.loc = skip_visited(entry_pointer{acc.tuple.my_bucket, 0});
                    if (acc.loc.np != nullptr) return *this;
                }
                // All entries visited, reset lock and goto next bucket
                inc_hash();
                acc.tuple = bucket_tuple{};
                // inc_hash() overflow, end of iteration
                if (hash == Hash{}) break;

                acc.tuple = acc.map->load_and_lock_buckets(hash);
                acc.tuple.my_bucket = acc.tuple.bucket1;
                acc.loc = entry_pointer{acc.tuple.my_bucket, 0};
            }
            *this = iterator{*acc.map};
            return *this;
        }

        // No copy because iterator holds locks.
        iterator operator++(int) = delete;

        // Only two end iterators are equal
        // Two iterators cannot be equal as they acquire locks on buckets
        bool operator==(iterator const& rhs) const
        {
            return (acc.loc.np == nullptr) && (rhs.acc.loc.np == nullptr);
        }

        bool operator!=(iterator const& rhs) const
        {
            return !operator==(rhs);
        }

        accessor& operator*()
        {
            return acc;
        }

        accessor const& operator*() const
        {
            return acc;
        }

        accessor* operator->()
        {
            return &acc;
        }

        accessor const* operator->() const
        {
            return &acc;
        }

    private:
        friend hashptr_map;

        accessor acc;
        Hash hash;
        Hash rev_hash;
        Hash rev_lim;

        // End iterator
        iterator(hashptr_map& map) : acc{map}, hash{}, rev_hash{} {}

        iterator(hashptr_map& map, Hash h, Hash lim = -1UL) : acc{map}, hash{h}, rev_hash{reverse_bits(h)}, rev_lim{reverse_bits(lim)}
        {
            while (true)
            {
                acc.tuple = acc.map->load_and_lock_buckets(hash);
                acc.tuple.my_bucket = acc.tuple.bucket1;
                acc.loc = skip_visited(entry_pointer{acc.tuple.my_bucket, 0});
                // Found unvisited entry in bucket1
                if (acc.loc.np != nullptr) return;
                acc.tuple.my_bucket = acc.tuple.bucket2;
                acc.loc = skip_visited(entry_pointer{acc.tuple.my_bucket, 0});
                // Found unvisited entry in bucket2
                if (acc.loc.np != nullptr) return;
        
                // All entries visited, reset lock and goto next bucket
                inc_hash();
                acc.tuple = bucket_tuple{};
                // inc_hash() overflow, end of iteration
                if (hash == Hash{}) break;
            }
            *this = iterator{map};
        }

        // For 2^N buckets, low N bits determines the bucket
        // To increment the bucket index, 
        // we need to increment the (N-1)th highest bit in reversed hash,
        // and clear all lower bits.
        // Must be called when iterator holds a valid accessor!
        void inc_hash()
        {
            std::uint64_t v = reverse_bits(acc.tuple.b_cnt >> 1);
            rev_hash &= ~(v - 1);
            rev_hash += v;
            hash = reverse_bits(rev_hash);
        }

        /// TODO: examine if we may miss some entries
        bool visited(entry_pointer pos)
        {
            dynamic_assert(pos.np != nullptr, "");
            Pointer ptr = pos.np->load_entry(pos.idx).pointer();
            Hash h = acc.map->hasher(acc.map->extract(ptr));
            h = reverse_bits(h);
            return (h < rev_hash) || (rev_lim < h);
        }

        // Seek first non-empty entry in the bucket from pos.
        entry_pointer skip_empty(entry_pointer pos)
        {
            if (pos.np == nullptr) return entry_pointer{};
            if (pos.np->load_entry(pos.idx).pointer() != nullptr) return pos;
            pos = entry_pointer::next(pos, acc.map->alloc);
            return skip_empty(pos);
        }

        // Seek first non-empty entry not visited yet in the bucket from pos.
        entry_pointer skip_visited(entry_pointer pos)
        {
            pos = skip_empty(pos);
            if (pos.np == nullptr) return entry_pointer{};
            if (!visited(pos)) return pos;
            return skip_visited(entry_pointer::next(pos, acc.map->alloc));
        }
    };

    hashptr_map(size_t buckets, KeyExtract const& ext, HashFunc const& hash = HashFunc{}, KeyEqual const& eq = KeyEqual{}, Allocator const& a = Allocator{}) :
        alloc{a}, hasher{hash}, equal{eq}, extract{ext} 
    {
        node_blocks[0] = &embedded_block[0];
        size_t b_cnt = bucket_count();
        while (b_cnt < buckets)
        {
            node_pointer new_block = node_alloc_traits::allocate(alloc, b_cnt);
            for (size_t i = 0; i < b_cnt; i++)
                node_alloc_traits::construct(alloc, new_block + i, false);
            node_blocks[block_index(b_cnt)] = new_block;
            active_blocks.fetch_add(active_blocks.load() + 1);
            b_cnt = bucket_count();
        }
    }

    ~hashptr_map()
    {
        for (size_t i = 1; i < node_blocks.size(); i++)
        {
            node_pointer old_block = node_blocks[i];
            if (old_block == nullptr) continue;
            size_t cnt = (embed_cnt << i) >> 1;
            for (size_t i = 0; i < cnt; i++)
            {
                node_pointer np = old_block + i;
                node::remove_appended_nodes(np, alloc);
                node_alloc_traits::destroy(alloc, np);
            }
            node_alloc_traits::deallocate(alloc, old_block, cnt);
        }
        for (size_t i = 0; i < embedded_block.size(); i++)
            node::remove_appended_nodes(&embedded_block.at(i), alloc);
    }

    Pointer lookup(Key const& key, lockless_t) const
    {
        Hash h = hasher(key);
        tag_type tag = make_tag(h);
        while (true)
        {
            Pointer p = nullptr;
            bucket_tuple tuple = load_buckets(h);
            if (!is_relocating(tuple.old_svar))
            {
                p = search_backward(key, tag, tuple.bucket1);
            }
            else
            {
                // Relocation from bucket1 to bucket2
                using std::swap;
                if (is_shrk_proc(tuple.old_svar)) swap(tuple.bucket1, tuple.bucket2);

                // Look up the moved-from bucket first, then moved-to bucket.
                p = search_backward(key, tag, tuple.bucket1);
                p = (p != nullptr) ? p : search_backward(key, tag, tuple.bucket2);             
            }

            // svar not incremented, lookup result is valid, return.
            if (tuple.old_svar == svar.load()) return p;
        }
    }

    // deref_lim should be larger than 1.
    // 1 is merely enough to maintain load factor at current level 
    // if we keep inserting and try_double_capacity().
    accessor lookup(Key const& key, acquire_lock_t, size_t deref_lim = 4)
    {
        Hash h = hasher(key);
        bucket_tuple tuple = load_and_lock_buckets(h);

        // Help relocation
        if (is_relocating(tuple.old_svar))
        {
            if (tuple.bucket1->meta_mark() != mark_of(tuple.old_svar))
                deref_lim -= is_exp_proc(tuple.old_svar)
                            ? relocate_bucket(tuple.bucket2, tuple.bucket1, deref_lim)
                            : relocate_bucket(tuple.bucket1, tuple.bucket2, deref_lim);
            relocate_global(deref_lim, tuple.old_svar);
        }

        // Actual lookup, done after relocation to prevent loc from
        // being invalidated.
        tag_type tag = make_tag(h);
        entry_pointer loc{};
        if (!is_relocating(tuple.old_svar))
        {
            loc = search_forward(key, tag, tuple.bucket1);
        }
        else
        {
            loc = search_forward(key, tag, tuple.bucket1);  
            if (loc.np == nullptr)
                loc = search_forward(key, tag, tuple.bucket2);
        }
        return accessor{*this, std::move(tuple), loc};
    }

    iterator begin()
    {
        return iterator{*this, 0};
    }

    iterator end()
    {
        return iterator{*this};
    }

    // Estimate load factor by checking embedded nodes
    double approx_load_factor() const
    {
        size_t cnt = 0;
        for (size_t i = 0; i < embed_cnt; i++)
            cnt += entry_count(const_cast<node_pointer>(&embedded_block[i]));
        return static_cast<double>(cnt) / ((field_cnt - 1) * embed_cnt);
    }

    // Triggers rehash by write threads.
    // Return false if rehash is under progress.
    bool try_double_capacity()
    {
        std::uint64_t old_svar = svar.load();
        if (old_svar % 8 != 0 || !svar.compare_exchange_strong(old_svar, old_svar + 1))
            return false;
        
        old_svar = svar.load();
        size_t cnt = bucket_count();
        node_pointer new_block = node_alloc_traits::allocate(alloc, cnt);
        for (size_t i = 0; i < cnt; i++)
        {
            // Newly allocated buckets do not need relocation.
            // For convenience, the new bucket has same mark as its conjugated bucket,
            // the mark is cleared after scan
            node_alloc_traits::construct(alloc, new_block + i, !mark_of(old_svar));
        }
        // activate new block
        node_blocks[block_index(cnt)] = new_block;
        dynamic_assert(svar.load() % 4 == 1, "");
        pvar.store(0UL);
        svar.fetch_add(1UL);
        return true;
    }

    // Triggers rehash by write threads.
    // Return false if rehash is under progress.
    bool try_halve_capacity()
    {
        std::uint64_t old_svar = svar.load();
        if (old_svar % 8 != 0 || !svar.compare_exchange_strong(old_svar, old_svar + 5))
        {
            return false;
        }
        if (active_blocks.load() == 0x0001UL)
        {
            // Cannot shrink embedded block
            svar.fetch_add(3U);
            return false;
        }
        // The last block is logically deactivated.
        // A bucket pair forms a logical bucket internally.
        active_blocks.fetch_sub((active_blocks.load() + 1) >> 1);
        dynamic_assert(svar.load() % 4 == 1, "");
        pvar.store(0UL);
        svar.fetch_add(1);
        return true;
    }

    size_t help_relocate()
    {
        size_t old_svar = svar.load();
        if (!is_relocating(old_svar)) return 0;
        
        size_t n = relocate_global(-1UL, svar.load());
        help_end_relocation();
        return n;
    }

    // Logical count of buckets
    // During relocation, a pair of conjugated buckets are considered as
    // a logical bucket.
    // After shrinkage, the higher half of buckets are removed.
    // After expansion, the pair is seperated.
    size_t bucket_count() const
    {
        std::uint64_t active = active_blocks.load();
        return embed_cnt * (active + 1) / 2;
    }

  private:
    static tag_type make_tag(Hash h)
    {
        return (h >> (pointer_bits + 1));
    }

    enum class stage : std::uint64_t
    {
        // No rehashing
        stable = 0,

        // Initializing expansion, allocate space
        exp_init = 1,
        // Processing expansion, relocating entries
        exp_proc = 2,
        // Finalizing expansion
        exp_fin = 3,

        // Block rehashing
        blocking = 4,

        // Initializing shrinkage
        shrk_init = 5,
        // Processing shrinkage, relocating entries
        shrk_proc = 6,
        // Finalizing shrinkage, modify node_block pointers
        shrk_fin = 7
    };
    // Initialization and finalizing stage are ignored by accessors.
    // Accessors help relocation during processing stage.

    static stage stage_of(std::uint64_t old_svar)
    {
        return static_cast<stage>(old_svar % 8);
    }

    // Mark of svar is flipped each time 
    // try_halve_capacity() or try_double_capacity() is called.
    // Relocation will check buckets and flip their marks accordingly.
    static bool mark_of(std::uint64_t old_svar)
    {
        return ((old_svar + 7) / 8) % 2 == 1;
    }

    static bool is_relocating(std::uint64_t old_svar)
    {
        return (is_exp_proc(old_svar) || is_shrk_proc(old_svar));
    }

    static bool is_exp_proc(std::uint64_t old_svar)
    {
        return (stage_of(old_svar) == stage::exp_proc);
    }

    static bool is_shrk_proc(std::uint64_t old_svar)
    {
        return (stage_of(old_svar) == stage::shrk_proc);
    }

    static size_t block_index(size_t bucket_index)
    {
        return 64 - __lzcnt64(bucket_index / embed_cnt);
    }

    static size_t block_offset(size_t bucket_index)
    {
        return bucket_index - ((1UL << block_index(bucket_index)) >> 1) * embed_cnt;
    }

    node_pointer locate_bucket(size_t idx) const
    {
        size_t block_idx = block_index(idx);
        size_t block_off = block_offset(idx);
        node_pointer base = node_blocks[block_idx];
        return base + block_off;
    }

    // Load a consistent triplet of <old_svar, low_bucket, high_bucket>
    bucket_tuple load_buckets(Hash h) const
    {
        while (true)
        {
            std::uint64_t old_svar = svar.load();
            size_t b_cnt = bucket_count();
            node_pointer b1 = locate_bucket(h % b_cnt);
            node_pointer b2 = locate_bucket(h % b_cnt + b_cnt);

            /// TODO: Investigate this carefully!!!!
            node_pointer b = is_exp_proc(old_svar) ? locate_bucket(h % (b_cnt * 2)) : b1;
            // Initializing stages and finalizing stages are dangerous zones!
            /// TODO: Investigate extremely carefully and optimize.
            if (old_svar != svar.load() || old_svar % 2 != 0)
            {
                mm_pause();
                continue;
            }
            if (!is_relocating(old_svar))
                return bucket_tuple{old_svar, b_cnt, b1, nullptr, b};
            else
                return bucket_tuple{old_svar, b_cnt, b1, b2, b};
        }
    }

    bucket_tuple load_and_lock_buckets(Hash h)
    {
        while (true)
        {
            std::uint64_t old_svar = svar.load();
            size_t b_cnt = bucket_count();
            node_pointer b1 = locate_bucket(h % b_cnt);
            node_pointer b2 = locate_bucket(h % b_cnt + b_cnt);

            node_pointer mb = is_exp_proc(old_svar) ? locate_bucket(h % (b_cnt * 2)) : b1;
            if (!is_relocating(old_svar))
            {
                /// TODO: mm_pause()?
                std::unique_lock<node> g1{*b1, std::try_to_lock};
                if (!g1.owns_lock()) continue;
                if (old_svar != svar.load() || old_svar % 2 != 0) continue;

                return bucket_tuple{old_svar, b_cnt, b1, nullptr, mb, std::move(g1)};
            }
            else
            {
                std::unique_lock<node> g1{*b1, std::try_to_lock};
                if (!g1.owns_lock()) continue;
                std::unique_lock<node> g2{*b2, std::try_to_lock};
                if (!g2.owns_lock()) continue;
                if (old_svar != svar.load() || old_svar % 2 != 0) continue;
                
                return bucket_tuple{old_svar, b_cnt, b1, b2, mb, std::move(g1), std::move(g2)};
            }
        }
    }

    entry_pointer allocate_empty_entry(node_pointer np)
    {
        dynamic_assert(np != nullptr, "");
        for (size_t i = 0; i < field_cnt - 1; i++)
            if (np->load_entry(i).pointer() == nullptr)
                return entry_pointer{np, i};
        np = (np->next_node(alloc) == nullptr) 
             ? np->append_new_node(alloc) 
             : np->next_node(alloc);
        return allocate_empty_entry(np);
    }

    size_t entry_count(node_pointer node) const
    {
        if (node == nullptr) return 0;
        size_t cnt = 0;
        for (size_t i = 0; i < field_cnt - 1; i++)
            if (node->load_entry(i).pointer() != nullptr)
                ++cnt;
        return cnt + entry_count(node->next_node(alloc));
    }

    void print(node_pointer node)
    {
        std::cout << std::endl;
        if (node == nullptr) return;
        for (size_t i = 0; i < field_cnt - 1; i++)
        {
            Pointer p = node->load_entry(i).pointer();
            if (p == nullptr) std::cout << "nullptr\t";
            else std::cout << extract(p) << "\t";
        }
        print(node->next_node(alloc));
    }

    static void relocate(node_pointer dst_node, size_t dst_idx, node_pointer src_node, size_t src_idx)
    {
        tagged_pointer tp = src_node->load_entry(src_idx);
        dynamic_assert(tp.pointer() != nullptr, "");
        // Copy then erase, so that lockless readers will not miss the entry
        dst_node->set_entry(dst_idx, tp);
        src_node->erase_entry(src_idx);
    }

    void compress(node_pointer bkt)
    {
        std::deque<entry_pointer> empties;
        node_pointer np = bkt;
        while (np != nullptr)
        {
            for (size_t i = 0; i < field_cnt - 1; i++)
            {
                tagged_pointer tp = np->load_entry(i);
                if (tp.pointer() == nullptr)
                {
                    // Empty entry, reserved for compaction
                    empties.emplace_back(np, i);
                    continue;
                }

                if (empties.empty()) continue;

                // Relocate
                auto dst = empties.front();
                empties.pop_front();
                relocate(dst.np, dst.idx, np, i);
                empties.emplace_back(np, i);
            }
            np = np->next_node(alloc);
        }
        node::remove_empty_nodes(bkt, alloc);
    }

    // Recursively flip marks in a bucket that won't need relocation
    // During shrinkage, lower half of buckets does not need relocation,
    // but their mark has to be flipped.
    void flip_mark(node_pointer node)
    {
        if (node == nullptr) return;

        flip_mark(node->next_node(alloc));
        
        for (size_t i = 0; i < field_cnt - 1; i++)
            node->entry_mark_flip(i);

        node->meta_mark_flip();
        return;
    }

    // Scan and relocate entries in sub-chain led by src_node in src_bucket
    // It's guaranteed that the mark of dst_bucket and src_bucket are flipped together.
    size_t relocate_helper(node_pointer dst_bucket, node_pointer src_bucket, node_pointer src_node, size_t deref_lim)
    {
        if (src_node == nullptr) return 0;

        std::uint64_t old_svar = svar.load();
        dynamic_assert(is_relocating(old_svar), "");
        bool mark = mark_of(old_svar);
        dynamic_assert(src_bucket->meta_mark() != mark, "");
        dynamic_assert(dst_bucket->meta_mark() != mark, "");

        // The node already scanned and relocated
        if (src_node->meta_mark() == mark) return 0;
        
        size_t deref_cnt = relocate_helper(dst_bucket, src_bucket, src_node->next_node(alloc), deref_lim);
        deref_lim -= deref_cnt;
        dynamic_assert(old_svar == svar.load(), "");
        if (deref_lim == 0) return deref_cnt;

        size_t b_cnt = bucket_count();

        for (size_t i = 0; i < field_cnt - 1; i++)
        {
            if (deref_lim == 0) return deref_cnt;

            // Already relocated, skip
            if (src_node->entry_mark(i) == mark) continue;

            src_node->entry_mark_flip(i);
            tagged_pointer tp = src_node->load_entry(i);

            // Empty entry, skip
            if (tp.pointer() == nullptr) continue;

            // extract() is the potential bottleneck
            --deref_lim;
            ++deref_cnt;
            Hash h = hasher(extract(tp.pointer()));           

            // No need to relocate, skip
            if (h % (b_cnt * 2) < b_cnt) continue;

            // Relocation necessary, find an empty entry
            entry_pointer dst = allocate_empty_entry(dst_bucket);
            relocate(dst.np, dst.idx, src_node, i);
                    
            dynamic_assert(old_svar == svar.load(), "");
        }
        // Whole node scanned
        // When src_node is the leading node, 
        // flip the mark means the whole bucket is relocated.
        src_node->meta_mark_flip();

        // Relocation is done
        if (src_bucket == src_node)
        {
            compress(src_bucket);
            compress(dst_bucket);
            flip_mark(dst_bucket);
        }

        dynamic_assert(old_svar == svar.load(), "");
        return deref_cnt;
    }

    size_t relocate_bucket(node_pointer dst_bucket, node_pointer src_bucket, size_t deref_lim)
    {
        return relocate_helper(dst_bucket, src_bucket, src_bucket, deref_lim);
    }

    void help_end_relocation()
    {
        std::uint64_t old_svar = svar.load();
        if (!is_relocating(old_svar)) return;

        // Not done yet.
        size_t b_cnt = bucket_count();
        if (pvar.load() != b_cnt) return;

        // Only one thread is allowed to continue
        if (!pvar.compare_exchange_strong(b_cnt, -1UL)) return;
        if (is_exp_proc(old_svar))
        {
            // End of expansion
            svar.fetch_add(1);
            active_blocks.fetch_add(active_blocks.load() + 1);
            svar.fetch_add(5);
        }
        else
        {
            dynamic_assert(is_shrk_proc(old_svar), "");
            // End of shrinkage
            svar.fetch_add(1UL);
            node_pointer old_block = node_blocks[block_index(b_cnt)];
            node_blocks[block_index(b_cnt)] = nullptr;
            svar.fetch_add(1UL);
            std::this_thread::sleep_for(std::chrono::milliseconds{1000});
            for (size_t i = 0; i < b_cnt; i++)
            {
                node_pointer np = old_block + i;
                node::remove_appended_nodes(np, alloc);
                node_alloc_traits::destroy(alloc, np);
            }
            node_alloc_traits::deallocate(alloc, old_block, b_cnt);
        } 
    }
    
    // When bucket associated with accessor is relocated,
    // accessor helps relocate entries in other buckets.
    // Since locks are acquired inside, we have to pass old_svar from outside
    /// TODO: can we load old_svar inside?
    size_t relocate_global(size_t deref_lim, std::uint64_t old_svar)
    {
        // Logical bucket count
        size_t b_cnt = bucket_count();
        size_t deref_cnt = 0;

        for (size_t idx = pvar.load(); idx < b_cnt && deref_lim != 0; idx++)
        {
            // Loading a new bucket pair also counts as a dereference
            deref_lim--;
            bucket_tuple tuple = load_buckets(idx);
            if (old_svar != tuple.old_svar) return deref_cnt;
            std::unique_lock<node> guard1{*tuple.bucket1, std::try_to_lock};
            if (!guard1.owns_lock())
            {
                idx += __rdtsc() % (b_cnt - idx);
                continue;
            }
            std::unique_lock<node> guard2{*tuple.bucket2, std::try_to_lock};
            if (!guard2.owns_lock()) continue;

            if (tuple.bucket1->meta_mark() != mark_of(old_svar))
            {
                // Both buckets locked,
                // meta_mark indicates necessity for relocation
                // svar not incremented
                dynamic_assert(is_relocating(old_svar), "");
                size_t cnt = is_exp_proc(old_svar)
                           ? relocate_bucket(tuple.bucket2, tuple.bucket1, deref_lim)
                           : relocate_bucket(tuple.bucket1, tuple.bucket2, deref_lim);
                deref_lim -= cnt;
                deref_cnt += cnt;
            }
            
            // Relocated bucket pair, refresh relocation progress
            // pvar is protected by lock on bucket indexed by idx1
            if (tuple.bucket1->meta_mark() == mark_of(old_svar))
                if (idx == pvar.load())
                    pvar.fetch_add(1U);
        }
        return deref_cnt;
    }

    // Recursively traverse a bucket backwards.
    // Since compaction only moves entry from back of a bucket to front,
    // we avoid missing an entry during rehash.
    Pointer search_backward(Key const& key, tag_type tag, node_pointer np) const
    {
        if (np == nullptr) return nullptr;

        Pointer p = search_backward(key, tag, np->next_node(alloc));
        if (p != nullptr) return p;

        // Next node does not exist or does not contain entry of interest.
        // Lookup key in current node.
        for (size_t i = 0; i < field_cnt - 1; i++)
        {
            size_t idx = field_cnt - 2 - i;
            tagged_pointer tp = np->load_entry(idx);

            if (tp.pointer() == nullptr || tp.tag() != tag) continue;
            if (!equal(extract(tp.pointer()), key)) continue;
            return tp.pointer();
        }
        return nullptr;
    }

    // Traverse a bucket in normal order.
    entry_pointer search_forward(Key const& key, tag_type tag, node_pointer node)
    {
        if (node == nullptr) return entry_pointer{};

        mask_type match = node->match_tag(tag);
        for (size_t idx = node::consume_mask(match); idx < field_cnt - 1; idx = node::consume_mask(match))
            if (equal(extract(node->load_entry(idx).pointer()), key))
                return entry_pointer{node, idx};

        return search_forward(key, tag, node->next_node(alloc));
    }

  private:
    node_alloc alloc;
    // Nodes are organized into blocks.
    // First embed_cnt nodes are put in embedded_block,
    // and when rehashed, new nodes are allocated into node_blocks.
    // allocated_nodes[0] points to embedded_block and
    // allocated_nodes[k] contains embed_cnt * 2^(k-1) nodes
    std::array<node, embed_cnt> embedded_block{};
    std::array<node_pointer, max_scale> node_blocks{};
    // Initially only embedded_block is active
    std::atomic_uint64_t active_blocks{1U};

    // state variable indicating the map shrinking or expanding
    std::atomic_uint64_t svar{0UL};
    // progress variable indicating relocation progress
    // 0 for start, -1 for finish
    std::atomic<size_t> pvar{-1UL};

    typename maybe_add_pointer<HashFunc>::type hasher;
    typename maybe_add_pointer<KeyEqual>::type equal;
    typename maybe_add_pointer<KeyExtract>::type extract;
};

} // KVDK_NAMESPACE
