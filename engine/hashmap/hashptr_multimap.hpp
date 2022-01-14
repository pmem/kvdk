#include <cassert>
#include <cstdint>
#include <cstring>

#include <atomic>
#include <bitset>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <type_traits>
#include <utility>

#include <immintrin.h>

template <typename HashType, typename Pointer, size_t NSlot = 16, typename Alloc = std::allocator<Pointer>>
class HashPointerMultimap
{
  public:
    using index_type = std::uint32_t;
    using mask_type = std::uint16_t;
    using tag_type = std::uint16_t;

  private:
    static_assert(sizeof(Pointer) == 8);
    static_assert(sizeof(HashType) == 8);
    static_assert(NSlot == 16 || NSlot == 8);

    static constexpr index_type n_logical_field = NSlot;
    static constexpr index_type n_data_field = n_logical_field - 1;
    static constexpr size_t n_field_per_cacheline = 8;

    class Bucket;
    using BucketAllocatorType = typename std::allocator_traits<Alloc>::template rebind_alloc<Bucket>;
    using BucketAllocatorTraits = typename std::allocator_traits<Alloc>::template rebind_traits<Bucket>;
    /// TODO: use BucketPointer
    using BucketPointer = typename BucketAllocatorTraits::pointer;

  private:
    class BucketMeta
    {
    //   private:
    public:
        using value_type = std::uint64_t;
        using atomic_type = std::atomic<value_type>;

        static_assert(sizeof(value_type) == 8);
        static_assert(sizeof(atomic_type) == 8);

        atomic_type storage;

      public:
        static constexpr value_type tag_mask = 0xFFFF000000000000;
        static constexpr value_type ptr_mask = ~tag_mask;
        static constexpr value_type lock_bit = 0x0001000000000000;
        static constexpr mask_type bitmap_mask = 0xFFFE;

      public:
        explicit inline BucketMeta() noexcept = default;
        BucketMeta(BucketMeta const &other) = delete;

        // Caution: Always lock before calling other methods!
        inline void lock() noexcept
        {
            while (true)
            {
                while (storage.load() & lock_bit)
                {
                    for (size_t i = 0; i < 64; i++)
                    {
                        _mm_pause();
                    }
                }
                // Test and set
                auto old = storage.fetch_or(lock_bit);
                if (!(old & lock_bit))
                {
                    return;
                }
                else
                {
                    continue;
                }
            }
        }
        
        inline void unlock() noexcept
        {
            assert(storage.load() & lock_bit);
            storage.fetch_xor(lock_bit);
        }
        
      private:
        friend class Bucket;

        inline mask_type occupied() const noexcept
        {
            auto old = storage.load();
            return ((old & ~lock_bit) >> 48);
        }

        inline mask_type available() const noexcept
        {
            auto old = storage.load();
            return ((~old & ~lock_bit) >> 48);
        }

        inline void set_bit(index_type index) noexcept
        {
            value_type mask = bitmask(index);
            assert(!(storage.load() & mask));
            storage.fetch_or(mask);
        }

        inline void reset_bit(index_type index) noexcept
        {
            value_type mask = bitmask(index);
            assert(storage.load() & mask);
            storage.fetch_xor(mask);
        }

        inline void set(Bucket* p)
        {
            value_type u64 = reinterpret_cast<value_type>(p);
            u64 &= ptr_mask;
            storage.fetch_and(tag_mask);
            storage.fetch_or(u64);
        }

        Bucket* pointer() const
        {
            value_type u64 = storage.load();
            u64 &= ptr_mask;
            return reinterpret_cast<Bucket*>(u64);
        }

        inline static value_type bitmask(index_type idx)
        {
            assert(idx < n_data_field);
            return (0x0002ULL << (idx + 48));
        }
    };

    class TaggedPointer
    {
        using storage_type = std::uint64_t;

        static constexpr storage_type tag_mask = 0xFFFF000000000000;
        static constexpr storage_type ptr_mask = ~tag_mask;

        storage_type u64;

    public:
        inline explicit TaggedPointer() noexcept = default;
        TaggedPointer(TaggedPointer const &) = delete;


        inline tag_type tag() const noexcept
        {
            return static_cast<tag_type>(u64 >> 48);
        }
        inline Pointer pointer() const noexcept
        {
            return reinterpret_cast<Pointer>(u64 & ptr_mask);
        }
        inline void set(tag_type tag) noexcept
        {
            storage_type u64_new = (u64 & ptr_mask);
            u64_new |= (static_cast<storage_type>(tag) << 48);
            u64 = u64_new;
        }
        inline void set(Pointer p) noexcept
        {
            storage_type u64_new = reinterpret_cast<storage_type>(p);
            assert(!(u64_new & tag_mask));
            u64_new &= ptr_mask;
            u64_new |= (u64 & tag_mask);
            u64 = u64_new;
        }
        inline void set(tag_type tag, Pointer p) noexcept
        {
            storage_type u64_new = reinterpret_cast<storage_type>(p);
            assert(!(u64_new & tag_mask));
            u64_new &= ptr_mask;
            u64_new |= (static_cast<storage_type>(tag) << 48);
            u64 = u64_new;
        }
    };


    class Bucket
    {
        private:
        union alignas(64)
        {
            __m512i m512[n_logical_field / n_field_per_cacheline];
            struct alignas(64)
            {
                BucketMeta meta;
                TaggedPointer data[n_data_field];
            };
        };

    public:
        inline explicit constexpr Bucket() noexcept = default;

        Bucket(Bucket const &) = delete;

        inline mask_type match_tag(tag_type tag) const noexcept
        {            
            union
            {
                __m128i m128;
                std::uint64_t mask[n_logical_field / n_field_per_cacheline];
            } results{};
            // mask_type empty[2]{};

            std::uint64_t high_mask = 0x8080808080808080;
            std::uint64_t low_mask = 0x4040404040404040;
            __m512i low_tag = _mm512_set1_epi8(static_cast<std::uint8_t>(tag));
            __m512i high_tag = _mm512_set1_epi8(static_cast<std::uint8_t>(tag >> 8));

            // empty[0] = _mm512_cmpeq_epi64_mask(_mm512_set1_epi64(0ULL), m512[0]);
            results.mask[0] = (_mm512_mask_cmpeq_epi8_mask(low_mask, low_tag, m512[0]) |
                               _mm512_mask_cmpeq_epi8_mask(high_mask, high_tag, m512[0]));
            if (n_logical_field == 16) // Compiler will remove branch
            {
                // empty[1] = _mm512_cmpeq_epi64_mask(_mm512_set1_epi64(0ULL), m512[1]);
                results.mask[1] = (_mm512_mask_cmpeq_epi8_mask(low_mask, low_tag, m512[1]) |
                                   _mm512_mask_cmpeq_epi8_mask(high_mask, high_tag, m512[1]));
            }

            // mask_type valid_fields = ~(empty[0] | (empty[1] << 8)) & BucketMeta::bitmap_mask;

            __m128i compress_mask = _mm_set1_epi8(0xC0);
            mask_type compressed = _mm_cmpeq_epi8_mask(results.m128, compress_mask);

            // return (compressed & valid_fields);
            return (compressed & meta.occupied());
        }

        inline static index_type consume_mask(mask_type& match_result)
        {
            index_type tz = __tzcnt_u16(match_result);
            match_result ^= (0x0001 << tz);
            return tz - 1;
        }

        inline Pointer get(index_type index) const noexcept
        {
            assert(index < n_data_field);

            return data[index].pointer();
        }

        inline void set(index_type index, tag_type tag, Pointer p) noexcept
        {
            data[index].set(tag, p);
            meta.set_bit(index);
        }

        inline void erase(index_type index) noexcept
        {
            data[index].set(0x0000, nullptr);
            meta.reset_bit(index);
        }

        inline void reset(index_type index, Pointer p) noexcept
        {
            assert((meta.occupied() & (0x0002 << index)) && "Bit not set yet!");

            data[index].set(p);
        }

        inline Bucket *next_bucket() const noexcept
        {
            return meta.pointer();
        }

        inline BucketMeta* mutex() noexcept
        {
            return &meta;
        }

        inline Bucket *append_new_bucket(BucketAllocatorType &alloc)
        {
            assert(next_bucket() == nullptr);
            Bucket *p_new_sub_bucket = BucketAllocatorTraits::allocate(alloc, 1);
            BucketAllocatorTraits::construct(alloc, p_new_sub_bucket);
            meta.set(p_new_sub_bucket);
            return p_new_sub_bucket;
        }

        inline mask_type available_slots() const noexcept
        {
            return meta.available();
        }

        inline static void destroy_appended_buckets(Bucket *p_bucket, BucketAllocatorType &alloc)
        {
            if (!p_bucket)
                return;

            destroy_appended_buckets(p_bucket->next_bucket(), alloc);
            p_bucket->meta.set(nullptr);
            BucketAllocatorTraits::destroy(alloc, p_bucket);
            BucketAllocatorTraits::deallocate(alloc, p_bucket, 1);
        }

        friend std::ostream& operator<<(std::ostream& out, Bucket const& bucket)
        {
            out
                << std::bitset<64>(bucket.meta.storage.load()) << "\n";
            for (size_t i = 0; i < 15; i++)
            {
            out
                << std::bitset<16>(bucket.data[i].tag()) << "\t" << bucket.data[i].pointer() << "\n";
            }
            
            return out;
        }
    };
    static_assert(sizeof(Bucket) == NSlot * 8);
    static_assert(alignof(Bucket) == 64);

  public:
    // Meets requirements of a forward iterator
    class iterator
    {
        friend class HashPointerMultimap;

        struct iterator_state
        {
          private:
            Bucket *p_bucket;
            tag_type tag;
            mask_type matches;
            index_type idx;

            friend class iterator;
            friend class HashPointerMultimap;

          public:
            Pointer get() const noexcept
            {
                return p_bucket->get(idx);
            }

            void set(Pointer p) noexcept
            {
                p_bucket->reset(idx, p);
            }

            friend bool operator==(iterator_state lhs, iterator_state rhs)
            {
                return (lhs.p_bucket == rhs.p_bucket) && 
                        (lhs.tag == rhs.tag) &&
                        (lhs.matches == rhs.matches) && 
                        (lhs.idx == rhs.idx);
            }
        };

      private:
        iterator_state state;

      public:
        // Constructs end() iterator
        inline explicit iterator()
        {
            state.p_bucket = nullptr;
            state.tag = tag_type{};
            state.matches = mask_type{};
            state.idx = n_data_field;
        }

        // Constructs normal iterator, seek to first match
        inline explicit iterator(tag_type tag, Bucket *p_bucket)
        {
            state.p_bucket = p_bucket;
            state.tag = tag;

            do
            {
                // Set position to first match
                state.matches = state.p_bucket->match_tag(state.tag);
                state.idx = p_bucket->consume_mask(state.matches);
                if (state.idx < n_data_field)
                {
                    return;
                }
                // Current Bucket no match, goto next bucket
                state.p_bucket = state.p_bucket->next_bucket();
            } while (state.p_bucket);
            // No match, set to end
            *this = iterator{};
        }

        inline iterator_state &operator*()
        {
            return state;
        }

        inline iterator_state const &operator*() const
        {
            return state;
        }

        inline iterator_state *operator->()
        {
            return &state;
        }

        inline iterator_state const *operator->() const
        {
            return &state;
        }

        inline iterator &operator++()
        {
            assert(state.p_bucket && "Trying to increment end()!");
            while (true)
            {
                state.idx = state.p_bucket->consume_mask(state.matches);
                // Set position to next match
                if (state.idx < n_data_field)
                {
                    return *this;
                }
                // Current Bucket no match, goto next bucket
                state.p_bucket = state.p_bucket->next_bucket();
                if (!state.p_bucket)
                {
                    // No match, return end() iterator
                    *this = iterator{};
                    return *this;
                }
                state.matches = state.p_bucket->match_tag(state.tag);
            }
            return *this;
        }

        inline iterator operator++(int)
        {
            iterator copy{*this};
            ++*this;
            return copy;
        }

      private:
        friend class HashPointerMultimap;

        inline friend bool operator==(iterator const &lhs, iterator const &rhs)
        {
            return lhs.state == rhs.state;
        }

        // Only two iterators at end or with exact internal state are considered equal
        inline friend bool operator!=(iterator const &lhs, iterator const &rhs)
        {
            return !(lhs == rhs);
        }
    };

  private:
    size_t n_bucket;
    BucketAllocatorType alloc;
    Bucket *const bucket_base;

  public:
    using lock_type = BucketMeta;

    HashPointerMultimap() = delete;

    explicit HashPointerMultimap(size_t n, Alloc const &a = Alloc{})
        : n_bucket{n}, alloc{a}, bucket_base{BucketAllocatorTraits::allocate(alloc, n_bucket)}
    {
        {
            Bucket *p_bucket = nullptr;
            for (size_t i = 0; i < n_bucket; i++)
            {
                p_bucket = bucket_base + i;
                BucketAllocatorTraits::construct(alloc, p_bucket);
            }
        }
    }

    HashPointerMultimap(HashPointerMultimap const &) = delete;

    ~HashPointerMultimap()
    {
        Bucket *p_bucket = nullptr;
        for (size_t i = 0; i < n_bucket; i++)
        {
            p_bucket = bucket_base + i;
            Bucket::destroy_appended_buckets(p_bucket->next_bucket(), alloc);
            BucketAllocatorTraits::destroy(alloc, p_bucket);
        }
        // Free huge space addressed by bucket_base
        BucketAllocatorTraits::deallocate(alloc, bucket_base, n_bucket);
    }

    std::pair<iterator, iterator> equal_range(HashType hash)
    {
        tag_type tag = get_tag(hash);
        size_t index = get_index(hash);
        Bucket *p_bucket = bucket_base + index;

        iterator begin{tag, p_bucket};
        iterator end{};

        return std::make_pair(std::move(begin), std::move(end));
    }

    /// Lock the Bucket that may contain hash.
    std::unique_lock<lock_type> acquire_lock(HashType hash)
    {
        size_t bucket_index = get_index(hash);
        Bucket *const p_bucket = bucket_base + bucket_index;
        return std::unique_lock<lock_type>{*p_bucket->mutex()};
    }

    lock_type* mutex(HashType hash)
    {
        size_t bucket_index = get_index(hash);
        Bucket *const p_bucket = bucket_base + bucket_index;
        return p_bucket->mutex();
    }

    /// Emplace a hash-value pair in to hash_ptr_multimap
    void emplace(HashType hash, Pointer value)
    {
        tag_type tag = get_tag(hash);
        size_t bucket_index = get_index(hash);
        
        Bucket *p_bucket = bucket_base + bucket_index;
        while (true)
        {
            mask_type empty_slots = p_bucket->available_slots();
            index_type index = p_bucket->consume_mask(empty_slots);
            if (index < n_data_field)
            {
                p_bucket->set(index, tag, value);
                return;
            }
            Bucket *p_next_bucket = p_bucket->next_bucket();
            if (p_next_bucket)
            {
                p_bucket = p_next_bucket;
                continue;
            }
            else
            {
                break;
            }
        }
        // Bucket is full, expand then insert
        p_bucket = p_bucket->append_new_bucket(alloc);
        p_bucket->set(0, tag, value);

        return;
    }

    // Erase a kv-pair indicated by iterator
    iterator erase(iterator iter)
    {
        assert((iter != iterator{}) && "Trying to erase end() iterator!");
        iter.state.p_bucket->erase(iter.state.idx);
        return ++iter;
    }

  private:
    inline size_t get_index(HashType hash) const noexcept
    {
        return hash % n_bucket;
    }

    inline tag_type get_tag(HashType hash) const noexcept
    {
        return (reinterpret_cast<std::uint64_t>(hash) >> 48);
    }
};
