#include <cinttypes>
#include <new>

#include "hashptr_multimap.hpp"

template <typename Key, typename Pointer, typename HashF = std::hash<Key>, typename KeyEqual = std::equal_to<Key>,
          typename PtrAlloc = std::allocator<Pointer>, size_t NSlot = 8>
class HashMap
{
  private:
    using HashType = typename std::result_of<HashF(Key)>::type;
    using hashptr_map_type = HashPointerMultimap<HashType, Pointer, NSlot, PtrAlloc>;

    hashptr_map_type hpmap;
    HashF hash;
    KeyEqual equal;

  public:
    using lock_type = typename hashptr_map_type::lock_type;
    using size_type = size_t;

    explicit HashMap(size_type bucket_cnt, HashF const &ha = HashF{}, KeyEqual const &ke = KeyEqual{},
                     PtrAlloc const &pa = PtrAlloc{})
        : hpmap{bucket_cnt / (NSlot - 1), pa}, hash{ha}, equal{ke}
    {
    }

    template<typename KeyExtractor>
    Pointer find(Key const& key, KeyExtractor extract_key)
    {
      HashType h = hash(key);
      auto range = hpmap.equal_range(h);
      for (auto iter = range.first; iter != range.second; ++iter)
      {
        Pointer p = iter->get();
        if (equal(key, extract_key(p)))
        {
          return p;
        }
      }
      return nullptr;
    }

    template<typename KeyExtractor>
    Pointer insert(Key const& key, Pointer p, KeyExtractor extract_key)
    {
      HashType h = hash(key);
      auto range = hpmap.equal_range(h);
      for (auto iter = range.first; iter != range.second; ++iter)
      {
        Pointer old_p = iter->get();
        if (equal(key, extract_key(old_p)))
        {
          iter->set(p);
          return old_p;
        }
      }
      hpmap.emplace(h, p);
      return nullptr;
    }

    template<typename KeyExtractor>
    Pointer erase(Key const& key, KeyExtractor extract_key)
    {
      HashType h = hash(key);
      auto range = hpmap.equal_range(h);
      for (auto iter = range.first; iter != range.second; ++iter)
      {
        Pointer old_p = iter->get();
        if (equal(key, extract_key(old_p)))
        {
          hpmap.erase(iter);
          return old_p;
        }
      }
      return nullptr;
    }

    std::unique_lock<lock_type> acquire_lock(Key const& key)
    {
      HashType h = hash(key);
      return hpmap.acquire_lock(h);
    }

    lock_type* mutex(Key const& key)
    {
      HashType h = hash(key);
      return hpmap.mutex(h);
    }
};