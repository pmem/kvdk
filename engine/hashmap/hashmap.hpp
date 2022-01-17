#include <cinttypes>
#include <new>
#include "hashptr_multimap.hpp"

template <typename Key, typename Pointer, typename ExtractKey, typename HashF = std::hash<Key>, typename KeyEqual = std::equal_to<Key>,
          typename PtrAlloc = std::allocator<Pointer>, size_t NSlot = 8>
class HashMap
{
  private:
    using HashType = typename std::result_of<HashF(Key)>::type;
    using hashptr_map_type = HashPointerMultimap<HashType, Pointer, NSlot, PtrAlloc>;

    hashptr_map_type hpmap;
    typename std::conditional<std::is_function<ExtractKey>::value, typename std::add_pointer<ExtractKey>::type, ExtractKey>::type extract_key;
    typename std::conditional<std::is_function<HashF>::value, typename std::add_pointer<HashF>::type, HashF>::type hash;
    typename std::conditional<std::is_function<KeyEqual>::value, typename std::add_pointer<KeyEqual>::type, KeyEqual>::type equal;

  public:
    using lock_type = typename hashptr_map_type::lock_type;
    using size_type = size_t;

    explicit HashMap() : hpmap{} {}

    explicit HashMap(size_type bucket_cnt, ExtractKey const& e = ExtractKey{}, HashF const &ha = HashF{}, KeyEqual const &ke = KeyEqual{},
                     PtrAlloc const &pa = PtrAlloc{})
        : hpmap{bucket_cnt / (NSlot - 1), pa}, extract_key{e}, hash{ha}, equal{ke}
    {
    }

    HashMap(HashMap&& other) : HashMap{}
    {
      using std::swap;
      swap(hpmap, other.hpmap);
      swap(extract_key, other.extract_key);
      swap(hash, other.hash);
      swap(equal, other.equal);
    }

    Pointer find(Key const& key) 
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

    Pointer insert(Key const& key, Pointer p)
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

    Pointer erase(Key const& key)
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

template <typename Key, typename Pointer, typename ExtractKey, typename HashF, typename KeyEqual,
          typename PtrAlloc>
HashMap<Key, Pointer, ExtractKey, HashF, KeyEqual, PtrAlloc, 8> 
construct_hashmap(size_t bucket_cnt, ExtractKey const& e, HashF const &ha = HashF{}, KeyEqual const &ke = KeyEqual{},
PtrAlloc const &pa = PtrAlloc{})
{
  return HashMap<Key, Pointer, ExtractKey, HashF, KeyEqual, PtrAlloc, 8>{bucket_cnt, e, ha, ke, pa};
}
