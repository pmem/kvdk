# Key-Value Development Kit(KVDK)

## 0. Overview

KVDK (Key-Value Development Kit) is a persistent key-value store library implemented in C++ language. It is designed and optimized for persistent memory. Besides providing the basic APIs of key-value store, it offers several advanced features, like transaction, snapshot, etc.

Currently KVDK supports read/write operations based on hash index. It also supports sorted collections, where user can perform prefix searching and range scan.

A KVDK instance can be recovered after unexpected shutdown using the data persistence feature of Intel Optane PMem.

KVDK organizes Key-Value Pairs(KV-Pairs) by `Collection`. KV-Pairs can be saved in the anonymous global collection or named local collection. 
- KV-Pairs in the anonymous global collection can be accessed directly by `Key`.
- KV-Pairs in named local collection can be directly accessed by collection name and Key, or accessed by collection name and using Iterator.

## 1. Hash-based Key-Value Engine

KVDK supports read/write access to Key-Value pairs. Keys and Values are stored as strings, thus KVDK can store any serializable object.

### 1.1 Architecture

Every KV-Pair is persisted as a `Record` in PMem. A `Record` consists of `DataEntry` and actual `Key` and `Value`.

Every `Record` on PMem is indexed by the `HashTable` in DRAM. `HashTable` stores the address of every `Record` and other information.

Image 1-1 illustrates the overall architecture of KVDK.

Data in DRAM are not persisted. These data are recovered by scanning through PMem and rebuilding the KVDK instance.

| ![image-1-1](image/KVEngine.png) | 
|:--:| 
| Image 1-1 |

### 1.1.1 HashTable in DRAM

`HashTable` is a fine-grained hash table. It stores the address of every `Record` in PMem.
- The `HashTable` consists of multiple logical `Slot`. 
- Each `Slot` contains multiple `HashBucket`. 
- Each `HashBucket` contains multiple `HashEntry`.

#### 1.1.1.1 HashEntry
A `Record` is associated with a `HashEntry` by the 64-bit hash of `Key`. The `HashEntry` use part of the 32-bit subfix of the hash as its address on `HashTable` and stores the 32-bit prefix of the hash in itself.

`HashEntry` has four fields, namingly `key_prefix`, `reference`, `type` and `offset`.

- `key_prefix` is the 32-bit prefix of the hash of the key. This field is accessed when searching for the `HashEntry` corresponding to a certain `Key`.

- `type` is of enum type `DATA_ENTRY_TYPE` for `HashEntry`. 
  - A `HashEntry` can be of `STRING_DATA_RECORD` or `STRING_DELETE_RECORD`. `HashEntry` with `Key` not deleted yet has `STRING_DATA_RECORD` and `HashEntry` associated with deleted KV-Pair has `type` of `STRING_DELETE_RECORD`.
  - `Record` in PMem also has a field of `type`. A `HashEntry` and its corresponding `Record` have the same `type`. 
  - Other `type` are introduced in section `2.1` in this document.

- `reference` logs the number of different `Record` of the same `Key`. This field is used for determining whether the PMem space allocated to a `Record` can be recycled. Recycling of PMem space is documented in subsection `1.2.5`.

- `offset` stores the offset of a `Record` relative to the beginning address of the Key-Value Storage in PMem.

| ![image-1-2](image/HashEntry.png) | 
|:--:| 
| Image 1-2 |

#### 1.1.1.2 HashBucket
`HashEntry` are grouped by N-bit subfix of hash of the `Key` and put into a `HashBucket` with this N-bit subfix as offset relative to the beginning of the `HashTable`. N is defaulted to 27 so there are 128M `HashBucket` in DRAM.

In Image 1-1, the offset of last `HashBucket` is 0x07FFFFFF=2^27-1. N is a adjustable parameter corresponding to `kvdk::Configs::num_hash_buckets`, which can be supplied when initializing a KVDK instance. 

  - `kvdk::Configs::num_hash_buckets` should be equal to 2^N.

  - Larger N implies larger space required by `HashTable`, but it will reduce the time for looking up a `HashEntry` in the `HashTable` because less `HashEntry` will fall in the same `HashBucket`, reducing the time to scan the `HashBucket` during searching.

  - A `HashBucket` adopts singly-linked array of `HashEntry` as its data structure. A singly-linked array has better locality than a singly-linked list when it comes to caching.

  - The size of `HashBucket` is configurable during initialization by `kvdk::Configs::hash_bucket_size`, which is defaulted to 128(Bytes) to hold 7 `HashEntry`. In Image 1-3, `kvdk::Configs::hash_bucket_size` is set to 64(Bytes)(with 8B padding) and thus can hold 3 `HashEntry`.

| ![image-1-3](image/HashBucket.png) | 
|:--:| 
| Image 1-3 |

#### 1.1.1.3 Slot

Multiple `HashBucket` are logically grouped into a `Slot`. A `Slot` is a basic unit for locking and caching.

Every `Slot` contains M `HashBucket`. M is defaulted to 16 and is configured by `kvdk::Configs::num_buckets_per_slot` at initialization of the KVDK instance. In Image 1-1 and Image 1-4, M=2.

Every `Slot` holds a `HashCache` field and a `SpinMutex` field. It also logically holds M `HashBucket`.
  - `HashCache` holds a pointer towards the latest accessed `HashEntry`. This `HashEntry` is checked first when looking up a certain `HashEntry` in the `HashBucket` in this `Slot`. If this `HashEntry` does not match the `Key` looked up, the searching procedure will iterate through `HashEntry` in `HashBucket` one by one until one `HashEntry` matches the `Key`.
  - `SpinMutex` is a spin lock. When inserting, updating or deleting a `Key` that belongs to this `Slot`, the procedure will try to acquire this `SpinMutex` until success. Read from a `key` belonging to this `Slot` will not acquire or be affected by this `SpinMutex`.
  - Since `Slot` is a fine-grained unit for locking, lock contention seldom happens and spin lock is used instead of sleeping lock to avoid switching between user mode and kernel mode.

| ![image-1-4](image/Slot.png) | 
|:--:| 
| Image 1-4 |

### 1.1.2 Key-Value Storage on PMem

PMem is mapped as file by system and allocated to KVDK instances. In a KVDK instance this space is allocated, freed and defragmented by `PMEMAllocator`.

`Record` is the format for persisted KV-Pairs on PMem. A `Record` consists of `DataEntry`, `Key` and `Value`.
  - `DataEntry` holds necessary information for extracting the raw c-strings of `Key` and `Value`, as well as checksums and other information to guarantee validity of data.
  - A `DataEntry` consists of a `DataHeader` and a `Meta` part. `Key` is stored right after `DataEntry` and `Value` is right after `Key` in the actual layout on PMem occupied by the `Record`.
  - `DataHeader` has a `checksum` field and a `b_size` field.
    - `checksum` is the checksum of `Meta`, `Key` and `Value`. It is checked for the validity of the `Record` when recovering from a system failure.
    - `b_size` records the actual space occupied by `Record`. This space is slightly larger than `Record` because of inner fragmentation. `b_size` is measured in the unit of `block`. The size of a `block` is defined by `kvdk::Configs::pmem_block_size` at initialization.
  - `Meta` holds `timestamp`, `type`, `k_size` and `v_size` field.
    - `timestamp` is a global time stamp. It is used as the "version number" of the `Record`. The `timestamp` is acquired from TSC(Time Stamp Counter) from CPU. Every `Record` holds a `timestamp` that records the total  KVDK instance CPU cycles from the instance initialization to the time point of the insertion of the `Record`.
    - `type` is of enum type `DATA_ENTRY_TYPE`. A `Record` holds the same `type` as its associated `HashEntry`.
      - When a `Key` is inserted,  a `Record` of `STRING_DATA_RECORD` is inserted into PMem.
      - When a `Key` is updated, a `Record` of `STRING_DATA_RECORD` is inserted into PMem while the old `Record` remains unchanged. However the space of the old `Record` is freed by `PMEMAllocator` for reuse.
      - When a `Key` is deleted, a `Record` of `STRING_DELTE_RECORD` is inserted into PMem while the old `Record` remains unchanged. However the space of the old `Record` is freed by `PMEMAllocator` for reuse. 
      - `STRING_DELTE_RECORD` exists for persisting the "Deleted" status of the `Key`, otherwise the `Key` may be recovered after a system failure because older `Record` is persisted on PMem.
      - The space of older `Record` is freed always after the insertion of a new `STRING_DELTE_RECORD` or `STRING_DATA_RECORD`. A system failure during the update or deletion of a `Key` will cause KVDK to roll back to previous status, thus preventing data loss.

| ![image-1-5](image/DataEntry.png)| 
|:--:| 
| Image 1-5 |

## 1.2 Operations on KVDK Anonymous Global Collection
KVDK organize KV-Pairs into collections. There is one Anonymous Global Collection built in each KVDK instance. Users can also create named collections.

Elements in Anonymous Global Collection are accessed by `Get`, `Set` and `Delete` methods for read, insert/update and delete operations.

- `1.2.1` and `1.2.2` document two auxiliary procedures shared by `Get`, `Set` and `Delete`. User should not directly call these procedures.

### 1.2.1 Search a HashEntry on HashTable by hash
This procedure is implemented by `kvdk::HashTable::Search`. To access a `Record` in PMem by `Key`, KVDK calculate the hash of `Key`, then searches `HashTable` for corresponding `HashEntry` and extract the address of the `Record` from it.

Pseudo Code for `kvdk::HashTable::Search`
```
1. Hash of Key is passed in. 
2. HashBucket on HashTable is indexed by the N-bit(defaulted to 27) subfix of the hash.
3. Check the Slot the HashBucket belongs to. 
   Calling HashTable::MatchHashEntry to check whether the HashCache pointed from HashCache matches the Key.
   If the HashCache hits, return a copy and the address of the HashEntry. Return Status::Ok. 
   Otherwise goto step 4.
4. Iterating through the HashBucket, calling HashTable::MatchHashEntry on every HashEntry to check for match. 
   If a HashEntry matches, change HashCache to point to this HashEntry and return a copy and the address of the HashEntry. Return Status::Ok.
   otherwise return Status::NotFound.
```

Pseudo Code for `HashTable::MatchHashEntry`
```
1. Check whether the hash of key matches the key_prefix in the HashEntry.
   If matches, goto step 2.
   Otherwise return false.
2. Read offset field in HashEntry and access the Record on PMem, check whether the Key looked up matches the Key in Record.
   If matches, return true.
   Otherwise return false.
```
`HashTable::MatchHashEntry` first checks `HashEntry` in DRAM, which is faster. It only visits `Record` in PMem if the `key_prefix` matches, thus preventing unnecessary access to PMem, which is slower.

When `HashTable::Search` scans `HashBucket`, it also looks for `HashEntry` with type `STRING_DELETE_RECORD` for DRAM space reuse. Only `HashEntry` with `reference == 0` will be reused otherwise PMem leak will occur. Please refer to `1.2.5` for details.

### 1.2.2 Insert or Update a HashEntry
This procedure is implemented by `HashTable::Insert` and shared by `Get`, `Set` and `Delete`.

When inserting a `HashEntry` of a new `Key`
```
1. If KVDK reuse DRAM space of a HashEntry, which is supplied by HashTable::Search, 
   KVDK free the STRING_DELETE_RECORD from PMem first, 
   then KVDK overwrite the old HashEntry with the new HashEntry on HashTable.
2. Otherwise KVDK is writing on newly allocated space in DRAM.
   KVDK directly writes the new HashEntry onto HashTable.
```
When updating or deleting an existing `Key`
```
1. Copy the old HashEntry.
2. KVDK overwrites the old HashEntry with new HashEntry. 
   The new HashEntry may have a different type than the old one and have a different offset pointing to a new Record on PMem.
   The new Record may be HASH_STRING_ENTRY or HASH_DELETE_ENTRY, depending on whether we are performing update or deletion operation.
3. Free old Record indexed by the copy of the old `HashEntry`.
```
By free the old `Record` after inserting a new `Record` and its associated `HashEntry`, KVDK can always roll back to a valid state after system failure.

### 1.2.3 Get

`Get` is implemented by `KVEngine::HashGetImpl`.

To `Get` a `Value` by `Key` in Anonymous Global Collection, 
```
1. Calculate the hash of Key. The hash is used in multiple subordinate calls.
2. Call HashTable::Search to search for the HashEntry corresponding to the Key.
   2.1. If no HashEntry is found then return Status::NotFound.
   2.2. Otherwise copy the HashEntry and its address.
3. If this HashEntry is STRING_DELETE_RECORD, KVDK returns Status::NotFound.
   Otherwise access the Record on PMem by the offset offered by the copied HashEntry. Copy Value in Record for return.
4. Compare the copied HashEntry and the copied-from HashEntry on HashTable.
   The HashEntry may be overwritten by other threads when current thread performs step 3.
   If the HashEntry was overwritten, then current thread copies the HashEntry on HashTable and go back to step 3.
   Otherwise the HashEntry hasn't been overwritten, KVDK returns the Value and Status::Ok.
```
Calling `Get` will not acquire or be affected by `SpinMutex` in `Slot`, thus avoiding lock contention for frequently accessed data.

### 1.2.4 Set

- 当不存在该Key对应的旧的Record时，PMEMDB中的Set操作在PMEM中插入一条新的Record，并在HashTable中插入对应该Key的HashEntry，其offset指向新的Record。如果插入新HashEntry时复用了被Delete的Key的HashEntry的空间，则释放被复用HashEntry对应的Record所占据的PMEM空间。
- 当存在该Key对应的旧的Record时，PMEMDB中的Set操作在PMEM中插入一条新的Record，并更新HashTable中对应该Key的HashEntry，使其offset指向新的Record，同时Free原有的Record所占据的PMEM空间。

- Set操作分为以下步骤：
<!---Mysterious comment for formatting--->
    1. RAII获取该Key在HashTable上从属的Slot的自旋锁。该自旋锁仅影响插入、更新和删除操作(Set&Delete)。
    2. 调用HashTable::Search函数，找到特定Key所对应的HashEntry并对其进行拷贝。
    3. 调用PMEMAllocator为新的Record分配空间。插入新的Record和更新已有的Record都是在PMEM上新分配的地址上操作的。
    4. 获得当前timestamp，如果该timestamp比已有的Record的timestamp旧，则放弃更新操作，Free已分配的PMEM的空间。返回Status::Okay。
    5. 调用PersistDataEntry写入Record。依次写入DataEntry(包含b_size、timestamp、type、k_size、v_size信息)、Key、Value三项。再根据Key的hash计算出checksum写入DataEntry中的checksum字段。这样保证只有完整的Record才具有合法的checksum。
    6. 调用HashTable::Insert插入或更新HashTable中对应该Record的HashEntry。此时会Free旧的Record的空间。
    7. RAII释放该Slot的自旋锁。

- 如果写入时系统发生断电，未写入完整的Record，重启后Recover时会检查checksum。这样能保证如果存在合法的Record，那么PMEMDB恢复出来的一定是最新的Record及其HashEntry。
- 被Free的PMEM空间会被交给PMEMAllocator处理，上面的数据并不会立即被清除，但是随时可能被写入新的Record覆盖。

### 1.2.5 Delete
- Delete与Set操作类似，共用了大部分的代码。
- 当Delete不存在或已删除的Key时，直接返回Status::Okay。
- 当存在该Key对应的旧的Record时，PMEMDB中的Set操作在PMEM中插入一条新的Record，即STRING_DELETE_RECORD，并更新HashTable中对应该Key的HashEntry，使其offset指向新的Record，同时Free原有的Record所占据的PMEM空间。

- Delete操作分为以下步骤：
<!---Mysterious comment for formatting--->
    1. RAII获取该Key在HashTable上从属的Slot的自旋锁。该自旋锁仅影响插入、更新和删除操作(Set&Delete)。
    2. 调用HashTable::Search函数，找到特定Key所对应的HashEntry并对其进行拷贝。如果未找到相应HashEntry或该HashEntry的type为STRING_DELETE_RECORD，说明该Key不存在或已被删除，直接返回Status::Okay。
    3. 调用PMEMAllocator为新的Record分配空间。
    4. 获得当前timestamp，如果该timestamp比已有的Record的timestamp旧，则放弃更新操作，Free已分配的PMEM的空间。返回Status::Okay。
    5. 调用PersistDataEntry写入Record。依次写入DataEntry(包含b_size、timestamp、type、k_size、v_size信息)、Key、Value三项。再根据Key的hash计算出checksum写入DataEntry中的checksum字段。这样保证只有完整的Record才具有合法的checksum。
    6. 调用HashTable::Insert更新HashTable中对应该Record的HashEntry。此时会Free旧的Record的空间。
    7. RAII释放该Slot的自旋锁。

- 当Delete一个存在Record的Key时，HashTable中该Key对应的HashEntry的type会被标记为STRING_DELETE_RECORD，但这一HashEntry并不会立即被复用。
    - HashEntry中的reference记录了历史版本的Record的数量，新插入的Key的reference为1，每次更新该Key时reference会递增1。
    - 更新时Free历史版本Record时reference不会立即更新。只有PMEMAllocator将历史版本Record占用的空间分配给其他Record使用，覆盖了原先的Record后，该reference的计数会减去1。
    - 删除该Key时不会更新reference计数。
    - 当HashEntry中reference归零且type为STRING_DELETE_RECORD时，该HashEntry的空间被允许复用，此时才会Free该STRING_DELETE_RECORD所占用的PMEM空间。这样保证了在Recover时如果该Key处于被删除的状态，那么能通过STRING_DELETE_RECORD恢复出该状态，或者PMEM上不存在该Key相关的Record，不会出现STRING_DELETE_RECORD的空间被复用导致恢复出该Key的历史版本的情形。

### 1.2.6 Recovery

- 系统在启动时重建HashTable和PMEMAllocator，多个线程以Segment为单位遍历PMEM上的DataEntry。Segment是PMEMAllocator分配空间的最大粒度。步骤为：

1. 线程请求一块Segment。如果所有Segment被遍历完，则Recovery完成。
2. 按照PMEM上分配空间的最小粒度block遍历Segment，尝试读取Record，并根据checksum检验是否是合法的Record。
3. 搜索HashTable查看是否已存在相同的Key，如果该Record比历史版本新则更新HashTable。较旧版本的Record所有的PMEM空间将被Free。
4. 扫描该Segment上的其他Record。如果该Segment被遍历完则返回1。

- Recovery结束时记录下当前最大timestamp，后续数据的timestamp以该timestamp为基础增加。timestamp是自PMEMDB创建以来的总CPU cycle。

# 2. Collection
- PMEMDB支持存储PersistentList。PersistentList是持久化在PMEM上的集合数据结构。PersistentList分为Skiplist和后续会实现的Hashlist。
- Skiplist是有序的集合，使用了带双向链表的skip list数据结构。支持对Skiplist内元素的读写、删除操作，同时支持前缀搜索、对Skiplist全体或部分元素进行顺序或逆序遍历的操作。
- Hashlist是无序的集合，使用了双向链表数据结构。支持对Hashlist内元素的读写、删除操作，同时支持对Hashlist内全体元素进行遍历的操作。

## 2.1 Sorted Collection
## 2.1 Skiplist Architecture
- Skiplist使用了和1.1中描述相同的架构，但是在PMEM Storage中的DataEntry上加入了前驱prev与后继next两个字段，从而将Record的排序也持久化记录到PMEM上。
- 每一个Skiplist是一个PersistentList，该PersistentList以其名称list_name和特有的id组成KV-Pair被持久化存储于PMEM上(下文记作HeaderRecord，区别于一般的Record)。用户通过list_name来访问该PersistentList及其中的元素。
- Skiplist的id和其中元素的Key(下文记作UserKey)拼接起来组成实际存储在PMEM上Key-Value Storage中的SkiplistKey。按照UserKey访问特定Skiplist中的特定SkiplistKey时，首先根据Skiplist的list_name以类似1.2.3中Get函数的方式访问HeaderRecord，读取其id，再由id和UserKey拼接为SkiplistKey，再以类似1.2中的Get、Set和Delete函数类似的方式读取、修改、删除存储于PMEMDB上的Record。

### 2.1.1 HashTable of Skiplist in DRAM
- 同1.1.1。Skiplist中的元素以完全一样的方式被HashTable索引。
- Skiplist的list_name和id组成的KV-Pair也被HashTable索引。
- PMEMDB中Skiplist的元素和其本身list_name所对应的HashEntry与最基本的string的KV-Pair的HashEntry共同存放于同一个HashTable内，但三者具有不同的类型。
    - String的类型的Record对应的HashEntry的type为STRING_DATA_RECORD或STRING_DELETE_RECORD。
    - Skiplist中的元素的Record对应的HashEntry的type为SORTED_DATA_RECORD或SORTED_DELETE_RECORD。未被删除的KV-Pair的HashEntry的type为SORTED_DATA_RECORD，被删除的KV-Pair的HashEntry的type为SORTED_DELETE_RECORD。
    - Skiplist本身的list_name和id组成的KV-Pair对应的HashEntry的type为SORTED_HEADER_RECORD。目前不支持删除或更新Skiplist本身，只能删除Skiplist中的元素。

### 2.1.2 Key-Value Storage of Skiplist on PMEM

- Skiplist的Record(SORTED_RECORD)以类似一般String的Record(STRING_RECORD)的形式存储与PMEM上。但与STRING_RECORD不同，STRING_RECORD头部不再是DataEntry而是DLDataEntry(Doubly Linked DataEntry)，相较于DataEntry多了prev和next字段用于记录排序的信息。
- DLDataEntry中的Header和Meta字段与DataEntry几乎一致，区别在于checksum的算法略有不同，以及type不同。
    - type：Record的类型，与HashEntry的类型一致。有SORTED_DATA_RECORD, SORTED_DELETE_RECORD两类。
        - SORTED_DATA_RECORD是一般的DataEntry，新插入的KV-Pair均为SORTED_DATA_RECORD类型。
        - SORTED_DELETE_RECORD是一类特殊的Record，被删除的Key在HashTable中对应的DataEntry的type会被更新为SORTED_DELETE_RECORD，并在PMEM中放入type为SORTED_DELETE_RECORD的DataEntry。SORTED_DELETE_RECORD用于在断电重启时恢复Skiplist中的数据。
        - SORTED_HEADER_RECORD是一类特殊的Record，记录Skiplist本身的list_name和id，并且在其next域内存储Skiplist内第一个元素的在PMEM上Record的offset。
    - prev, next：记录前驱和后继的offset的指针，用于将单个Skiplist上的Record排序并持久化。

- Record，即DLDataEntry连同后面的KV-Pair在PMEM上的layout如图2-2所示。

| ![image-2-2](image/DLDataEntry.png)| 
|:--:| 
| 图 2-2 |

### 2.1.3 Skip list in DRAM
- Skip list(指具体的数据结构)是Skiplist(指每一个存放复数个元素的集合)上特有的数据结构，是一个单链表连接的跳转表，用于快速查找，存储于DRAM上。
- Skip list数据结构参照图2-3。

| ![image-2-3](image/Skiplist.png)| 
|:--:| 
| 图 2-3 |

- PMEM上每一条Record对应一个SkiplistNode存储于DRAM上，该SkiplistNode内存储了指向下一个节点的塔std::atomic<SkiplistNode*> next[]，塔的高度height，以及用于快速比较的cached_key及其大小cached_key_size，和该SkiplistNode所对应的Record的地址DLDataEntry* data_entry。

## 2.2 Operations on Skiplist in PMEMDB
- Skiplist基本的读取SGet、写入SSet和删除SDelete操作和针对STRING_RECORD的Get、Set和Delete操作类似，但因为要维护DRAM上的skip list所以步骤上会更加复杂。
- 访问Skiplist上的元素时，首先会根据Skiplist自身的名称list_name访问其在PMEM上持久化存储的HeaderRecord(2.1.1)，读取其id。再将id与Skiplist中元素的UserKey拼接组成SkiplistKey，再按照类似1.2中的逻辑访问该元素。
- 2.2.1是辅助函数，为SGet、SSet、SDelete共用。用户不会调用。

### 2.2.1 Search for HeaderRecord on HashTable
- HeaderRecord(2.1.1)是Skiplist的入口。通过函数KVEngine::SearchOrInitSkiplist获取Skiplist的地址。
- 当该Skiplist存在时，返回PMEM上HeaderRecord的地址。返回Status::Ok。
- 当该Skiplist不存在时，在SSet和SDelete中，会在PMEM上写入新的HeaderRecord并生成id，返回其地址。返回Status::Ok。
- 通过SGet试图访问不存在的Skiplist中的元素时，不会生成新的Skiplist。返回Status::NotFound。

### 2.2.2 Seek for a key on skip list
- Seek函数返回Splice结构，Splice是一个记录位置的结构，处于skip list的两个tower的中间位置，记录着前后两个tower对应的DLDataEntry的地址以及该位置在不同高度下前后两个tower所对应的SkiplistNode的地址。
- 当Seek的SkiplistKey存在时，返回的Splice处在该SkiplistKey之后的位置。
- 当Seek的SkiplistKey不存在时，返回的Splice处在该Skiplist应该被插入的前后两个节点之间的位置。

### 2.2.3 SGet
- 用户按照特定Skiplist的名称list_name访问特定UserKey时，
<!---
Escape symbol :: below
--->
    1. 调用KVEngine::SearchOrInitSkiplist获取Skiplist的地址。如该函数返回Status::Ok以外的状态，则直接将该状态返回给Caller处理。
    2. 根据Skiplist在PMEM上的SORTED_HEADER_RECORD的地址读出该Record上记录的Skiplist的id。将该id与UserKey拼接为SkiplistKey。
    3. 调用KVEngine::Get的底层实现KVEngine::HashGetImpl读取SkiplistKey对应的Record上的Value。

- 调用SGet不会对Slot加锁，避免了在热点访问场景下的锁冲突。

### 1.2.3 SSet

- SSet操作分为以下步骤：
<!---Mysterious comment for formatting--->
    1. 调用KVEngine::SearchOrInitSkiplist获取Skiplist的地址。如该函数返回Status::Ok以外的状态，则直接将该状态返回给Caller处理。
    2. 根据Skiplist在PMEM上的SORTED_HEADER_RECORD的地址读出该Record上记录的Skiplist的id。将该id与UserKey拼接为SkiplistKey。
    3. 为新插入或要更新的Record分配PMEM空间。
    4. RAII获取该SkiplistKey在HashTable上从属的Slot的自旋锁。该自旋锁仅影响插入、更新和删除操作(SSet&SDelete)。
    5. 调用HashTable::Search函数，找到特定Key所对应的HashEntry并对其进行拷贝。该HashEntry将在写入新的Record后被更新。
    6. 获得当前timestamp，如果该timestamp比已有的Record的timestamp旧，则放弃更新操作，Free已分配的PMEM的空间。返回Status::Okay，RAII释放自旋锁。
    7. 如未找已有的Record，则调用2.2.2中的Seek找到应插入该Record的位置，记录于Splice中。如已找到，则将Splice置于该Record后方的位置。
    8. 根据Splice对插入位置的前后两个Record对应的HashEntry从属的Slot加自旋锁。
    9. 将新的Record写入PMEM上，在skip list中插入相应的SkiplistNode。插入时对前驱后继的更新使用了原子操作，无需加锁。
    10. 调用HashTable::Insert插入或更新HashTable中对应该Record的HashEntry。此时会Free旧的Record的空间。
    11. 释放前驱后继的自旋锁，RAII释放该Slot的自旋锁。

### 1.2.5 SDelete
- SDelete与SSet操作类似，共用了大部分的代码。
- 当试图SDelete不存在的Skiplist中的元素或Skiplist中不存在的元素时，会直接返回。
- 删除存在的Skiplist中的元素时，等同于通过SSet写入一条SORTED_DELETE_RECORD。

### 2.2.6 Recovery

Skiplist的Recovery流程和1.2.6类似，但需要对SORTED_RECORD做特殊处理：
1. 对每个SORTED_RECORD，需查看其前驱、后继结点的next、prev指针是否均指向自己，判断该结点是否有效
2. HashTable重建完成后，遍历每个Skiplist中的元素重建DRAM中的skip list。

