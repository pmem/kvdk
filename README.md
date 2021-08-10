# **KVDK**

`KVDK` (Key-Value Development Kit) is a key-value store library implemented in C++ language. It is designed for persistent memory and provides unified APIs for both volatile and persistent scenarios. It also demonstrates several optimization methods for high performance with persistent memory. Besides providing the basic APIs of key-value store, it offers several advanced features, like transaction, snapshot as well.

## Features
*  The basic get/set/update/delete opertions on unsorted keys.
*  The basic get/set/update/delete/iterate operations on sorted keys.
*  Multiple changes on unsorted keys can be made in one atomic batch.
*  User can create multiple collections of sorted keys.
*  Support read-committed transaction. (TBD)
*  Support snapshot to get a consistent view of data. (TBD)

# Limitations
*  Maximum supported key-value size are 64KB-64MB.
*  The maximum write thread number can't be dynamicly changed after start-up.
*  No support of key-value compression.
*  Persistent memory space can't be expanded on the fly.

## Getting the Source
```bash
git clone --recurse-submodules https://github.com/pmem/kvdk.git
```

## Building
```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release && cmake --build .
```

## Benchmarks
[Here](./doc/benchmark.md) are the examples on how to benchmark the performance of KVDK on your own systems.

## Documentations

### User Guide

Please reference to [User guide](./doc/user_doc.md) for API introductions of KVDK.

### Architecture
