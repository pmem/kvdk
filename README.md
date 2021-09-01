# **KVDK**

`KVDK` (Key-Value Development Kit) is a key-value store library implemented in C++ language. It is designed for persistent memory and provides unified APIs for both volatile and persistent scenarios. It also demonstrates several optimization methods for high performance with persistent memory. Besides providing the basic APIs of key-value store, it offers several advanced features, like transaction, snapshot.

## Features
*  The basic get/set/update/delete opertions on unsorted keys.
*  The basic get/set/update/delete/iterate operations on sorted keys.
*  Provide APIs to write multiple key-value pairs in an atomic batch.
*  User can create multiple collections of sorted keys.
*  Support read-committed transactions. (TBD)
*  Support snapshot to get a consistent view of data. (TBD)

# Limitations
*  Maximum supported key-value size is 64KB-64MB.
*  No support of changing the maximum write thread number after start-up.
*  No approval of key-value compression.
*  Users can't expand the persistent memory space in the fly.

## Getting the Source
```bash
git clone --recurse-submodules https://github.com/pmem/kvdk.git
```

## Building
```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DCHECK_CPP_STYLE=ON && make -j
```

## Benchmarks
[Here](./doc/benchmark.md) are the examples of how to benchmark the performance of KVDK on your systems.

## Documentations

### User Guide

Please refer to [User guide](./doc/user_doc.md) for API introductions of KVDK.

### Architecture
