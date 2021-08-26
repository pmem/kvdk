# **kvrocks**

`Kvrocks` is an open-source key-value database which is based on rocksdb and compatible with Redis protocol. We demonstrate how to use KVDK to replace rocksdb as the backend engine of kvrocks to get higher performance with persistent memory.

# Build Instructions
*  Install gflags which is a dependent library of kvrocks.
```
git clone https://github.com/gflags/gflags
cmake . -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_gflags_LIB=ON
make -j
make install
``` 
*  Clone kvrocks source code on specific tag.
```
git clone --recursive https://github.com/kvrockslabs/kvrocks.git
git checkout v2.0.2
```
*  Apply patchs
```
cd kvrocks
git apply ../0001-use-KVDK-for-set-get-operations.patch
```
*  Copy kvdk library to kvrocks
```
mkdir -p external/kvdk/include
cp ../../../include/kvdk/*  external/kvdk/include/
cp ../../../build/libengine.so external/kvdk/
```
*  Build kvrocks
```
make -j
```
## Benchmark
