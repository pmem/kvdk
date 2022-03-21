# **KVDK**

`KVDK` (Key-Value Development Kit) is a key-value store library implemented in C++ language. It is designed for persistent memory and provides unified APIs for both volatile and persistent scenarios. It also demonstrates several optimization methods for high performance with persistent memory. Besides providing the basic APIs of key-value store, it offers several advanced features, like transaction, snapshot.

## Features
*  The basic get/set/update/delete opertions on unsorted keys.
*  The basic get/set/update/delete/iterate operations on sorted keys.
*  Provide APIs to write multiple key-value pairs in an atomic batch.
*  User can create multiple collections of sorted keys.
*  Support checkpoint to get a consistent view of data.
*  Support read-committed transactions. (TBD)

# Limitations
*  Maximum supported key-value size is 64KB-64MB.
*  No support of changing the maximum access thread number after start-up.
*  No approval of key-value compression.
*  Users can't expand the persistent memory space in the fly.

## Getting the Source
```bash
git clone --recurse-submodules https://github.com/pmem/kvdk.git
```

## Building
### Install dependent tools and libraries on Ubuntu 18.04
```bash
sudo apt install make clang-format-9 pkg-config g++ autoconf libtool asciidoctor libkmod-dev libudev-dev uuid-dev libjson-c-dev libkeyutils-dev pandoc libhwloc-dev libgflags-dev libtext-diff-perl bash-completion systemd wget git

git clone https://github.com/pmem/ndctl.git
cd ndctl
git checkout v70.1
./autogen.sh
./configure CFLAGS='-g -O2' --prefix=/usr --sysconfdir=/etc --libdir=/usr/lib
make
sudo make install

git clone https://github.com/pmem/pmdk.git
cd pmdk
git checkout 1.11.1
make
sudo make install

wget https://github.com/Kitware/CMake/releases/download/v3.12.4/cmake-3.12.4.tar.gz
tar vzxf cmake-3.12.4.tar.gz
cd cmake-3.12.4
./bootstrap
make
sudo make install

```

### Install dependent tools and libraries on CentOS 8
```bash
yum config-manager --add-repo /etc/yum.repos.d/CentOS-Linux-PowerTools.repo
yum config-manager --set-enabled PowerTools

yum install -y git gcc gcc-c++ autoconf automake asciidoc bash-completion xmlto libtool pkgconfig glib2 glib2-devel libfabric libfabric-devel doxygen graphviz pandoc ncurses kmod kmod-devel libudev-devel libuuid-devel json-c-devel keyutils-libs-devel gem make cmake libarchive clang-tools-extra hwloc-devel perl-Text-Diff gflags-devel

git clone https://github.com/pmem/ndctl.git
cd ndctl
git checkout v70.1
./autogen.sh
./configure CFLAGS='-g -O2' --prefix=/usr --sysconfdir=/etc --libdir=/usr/lib
make
sudo make install

git clone https://github.com/pmem/pmdk.git
cd pmdk
git checkout 1.11.1
make
sudo make install

wget https://github.com/Kitware/CMake/releases/download/v3.12.4/cmake-3.12.4.tar.gz
tar vzxf cmake-3.12.4.tar.gz
cd cmake-3.12.4
./bootstrap
make
sudo make install
```

### Compile KVDK
```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DCHECK_CPP_STYLE=ON && make -j
```

### How to test it on a system without PMEM
```bash
# set the correct path for pmdk library
export LD_LIBRARY_PATH=/usr/local/lib64

# setup a tmpfs for test
mkdir /mnt/pmem0
mount -t tmpfs -o size=2G tmpfs /mnt/pmem0

# force the program work on non-pmem directory
export PMEM_IS_PMEM_FORCE=1

cd kvdk/build/examples
# Note: this requires CPU supporting AVX512
./cpp_api_tutorial

```

## Benchmarks
[Here](./doc/benchmark.md) are the examples of how to benchmark the performance of KVDK on your systems.

## Documentations

### User Guide

Please refer to [User guide](./doc/user_doc.md) for API introductions of KVDK.

### Architecture

# Support
Welcome to join the wechat group or slack channel for KVDK tech discussion.
- [Wechat](https://github.com/pmem/kvdk/issues/143)
- [Slack Channel](https://join.slack.com/t/kvdksupportcommunity/shared_invite/zt-12b66vg1c-4FGb~Ri4w8K2_msau6v86Q)
