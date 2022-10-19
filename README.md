<div align="center">
<p align="center"> <img src=".pic/kvdk_logo.png" height="180px"><br></p>
</div>

`KVDK` (Key-Value Development Kit) is a key-value store library implemented in C++ language. It is designed for supporting Optane persistent memory, DRAM and CXL memory pool. It also demonstrates several optimization methods for high performance with tiered memory. Besides providing the basic APIs of key-value store, it offers several advanced features, like read-modify-write, checkpoint, etc.

**Notice: The DRAM engine and CXL memory pool are in development, you can checkout to release version 1.0 for a stable version of PMEM engine.**

## Getting the Source
```bash
git clone --recurse-submodules https://github.com/pmem/kvdk.git
```

## User guide

KVDK of different storage type are independently modules, please refer to [persistent](./persistent/README.md) and [volatile](./volatile/README.md) for building, test and docs.

# Support
Welcome to join the wechat group or slack channel for KVDK tech discussion.
- [Wechat](https://github.com/pmem/kvdk/issues/143)
- [Slack Channel](https://join.slack.com/t/kvdksupportcommunity/shared_invite/zt-12b66vg1c-4FGb~Ri4w8K2_msau6v86Q)
