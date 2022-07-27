## Build

### System requirements
* JDK >= 1.8
* Apache Maven >= 3.1.0

### Build KVDK JNI library
Add `-DWITH_JNI=ON` to cmake command options. For example:

```bash
cd kvdk
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DCHECK_CPP_STYLE=ON -DWITH_JNI=ON && make -j
```

### Unit Test with Apache Maven
```bash
cd kvdk/java
mvn clean test
``` 

## Release
### Install to local Maven repository
```bash
cd kvdk/java
mvn clean install
```

### Release to Maven Central
1. Sign and deploy jar [to OSSRH](https://central.sonatype.org/publish/publish-manual/)
2. Release deployment [from OSSRH to Maven Central](https://central.sonatype.org/publish/release/)

## Run examples
```bash
cd kvdk/java/examples
mvn clean package

export PMEM_IS_PMEM_FORCE=1
mkdir -p /tmp/kvdk-test-dir

java -cp target/kvdkjni-examples-1.0.0-SNAPSHOT.jar:../target/kvdkjni-1.0.0-SNAPSHOT.jar io.pmem.kvdk.examples.KVDKExamples
```

## Run benchmark
```bash
cd kvdk/java/benchmark
mvn clean package

mkdir -p /mnt/pmem0/kvdk/bench-dir

export type=string
# or export type=sorted

# fill example
java -cp target/kvdkjni-benchmark-1.0.0-SNAPSHOT.jar:../target/kvdkjni-1.0.0-SNAPSHOT.jar io.pmem.kvdk.benchmark.KVDKBenchmark -fill=true -latency=true -path=/mnt/pmem0/kvdk/bench-dir -space=412316860416 -num_kv=536870912 -num_operations=536870912 -type=$type -threads=32

# random batch insert example
java -cp target/kvdkjni-benchmark-1.0.0-SNAPSHOT.jar:../target/kvdkjni-1.0.0-SNAPSHOT.jar io.pmem.kvdk.benchmark.KVDKBenchmark -fill=false -latency=true -path=/mnt/pmem0/kvdk/bench-dir -space=412316860416 -num_kv=536870912 -num_operations=536870912 -type=$type -threads=32 -timeout=30 -read_ratio=0 -existing_keys_ratio=0 -batch_size=100

# random insert example
java -cp target/kvdkjni-benchmark-1.0.0-SNAPSHOT.jar:../target/kvdkjni-1.0.0-SNAPSHOT.jar io.pmem.kvdk.benchmark.KVDKBenchmark -fill=false -latency=true -path=/mnt/pmem0/kvdk/bench-dir -space=412316860416 -num_kv=536870912 -num_operations=536870912 -type=$type -threads=32 -timeout=30 -read_ratio=0 -existing_keys_ratio=0

# range scan example
java -cp target/kvdkjni-benchmark-1.0.0-SNAPSHOT.jar:../target/kvdkjni-1.0.0-SNAPSHOT.jar io.pmem.kvdk.benchmark.KVDKBenchmark -fill=false -latency=true -path=/mnt/pmem0/kvdk/bench-dir -space=412316860416 -num_kv=536870912 -num_operations=10737418240 -type=$type -threads=32 -timeout=30 -read_ratio=1 -scan=1

# random read example
java -cp target/kvdkjni-benchmark-1.0.0-SNAPSHOT.jar:../target/kvdkjni-1.0.0-SNAPSHOT.jar io.pmem.kvdk.benchmark.KVDKBenchmark -fill=false -latency=true -path=/mnt/pmem0/kvdk/bench-dir -space=412316860416 -num_kv=536870912 -num_operations=10737418240 -type=$type -threads=32 -timeout=30 -read_ratio=1

# random read write example (9R:1W)
java -cp target/kvdkjni-benchmark-1.0.0-SNAPSHOT.jar:../target/kvdkjni-1.0.0-SNAPSHOT.jar io.pmem.kvdk.benchmark.KVDKBenchmark -fill=false -latency=true -path=/mnt/pmem0/kvdk/bench-dir -space=412316860416 -num_kv=536870912 -num_operations=536870912 -type=$type -threads=32 -timeout=30 -read_ratio=0.9

# random update example
java -cp target/kvdkjni-benchmark-1.0.0-SNAPSHOT.jar:../target/kvdkjni-1.0.0-SNAPSHOT.jar io.pmem.kvdk.benchmark.KVDKBenchmark -fill=false -latency=true -path=/mnt/pmem0/kvdk/bench-dir -space=412316860416 -num_kv=536870912 -num_operations=10737418240 -type=$type -threads=32 -timeout=30 -read_ratio=0
```

## Cross Platform

The KVDK Java library contains the needed shared libaries (`.so` files), which will be loaded when they are not present in system library paths.

A Java application relying on KVDK Java library can be run on **64-bit** servers without building the KVDK C++ code or installing its dependencies (e.g. ndctl, pmdk).

The only requirement is the `libc` version `GLIBC_2.18 or higher` can be found.

We tested without installing KVDK C++ library or its dependencies on below platforms, in which the `libc` is new enough:
* Fedora >= 20
* Ubuntu >= 14.04
* Centos >= 8

For older platforms, you may need to upgrade `libc` in system path or where `LD_LIBRARY_PATH` points to.
