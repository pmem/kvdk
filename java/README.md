## Build

### System requirements
* JDK >= 1.8
* Apache Maven >= 3.1.0

### Build KVDK JNI library
Add `-DWITH_JNI=ON` to cmake command options. For example:

```bash
cd kvdk
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DCHECK_CPP_STYLE=ON -DWITH_JNI && make -j
```

### Unit Test with Apache Maven
```
mvn clean test
``` 

## Release
### Install to local Maven repository
```
mvn clean install
```

### Release to Maven Central
1. Sign and deploy jar [to OSSRH](https://central.sonatype.org/publish/publish-manual/)
2. Release deployment [from OSSRH to Maven Central](https://central.sonatype.org/publish/release/)

## Run examples
```
cd examples
mvn clean package

export PMEM_IS_PMEM_FORCE=1
mkdir -p /tmp/kvdk-test-dir

java -cp target/kvdkjni-examples-1.0.0-SNAPSHOT.jar:../target/kvdkjni-1.0.0-SNAPSHOT.jar io.pmem.kvdk.examples.KVDKExamples
```

## Cross Platform

The KVDK Java library contains the needed shared libaries (`.so` files) in the jar.

A Java application relying on KVDK Java library can be run on 64-bit Linux servers without building the KVDK C++ code, when the C++ Standard Library is new enough so that `libstdc++.so.6` includes `GLIBCXX_3.4.22` and `CXXABI_1.3.11`.

We tested on `Centos-8`, `Fedora 29` and `Ubuntu-18.04`, in which the `libstdc++.so.6` is new enough. And we don't need to build the KVDK C++ code to run Java applications using KVDK.
