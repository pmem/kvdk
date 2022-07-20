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
TODO


## Run examples
```
cd examples
mvn clean package

export PMEM_IS_PMEM_FORCE=1
mkdir -p /tmp/kvdk-test-dir

java -cp target/kvdkjni-examples-1.0.0-SNAPSHOT.jar:../target/kvdkjni-1.0.0-SNAPSHOT.jar io.pmem.kvdk.examples.KVDKExamples
```

Note:

If the examples are planned to be run on another server, not the one building KVDK on, you should make sure the C++ Standard Library is new enough so that `libstdc++.so.6` includes `GLIBCXX_3.4.22` and `CXXABI_1.3.11`.

Otherwise, you may need to install newer version of `gcc` or set the `LD_LIBRARY_PATH` to proper directory that has newer version of `libstdc++.so.6`.

We tested on `Centos-8` and `Ubuntu-18.04`, the version of `libstdc++.so.6` is OK.
