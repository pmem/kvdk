#!/bin/bash

BASEDIR=$(dirname "$0")

# Note: The directory $pmem_path will be removed from file system automatically before benchmarking
pmem_path=/mnt/pmem0/kvdk/bench-dir
space=412316860416
num_kv=536870912

# languages
for lang in java native; do
    echo "#################################### $lang ####################################"

    bin="java -cp $BASEDIR/../target/kvdkjni-benchmark-1.0.0-SNAPSHOT.jar:$BASEDIR/../../target/kvdkjni-1.0.0-SNAPSHOT.jar io.pmem.kvdk.benchmark.KVDKBenchmark"
    if [ "$lang" == "native" ]; then
        bin=$BASEDIR/../../../build/bench
    fi

    # value_sizes
    for value_size in 120 4096; do
        echo "#################################### value_size: $value_size ####################################"
        if [[ "$value_size" == "4096" ]]; then
            # calculation logic for num_kv:
            # header_size = type == string ? 24 : 48
            # key_size = 8
            # num_kv = space // 4 // (header_size + key_size + average_value_size)

            num_kv=24778657
        fi

        # benchmarks
        for type in string sorted blackhole; do
            echo "#################################### $type ####################################"
            rm -rf $pmem_path

            num_ops1=$num_kv
            num_ops2=10737418240

            if [ "$type" == "blackhole" ]; then
                num_ops1=$num_ops2
            fi

            # fill
            echo "#################################### fill"
            numactl -N 0 -m 0 $bin -populate=true -latency=true -path=$pmem_path -space=$space -value_size=$value_size -num_kv=$num_kv -num_operations=$num_ops1 -max_access_threads=64 -threads=64 -type=$type -num_collection=16 -fill=true

            # batch insert random
            echo "#################################### batch insert random"
            numactl -N 0 -m 0 $bin -populate=true -latency=true -path=$pmem_path -space=$space -value_size=$value_size -num_kv=$num_kv -num_operations=$num_ops1 -max_access_threads=64 -threads=64 -type=$type -num_collection=16 -fill=false -timeout=30 -read_ratio=0 -existing_keys_ratio=0 -batch_size=100

            # insert random
            echo "#################################### insert random"
            numactl -N 0 -m 0 $bin -populate=true -latency=true -path=$pmem_path -space=$space -value_size=$value_size -num_kv=$num_kv -num_operations=$num_ops1 -max_access_threads=64 -threads=64 -type=$type -num_collection=16 -fill=false -timeout=30 -read_ratio=0 -existing_keys_ratio=0

            # range scan
            echo "#################################### range scan"
            numactl -N 0 -m 0 $bin -populate=true -latency=true -path=$pmem_path -space=$space -value_size=$value_size -num_kv=$num_kv -num_operations=$num_ops2 -max_access_threads=64 -threads=64 -type=$type -num_collection=16 -fill=false -timeout=30 -read_ratio=1 -scan=1

            # read random
            echo "#################################### read random"
            numactl -N 0 -m 0 $bin -populate=true -latency=true -path=$pmem_path -space=$space -value_size=$value_size -num_kv=$num_kv -num_operations=$num_ops2 -max_access_threads=64 -threads=64 -type=$type -num_collection=16 -fill=false -timeout=30 -read_ratio=1

            # read write random (9R:1W)
            echo "#################################### read write random 9R:1W"
            numactl -N 0 -m 0 $bin -populate=true -latency=true -path=$pmem_path -space=$space -value_size=$value_size -num_kv=$num_kv -num_operations=$num_ops1 -max_access_threads=64 -threads=64 -type=$type -num_collection=16 -fill=false -timeout=30 -read_ratio=0.9

            # update random
            echo "#################################### update random"
            numactl -N 0 -m 0 $bin -populate=true -latency=true -path=$pmem_path -space=$space -value_size=$value_size -num_kv=$num_kv -num_operations=$num_ops2 -max_access_threads=64 -threads=64 -type=$type -num_collection=16 -fill=false -timeout=30 -read_ratio=0
        done
    done
done
