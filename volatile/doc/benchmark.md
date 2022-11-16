# Benchmark tool

To test performance of KVDK, you can run our benchmark tool "bench", the tool is auto-built along with KVDK library in the build dir. 

Here is an example to run benchmarks on `string` type:
```bash
./bench -path=./kvdk_bench_dir -type=string -num_kv=8388608 -num_operations=1048576 -threads=10 -max_access_threads=64 -value_size=120 -latency=0
```

To benchmark performance when KVs are stored on separted memory nodes, we can use `numactl`:
```bash
numactl --cpunodebind=0 --membind=0 ./bench -path=./kvdk_bench_dir -type=string -num_kv=8388608 -num_operations=1048576 -threads=10 -max_access_threads=64 -value_size=120 -latency=0 -dest_memory_nodes=1
```

The above configurations will consume ~5.4 GB memory. Specifically, ~4.5 GB memory is used by KVDK. ~0.9 GB memory is used by the benchmark itself.

Explanation of arguments:

    -path: KVDK initialized here

    -type: Type of key-value pairs to benchmark, it can be string/sorted/hash/list/blackhole.

    -num_kv: Number of KV when benchmarking fill/insert.

    -num_operations: Number of operations running benchmarks other than fill/insert.

    -threads: Number of threads of benchmark. `max_access_threads` will override this when benchmarking `fill`.

    -max_access_threads: Max concurrent access threads in the KVDK instance, set it to the number of the hyper-threads for performance consideration. You can call KVDK API with any number of threads, but if your parallel threads more than max_access_threads, the performance will be degraded due to synchronization cost.

    -value_size: Value length of values in Byte.

    -latency: Print latencies of operations or not. Enable this makes benchmark use more memory.

    -dest_memory_nodes: The memory nodes to store KV data.

## More configurations

For more configurations of the benchmark tool, please reference to "benchmark/bench.cpp".
