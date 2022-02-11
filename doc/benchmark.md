# Benchmark tool

To test performance of KVDK, you can run our benchmark tool "bench", the tool is auto-built along with KVDK library in the build dir. 

You can manually run individual benchmark follow the examples as shown bellow, or simply run our basic benchmark script "scripts/run_benchmark.py" to test all the basic read/write performance.

## Fill data to new instance

To test performance, we need to first fill key-value pairs to the KVDK instance. Since KVDK did not support cross-socket access yet, we need to bind bench program to a numa node: 

    numactl --cpunodebind=0 --membind=0 ./bench -fill=1 -value_size=120 -threads=64 -path=/mnt/pmem0/kvdk -space=274877906944 -num=838860800 -max_write_threads=64 -type=string -populate=1

This command will fill 83886088 uniform distributed string-type key-value pairs to the KVDK instance that located at /mnt/pmem0/kvdk.

Explanation of arguments:

    -fill: Indicates filling data to a new instance.

    -threads: Number of threads of benchmark.

    -space: PMem space that allocate to the KVDK instance.

    -max_access_threads: Max concurrent access threads of the KVDK instance, set it to the number of the hyper-threads for performance consideration.

    -type: Type of key-value pairs to benchmark, it can be "string", "hash" or "sorted".

    -populate: Populate pmem space while creating new KVDK instance for best write performance in runtime, see "include/kvdk/configs.hpp" for explanation.

## Test read/write performance

### Read performance

After fill the instance, we can test read performance with the command below:

    numactl --cpunodebind=0 --membind=0 ./bench -fill=0 -time=10 -value_size=120 -threads=64 -read_ratio=1 -existing_keys_ratio=1 -path=/mnt/pmem0/kvdk -space=274877906944 -num=838860800 -max_write_threads=64 -type=string

This will read key-value pairs from the KVDK instance with 48 threads in 10 seconds.

Explanation of arguments:

    -read_ratio: Ratio of read threads among benchmark threads, for example, if set it to 0.5, then there will be 24 write threads and 24 read threads.
    
    -existing_keys_ratio: Ratio of keys among key-value pairs to read that already filled in the instance. For example, if set it to 0.5, then 50% read operations will return NotFound.

Benchmark tool will print performance stats to stdout, include throughput in each second and average ops:

    $numactl --cpunodebind=0 --membind=0 ./bench -fill=0 -time=10 -value_size=120 -threads=64 -read_ratio=1 -existing_keys_ratio=1 -path=/mnt/pmem0/kvdk -space=274877906944 -num=838860800 -max_write_threads=64 -type=string
    
    [LOG] time 0 ms: Initializing PMem size 274877906944 in file /mnt/pmem0/kvdk/data
    [LOG] time 1864 ms: Map pmem space done
    [LOG] time 9033 ms: In restoring: iterated 840882543 records
    init 0 write threads
    init 64 read threads
    ------- ops in seconds -----------
    time (ms),   read ops,   not found,  write ops,  total read,  total write
    1000        73691000    0           0           73691000     0
    2001        73613000    0           0           147304000    0
    3002        73643000    0           0           220947000    0
    4003        73656000    0           0           294603000    0
    5004        73675000    0           0           368278000    0
    6005        73667000    0           0           441945000    0
    7006        73699000    0           0           515644000    0
    8007        73647000    0           0           589291000    0
    9008        73634000    0           0           662925000    0
    10009       73677000    0           0           736602000    0
    finish bench
     ------------ statistics ------------
    read ops 73660400, write ops 0
    [LOG] time 19051 ms: instance closed



### Write performance

Similarily, to test write performance, we can simply modify "read_ratio":

    numactl --cpunodebind=0 --membind=0 ./bench -fill=0 -time=10 -value_size=120 -threads=64 -read_ratio=0 -existing_keys_ratio=0 -path=/mnt/pmem0/kvdk -space=274877906944 -num=838860800 -max_write_threads=64 -type=string

This command will insert new key-value pairs to the KVDK instance in 10 seconds. Likely wise, by modify "existing_keys_ratio", we can control how many write operations are updates.

    $numactl --cpunodebind=0 --membind=0 ./bench -fill=0 -time=10 -value_size=120 -threads=64 -read_ratio=0     -existing_keys_ratio=0 -path=/mnt/pmem0/kvdk -space=274877906944 -num=838860800 -max_write_threads=64 -type=string

    [LOG] time 0 ms: Initializing PMem size 274877906944 in file /mnt/pmem0/kvdk/data
    [LOG] time 1865 ms: Map pmem space done
    [LOG] time 9015 ms: In restoring: iterated 840882543 records
    init 64 write threads
    init 0 read threads
    ------- ops in seconds -----------
    time (ms),   read ops,   not found,  write ops,  total read,  total write
    1000        0           0           50610000    0            50610000
    2007        0           0           50053000    0            100663000
    3016        0           0           49669000    0            150332000
    4017        0           0           49048000    0            199380000
    5018        0           0           48540000    0            247920000
    6022        0           0           48210000    0            296130000
    7023        0           0           47725000    0            343855000
    8024        0           0           47354000    0            391209000
    9027        0           0           47080000    0            438289000
    10028       0           0           46544000    0            484833000
    finish bench
     ------------ statistics ------------
    read ops 0, write ops 48483400
    [LOG] time 19055 ms: instance closed


### Stat latencies

We can also stat latency information by add "-latency=1" to the benchmark command.

    $ numactl --cpunodebind=0 --membind=0 ./bench -fill=0 -time=10 -value_size=120 -threads=64 -read_ratio=0.5 -existing_keys_ratio=1 -path=/mnt/pmem0/kvdk -space=274877906944 -num=838860800 -max_write_threads=64 -type=string -latency=1

    [LOG] time 0 ms: Initializing PMem size 274877906944 in file /mnt/pmem0/kvdk/data
    [LOG] time 1869 ms: Map pmem space done
    [LOG] time 14963 ms: In restoring: iterated 1323729106 records
    calculate latencies
    init 6 write threads
    init 58 read threads
    ------- ops in seconds -----------
    time (ms),   read ops,   not found,  write ops,  total read,  total write
    1000        62763000    0           3933000     62763000     3933000
    2001        62297000    0           4303000     125060000    8236000
    3002        62190000    0           4530000     187250000    12766000
    4003        62194000    0           4530000     249444000    17296000
    5004        62206000    0           4531000     311650000    21827000
    6005        62172000    0           4527000     373822000    26354000
    7006        62194000    0           4530000     436016000    30884000
    8007        62227000    0           4535000     498243000    35419000
    9008        62196000    0           4529000     560439000    39948000
    10009       62190000    0           4527000     622629000    44475000
    finish bench
     ------------ statistics ------------
    read ops 62263100, write ops 4447500
    read lantencies (us): Avg: 0.89, P50: 0.83, P99: 1.54, P99.5: 1.67, P99.9: 2.77, P99.99: 4.20
    write lantencies (us): Avg: 0.09, P50: 1.22, P99: 2.64, P99.5: 3.25, P99.9: 4.22, P99.99: 5.35
    [LOG] time 28382 ms: instance closed

## More configurations

For more configurations of the benchmark tool, please reference to "benchmark/bench.cpp" and "scripts/basic_benchmarks.py".



    
