# **kvrocks**

`Kvrocks` is an open-source key-value database which is based on rocksdb and compatible with Redis protocol. We demonstrate how to use `KVDK` to replace rocksdb as the backend engine of kvrocks to get higher performance with persistent memory.

# Build Instructions
*  Install gflags which is a dependent library of kvrocks
```
git clone https://github.com/gflags/gflags
cmake . -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_gflags_LIB=ON
make -j
make install
``` 
*  Clone kvrocks source code on specific tag
```
git clone --recursive https://github.com/kvrockslabs/kvrocks.git
cd kvrocks
git checkout v2.0.2
```
*  Apply patchs
```
git apply ../0001-use-KVDK-for-set-get-operations.patch
```
*  Copy kvdk library to kvrocks (build kvdk first)
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
*  Start kvrocks
```
sed -i 's/workers 8/workers 32/' kvrocks.conf
export LD_LIBRARY_PATH=external/kvdk/
src/kvrocks -c kvrocks.conf
```
*  Stress with redis-benchmark (You need to install redis-benchmark first)
```
redis-benchmark -t set -n 30000000 -r 100000000 -p 6666 -d 128 --threads 32
```

Example output of redis-benhmark
```
====== SET ======
  30000000 requests completed in 38.59 seconds
  50 parallel clients
  128 bytes payload
  keep alive: 1
  multi-thread: yes
  threads: 32

Latency by percentile distribution:
0.000% <= 0.031 milliseconds (cumulative count 121646)
50.000% <= 0.047 milliseconds (cumulative count 15413451)
75.000% <= 0.063 milliseconds (cumulative count 23331779)
87.500% <= 0.095 milliseconds (cumulative count 26815372)
93.750% <= 0.119 milliseconds (cumulative count 28508474)
96.875% <= 0.135 milliseconds (cumulative count 29134134)
98.438% <= 0.159 milliseconds (cumulative count 29648506)
99.219% <= 0.175 milliseconds (cumulative count 29815587)
99.609% <= 0.191 milliseconds (cumulative count 29900481)
99.805% <= 0.207 milliseconds (cumulative count 29950128)
99.902% <= 0.223 milliseconds (cumulative count 29974836)
99.951% <= 0.255 milliseconds (cumulative count 29986572)
99.976% <= 0.583 milliseconds (cumulative count 29992715)
99.988% <= 0.679 milliseconds (cumulative count 29996381)
99.994% <= 0.775 milliseconds (cumulative count 29998234)
99.997% <= 0.895 milliseconds (cumulative count 29999114)
99.998% <= 1.063 milliseconds (cumulative count 29999546)
99.999% <= 1.279 milliseconds (cumulative count 29999775)
100.000% <= 1.687 milliseconds (cumulative count 29999887)
100.000% <= 2.751 milliseconds (cumulative count 29999943)
100.000% <= 13.807 milliseconds (cumulative count 29999972)
100.000% <= 24.431 milliseconds (cumulative count 29999986)
100.000% <= 25.087 milliseconds (cumulative count 29999994)
100.000% <= 25.231 milliseconds (cumulative count 29999997)
100.000% <= 25.375 milliseconds (cumulative count 29999999)
100.000% <= 25.471 milliseconds (cumulative count 30000000)
100.000% <= 25.471 milliseconds (cumulative count 30000000)

Cumulative distribution of latencies:
91.514% <= 0.103 milliseconds (cumulative count 27454116)
99.834% <= 0.207 milliseconds (cumulative count 29950128)
99.968% <= 0.303 milliseconds (cumulative count 29990372)
99.968% <= 0.407 milliseconds (cumulative count 29990468)
99.968% <= 0.503 milliseconds (cumulative count 29990476)
99.978% <= 0.607 milliseconds (cumulative count 29993379)
99.990% <= 0.703 milliseconds (cumulative count 29997079)
99.995% <= 0.807 milliseconds (cumulative count 29998551)
99.997% <= 0.903 milliseconds (cumulative count 29999151)
99.998% <= 1.007 milliseconds (cumulative count 29999486)
99.999% <= 1.103 milliseconds (cumulative count 29999575)
99.999% <= 1.207 milliseconds (cumulative count 29999716)
99.999% <= 1.303 milliseconds (cumulative count 29999780)
99.999% <= 1.407 milliseconds (cumulative count 29999817)
100.000% <= 1.503 milliseconds (cumulative count 29999856)
100.000% <= 1.607 milliseconds (cumulative count 29999873)
100.000% <= 1.703 milliseconds (cumulative count 29999891)
100.000% <= 1.807 milliseconds (cumulative count 29999907)
100.000% <= 1.903 milliseconds (cumulative count 29999911)
100.000% <= 2.007 milliseconds (cumulative count 29999913)
100.000% <= 2.103 milliseconds (cumulative count 29999921)
100.000% <= 3.103 milliseconds (cumulative count 29999946)
100.000% <= 4.103 milliseconds (cumulative count 29999949)
100.000% <= 5.103 milliseconds (cumulative count 29999951)
100.000% <= 6.103 milliseconds (cumulative count 29999952)
100.000% <= 8.103 milliseconds (cumulative count 29999958)
100.000% <= 10.103 milliseconds (cumulative count 29999962)
100.000% <= 12.103 milliseconds (cumulative count 29999967)
100.000% <= 13.103 milliseconds (cumulative count 29999969)
100.000% <= 14.103 milliseconds (cumulative count 29999974)
100.000% <= 15.103 milliseconds (cumulative count 29999977)
100.000% <= 24.111 milliseconds (cumulative count 29999978)
100.000% <= 25.103 milliseconds (cumulative count 29999994)
100.000% <= 26.111 milliseconds (cumulative count 30000000)

Summary:
  throughput summary: 777363.19 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.058     0.024     0.047     0.119     0.167    25.471
```

