## GraphSim
A small graph db simulator in kuaishou graph situation.
The project simulate the basic vertex,edge,and edgelist in graph storage.And the popular algos in graph database: 
like the bfs search in social network and the TopN algo to find some 'person' who has the most fans in social network.

## Features
- The basic vertex and edge construct in kuaishou.(src/graph_impl.hpp)
- The basic algos used in kuaishou.(src/graph_algorithm && src/graph_impl.hpp)
- Compare the workload with different engine: kvdk, rocksdb,etc.(src/kv_engines)

## Building
The GraphSim compiling with kvdk.
When you compile KVDK, just add some build options in your command.
```shell
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DCHECK_CPP_STYLE=ON -DBUILD_GRAPH_SIMULATOR=ON
make -j
```
If you want to compare the kvdk with rocksdb, then add `-DBUILD_ROCKSDB=ON`, the rocksdb engine will be add.

After build success, the result binary will be produced at `build/example/graph_sim`.

## Benchmarks
The bench mark is for construct the graph data and run the different algos.
The main options bellow:
- `construct=true`, construct the graphdb
- `vertex_nums=100000000`, the number of vertexes that you add during construct
- `vertex_id_range=30000000`, the range of vertexes, if you want to create 1 million vertexes and 1 billion vertexes，this option should
be set million.
- `client_threads=32`, the benchmark threads in construct.
- `engine_name=kvdk`, there are some engines for you to choose: kvdk,rocksdb,memory. You just need input the name of the engine.

- `search_degree=true`, bfs search algo.
- `degree_level=2`, control the degree depth when you search the vertexe's relation ship.
- `degree_nums=128`, the number of vertexes you want to choose at begining, we will random choose the number of vertexes
and begin the bfs search.

- `topn=true`, top n algo.
- `topn_num=10`, top 10 vertexes which have the most in edges.
- `topn_collection`, because of the topn need scan a who database, so it's better to use the collection in kvdk, if you 
use the other engines, the options is useless.


### performance test
1. construct 30,000,000 vertexes and 200,000,000 edges, it's random workload with read and write in engine.
2. bfs search 100,000 vertexes and 4 degree vertexes
3. top 10 vertexes
```shell
numactl --membind=0 --cpubind=0 \
./graph_bench \
-construct=true \
-search_degree=false \
-vertex_nums=200000000 \
-vertex_id_range=30000000 \
-client_threads=40 \
-engine_name=kvdk \
-vertex_info_len=64 \
-edge_info_len=128 \
-search_degree=true \
-degree_level=4 \
-degree_nums=100000
-topn=true \
-topn_num=10 \
-topn_collection="cold-data" \
```

1. 1,000,000 vertexes and 1,000,000,000 edges, it's hot data with read and write.
2. bfs search 100,000 vertexes and 4 degree vertexes
3. topn 10 vertexes
```shell
numactl --membind=0 --cpubind=0 \
./graph_bench \
-construct=true \
-search_degree=false \
-vertex_nums=1000000000 \
-vertex_id_range=1000000 \
-client_threads=40 \
-engine_name=kvdk \
-vertex_info_len=64 \
-edge_info_len=128
-search_degree=true \
-degree_level=4 \
-degree_nums=100000 \
-topn=true \
-topn_num=10 \
-topn_collection="host-data" \
```
