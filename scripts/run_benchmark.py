import benchmark_impl
import sys
import itertools

numanode = 0
pmem_path = "/mnt/pmem{}/kvdk_benchmark".format(numanode)
bin = "../build/bench"
# env = "MIMALiLOC_SHOW_STATS=1 LD_PRELOAD=/usr/local/lib/libmimalloc.so"
env = "LD_PRELOAD=/usr/lib/gcc/x86_64-linux-gnu/8/libasan.so"
exec = "{2} numactl --cpunodebind={0} --membind={0} {1}".format(numanode, bin, env)

num_thread = 64
value_size = 120
# constant: value size always be "value_size",
# random: value size randomly distributed in [1, value_size]
value_size_distributions = ['constant']
key_distributions = ['random', 'zipf']
test_duration = 10                         # For operations other than fill
populate_on_fill = 1                    # For fill only
pmem_size = 384 * 1024 * 1024 * 1024  # we need enough space to test insert
fill_data_size = 96 * 1024 * 1024 * 1024
num_collection = 16

benchmarks = [
    benchmark_impl.read_random, 
    benchmark_impl.range_scan, 
    benchmark_impl.insert_random,
    benchmark_impl.batch_insert_random, 
    benchmark_impl.update_random, 
    benchmark_impl.read_write_random]

data_types = []

if __name__ == "__main__":
    usage = 'usage: run_benchmark.py [data type]\n\
        data type can be "string", "sorted", "hash", "queue", or "all" to bench them all'
    if len(sys.argv) != 2:
        print(usage)
        exit(1)
    if sys.argv[1] == 'string':
        data_types = ['string']
    elif sys.argv[1] == 'sorted':
        data_types = ['sorted']
    elif sys.argv[1] == 'hash':
        data_types = ['hash']
    elif sys.argv[1] == 'queue':
        data_types = ['queue']
    elif sys.argv[1] == 'blackhole':
        data_types = ['blackhole']
    elif sys.argv[1] == 'all':
        data_types = ['blackhole', 'string', 'sorted', 'hash', 'queue']
    else:
        print(usage)
        exit(1)
    for [data_type, vsz_dist, k_dist] in itertools.product(data_types, value_size_distributions, key_distributions):
        benchmark_impl.run_benchmark(data_type, exec, pmem_path, pmem_size, populate_on_fill, fill_data_size,
                                     num_thread, num_collection, test_duration, k_dist, value_size, vsz_dist, benchmarks)
