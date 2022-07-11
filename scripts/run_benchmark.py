import benchmark_impl
import sys
import itertools

numanode = 0
pmem_path = "/mnt/pmem{}/kvdk_benchmark".format(numanode)
bin = "../build/bench"
exec = "numactl --cpunodebind={0} --membind={0} {1}".format(numanode, bin)

num_thread = 64
value_sizes = [120]
# constant: value size always be "value_size",
# random: value size uniformly distributed in [1, value_size]
value_size_distributions = ['constant']
timeout = 30                          # For operations other than fill
populate_on_fill = 1                  # For fill only
pmem_size = 384 * 1024 * 1024 * 1024  # we need enough space to test insert
num_collection = 16

benchmarks = [
    benchmark_impl.batch_insert_random,
    benchmark_impl.insert_random,
    benchmark_impl.range_scan,
    benchmark_impl.read_random,
    benchmark_impl.read_write_random,
    benchmark_impl.update_random]

data_types = []

if __name__ == "__main__":
    usage = 'usage: run_benchmark.py [data type] [key distribution]\n\
        data type can be "string", "sorted", "hash", "list", or "all"\n\
        key distribution can be "random" or "zipf" or "all".'
    if len(sys.argv) != 3:
        print(usage)
        exit(1)
    if sys.argv[1] == 'string':
        data_types = ['string']
    elif sys.argv[1] == 'sorted':
        data_types = ['sorted']
    elif sys.argv[1] == 'hash':
        data_types = ['hash']
    elif sys.argv[1] == 'list':
        data_types = ['list']
    elif sys.argv[1] == 'blackhole':
        data_types = ['blackhole']
    elif sys.argv[1] == 'all':
        data_types = ['blackhole', 'string', 'sorted', 'hash', 'list']
    else:
        print(usage)
        exit(1)
    if sys.argv[2] == 'random':
        key_distributions = ['random']
    elif sys.argv[2] == 'zipf':
        key_distributions = ['zipf']
    elif sys.argv[2] == 'all':
        key_distributions = ['random', 'zipf']
    else:
        print(usage)
        exit(1)

    for [data_type, value_size, vsz_dist, k_dist] in itertools.product(data_types, value_sizes, value_size_distributions, key_distributions):
        benchmark_impl.run_benchmark(data_type, exec, pmem_path, pmem_size, populate_on_fill,
                                     num_thread, num_collection, timeout, k_dist, value_size, vsz_dist, benchmarks)
