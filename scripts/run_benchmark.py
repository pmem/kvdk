import benchmark_impl
import sys

numanode = 0
pmem_path = "/mnt/pmem{}/kvdk_benchmark".format(numanode)
bin = "../build/bench"
exec = "numactl --cpunodebind={0} --membind={0} {1}".format(numanode, bin)

num_thread = 64
value_size = 120                          # About 256B/kv
value_size_distributions = ['constant', 'random']
test_duration = 10                         # For operations other than fill
populate_on_fill = 1                    # For fill only
pmem_size = 384 * 1024 * 1024 * 1024  # we need enough space to test insert
fill_data_size = 96 * 1024 * 1024 * 1024
num_collection = 16


def bench_string(benchmarks=[]):
    data_type = "string"
    if len(benchmarks) == 0:
        benchmarks = [benchmark_impl.read_random, benchmark_impl.insert_random,
                      benchmark_impl.batch_insert_random, benchmark_impl.update_random, benchmark_impl.read_write_random]
    for value_dist in value_size_distributions:
        benchmark_impl.run_bench_mark(exec, pmem_path, pmem_size, data_type, populate_on_fill, fill_data_size,
                                      num_thread, num_collection, test_duration, value_size, value_dist, benchmarks)


def bench_sorted(benchmarks=[]):
    data_type = "sorted"
    if len(benchmarks) == 0:
        benchmarks = [benchmark_impl.read_random, benchmark_impl.range_scan, benchmark_impl.insert_random,
                      benchmark_impl.update_random, benchmark_impl.read_write_random]
    for value_dist in value_size_distributions:
        benchmark_impl.run_bench_mark(exec, pmem_path, pmem_size, data_type, populate_on_fill, fill_data_size,
                                      num_thread, num_collection, test_duration, value_size, value_dist, benchmarks)


def bench_hash(benchmarks=[]):
    data_type = "hash"
    if len(benchmarks) == 0:
        benchmarks = [benchmark_impl.read_random, benchmark_impl.range_scan, benchmark_impl.insert_random,
                      benchmark_impl.update_random, benchmark_impl.read_write_random]
    for value_dist in value_size_distributions:
        benchmark_impl.run_bench_mark(exec, pmem_path, pmem_size, data_type, populate_on_fill, fill_data_size,
                                      num_thread, num_collection, test_duration, value_size, value_dist, benchmarks)


def bench_queue(benchmarks=[]):
    data_type = "queue"
    if len(benchmarks) == 0:
        benchmarks = [benchmark_impl.read_random, benchmark_impl.insert_random,
                      benchmark_impl.read_write_random]
    for value_dist in value_size_distributions:
        benchmark_impl.run_bench_mark(exec, pmem_path, pmem_size, data_type, populate_on_fill, fill_data_size,
                                      num_thread, num_collection, test_duration, value_size, value_dist, benchmarks)


def bench_all():
    bench_string()
    bench_sorted()
    bench_hash()
    bench_queue()


if __name__ == "__main__":
    usage = 'usage: run_benchmark.py [data type]\n\
        data type can be "string", "sorted", "hash", "queue", or "all" to bench them all'
    if len(sys.argv) != 2:
        print(usage)
        exit(1)
    if sys.argv[1] == 'string':
        bench_string()
    elif sys.argv[1] == 'sorted':
        bench_sorted()
    elif sys.argv[1] == 'hash':
        bench_hash()
    elif sys.argv[1] == 'queue':
        bench_queue()
    elif sys.argv[1] == 'all':
        bench_all()
    else:
        print(usage)
        exit(1)
