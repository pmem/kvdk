import bench_string
import bench_sorted
import bench_hashes
import bench_queue

n_thread = 64
sz_value = 120                          # About 256B/kv
t_duration = 10                         # For operations other than fill
populate_if_fill = 1                    # For fill only
sz_pmem_file = 384 * 1024 * 1024 * 1024 # we need enough space to test insert
sz_fill_data = 96 * 1024 * 1024 * 1024
n_collection = 16

if __name__ == "__main__":
    # bench_string.run_bench_mark(n_thread=n_thread, sz_value=sz_value, t_duration=t_duration, populate_if_fill=populate_if_fill, sz_pmem_file=sz_pmem_file, sz_fill_data=sz_fill_data, n_collection=n_collection)
    # bench_sorted.run_bench_mark(n_thread=n_thread, sz_value=sz_value, t_duration=t_duration, populate_if_fill=populate_if_fill, sz_pmem_file=sz_pmem_file, sz_fill_data=sz_fill_data, n_collection=n_collection)
    # bench_hashes.run_bench_mark(n_thread=n_thread, sz_value=sz_value, t_duration=t_duration, populate_if_fill=populate_if_fill, sz_pmem_file=sz_pmem_file, sz_fill_data=sz_fill_data, n_collection=n_collection)
    for n_coll in [384, 192, 96, 48, 24, 16]:
        bench_queue.run_bench_mark(n_thread=48, sz_value=sz_value, t_duration=30, populate_if_fill=True, sz_pmem_file=sz_pmem_file, sz_fill_data=sz_fill_data, n_collection=n_coll)


