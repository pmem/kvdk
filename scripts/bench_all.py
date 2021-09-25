import bench_string
import bench_sorted
import bench_hashes

n_thread = 48
sz_value = 120
t_duration = 30                         # For operations other than fill
populate_if_fill = 1                    # For fill only
sz_pmem_file = 384 * 1024 * 1024 * 1024 # we need enough space to test insert
sz_fill_data = 96 * 1024 * 1024 * 1024
n_collections = 96

if __name__ == "__main__":
    bench_string.run_bench_mark(n_thread=n_thread, sz_value=sz_value, t_duration=t_duration, populate_if_fill=populate_if_fill, sz_pmem_file=sz_pmem_file, sz_fill_data=sz_fill_data,n_collections=n_collections)
    bench_sorted.run_bench_mark(n_thread=n_thread, sz_value=sz_value, t_duration=t_duration, populate_if_fill=populate_if_fill, sz_pmem_file=sz_pmem_file, sz_fill_data=sz_fill_data,n_collections=n_collections)
    bench_hashes.run_bench_mark(n_thread=n_thread, sz_value=sz_value, t_duration=t_duration, populate_if_fill=populate_if_fill, sz_pmem_file=sz_pmem_file, sz_fill_data=sz_fill_data,n_collections=n_collections)
