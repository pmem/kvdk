import os
import datetime

n_thread = 64
bench_string = True
bench_sorted = True

path = "/mnt/pmem0/kvdk"
report_path = "./results-{}/".format(datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
value_sizes = [128]
data_size = 50 * 1024 * 1024 * 1024
instance_space = 384 * 1024 * 1024 * 1024  # we need enough space to test insert
benchmark_threads = n_thread
kvdk_max_write_threads = n_thread
duration = 10
populate = 0
collections = 16

numanode = 0
bin = "../build/bench"
exec = "numactl --cpunodebind={0} --membind={0} {1}".format(numanode, bin)


def Confirm(dir):
    y = input("Instance path : {}, it will be removed and recreated, confirm? (y/n)".format(dir))
    if y != 'y':
        exit(1)


if __name__ == "__main__":
   # Confirm(path)
    os.system("mkdir -p {}".format(report_path))
    for vs in value_sizes:
        num = data_size // (vs + 8)
        para = "-populate={} -value_size={} -threads={} -time={} -path={} -num={} -space={} -max_write_threads={} -collections={}".format(
            populate,
            vs,
            benchmark_threads,
            duration,
            path, num,
            instance_space,
            kvdk_max_write_threads,
            collections)

        if (bench_string):
            os.system("rm -rf {0}".format(path))

            # Benchmark string-type data
            # fill uniform distributed kv
            new_para = para + " -fill=1 -type=string"
            report = report_path + "string_vs{}_fill_thread{}".format(vs, benchmark_threads)
            print("Fill string-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)
