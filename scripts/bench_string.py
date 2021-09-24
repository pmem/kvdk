import os
import datetime

n_thread = 48

path = "/mnt/pmem0/kvdk_string"
report_path = "./results-string-threads-{}-{}/".format(n_thread, datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
value_sizes = [120]
data_size = 100 * 1024 * 1024 * 1024
instance_space = 384 * 1024 * 1024 * 1024  # we need enough space to test insert
benchmark_threads = n_thread
kvdk_max_write_threads = n_thread
duration = 10
populate = 1

numanode = 0
bin = "../build/bench"
exec = "numactl --cpunodebind={0} --membind={0} {1}".format(numanode, bin)


def Confirm(dir):
    y = input("Instance path : {}, it will be removed and recreated, confirm? (y/n)".format(dir))
    if y != 'y':
        exit(1)

if __name__ == "__main__":
    Confirm(path)
    os.system("mkdir -p {}".format(report_path))
    for vs in value_sizes:
        num = data_size // (vs + 8)
        para = "-type=string -populate={} -value_size={} -threads={} -time={} -path={} -num={} -space={} -max_write_threads={}".format(
            populate,
            vs,
            benchmark_threads,
            duration,
            path, num,
            instance_space,
            kvdk_max_write_threads)
        print("{0} {1} > {2}".format(exec, para, report_path))

        os.system("rm -rf {0}".format(path))

        # Benchmark string-type data
        # fill uniformly distributed kv
        new_para = para + " -fill=1"
        report = report_path + "string_vs{}_fill_thread{}".format(vs, benchmark_threads)
        print("Fill string-type kv")
        os.system("{0} {1} > {2}".format(exec, new_para, report))

        # random read
        new_para = para + " -fill=0 -read_ratio=1 key_distribution=random"
        report = report_path + "string_vs{}_ramdom_read_thread{}".format(vs, benchmark_threads)
        print("Random read string-type kv")
        os.system("{0} {1} > {2}".format(exec, new_para, report))

        # zipf read
        new_para = para + " -fill=0 -read_ratio=1 key_distribution=zipf"
        report = report_path + "string_vs{}_zipf_read_thread{}".format(vs, benchmark_threads)
        print("Zipf read string-type kv")
        os.system("{0} {1} > {2}".format(exec, new_para, report))

        # insert new kv
        new_para = para + " -fill=0 -read_ratio=0 -existing_keys_ratio=0"
        report = report_path + "string_vs{}_insert_thread{}".format(vs, benchmark_threads)
        print("Insert new string-type kv")
        os.system("{0} {1} > {2}".format(exec, new_para, report))

        # batch insert
        new_para = para + " -fill=0 -read_ratio=0 -batch=100 -existing_keys_ratio=0"
        report = report_path + "string_vs{}_batch_insert_thread{}".format(vs, benchmark_threads)
        print("Batch write string-type kv")
        os.system("{0} {1} > {2}".format(exec, new_para, report))

        # random update
        new_para = para + " -fill=0 -read_ratio=0 -key_distribution=random"
        report = report_path + "string_vs{}_random_update_thread{}".format(vs, benchmark_threads)
        print("Random update string-type kv")
        os.system("{0} {1} > {2}".format(exec, new_para, report))

        # zipf update
        new_para = para + " -fill=0 -read_ratio=0 -key_distribution=zipf"
        report = report_path + "string_vs{}_zipf_update_thread{}".format(vs, benchmark_threads)
        print("Zipf update string-type kv")
        os.system("{0} {1} > {2}".format(exec, new_para, report))

        # read + update
        new_para = para + " -fill=0 -read_ratio=0.9"
        report = report_path + "string_vs{}_ru91_thread{}".format(vs, benchmark_threads)
        print("Mixed read/update string-type kv")
        os.system("{0} {1} > {2}".format(exec, new_para, report))

        os.system("rm -rf {0}".format(path))

