import os
import datetime

n_thread = 64
bench_string = True
bench_sorted = True

path = "/mnt/pmem0/kvdk"
report_path = "./results_QPS_data/"
value_sizes = [120]
data_size = 100 * 1024 * 1024 * 1024
instance_space = 384 * 1024 * 1024 * 1024  # we need enough space to test insert
benchmark_threads = n_thread
kvdk_max_write_threads = n_thread
duration = 10
populate = 0
collections = 16

numanode = 0
bin = "../build/bench"
exec = "numactl --cpunodebind={0} --membind={0} {1}".format(numanode, bin)


#def Confirm(dir):
#    y = input("Instance path : {}, it will be removed and recreated, confirm? (y/n)".format(dir))
#    if y != 'y':
#        exit(1)


if __name__ == "__main__":
    Confirm(path)
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

            # random read
            new_para = para + " -fill=0 -type=string -read_ratio=1 -key_distribution=random"
            report = report_path + "string_vs{}_ramdom_read_thread{}".format(vs, benchmark_threads)
            print("Random read string-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)

            # zipf read
            new_para = para + " -fill=0 -type=string -read_ratio=1 -key_distribution=zipf"
            report = report_path + "string_vs{}_zipf_read_thread{}".format(vs, benchmark_threads)
            print("Zipf read string-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)

            # insert new kv
            new_para = para + " -fill=0 -type=string -read_ratio=0 -existing_keys_ratio=0"
            report = report_path + "string_vs{}_insert_thread{}".format(vs, benchmark_threads)
            print("Insert new string-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)

            # batch insert
            new_para = para + " -fill=0 -type=string -read_ratio=0 -batch=100 -existing_keys_ratio=0"
            report = report_path + "string_vs{}_batch_insert_thread{}".format(vs, benchmark_threads)
            print("Batch write string-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)

            # random update
            new_para = para + " -fill=0 -type=string -read_ratio=0 -key_distribution=random"
            report = report_path + "string_vs{}_random_update_thread{}".format(vs, benchmark_threads)
            print("Random update string-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)

            # zipf update
            new_para = para + " -fill=0 -type=string -read_ratio=0 -key_distribution=zipf"
            report = report_path + "string_vs{}_zipf_update_thread{}".format(vs, benchmark_threads)
            print("Zipf update string-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)

            # read + update
            new_para = para + " -fill=0 -type=string -read_ratio=0.9"
            report = report_path + "string_vs{}_ru91_thread{}".format(vs, benchmark_threads)
            print("Mixed read/update string-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)

        if (bench_sorted):
            os.system("rm -rf {0}".format(path))

            # Benchmark sorted-type data
            # fill uniform distributed kv
            new_para = para + " -fill=1 -type=sorted"
            report = report_path + "sorted_vs{}_fill_thread{}".format(vs, benchmark_threads)
            print("Fill sorted-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)

            # random read
            new_para = para + " -fill=0 -type=sorted -read_ratio=1 -key_distribution=random"
            report = report_path + "sorted_vs{}_random_read_thread{}".format(vs, benchmark_threads)
            print("Random read sorted-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)

            # zipf read
            new_para = para + " -fill=0 -type=sorted -read_ratio=1 -key_distribution=zipf"
            report = report_path + "sorted_vs{}_zipf_read_thread{}".format(vs, benchmark_threads)
            print("Zipf read sorted-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)

            # insert new kvs
            new_para = para + " -fill=0 -type=sorted -read_ratio=0 -existing_keys_ratio=0"
            report = report_path + "sorted_vs{}_insert_thread{}".format(vs, benchmark_threads)
            print("Insert new sorted-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)

            # random update
            new_para = para + " -fill=0 -type=sorted -read_ratio=0 -key_distribution=random"
            report = report_path + "sorted_vs{}_random_update_thread{}".format(vs, benchmark_threads)
            print("Random update sorted-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)

            # zipf update
            new_para = para + " -fill=0 -type=sorted -read_ratio=0 -key_distribution=zipf"
            report = report_path + "sorted_vs{}_zipf_update_thread{}".format(vs, benchmark_threads)
            print("Zipf update sorted-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)

            # range scan
            new_para = para + " -fill=0 -type=sorted -read_ratio=1 -scan=1"
            report = report_path + "sorted_vs{}_scan_thread{}".format(vs, benchmark_threads)
            print("Scan sorted-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)

            # read + update
            new_para = para + " -fill=0 -type=sorted -read_ratio=0.9"
            report = report_path + "sorted_vs{}_ru91_thread{}".format(vs, benchmark_threads)
            print("Mixed read/update sorted-type kv")
            cmd = "{0} {1} > {2}".format(exec, new_para, report)
            print(cmd)
            os.system(cmd)
