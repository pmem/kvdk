import os
import datetime

n_thread = 48
sz_key = 8  # Don't modify, for now
sz_value = 120
n_collections = 96
timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M")
path_report = "./results-string-n_threads-{}-sz_key-{}-sz_value-{}-n_collections-{}-timestamp-{}/".format(
    n_thread, 
    sz_key,
    sz_value,
    n_collections,
    timestamp)

path_pmem = "/mnt/pmem0/kvdk_string"
t_duration = 60                         # For operations other than fill
populate_if_fill = 1                    # For fill only
sz_data_on_pmem = 96 * 1024 * 1024 * 1024
sz_pmem_file = 384 * 1024 * 1024 * 1024  # we need enough space to test insert
n_thread_total = n_thread
n_thread_write = n_thread
numanode = 0
bin = "../build/bench"
exec = "numactl --cpunodebind={0} --membind={0} {1}".format(numanode, bin)

def Confirm(dir):
    y = input("Instance path : {}, it will be removed and recreated, confirm? (y/n)".format(dir))
    if y != 'y':
        exit(1)

if __name__ == "__main__":
    Confirm(path_pmem)
    os.system("mkdir -p {}".format(path_report))
    n_operations = sz_data_on_pmem // (sz_value + sz_key)
    para_common = "-type=string -value_size={} -threads={} -max_write_threads={} -time={} -path={} -num={} -space={} -collections={}".format(
        sz_value,
        n_thread_total,
        n_thread_write,
        t_duration,
        path_pmem, 
        n_operations,
        sz_pmem_file,
        n_collections)
    print("{0} {1} > {2}".format(exec, para_common, path_report))


    # Benchmark string
    # fill uniformly distributed kv
    os.system("rm -rf {0}".format(path_pmem))
    new_para = para_common + " -fill=1 -populate={}".format(populate_if_fill)
    report = path_report + "1.fill"
    print("Fill string-type kv")
    os.system("{0} {1} > {2}".format(exec, new_para, report))

    # random read
    new_para = para_common + " -fill=0 -read_ratio=1 -key_distribution=random"
    report = path_report + "2.read_random"
    print("Random read string-type kv")
    os.system("{0} {1} > {2}".format(exec, new_para, report))

    # zipf read
    new_para = para_common + " -fill=0 -read_ratio=1 -key_distribution=zipf"
    report = path_report + "3.read_zipf"
    print("Zipf read string-type kv")
    os.system("{0} {1} > {2}".format(exec, new_para, report))

    # read + update
    new_para = para_common + " -fill=0 -read_ratio=0.9"
    report = path_report + "4.read_write_91"
    print("Mixed read/update string-type kv")
    os.system("{0} {1} > {2}".format(exec, new_para, report))

    # insert new kvs
    new_para = para_common + " -fill=0 -read_ratio=0 -existing_keys_ratio=0"
    report = path_report + "5.insert"
    print("Insert new string-type kv")
    os.system("{0} {1} > {2}".format(exec, new_para, report))

    # batch insert
    new_para = para_common + " -fill=0 -read_ratio=0 -batch=100 -existing_keys_ratio=0"
    report = path_report + "6.insert_batch"
    print("Batch write string-type kv")
    os.system("{0} {1} > {2}".format(exec, new_para, report))

    # random update
    new_para = para_common + " -fill=0 -read_ratio=0 -key_distribution=random"
    report = path_report + "7.update_random"
    print("Random update string-type kv")
    os.system("{0} {1} > {2}".format(exec, new_para, report))

    # zipf update
    new_para = para_common + " -fill=0 -read_ratio=0 -key_distribution=zipf"
    report = path_report + "8.update_zipf"
    print("Zipf update string-type kv")
    os.system("{0} {1} > {2}".format(exec, new_para, report))

    os.system("rm -rf {0}".format(path_pmem))
