import os
import datetime
import sys
import datetime
import git
from select import select

from git import repo


def __fill(exec, shared_para, data_type, report_path):
    new_para = shared_para + " -fill=1 -type={}".format(data_type)
    report = report_path + "fill"
    print("Fill {}".format(data_type))
    cmd = "{0} {1} > {2}".format(exec, new_para, report)
    print(cmd)
    os.system(cmd)


def read_random(exec, shared_para, data_type, report_path):
    new_para = shared_para + \
        " -fill=0 -type={} -read_ratio=1".format(
            data_type)
    report = report_path + "read_random"
    print("Read random {}".format(data_type))
    cmd = "{0} {1} > {2}".format(exec, new_para, report)
    print(cmd)
    os.system(cmd)


def insert_random(exec, shared_para, data_type, report_path):
    new_para = shared_para + \
        " -fill=0 -type={} -read_ratio=0 -existing_keys_ratio=0".format(
            data_type)
    report = report_path + "insert_random"
    print("Insert random {}".format(data_type))
    cmd = "{0} {1} > {2}".format(exec, new_para, report)
    print(cmd)
    os.system(cmd)


def batch_insert_random(exec, shared_para, data_type, report_path):
    if data_type != "string":
        return
    new_para = shared_para + \
        " -fill=0 -type={} -read_ratio=0 -batch_size=100 -existing_keys_ratio=0".format(
            data_type)
    report = report_path + "batch_insert_random"
    print("Batch insert random {}".format(data_type))
    cmd = "{0} {1} > {2}".format(exec, new_para, report)
    print(cmd)
    os.system(cmd)


def update_random(exec, shared_para, data_type, report_path):
    new_para = shared_para + \
        " -fill=0 -type={} -read_ratio=0".format(
            data_type)
    report = report_path + "update_random"
    print("Update random {}".format(data_type))
    cmd = "{0} {1} > {2}".format(exec, new_para, report)
    print(cmd)
    os.system(cmd)


def read_write_random(exec, shared_para, data_type, report_path):
    new_para = shared_para + \
        " -fill=0 -type={} -read_ratio=0.9".format(
            data_type)
    report = report_path + "read_write_random"
    print("Read write random {}".format(data_type))
    cmd = "{0} {1} > {2}".format(exec, new_para, report)
    print(cmd)
    os.system(cmd)


def range_scan(exec, shared_para, data_type, report_path):
    if data_type == "string" or data_type == "queue":
        return
    new_para = shared_para + \
        " -fill=0 -type={} -read_ratio=1 -scan=1".format(data_type)
    report = report_path + "range_scan"
    print("Range scan {}".format(data_type))
    cmd = "{0} {1} > {2}".format(exec, new_para, report)
    print(cmd)
    os.system(cmd)


def confirm(dir):
    timeout = 60
    print("Instance path : {}, it will be removed and recreated, confirm? (y/n) (Automatically confirm in {} seconds)".format(dir, timeout))
    rlist, _, _ = select([sys.stdin], [], [], timeout)
    y = 'n'
    if rlist:
        y = sys.stdin.readline()
    else:
        print("Automatically confirmed after {} seconds!".format(timeout))
        y = 'y'
    if y != 'y' and y != 'y\n':
        exit(1)


def run_benchmark(
    data_type,
    exec,
    pmem_path,
    pmem_size,
    populate_on_fill,
    n_thread,
    num_collection,
    timeout,
    key_distribution,
    value_size,
    value_size_distribution,
    benchmarks,
):
    confirm(pmem_path)
    os.system("rm -rf {0}".format(pmem_path))

    # calculate num kv to fill
    header_sz = 24 if (key_distribution == 'string') else 40
    k_sz = 8
    avg_v_sz = value_size if (value_size_distribution == 'constant') else (value_size / 2)
    avg_kv_size = (header_sz + k_sz + avg_v_sz + 63) // 64 * 64

    # 1/4 for fill, 1/4 for insert, 1/4 for batch_write
    num_kv = pmem_size // 4 // avg_kv_size
    num_operations = 1024 * 1024 * 1024 * 4

    # create report dir
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M")
    git_hash = git.Repo(search_parent_directories=True).head.object.hexsha
    report_path = "./results-{0}-commit-{1}-threads-{2}-key_dist-{7}-vsize-{3}-vsize_dist-{4}-collections-{5}-{6}/".format(
        data_type,
        git_hash[0:8], 
        n_thread, 
        value_size,
        value_size_distribution, 
        num_collection, 
        timestamp,
        key_distribution)
    os.system("mkdir -p {}".format(report_path))

    # run benchmarks
    print("Run benchmarks for data type :{}, value size distribution: {}".format(
        data_type, value_size_distribution))
    shared_para = \
        "-path={0} "\
        "-space={1} "\
        "-populate={2} "\
        "-num_kv={3} "\
        "-threads={4} "\
        "-max_write_threads={5} "\
        "-num_collection={6} "\
        "-timeout={7} "\
        "-value_size={8} "\
        "-value_size_distribution={9} "\
        "-key_distribution={10} "\
        "-num_operations={11}".format(
        pmem_path, 
        pmem_size, 
        populate_on_fill, 
        num_kv, 
        n_thread, 
        n_thread, 
        num_collection, 
        timeout, 
        value_size, 
        value_size_distribution,
        key_distribution,
        num_operations)
    # we always fill data before run benchmarks
    __fill(exec, shared_para, data_type, report_path)
    for benchmark in benchmarks:
        benchmark(exec, shared_para, data_type, report_path)

    os.system("rm -rf {0}".format(pmem_path))
