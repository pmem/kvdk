import os
import datetime
import sys
import datetime
import git
from select import select

from git import repo

use_experimental_hashmap = False

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
    sz_fill_data,
    n_thread,
    num_collection,
    bench_duration,
    key_distribution,
    value_size,
    value_size_distribution,
    benchmarks,
):
    confirm(pmem_path)
    os.system("rm -rf {0}".format(pmem_path))

    # calculate num kv to fill
    num_fill_kv = 0
    if value_size_distribution == 'constant':
        num_fill_kv = sz_fill_data//(value_size+8)
    elif value_size_distribution == 'random':
        num_fill_kv = sz_fill_data//(value_size + 8) // 2
    else:
        assert False

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
        "-path={} "\
        "-space={} "\
        "-populate={} "\
        "-num={} "\
        "-threads={} "\
        "-max_write_threads={} "\
        "-num_collection={} "\
        "-time={} "\
        "-value_size={} "\
        "-value_size_distribution={} "\
        "-key_distribution={}".format(
        pmem_path, 
        pmem_size, 
        populate_on_fill, 
        num_fill_kv, 
        n_thread, 
        n_thread, 
        num_collection, 
        bench_duration, 
        value_size, 
        value_size_distribution,
        key_distribution)

    shared_para = shared_para if (not use_experimental_hashmap or data_type != "string") else (shared_para + " -use_experimental_hashmap=1")
    # we always fill data before run benchmarks
    __fill(exec, shared_para, data_type, report_path)
    for benchmark in benchmarks:
        benchmark(exec, shared_para, data_type, report_path)

    os.system("rm -rf {0}".format(pmem_path))
