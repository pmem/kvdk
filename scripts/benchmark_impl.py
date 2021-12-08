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
    os.system(cmd)


def read_random(exec, shared_para, data_type, report_path):
    new_para = shared_para + \
        " -fill=0 -type={} -read_ratio=1 -key_distribution=random".format(
            data_type)
    report = report_path + "read_random"
    print("Read random {}".format(data_type))
    cmd = "{0} {1} > {2}".format(exec, new_para, report)
    os.system(cmd)


def insert_random(exec, shared_para, data_type, report_path):
    new_para = shared_para + \
        " -fill=0 -type={} -read_ratio=0 -key_distribution=random -existing_keys_ratio=0".format(
            data_type)
    report = report_path + "insert_random"
    print("Insert random {}".format(data_type))
    cmd = "{0} {1} > {2}".format(exec, new_para, report)
    os.system(cmd)


def batch_insert_random(exec, shared_para, data_type, report_path):
    assert data_type == "string"
    new_para = shared_para + \
        " -fill=0 -type={} -read_ratio=0 -batch=100 -key_distribution=random -existing_keys_ratio=0".format(
            data_type)
    report = report_path + "batch_insert_random"
    print("Batch insert random {}".format(data_type))
    cmd = "{0} {1} > {2}".format(exec, new_para, report)
    os.system(cmd)


def update_random(exec, shared_para, data_type, report_path):
    new_para = shared_para + \
        " -fill=0 -type={} -read_ratio=0 -key_distribution=random".format(
            data_type)
    report = report_path + "update_random"
    print("Update random {}".format(data_type))
    cmd = "{0} {1} > {2}".format(exec, new_para, report)
    os.system(cmd)


def read_write_random(exec, shared_para, data_type, report_path):
    new_para = shared_para + \
        " -fill=0 -type={} -read_ratio=0.9 -key_distribution=random".format(
            data_type)
    report = report_path + "read_write_random"
    print("Read write random {}".format(data_type))
    cmd = "{0} {1} > {2}".format(exec, new_para, report)
    os.system(cmd)


def range_scan(exec, shared_para, data_type, report_path):
    new_para = shared_para + \
        " -fill=0 -type={} -read_ratio=1 -scan=1".format(data_type)
    report = report_path + "range_scan"
    print("Range scan {}".format(data_type))
    cmd = "{0} {1} > {2}".format(exec, new_para, report)
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
    report_path = "./results-{}/commit-{}/threads-{}-vs-{}-vs_dist-{}-collections-{}/{}/".format(
        timestamp, git_hash, n_thread, value_size, value_size_distribution, num_collection, data_type)
    os.system("mkdir -p {}".format(report_path))

    # run benchmarks
    print("Run benchmarks for data type :{}, value size distribution: {}".format(
        data_type, value_size_distribution))
    shared_para = "-path={} -space={} -populate={} -num={} -threads={} -max_write_threads={} -collections={} -time={} -value_size={} -value_size_distribution={}".format(
        pmem_path, pmem_size, populate_on_fill, num_fill_kv, n_thread, n_thread, num_collection, bench_duration, value_size, value_size_distribution)
    # we always fill data before run benchmarks
    __fill(exec, shared_para, data_type, report_path)
    for benchmark in benchmarks:
        benchmark(exec, shared_para, data_type, report_path)

    os.system("rm -rf {0}".format(pmem_path))
