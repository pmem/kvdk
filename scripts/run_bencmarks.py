import os
import sys
from multiprocessing import Process
import time


value_sizes = [128, 512, 256]

path = "/mnt/pmem0/DB"

duration = 30
threads = [48, 1]

numanode = 0
binpath = "../build/"
binname = "bench"
exec = "numactl --cpunodebind={0} --membind={0} {1}".format(numanode, binpath + binname)

pmem_stat = "/root/wujy/pcm-memory.x"
pmem_stat_grep = "NODE {} PMM".format(numanode)

# pmem_stat = "/root/pcm-memory.x"
# pmem_stat_grep = "NODE {} DDR-T".format(numanode)

def cpu_stat(binname, report):
    os.system("echo \"start top stat\" > {}".format(report))
    os.system("top | grep \"{}\" > {}".format(binname, report))

def aep_stat(pmem_stat, report):
    cmd = "{} | grep \"{}\" > {}".format(pmem_stat, pmem_stat_grep, report)
    os.system(cmd)

if __name__ == "__main__":
    os.system("mkdir -p ./results")
    for thread in threads:
        for vs in value_sizes:
            para = "-value_size={} -thread_num={} -time={} -db_path={}".format(vs, thread, duration, path)

            for read_ratio in [0, 1, 0.9, 0.8]:
                if read_ratio < 1 and read_ratio > 0 and thread * (1 - read_ratio) < 1:
                    continue
                # clear old db
                os.system("rm {0}".format(path))

                # load
                new_para = para + " -load=1 -read_ratio=0 -thread_num=48"
                report_name = "./results/vs{}_load_thread48".format(vs, thread)
                time.sleep(5)
                os.system("echo 3 >/proc/sys/vm/drop_caches")
                #do stats
                top_p = Process(target = cpu_stat, args = (binname, report_name+".top",))
                aep_p = Process(target = aep_stat, args = (pmem_stat, report_name+".aep",))
                aep_p.start()
                top_p.start()
                os.system("{0} {1} > {2}".format(exec, new_para, report_name+".report"))
                aep_p.terminate()
                top_p.terminate()
                os.system("pkill top")
                os.system("pkill pcm-memory.x")

                print("load done", file=sys.stderr)

                # run
                report_name = "./results/vs{}_readratio{}_thread{}".format(vs, read_ratio, thread)
                new_para = para + " -latency=1 -read_ratio={}".format(read_ratio)
                time.sleep(5)
                os.system("echo 3 >/proc/sys/vm/drop_caches")
                #do stats
                top_p = Process(target = cpu_stat, args = (binname, report_name+".top",))
                aep_p = Process(target = aep_stat, args = (pmem_stat, report_name+".aep",))
                aep_p.start()
                top_p.start()
                os.system("{0} {1} > {2}".format(exec, new_para, report_name+".report"))
                aep_p.terminate()
                top_p.terminate()
                os.system("pkill top")
                os.system("pkill pcm-memory.x")
