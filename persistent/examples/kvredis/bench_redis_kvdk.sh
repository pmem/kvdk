#!/bin/bash
# The shell is for bench the redis with kvdk.

#! /bin/bash

if [ -e "redis" ]; then cd redis; fi

CUR_DIR=$(pwd)
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:CUR_DIR/deps/kvdk/

# src/redis-benchmark -t set -d 128 -n 30000000 --threads 8 -h 192.168.1.3
requests=(200000000)
random=(100000000)
clients=(50)
values_size=(128)
ops=("set" "get" "hset" "sadd" "lpush" "zadd")
num_threads=(1 8)
numa_cmd="numactl --cpunodebind=1"
ARGS=""
RES="${CUR_DIR}"/res

parseArgs() {
  while getopts "h:p:" opt; do
  case ${opt} in
  h)
    ARGS+=" -h ${OPTARG} "
    ;;
  p)
    ARGS+=" -p ${OPTARG} "
    ;;
  :)
    exit
    ;;
  ?)
    exit
    ;;
    esac
  done
}

parseArgs "$@"

mkdir -p "${RES}"

for op in ${ops[*]}; do
    for thread in ${num_threads[*]}; do
        for client in ${clients[*]}; do
            CLOG="./res/${op}_t${thread}_c${client}.csv"
            for ((i = 0; i < ${#values_size[*]}; ++i)); do
                LOG="./res/${op}_c${client}_v${values_size[$i]}_n${requests[$i]}_r${random[$i]}.csv"
                RUN_ARGS="-t $op -c $client -n ${requests[$i]} -r ${random[$i]} -d ${values_size[$i]} \
                      --threads $thread"
                echo "${numa_cmd} src/redis-benchmark $ARGS $RUN_ARGS --csv"
                ${numa_cmd} src/redis-benchmark $ARGS $RUN_ARGS --csv >$LOG
                if [ $i -eq 0 ]; then
                    cp $LOG $CLOG
                else
                    cat <(tail +2 $LOG) >>${CLOG}
                fi
                rm -rf $LOG
            done
        done
    done
done
