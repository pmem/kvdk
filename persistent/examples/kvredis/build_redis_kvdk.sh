#!/bin/bash
# The shell is for build the redis with kvdk.


# Clone redis
function clone_redis()
{
  if [ -e "redis" ]; then cd redis; return; fi
  echo "cloning redis, please wait a moment ......"
  git clone  https://github.com/redis/redis.git redis
  if [ $? -ne 0 ]; then echo "git clone redis failed."; exit 1;fi
  cd redis && git checkout 6.2.6
}

clone_redis

SOURCE_DIR=$(pwd)
DEPS_DIR=$SOURCE_DIR/deps/kvdk
MODULE_FILE=$SOURCE_DIR/src/modules/kvdk.c

#Apply Redis With KVDK
function apply_integrate_patch()
{
  if [ -f "$MODULE_FILE" ]; then return; fi
  echo "apply redis with kvdk integrate pitch ...."
  git apply ../0001-redis-with-kvdk.patch
  if [ $? -ne 0 ]; then echo "git apply integrate code failed."; exit 1;fi
}

apply_integrate_patch

# For using multi cores parallel compile
NUM_CPU_CORES=$(grep "processor" -c /proc/cpuinfo)
if [ -z "${NUM_CPU_CORES}" ] || [ "${NUM_CPU_CORES}" = "0" ] ; then
    NUM_CPU_CORES=1
fi

# build
mkdir -p "$DEPS_DIR"/ \
  && cp ../../../build/libengine.so "$DEPS_DIR"/ \
  && mkdir -p "$DEPS_DIR"/include \
  && cp -r ../../../include/* "$DEPS_DIR"/include \
  && make USE_KVDK=yes -j"$NUM_CPU_CORES" \
  && cd src/modules && make -j"$NUM_CPU_CORES" \
  && cd ../../ \

