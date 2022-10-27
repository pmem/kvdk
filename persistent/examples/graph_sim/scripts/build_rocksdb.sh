#!/bin/bash
# The shell is for build the rocksdb as a compared engine with kvdk in CmakeList.txt.

# Clone rocksdb
function clone_rocksdb()
{
  if [ -e "rocksdb" ]; then cd rocksdb; return; fi
  echo "cloning rocksdb, please wait a moment ......"
  git clone  https://github.com/facebook/rocksdb.git rocksdb
  if [ $? -ne 0 ]; then echo "git clone rocksdb failed."; exit 1;fi
  cd rocksdb
}

# Build gflags, that rocksdb could find it clearly.
function build_gflags()
{
  local source_dir
  local build_dir

  # dir is exists, there is no need to clone again
  if [ -e "$GFLAGS_DIR" ] && [ -e "$GFLAGS_DIR/build" ]; then
    return
  fi

  if [ ! -e "$GFLAGS_DIR" ];then
    git clone  https://github.com/gflags/gflags.git "$GFLAGS_DIR"
    if [ $? -ne 0 ]; then
      echo "git clone gflags failed."
      exit 1
    fi
  fi

  cd $GFLAGS_DIR
  source_dir=$(pwd)
  build_dir=$source_dir/build

  mkdir -p "$build_dir"/ \
    && cd "$build_dir"/ \
    && cmake .. -DCMAKE_INSTALL_PREFIX="$build_dir" -DBUILD_SHARED_LIBS=1 -DBUILD_STATIC_LIBS=1 \
    && make \
    && cd ../../
}

clone_rocksdb
GFLAGS_DIR=gflags
build_gflags

SOURCE_DIR=$(pwd)
BUILD_DIR=$SOURCE_DIR/build
GFLAGS_LIB_DIR="$GFLAGS_DIR"/build

# For using multi cores parallel compile
NUM_CPU_CORES=$(grep "processor" -c /proc/cpuinfo)
if [ -z "${NUM_CPU_CORES}" ] || [ "${NUM_CPU_CORES}" = "0" ] ; then
    NUM_CPU_CORES=1
fi

mkdir -p "$BUILD_DIR"/ \
  && cd "$BUILD_DIR"/ \
  && cmake "$SOURCE_DIR" -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=$GFLAGS_LIB_DIR \
  && make -j"$NUM_CPU_CORES" \

