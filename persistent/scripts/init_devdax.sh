#!/bin/bash
# Before execute:
#   1. ipmctl tool need install
#   2. ndctl tool need install
#   3. root execution
#
# Function: Create an adaptive kvdk devdax namespace and
# fsdax namespace on an available region
#
# Use: sh init_devdax.sh.
# After execute the shell, obey the command instruction to input your create args.

NAMESPACE_TYPE_LISTS="fsdax,devdax,raw,sector"
FSDAX_NAMESPACE_SIZE=33554432 # fsdax model need >= 16M
KVDK_PMEM_META_DIR="/mnt/kvdk-pmem-meta"

function error_record_and_exit() {
  err=${1}
  err_msg=${2}
  if [ "${err}" -ne 0 ];then
    echo ""
    echo " *** Last command failed ***"
    echo " *** ${error_msg} ***"
    return 1
  fi
}

# Check the ipmctl's version and ndctl's basic subcommand.
# Why check ipmctl verion:
#   We need subdividing a region into multi namespace mode.
# Such as:
#   region1 : [
#     namespace1 : fsdax
#     namespace2 : devdax
#     namespace3 : raw
#   ]
#
# So if the version of ipmctl is smaller than 02.xxx, the LSA of dimm
# lable is not support sub-dividing the region.
# Detail : https://github.com/pmem/ndctl/issues/181
function check_available_tools() {
  local ipmctl_version=`ipmctl version 2>/dev/null | \
                        awk -F"[.|[ ]" '{print $9}'`;
  if [ $? -ne 0 ];then
    error_record_and_exit $? "ipmctl version "
  fi

  if [ $ipmctl_version -ne 2 ];then
    error_record_and_exit $? "ipmctl version is less than 2,
                     can't sub_dividing the region.
                     It's better to recreate the region after upgrade impctl.
                     ref issue: https://github.com/pmem/ndctl/issues/181"
    exit $?
  fi

  ndctl list 2>/dev/null | error_record_and_exit $? "ndctl list "
}

function print_available_regions() {
  echo "Available regions infomation: "
  ndctl list --regions 2>/dev/null | \
  jq '.[] | if .persistence_domain=="memory_controller" then select(.available_size>0) else empty end'
  error_record_and_exit $? "ndctl list --regions | jq ..."
}

# Check input region name is exist and the namespace size is slower than available size.
function check_input_region() {
  region=${1}
  size=${2}

  # check input region is exists
  ndctl list --regions 2>/dev/null | jq '.[].dev' | grep "$region"
  error_record_and_exit $? "ndctl list --regions 2>/dev/null | jq '.[].dev' | grep -w '$region'"

  # check input size is satisfy the available size
  result=`ndctl list --regions 2>/dev/null | \
          jq '.[] | select(.dev=='"\"$region\""') | if .available_size-'"$FSDAX_NAMESPACE_SIZE"' >= '"$size"' then "true" else "false" end'`
  if [ $result == \""true\"" ];then
    ret=0
  else
    ret=1
  fi
  error_record_and_exit $ret "ndctl list --regions 2>/dev/null | jq '.[] | select(.dev=='"\"$region\""') "
}

function check_namespace_type() {
  namespace_type=${1}
  echo "$NAMESPACE_TYPE_LISTS" |grep -w "$namespace_type" | \
  error_record_and_exit $? 'echo "$NAMESPACE_TYPE_LISTS" |grep -w "$namespace_type'
}

function check_fs_type() {
  fs_type=${1}
  if [ "$fs_type" != "xfs" ] && [ "$fs_type" != "ext4" ];then
    error_record_and_exit 1 'input fs_type (xfs | ext4) error with "$fs_type"'
  fi
}

check_available_tools
print_available_regions

read -p "input region name: " region_name
read -p "input namespace size: " size
read -p "input namespace type (fsdax | devdax | raw | sector):" namespace_type

check_input_region "$region_name" "$size"
check_namespace_type "$namespace_type"

# Create the first mode
ndctl create-namespace --region="$region_name" --size="$size" --mode="$namespace_type" |\
error_record_and_exit $? "ndctl create-namespace ... create the $namespace_type namespace"
echo "Success: "
echo "create $namespace_type model with $size in $region_name"

# Create the second mode only if the first mode is devdax
#
# If create the devdax namespace, we need someother space
# from the same region to create a fsdax namespace.
# For we need to store the pending_batch data and immutable configs
# on fsdax mode.
if [ "$namespace_type" == "devdax" ];then
  echo "you're creating a devdax model, you must create a fsdax mode!"
  read -p "input fstype (xfs | ext4): " fs_type
  check_fs_type "$fs_type"

  device_name=$(ndctl create-namespace --region="$region_name" \
                --size="$FSDAX_NAMESPACE_SIZE" --mode=fsdax | jq -r '.blockdev')
  error_record_and_exit $? "ndctl create-namespace ... create the fsdax namespace"

  mkfs."$fs_type" /dev/"$device_name" | \
      error_record_and_exit $? "mkfs.$fs_type /dev/'$device_name'"
  mkdir -p $KVDK_PMEM_META_DIR | error_record_and_exit $? "mkdir "
  chmod -R 666 $KVDK_PMEM_META_DIR | error_record_and_exit $? "chmod "
  mount -o dax /dev/"$device_name" $KVDK_PMEM_META_DIR | \
      error_record_and_exit $? "mount -o dax /dev/'$device_name' $KVDK_PMEM_META_DIR "

  echo "Success: "
  echo "create fsdax model with $FSDAX_NAMESPACE_SIZE in $region_name."
  echo "mount /dev/$device_name at $KVDK_PMEM_META_DIR with $fs_type type"
fi
