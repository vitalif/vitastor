#!/bin/bash -ex
# Test the `no_same_sector_overwrites` mode

OSD_ARGS="--journal_no_same_sector_overwrites true --journal_sector_buffer_count 1024 --disable_data_fsync 1 --immediate_commit all"
GLOBAL_CONF='{"immediate_commit":"all"}'

. `dirname $0`/run_3osds.sh

#LSAN_OPTIONS=report_objects=true:suppressions=`pwd`/testdata/lsan-suppress.txt LD_PRELOAD=libasan.so.5 \
#    fio -thread -name=test -ioengine=build/src/libfio_vitastor_sec.so -bs=4k -fsync=128 `$ETCDCTL get /vitastor/osd/state/1 --print-value-only | jq -r '"-host="+.addresses[0]+" -port="+(.port|tostring)'` -rw=write -size=32M

# Test basic write

LD_PRELOAD="build/src/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 -rw=write -etcd=$ETCD_URL -pool=1 -inode=1 -size=1G -cluster_log_level=10

format_green OK
