#!/bin/bash -ex

. `dirname $0`/run_3osds.sh
check_qemu

#LD_PRELOAD=libasan.so.5 \
#    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor_sec.so -bs=4k -fsync=128 `$ETCDCTL get /vitastor/osd/state/1 --print-value-only | jq -r '"-host="+.addresses[0]+" -port="+(.port|tostring)'` -rw=write -size=32M

# Small sequential writes were causing various bugs at different moments

echo Small sequential writes

$VITASTOR_FIO -bs=4k -direct=1 -numjobs=1 -iodepth=16 \
    -rw=write -pool=1 -inode=1 -size=128M -runtime=10

# Random writes without immediate_commit were stalling OSDs

echo 68k random writes

$VITASTOR_FIO -bs=68k -direct=1 -numjobs=16 -iodepth=4 \
    -rw=randwrite -pool=1 -inode=1 -size=128M -runtime=10

# A lot of parallel syncs was crashing the primary OSD at some point

echo T64Q1 writes with fsync

$VITASTOR_FIO -bs=4k -direct=1 -numjobs=64 -iodepth=1 -fsync=1 \
    -rw=randwrite -pool=1 -inode=1 -size=128M -number_ios=100

echo Linear write

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write \
    -pool=1 -inode=1 -size=128M -cluster_log_level=10

echo T1Q1 writes with fsync=32

$VITASTOR_FIO -bs=4k -direct=1 -iodepth=1 -fsync=32 -buffer_pattern=0xdeadface \
    -rw=randwrite -pool=1 -inode=1 -size=128M -number_ios=1024

qemu-img convert -S 4096 -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:pool=1:inode=1:size=$((128*1024*1024))" \
    -O raw ./testdata/bin/read.bin

qemu-img convert -S 4096 -p \
    -f raw ./testdata/bin/read.bin \
    -O raw "vitastor:config_path=$VITASTOR_CFG:pool=1:inode=1:size=$((128*1024*1024))"

format_green OK
