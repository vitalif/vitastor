#!/bin/bash -ex

. `dirname $0`/run_3osds.sh

#LD_PRELOAD=libasan.so.5 \
#    fio -thread -name=test -ioengine=build/src/libfio_vitastor_sec.so -bs=4k -fsync=128 `$ETCDCTL get /vitastor/osd/state/1 --print-value-only | jq -r '"-host="+.addresses[0]+" -port="+(.port|tostring)'` -rw=write -size=32M

# Random writes without immediate_commit were stalling OSDs

LD_PRELOAD="build/src/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=68k -direct=1 -numjobs=16 -iodepth=4 \
        -rw=randwrite -etcd=$ETCD_URL -pool=1 -inode=1 -size=128M -runtime=10

# A lot of parallel syncs was crashing the primary OSD at some point

LD_PRELOAD="build/src/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4k -direct=1 -numjobs=64 -iodepth=1 -fsync=1 \
        -rw=randwrite -etcd=$ETCD_URL -pool=1 -inode=1 -size=128M -number_ios=100

LD_PRELOAD="build/src/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write -etcd=$ETCD_URL -pool=1 -inode=1 -size=128M -cluster_log_level=10

LD_PRELOAD="build/src/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4k -direct=1 -iodepth=1 -fsync=32 -buffer_pattern=0xdeadface \
        -rw=randwrite -etcd=$ETCD_URL -pool=1 -inode=1 -size=128M -number_ios=1024

qemu-img convert -S 4096 -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:pool=1:inode=1:size=$((128*1024*1024))" \
    -O raw ./testdata/read.bin

qemu-img convert -S 4096 -p \
    -f raw ./testdata/read.bin \
    -O raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:pool=1:inode=1:size=$((128*1024*1024))"

format_green OK
