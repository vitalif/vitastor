#!/bin/bash -ex

. `dirname $0`/run_3osds.sh

# Test basic write and snapshot

$ETCDCTL put /vitastor/config/inode/1/2 '{"name":"testimg","size":'$((32*1024*1024))'}'

LD_PRELOAD=libasan.so.5 \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write \
        -etcd=$ETCD_URL -pool=1 -inode=2 -size=32M -cluster_log_level=10

$ETCDCTL put /vitastor/config/inode/1/2 '{"name":"testimg@0","size":'$((32*1024*1024))'}'
$ETCDCTL put /vitastor/config/inode/1/3 '{"parent_id":2,"name":"testimg","size":'$((32*1024*1024))'}'

LD_PRELOAD=libasan.so.5 \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4k -direct=1 -iodepth=1 -fsync=32 -buffer_pattern=0xdeadface \
        -rw=randwrite -etcd=$ETCD_URL -image=testimg -number_ios=1024

LD_PRELOAD=libasan.so.5 \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 -rw=read -etcd=$ETCD_URL -pool=1 -inode=3 -size=32M

qemu-img convert -S 4096 -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:pool=1:inode=3:size=$((32*1024*1024))" \
    -O raw ./testdata/merged.bin

qemu-img convert -S 4096 -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:image=testimg@0" \
    -O raw ./testdata/layer0.bin

$ETCDCTL put /vitastor/config/inode/1/3 '{"name":"testimg","size":'$((32*1024*1024))'}'

qemu-img convert -S 4096 -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:image=testimg" \
    -O raw ./testdata/layer1.bin

node mon/merge.js ./testdata/layer0.bin ./testdata/layer1.bin ./testdata/check.bin

cmp ./testdata/merged.bin ./testdata/check.bin

format_green OK
