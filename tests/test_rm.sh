#!/bin/bash -ex

PG_COUNT=16
. `dirname $0`/run_3osds.sh

LD_PRELOAD=libasan.so.5 \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 \
        -end_fsync=1 -fsync=1 -rw=write -etcd=$ETCD_URL -pool=1 -inode=1 -size=128M -cluster_log_level=10

$ETCDCTL get --prefix '/vitastor/pg/state'

build/src/vitastor-cli rm --etcd_address $ETCD_URL --pool 1 --inode 1

format_green OK
