#!/bin/bash -ex

SCHEME=xor
PG_COUNT=16
PG_MINSIZE=2
. `dirname $0`/run_3osds.sh

build/src/cmd/vitastor-cli --etcd_address $ETCD_URL create -s 128M testimg

LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 \
        -end_fsync=1 -fsync=1 -rw=write -etcd=$ETCD_URL -image=testimg -size=128M -cluster_log_level=10

kill -9 $OSD3_PID
$ETCDCTL del /vitastor/osd/state/3

if build/src/cmd/vitastor-cli --etcd_address $ETCD_URL rm testimg --log_level 10 ; then
    format_error "Delete should not be successful with inactive OSDs"
fi

if ! ( build/src/cmd/vitastor-cli --etcd_address $ETCD_URL ls | grep testimg | grep DEL ) ; then
    format_error "Image should be marked as partially deleted"
fi

start_osd 3
sleep 5

# Now do the same but without del /vitastor/osd/state

LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 \
        -end_fsync=1 -fsync=1 -rw=write -etcd=$ETCD_URL -image=testimg -size=128M -cluster_log_level=10

kill -9 $OSD3_PID

if build/src/cmd/vitastor-cli --etcd_address $ETCD_URL rm testimg --log_level 10 ; then
    format_error "Delete should not be successful with inactive OSDs"
fi

if ! ( build/src/cmd/vitastor-cli --etcd_address $ETCD_URL ls | grep testimg | grep DEL ) ; then
    format_error "Image should be marked as partially deleted"
fi

format_green OK
