#!/bin/bash -ex

# Kill OSDs while writing

PG_SIZE=3

. `dirname $0`/run_7osds.sh

IMG_SIZE=960

$ETCDCTL put /vitastor/config/inode/1/1 '{"name":"testimg","size":'$((IMG_SIZE*1024*1024))'}'

LD_PRELOAD="build/src/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write \
        -mirror_file=./testdata/mirror.bin -etcd=$ETCD_URL -image=testimg -cluster_log_level=10

kill_osds()
{
    sleep 5

    kill -9 $OSD1_PID
    $ETCDCTL del /vitastor/osd/state/1

    for i in 2 3 4 5 6 7; do
        sleep 15
        echo Killing OSD $i and starting OSD $((i-1))
        p=OSD${i}_PID
        kill -9 ${!p}
        $ETCDCTL del /vitastor/osd/state/$i
        start_osd $((i-1))
        sleep 15
    done

    sleep 5
    start_osd 7

    sleep 5
}

kill_osds &

LD_PRELOAD="build/src/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4k -direct=1 -iodepth=16 -fsync=256 -rw=randwrite \
        -mirror_file=./testdata/mirror.bin -etcd=$ETCD_URL -image=testimg -loops=10 -runtime=120 2>/dev/null

qemu-img convert -S 4096 -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:image=testimg" \
    -O raw ./testdata/read.bin

diff ./testdata/read.bin ./testdata/mirror.bin

format_green OK
