#!/bin/bash -ex

. `dirname $0`/run_3osds.sh
check_qemu

# snapshot in another pool

build/src/cmd/vitastor-cli --etcd_address $ETCD_URL create-pool testpool2 -s 3 -n 4 --failure_domain osd

wait_pool_up 30 2 3 4

build/src/cmd/vitastor-cli --etcd_address $ETCD_URL create -s 128M testchain -p testpool

LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=1M -direct=1 -iodepth=4 -fsync=1 -rw=write \
        -etcd=$ETCD_URL -image=testchain -mirror_file=./testdata/bin/mirror.bin -buffer_pattern=0xabcd

build/src/cmd/vitastor-cli --etcd_address $ETCD_URL snap-create testchain@snap1 -p testpool2

LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=4k -direct=1 -iodepth=4 -end_fsync=1 -rw=randwrite -number_ios=32 \
        -etcd=$ETCD_URL -image=testchain -mirror_file=./testdata/bin/mirror.bin -buffer_pattern=0xabcd

build/src/cmd/vitastor-cli --etcd_address $ETCD_URL dd iimg=testchain of=./testdata/bin/res.bin bs=128k iodepth=4

cmp ./testdata/bin/res.bin ./testdata/bin/mirror.bin

build/src/cmd/vitastor-cli --etcd_address $ETCD_URL dd iimg=testchain of=./testdata/bin/res.bin bs=32k iodepth=4 conv=nosparse

cmp ./testdata/bin/res.bin ./testdata/bin/mirror.bin

qemu-img convert -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:image=testchain" \
    -O raw ./testdata/bin/res.bin

cmp ./testdata/bin/res.bin ./testdata/bin/mirror.bin

format_green OK
