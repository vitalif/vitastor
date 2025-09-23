#!/bin/bash -ex

SCHEME=${SCHEME:-ec}
. `dirname $0`/run_3osds.sh
check_qemu

build/src/cmd/vitastor-cli --etcd_address $ETCD_URL create -s 128M testchain

dd if=/dev/zero of=./testdata/bin/mirror.bin bs=4k seek=$(((128*1024-4)/4)) count=1

LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=32k -direct=1 -iodepth=4 -end_fsync=1 -rw=randwrite \
        -etcd=$ETCD_URL -image=testchain -mirror_file=./testdata/bin/mirror.bin -buffer_pattern=0xabcd -number_ios=1024

build/src/cmd/vitastor-cli --etcd_address $ETCD_URL snap-create testchain@snap1

LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=4k -direct=1 -iodepth=4 -end_fsync=1 -rw=randwrite -number_ios=32 \
        -etcd=$ETCD_URL -image=testchain -mirror_file=./testdata/bin/mirror.bin -buffer_pattern=0xabcd

# Now read from the snapshot

dd if=/dev/zero of=./testdata/bin/res.bin bs=4k seek=$(((128*1024-4)/4)) count=1

build/src/cmd/vitastor-cli --etcd_address $ETCD_URL dd iimg=testchain of=./testdata/bin/res.bin bs=$((PG_DATA_SIZE*128))k iodepth=4 --log_level 10

cmp ./testdata/bin/res.bin ./testdata/bin/mirror.bin

build/src/cmd/vitastor-cli --etcd_address $ETCD_URL dd iimg=testchain of=./testdata/bin/res.bin bs=$((PG_DATA_SIZE*128))k iodepth=4 conv=nosparse

cmp ./testdata/bin/res.bin ./testdata/bin/mirror.bin

qemu-img convert -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:image=testchain" \
    -O raw ./testdata/bin/res.bin

cmp ./testdata/bin/res.bin ./testdata/bin/mirror.bin

format_green OK
