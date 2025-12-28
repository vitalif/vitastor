#!/bin/bash -ex

SCHEME=${SCHEME:-ec}
. `dirname $0`/run_3osds.sh
check_qemu

$VITASTOR_CLI create -s 128M testchain

dd if=/dev/zero of=./testdata/bin/mirror.bin bs=4k seek=$(((128*1024-4)/4)) count=1

$VITASTOR_FIO -bs=32k -direct=1 -iodepth=4 -end_fsync=1 -rw=randwrite \
    -image=testchain -mirror_file=./testdata/bin/mirror.bin -buffer_pattern=0xabcd -number_ios=1024

$VITASTOR_CLI snap-create testchain@snap1

$VITASTOR_FIO -bs=4k -direct=1 -iodepth=4 -end_fsync=1 -rw=randwrite -number_ios=32 \
    -image=testchain -mirror_file=./testdata/bin/mirror.bin -buffer_pattern=0xabcd

# Now read from the snapshot

dd if=/dev/zero of=./testdata/bin/res.bin bs=4k seek=$(((128*1024-4)/4)) count=1

$VITASTOR_CLI dd iimg=testchain of=./testdata/bin/res.bin bs=$((PG_DATA_SIZE*128))k iodepth=4 --log_level 10

cmp ./testdata/bin/res.bin ./testdata/bin/mirror.bin

$VITASTOR_CLI dd iimg=testchain of=./testdata/bin/res.bin bs=$((PG_DATA_SIZE*128))k iodepth=4 conv=nosparse

cmp ./testdata/bin/res.bin ./testdata/bin/mirror.bin

qemu-img convert -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:image=testchain" \
    -O raw ./testdata/bin/res.bin

cmp ./testdata/bin/res.bin ./testdata/bin/mirror.bin

format_green OK
