#!/bin/bash -ex

. `dirname $0`/run_3osds.sh
check_qemu

# snapshot in another pool

$VITASTOR_CLI create-pool testpool2 -s 3 -n 4 --failure_domain osd

wait_pool_up 30 2 3 4

$VITASTOR_CLI create -s 128M testchain -p testpool

$VITASTOR_FIO -bs=1M -direct=1 -iodepth=4 -fsync=1 -rw=write \
    -image=testchain -mirror_file=./testdata/bin/mirror.bin -randrepeat=0

$VITASTOR_CLI snap-create testchain@snap1 -p testpool2

$VITASTOR_FIO -bs=4k -direct=1 -iodepth=4 -end_fsync=1 -rw=randwrite -number_ios=32 \
    -image=testchain -mirror_file=./testdata/bin/mirror.bin -randrepeat=0

# Read from the first snapshot

$VITASTOR_CLI dd iimg=testchain of=./testdata/bin/res.bin bs=128k iodepth=4 --log_level 10
cmp ./testdata/bin/res.bin ./testdata/bin/mirror.bin

# Create a second snapshot - there was a bug where snapshotted reads from another pool
# were working only when the image and the snapshot were modified in the same revision
# (i.e. there was only one snapshot)
$VITASTOR_CLI snap-create testchain@snap2 -p testpool2

$VITASTOR_FIO -bs=4k -direct=1 -iodepth=4 -end_fsync=1 -rw=randwrite -number_ios=32 \
    -image=testchain -mirror_file=./testdata/bin/mirror.bin -randrepeat=0

$VITASTOR_CLI dd iimg=testchain of=./testdata/bin/res.bin bs=128k iodepth=4 --log_level 10

cmp ./testdata/bin/res.bin ./testdata/bin/mirror.bin

$VITASTOR_CLI dd iimg=testchain of=./testdata/bin/res.bin bs=32k iodepth=4 conv=nosparse

cmp ./testdata/bin/res.bin ./testdata/bin/mirror.bin

qemu-img convert -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:image=testchain" \
    -O raw ./testdata/bin/res.bin

cmp ./testdata/bin/res.bin ./testdata/bin/mirror.bin

format_green OK
