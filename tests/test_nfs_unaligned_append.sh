#!/bin/bash -ex

GLOBAL_CONFIG=',"client_enable_writeback":false'
IMMEDIATE_COMMIT=1
PG_COUNT=16
. `dirname $0`/run_3osds.sh

$VITASTOR_CLI create -s 10G fsmeta
$VITASTOR_CLI modify-pool --used-for-app fs:fsmeta testpool
build/src/nfs/vitastor-nfs --config_path $VITASTOR_CFG start --fs fsmeta --portmap 0 --port 2050 --foreground 1 --trace 1 >>./testdata/nfs.log 2>&1 &
NFS_PID=$!

mkdir -p testdata/nfs
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
MNT=$(pwd)/testdata/nfs
trap "sudo umount -f $MNT"' || true; kill -9 $(jobs -p)' EXIT

# big file
$VITASTOR_CLI dd if=/dev/urandom of=./testdata/ref_data.bin bs=1M count=32 seek=1024B
$VITASTOR_CLI dd if=./testdata/ref_data.bin of=$MNT/testfile oflag=direct bs=1M iodepth=4 seek=1024B skip=1024B
cp $MNT/testfile ./testdata/nfs_data.bin
if ! diff -q ./testdata/ref_data.bin $MNT/testfile; then
    format_error 'Data lost during parallel unaligned writes to VitastorFS'
fi

# small shared file
$VITASTOR_CLI dd if=/dev/urandom of=./testdata/ref_small.bin bs=10 count=500 seek=15B
$VITASTOR_CLI dd if=./testdata/ref_small.bin of=$MNT/smallfile oflag=direct bs=10 iodepth=4 seek=15B skip=15B
cp $MNT/smallfile ./testdata/nfs_small.bin
if ! diff -q ./testdata/ref_small.bin $MNT/smallfile; then
    format_error 'Data lost during parallel unaligned writes to a small file in VitastorFS'
fi

format_green OK
