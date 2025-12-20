#!/bin/bash -ex

GLOBAL_CONFIG=',"client_enable_writeback":false'
IMMEDIATE_COMMIT=1
PG_COUNT=16
. `dirname $0`/run_3osds.sh

build/src/cmd/vitastor-cli --etcd_address $ETCD_URL create -s 10G fsmeta
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL modify-pool --used-for-app fs:fsmeta testpool
build/src/nfs/vitastor-nfs start --fs fsmeta --etcd_address $ETCD_URL --portmap 0 --port 2050 --foreground 1 --trace 1 >>./testdata/nfs.log 2>&1 &
NFS_PID=$!

mkdir -p testdata/nfs
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
MNT=$(pwd)/testdata/nfs
trap "sudo umount -f $MNT"' || true; kill -9 $(jobs -p)' EXIT

dd if=/dev/urandom of=./testdata/ref_data.bin bs=1M count=32 seek=1024B
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL dd if=./testdata/ref_data.bin of=$MNT/testfile oflag=direct bs=1M iodepth=4 seek=1024B skip=1024B
cp $MNT/testfile ./testdata/nfs_data.bin
if ! diff -q ./testdata/ref_data.bin $MNT/testfile; then
    format_error 'Data lost during parallel unaligned writes to VitastorFS'
fi

format_green OK
