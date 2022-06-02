#!/bin/bash -ex

OSD_COUNT=7
PG_COUNT=32
. `dirname $0`/run_3osds.sh

IMG_SIZE=256

$ETCDCTL put /vitastor/config/inode/1/1 '{"name":"testimg","size":'$((IMG_SIZE*1024*1024))'}'

NBD_DEV=$(sudo build/src/vitastor-nbd map --etcd_address $ETCD_URL --image testimg --logfile ./testdata/nbd.log &)

trap "sudo build/src/vitastor-nbd unmap $NBD_DEV"'; kill -9 $(jobs -p)' EXIT

sudo chown $(id -u) $NBD_DEV

dd if=/dev/urandom of=./testdata/img1.bin bs=1M count=$IMG_SIZE

dd if=./testdata/img1.bin of=$NBD_DEV bs=1M count=$IMG_SIZE oflag=direct

verify() {
    echo "Verifying before rebalance"
    dd if=$NBD_DEV of=./testdata/img2.bin bs=1M count=$IMG_SIZE iflag=direct
    diff ./testdata/img1.bin ./testdata/img2.bin

    $ETCDCTL put /vitastor/config/osd/1 '{"reweight":'$1'}'
    $ETCDCTL put /vitastor/config/osd/2 '{"reweight":'$1'}'
    $ETCDCTL put /vitastor/config/osd/3 '{"reweight":'$1'}'
    sleep 1

    for i in {1..10000}; do
        O=$(((RANDOM*RANDOM) % (IMG_SIZE*128)))
        dd if=$NBD_DEV of=./testdata/img2.bin bs=4k seek=$O skip=$O count=1 iflag=direct conv=notrunc
    done

    echo "Verifying during rebalance"
    diff ./testdata/img1.bin ./testdata/img2.bin

    # Wait for the rebalance to finish
    wait_finish_rebalance 60

    echo "Verifying after rebalance"
    dd if=$NBD_DEV of=./testdata/img2.bin bs=1M count=$IMG_SIZE iflag=direct
    diff ./testdata/img1.bin ./testdata/img2.bin
}

# Verify with regular reads

verify 0

# Same with chained reads

$ETCDCTL put /vitastor/config/inode/1/1 '{"name":"testimg0","size":'$((IMG_SIZE*1024*1024))'}'
$ETCDCTL put /vitastor/config/inode/1/2 '{"name":"testimg","size":'$((IMG_SIZE*1024*1024))',"parent_id":1}'
sleep 1

verify 1

format_green OK
