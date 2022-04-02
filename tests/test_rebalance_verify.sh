#!/bin/bash -ex

. `dirname $0`/run_7osds.sh

IMG_SIZE=256

$ETCDCTL put /vitastor/config/inode/1/1 '{"name":"testimg","size":'$((IMG_SIZE*1024*1024))'}'

NBD_DEV=$(sudo build/src/vitastor-nbd map --etcd_address $ETCD_URL --image testimg --logfile ./testdata/nbd.log &)

trap "sudo build/src/vitastor-nbd unmap $NBD_DEV" EXIT

sudo chown $(id -u) $NBD_DEV

dd if=/dev/urandom of=./testdata/img1.bin bs=1M count=$IMG_SIZE

dd if=./testdata/img1.bin of=$NBD_DEV bs=1M count=$IMG_SIZE oflag=direct

echo "Verifying before rebalance"
dd if=$NBD_DEV of=./testdata/img2.bin bs=1M count=$IMG_SIZE iflag=direct
diff ./testdata/img1.bin ./testdata/img2.bin

try_reweight 1 0

try_reweight 2 0

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

format_green OK
