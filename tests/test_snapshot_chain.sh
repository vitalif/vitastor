#!/bin/bash -ex

. `dirname $0`/run_3osds.sh
check_qemu

# Test multiple snapshots

build/src/cli/vitastor-cli --etcd_address $ETCD_URL create -s 32M testchain

LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write \
        -etcd=$ETCD_URL -image=testchain -mirror_file=./testdata/mirror.bin

for i in {1..10}; do
    # Create a snapshot
    build/src/cli/vitastor-cli --etcd_address $ETCD_URL snap-create testchain@$i
    # Check that the new snapshot is see-through
    qemu-img convert -p \
        -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:image=testchain" \
        -O raw ./testdata/check.bin
    cmp ./testdata/check.bin ./testdata/mirror.bin
    # Write something to it
    LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=4k -direct=1 -iodepth=1 -fsync=32 -rw=randwrite \
        -randrepeat=$((i <= 2)) -buffer_pattern=0x$((10+i))$((10+i))$((10+i))$((10+i)) \
        -etcd=$ETCD_URL -image=testchain -number_ios=1024 -mirror_file=./testdata/mirror.bin
    # Check the new content
    qemu-img convert -p \
        -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:image=testchain" \
        -O raw ./testdata/layer1.bin
    cmp ./testdata/layer1.bin ./testdata/mirror.bin
done

build/src/cli/vitastor-cli --etcd_address $ETCD_URL rm testchain@1 testchain@9

# Check the final image
qemu-img convert -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:image=testchain" \
    -O raw ./testdata/layer1.bin
cmp ./testdata/layer1.bin ./testdata/mirror.bin

# Check the last remaining snapshot
qemu-img convert -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:image=testchain@10" \
    -O raw ./testdata/layer0.bin
cmp ./testdata/layer0.bin ./testdata/check.bin

format_green OK
