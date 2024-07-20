#!/bin/bash -ex

. `dirname $0`/run_3osds.sh
check_qemu

# Test merge to child (without "inverse rename" optimisation)

build/src/cmd/vitastor-cli --etcd_address $ETCD_URL create -s 128M testchain

LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write \
        -etcd=$ETCD_URL -image=testchain -mirror_file=./testdata/bin/mirror.bin

# Create a snapshot
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL snap-create testchain@0

# Write something to it
LD_PRELOAD="build/src/client/libfio_vitastor.so" \
fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=1M -direct=1 -iodepth=4 -rw=randwrite \
    -randrepeat=0 -etcd=$ETCD_URL -image=testchain -number_ios=8 -mirror_file=./testdata/bin/mirror.bin

# Check the new content
qemu-img convert -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:image=testchain" \
    -O raw ./testdata/bin/layer1.bin
cmp ./testdata/bin/layer1.bin ./testdata/bin/mirror.bin

# Merge
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL rm testchain@0

# Check the final image
qemu-img convert -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:image=testchain" \
    -O raw ./testdata/bin/layer1.bin
cmp ./testdata/bin/layer1.bin ./testdata/bin/mirror.bin

format_green OK
