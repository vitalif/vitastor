#!/bin/bash -ex

. `dirname $0`/run_3osds.sh
check_qemu

# Test multiple snapshots

$VITASTOR_CLI create -s 32M testchain

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write \
    -image=testchain -mirror_file=./testdata/bin/mirror.bin

for i in {1..10}; do
    # Create a snapshot
    $VITASTOR_CLI snap-create testchain@$i
    # Check that the new snapshot is see-through
    qemu-img convert -p \
        -f raw "vitastor:config_path=$VITASTOR_CFG:image=testchain" \
        -O raw ./testdata/bin/check.bin
    cmp ./testdata/bin/check.bin ./testdata/bin/mirror.bin
    # Write something to it
    $VITASTOR_FIO -bs=4k -direct=1 -iodepth=1 -fsync=32 -rw=randwrite \
        -randrepeat=$((i <= 2)) -buffer_pattern=0x$((10+i))$((10+i))$((10+i))$((10+i)) \
        -image=testchain -number_ios=1024 -mirror_file=./testdata/bin/mirror.bin
    # Check the new content
    qemu-img convert -p \
        -f raw "vitastor:config_path=$VITASTOR_CFG:image=testchain" \
        -O raw ./testdata/bin/layer1.bin
    cmp ./testdata/bin/layer1.bin ./testdata/bin/mirror.bin
done

$VITASTOR_CLI rm testchain@1 testchain@9

# Check the final image
qemu-img convert -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:image=testchain" \
    -O raw ./testdata/bin/layer1.bin
cmp ./testdata/bin/layer1.bin ./testdata/bin/mirror.bin

# Check the last remaining snapshot
qemu-img convert -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:image=testchain@10" \
    -O raw ./testdata/bin/layer0.bin
cmp ./testdata/bin/layer0.bin ./testdata/bin/check.bin

format_green OK
