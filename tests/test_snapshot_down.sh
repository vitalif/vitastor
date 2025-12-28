#!/bin/bash -ex

. `dirname $0`/run_3osds.sh
check_qemu

# Test merge to child (without "inverse rename" optimisation)

$VITASTOR_CLI create -s 128M testchain

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write \
    -image=testchain -mirror_file=./testdata/bin/mirror.bin

# Create a snapshot
$VITASTOR_CLI snap-create testchain@0

# Write something to it
$VITASTOR_FIO -bs=1M -direct=1 -iodepth=4 -rw=randwrite \
    -randrepeat=0 -image=testchain -number_ios=8 -mirror_file=./testdata/bin/mirror.bin

# Check the new content
qemu-img convert -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:image=testchain" \
    -O raw ./testdata/bin/layer1.bin
cmp ./testdata/bin/layer1.bin ./testdata/bin/mirror.bin

# Merge
$VITASTOR_CLI rm testchain@0

# Check the final image
qemu-img convert -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:image=testchain" \
    -O raw ./testdata/bin/layer1.bin
cmp ./testdata/bin/layer1.bin ./testdata/bin/mirror.bin

format_green OK
