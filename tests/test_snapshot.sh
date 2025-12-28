#!/bin/bash -ex

. `dirname $0`/run_3osds.sh
check_qemu

# Test basic write and snapshot

$ETCDCTL put /vitastor/config/inode/1/2 '{"name":"testimg","size":'$((32*1024*1024))'}'

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write \
    -pool=1 -inode=2 -size=32M -cluster_log_level=10

$ETCDCTL put /vitastor/config/inode/1/2 '{"name":"testimg@0","size":'$((32*1024*1024))'}'
$ETCDCTL put /vitastor/config/inode/1/3 '{"parent_id":2,"name":"testimg","size":'$((32*1024*1024))'}'

$VITASTOR_FIO -bs=4k -direct=1 -iodepth=1 -fsync=32 -buffer_pattern=0xdeadface \
    -rw=randwrite -image=testimg -number_ios=1024

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 -rw=read -pool=1 -inode=3 -size=32M

qemu-img convert -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:pool=1:inode=2:size=$((32*1024*1024)):skip-parents=1" \
    -O qcow2 ./testdata/layer0.qcow2

qemu-img create -f qcow2 ./testdata/empty.qcow2 32M

qemu-img convert -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:pool=1:inode=3:size=$((32*1024*1024)):skip-parents=1" \
    -O qcow2 -o 'cluster_size=4k,backing_fmt=qcow2' -B empty.qcow2 ./testdata/layer1.qcow2

qemu-img convert -S 4096 -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:pool=1:inode=3:size=$((32*1024*1024))" \
    -O raw ./testdata/bin/merged.bin

qemu-img convert -S 4096 -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:image=testimg@0" \
    -O raw ./testdata/bin/layer0.bin

$ETCDCTL put /vitastor/config/inode/1/3 '{"name":"testimg","size":'$((32*1024*1024))'}'

qemu-img convert -S 4096 -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:image=testimg" \
    -O raw ./testdata/bin/layer1.bin

node tests/merge.js ./testdata/bin/layer0.bin ./testdata/bin/layer1.bin ./testdata/bin/check.bin

cmp ./testdata/bin/merged.bin ./testdata/bin/check.bin

# Test merge

$ETCDCTL put /vitastor/config/inode/1/3 '{"parent_id":2,"name":"testimg","size":'$((32*1024*1024))'}'

$VITASTOR_CLI rm testimg@0

qemu-img convert -S 4096 -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:image=testimg" \
    -O raw ./testdata/bin/merged-by-tool.bin

cmp ./testdata/bin/merged.bin ./testdata/bin/merged-by-tool.bin

# Test merge by qemu-img

qemu-img rebase -u -b layer0.qcow2 -F qcow2 ./testdata/layer1.qcow2

qemu-img convert -S 4096 -f qcow2 ./testdata/layer1.qcow2 -O raw ./testdata/bin/rebased.bin

cmp ./testdata/bin/merged.bin ./testdata/bin/rebased.bin

qemu-img rebase -u -b '' ./testdata/layer1.qcow2

qemu-img convert -S 4096 -f qcow2 ./testdata/layer1.qcow2 -O raw ./testdata/bin/rebased.bin

cmp ./testdata/bin/layer1.bin ./testdata/bin/rebased.bin

format_green OK
