#!/bin/bash -ex

OSD_SIZE=2048

. `dirname $0`/run_3osds.sh
check_qemu

$ETCDCTL put /vitastor/config/inode/1/1 '{"name":"debian9","size":'$((2048*1024*1024))'}'

qemu-img convert -S 4096 -p \
    -f raw ~/debian9-kvm.raw \
    -O raw "vitastor:config_path=$VITASTOR_CFG:image=debian9"

$ETCDCTL put /vitastor/config/inode/1/1 '{"name":"debian9@0","size":'$((2048*1024*1024))'}'
$ETCDCTL put /vitastor/config/inode/1/2 '{"parent_id":1,"name":"debian9","size":'$((2048*1024*1024))'}'

qemu-system-x86_64 -enable-kvm -m 1024 \
    -drive 'file=vitastor:config_path=$VITASTOR_CFG:image=debian9',format=raw,if=none,id=drive-virtio-disk0,cache=none \
    -device virtio-blk-pci,scsi=off,bus=pci.0,addr=0x5,drive=drive-virtio-disk0,id=virtio-disk0,bootindex=1,write-cache=off,physical_block_size=4096,logical_block_size=512 \
    -vnc 0.0.0.0:0

format_green OK
