#!/bin/bash -ex

OSD_ARGS="--data_csum_type crc32c --csum_block_size 32k --inmemory_journal false $OSD_ARGS"
OFFSET_ARGS="--data_csum_type crc32c --csum_block_size 32k --inmemory_journal false $OFFSET_ARGS"
PG_COUNT=${PG_COUNT:-1}
. `dirname $0`/run_3osds.sh
check_qemu

IMG_SIZE=128

PRIMARY=$($ETCDCTL get --print-value-only /vitastor/pg/config | jq -r '.items["1"]["1"].primary')

$ETCDCTL put /vitastor/config/inode/1/1 '{"name":"testimg","size":'$((IMG_SIZE*1024*1024))'}'

# Write
$VITASTOR_FIO -bs=1M -direct=1 -iodepth=4 \
    -mirror_file=./testdata/bin/mirror.bin -end_fsync=1 -rw=write -image=testimg -runtime=10

# Intentionally corrupt primary OSD data
data_offset=$(build/src/disk_tool/vitastor-disk simple-offsets ./testdata/bin/test_osd1.bin $OFFSET_ARGS | grep data_offset | awk '{print $2}')
truncate -s $data_offset ./testdata/bin/test_osd$PRIMARY.bin
dd if=/dev/zero of=./testdata/bin/test_osd$PRIMARY.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))

# Wait until start
wait_up 10

# Read everything back
qemu-img convert -S 4096 -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:image=testimg" \
    -O raw ./testdata/bin/read.bin

diff ./testdata/bin/read.bin ./testdata/bin/mirror.bin

format_green OK
