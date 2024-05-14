#!/bin/bash -ex

OSD_ARGS="--data_csum_type crc32c --csum_block_size 32k --inmemory_journal false $OSD_ARGS"
OFFSET_ARGS="--data_csum_type crc32c --csum_block_size 32k --inmemory_journal false $OFFSET_ARGS"
PG_COUNT=${PG_COUNT:-64}
. `dirname $0`/run_3osds.sh
check_qemu

IMG_SIZE=128

$ETCDCTL put /vitastor/config/inode/1/1 '{"name":"testimg","size":'$((IMG_SIZE*1024*1024))'}'

# Write
LD_PRELOAD="build/src/client/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/client/libfio_vitastor.so -bs=1M -direct=1 -iodepth=4 \
        -mirror_file=./testdata/mirror.bin -end_fsync=1 -rw=write -etcd=$ETCD_URL -image=testimg -runtime=10

# Intentionally corrupt OSD data and restart it
kill $OSD1_PID
data_offset=$(build/src/disk_tool/vitastor-disk simple-offsets ./testdata/test_osd1.bin $OFFSET_ARGS | grep data_offset | awk '{print $2}')
truncate -s $data_offset ./testdata/test_osd1.bin
dd if=/dev/zero of=./testdata/test_osd1.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))
start_osd 1

# FIXME: corrupt the journal WHEN OSD IS RUNNING and check reads too

# Wait until start
wait_up 10

# Read everything back
qemu-img convert -S 4096 -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:image=testimg" \
    -O raw ./testdata/read.bin

diff ./testdata/read.bin ./testdata/mirror.bin

format_green OK
