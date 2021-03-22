#!/bin/bash -ex

. `dirname $0`/common.sh

dd if=/dev/zero of=./testdata/test_osd1.bin bs=1024 count=1 seek=$((1024*1024-1))
dd if=/dev/zero of=./testdata/test_osd2.bin bs=1024 count=1 seek=$((1024*1024-1))
dd if=/dev/zero of=./testdata/test_osd3.bin bs=1024 count=1 seek=$((1024*1024-1))

build/src/vitastor-osd --osd_num 1 --bind_address 127.0.0.1 --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd1.bin 2>/dev/null) &>./testdata/osd1.log &
OSD1_PID=$!
build/src/vitastor-osd --osd_num 2 --bind_address 127.0.0.1 --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd2.bin 2>/dev/null) &>./testdata/osd2.log &
OSD2_PID=$!
build/src/vitastor-osd --osd_num 3 --bind_address 127.0.0.1 --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd3.bin 2>/dev/null) &>./testdata/osd3.log &
OSD3_PID=$!

cd mon
npm install
cd ..
node mon/mon-main.js --etcd_url http://$ETCD_URL --etcd_prefix "/vitastor" &>./testdata/mon.log &
MON_PID=$!

$ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"xor","pg_size":3,"pg_minsize":2,"parity_chunks":1,"pg_count":1,"failure_domain":"osd"}}'

sleep 2

if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only | jq -s -e '(. | length) != 0 and (.[0].items["1"]["1"].osd_set | sort) == ["1","2","3"]'); then
    format_error "FAILED: 1 PG NOT CONFIGURED"
fi

if ! ($ETCDCTL get /vitastor/pg/state/1/1 --print-value-only | jq -s -e '(. | length) != 0 and .[0].state == ["active"]'); then
    format_error "FAILED: 1 PG NOT UP"
fi

#LD_PRELOAD=libasan.so.5 \
#    fio -thread -name=test -ioengine=build/src/libfio_vitastor_sec.so -bs=4k -fsync=128 `$ETCDCTL get /vitastor/osd/state/1 --print-value-only | jq -r '"-host="+.addresses[0]+" -port="+(.port|tostring)'` -rw=write -size=32M

if ! cmp build/src/block-vitastor.so /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so; then
    sudo rm -f /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so
    sudo ln -s "$(realpath .)/build/src/block-vitastor.so" /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so
fi

LD_PRELOAD=libasan.so.5 \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write -etcd=$ETCD_URL -pool=1 -inode=1 -size=128M -cluster_log_level=10

LD_PRELOAD=libasan.so.5 \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4k -direct=1 -iodepth=1 -fsync=32 -buffer_pattern=0xdeadface \
        -rw=randwrite -etcd=$ETCD_URL -pool=1 -inode=1 -size=128M -number_ios=1024

qemu-img convert -S 4096 -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:pool=1:inode=1:size=$((128*1024*1024))" \
    -O raw ./testdata/read.bin

qemu-img convert -S 4096 -p \
    -f raw ./testdata/read.bin \
    -O raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:pool=1:inode=1:size=$((128*1024*1024))"

format_green OK
