#!/bin/bash -ex

. `dirname $0`/common.sh

OSD_SIZE=${OSD_SIZE:-1024}
PG_COUNT=${PG_COUNT:-1}

dd if=/dev/zero of=./testdata/test_osd1.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))
dd if=/dev/zero of=./testdata/test_osd2.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))
dd if=/dev/zero of=./testdata/test_osd3.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))

build/src/vitastor-osd --osd_num 1 --bind_address 127.0.0.1 $OSD_ARGS --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd1.bin 2>/dev/null) &>./testdata/osd1.log &
OSD1_PID=$!
build/src/vitastor-osd --osd_num 2 --bind_address 127.0.0.1 $OSD_ARGS --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd2.bin 2>/dev/null) &>./testdata/osd2.log &
OSD2_PID=$!
build/src/vitastor-osd --osd_num 3 --bind_address 127.0.0.1 $OSD_ARGS --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd3.bin 2>/dev/null) &>./testdata/osd3.log &
OSD3_PID=$!

cd mon
npm install
cd ..
node mon/mon-main.js --etcd_url http://$ETCD_URL --etcd_prefix "/vitastor" &>./testdata/mon.log &
MON_PID=$!

if [ -n "$GLOBAL_CONF" ]; then
    $ETCDCTL put /vitastor/config/global "$GLOBAL_CONF"
fi

$ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"xor","pg_size":3,"pg_minsize":2,"parity_chunks":1,"pg_count":'$PG_COUNT',"failure_domain":"osd"}}'

sleep 2

if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only | jq -s -e '(. | length) != 0 and ([ .[0].items["1"][] | select((.osd_set | sort) == ["1","2","3"]) ] | length) == '$PG_COUNT); then
    format_error "FAILED: $PG_COUNT PG(s) NOT CONFIGURED"
fi

if ! ($ETCDCTL get /vitastor/pg/state/1/ --prefix --print-value-only | jq -s -e '[ .[] | select(.state == ["active"]) ] | length == '$PG_COUNT); then
    format_error "FAILED: $PG_COUNT PG(s) NOT UP"
fi

if ! cmp build/src/block-vitastor.so /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so; then
    sudo rm -f /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so
    sudo ln -s "$(realpath .)/build/src/block-vitastor.so" /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so
fi
