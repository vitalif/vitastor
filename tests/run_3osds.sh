#!/bin/bash -ex

. `dirname $0`/common.sh

OSD_SIZE=${OSD_SIZE:-1024}
PG_COUNT=${PG_COUNT:-1}
PG_SIZE=${PG_SIZE:-3}
PG_MINSIZE=${PG_MINSIZE:-2}
OSD_COUNT=${OSD_COUNT:-3}
SCHEME=${SCHEME:-ec}

for i in $(seq 1 $OSD_COUNT); do
    dd if=/dev/zero of=./testdata/test_osd$i.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))
    build/src/vitastor-osd --osd_num $i --bind_address 127.0.0.1 $OSD_ARGS --etcd_address $ETCD_URL $(build/src/vitastor-cli simple-offsets --format options ./testdata/test_osd$i.bin 2>/dev/null) &>./testdata/osd$i.log &
    eval OSD${i}_PID=$!
done

cd mon
npm install
cd ..
node mon/mon-main.js --etcd_url $ETCD_URL --etcd_prefix "/vitastor" &>./testdata/mon.log &
MON_PID=$!

if [ -n "$GLOBAL_CONF" ]; then
    $ETCDCTL put /vitastor/config/global "$GLOBAL_CONF"
fi

if [ "$SCHEME" = "replicated" ]; then
    $ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"replicated","pg_size":'$PG_SIZE',"pg_minsize":'$PG_MINSIZE',"pg_count":'$PG_COUNT',"failure_domain":"osd"}}'
else
    $ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"xor","pg_size":'$PG_SIZE',"pg_minsize":'$PG_MINSIZE',"parity_chunks":1,"pg_count":'$PG_COUNT',"failure_domain":"osd"}}'
fi

sleep 2

if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only | jq -s -e '(. | length) != 0 and ([ .[0].items["1"][] | select(((.osd_set | select(. != 0) | sort | unique) | length) == '$PG_SIZE') ] | length) == '$PG_COUNT); then
    format_error "FAILED: $PG_COUNT PG(s) NOT CONFIGURED"
fi

if ! ($ETCDCTL get /vitastor/pg/state/1/ --prefix --print-value-only | jq -s -e '[ .[] | select(.state == ["active"]) ] | length == '$PG_COUNT); then
    format_error "FAILED: $PG_COUNT PG(s) NOT UP"
fi

if ! cmp build/src/block-vitastor.so /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so; then
    sudo rm -f /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so
    sudo ln -s "$(realpath .)/build/src/block-vitastor.so" /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so
fi
