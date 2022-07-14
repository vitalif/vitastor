#!/bin/bash -ex

PG_COUNT=16

. `dirname $0`/run_3osds.sh

LD_PRELOAD="build/src/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 -end_fsync=1 \
        -rw=write -etcd=$ETCD_URL -pool=1 -inode=1 -size=128M -cluster_log_level=10

for i in 4; do
    dd if=/dev/zero of=./testdata/test_osd$i.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))
    build/src/vitastor-osd --osd_num $i --bind_address 127.0.0.1 $OSD_ARGS --etcd_address $ETCD_URL $(build/src/vitastor-disk simple-offsets --format options ./testdata/test_osd$i.bin 2>/dev/null) &>./testdata/osd$i.log &
    eval OSD${i}_PID=$!
done

sleep 2

for i in {1..10}; do
    ($ETCDCTL get /vitastor/config/pgs --print-value-only |\
        jq -s -e '([ .[0].items["1"] | map(.osd_set)[][] ] | sort | unique == ["1","2","3","4"])') && \
        ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$PG_COUNT'') && \
        break
    sleep 1
done

if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only |\
    jq -s -e '([ .[0].items["1"] | map(.osd_set)[][] ] | sort | unique == ["1","2","3","4"])'); then
    format_error "FAILED: OSD NOT ADDED INTO DISTRIBUTION"
fi

if ! ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$PG_COUNT''); then
    format_error "FAILED: $PG_COUNT PGS NOT ACTIVE"
fi

format_green OK
