#!/bin/bash -ex

PG_COUNT=16

. `dirname $0`/run_3osds.sh

LD_PRELOAD="build/src/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 -end_fsync=1 \
        -rw=write -etcd=$ETCD_URL -pool=1 -inode=1 -size=128M -cluster_log_level=10

for i in 4; do
    dd if=/dev/zero of=./testdata/test_osd$i.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))
    start_osd $i
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

sleep 1
kill $OSD4_PID
sleep 1
$ETCDCTL del /vitastor/osd/state/4
$ETCDCTL del /vitastor/osd/stats/4
$ETCDCTL del /vitastor/osd/inodestats/4
$ETCDCTL del /vitastor/osd/space/4

sleep 2

for i in {1..10}; do
    ($ETCDCTL get /vitastor/config/pgs --print-value-only |\
        jq -s -e '([ .[0].items["1"] | map(.osd_set)[][] ] | sort | unique == ["1","2","3"])') && \
        ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$PG_COUNT'') && \
        break
    sleep 1
done

if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only |\
    jq -s -e '([ .[0].items["1"] | map(.osd_set)[][] ] | sort | unique == ["1","2","3"])'); then
    format_error "FAILED: OSD NOT REMOVED FROM DISTRIBUTION"
fi

if ! ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$PG_COUNT''); then
    format_error "FAILED: $PG_COUNT PGS NOT ACTIVE"
fi

format_green OK
