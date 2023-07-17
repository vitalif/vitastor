#!/bin/bash -ex

PG_COUNT=2048

. `dirname $0`/run_3osds.sh

LD_PRELOAD="build/src/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 -end_fsync=1 \
        -rw=write -etcd=$ETCD_URL -pool=1 -inode=1 -size=128M -cluster_log_level=10

start_osd 4

sleep 2

for i in {1..30}; do
    ($ETCDCTL get /vitastor/config/pgs --print-value-only |\
        jq -s -e '([ .[0].items["1"] | map(.osd_set)[][] ] | sort | unique == ["1","2","3","4"])') && \
        ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$PG_COUNT) && \
        break
    sleep 1
done

if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only |\
    jq -s -e '([ .[0].items["1"] | map(.osd_set)[][] ] | sort | unique == ["1","2","3","4"])'); then
    format_error "FAILED: OSD NOT ADDED INTO DISTRIBUTION"
fi

wait_finish_rebalance 60

sleep 1
kill -9 $OSD4_PID
sleep 1
build/src/vitastor-cli --etcd_address $ETCD_URL rm-osd --force 4

sleep 2

for i in {1..30}; do
    ($ETCDCTL get /vitastor/config/pgs --print-value-only |\
        jq -s -e '([ .[0].items["1"] | map(.osd_set)[][] ] | sort | unique == ["1","2","3"])') && \
        ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"] or .state == ["active", "left_on_dead"]) ] | length) == '$PG_COUNT'') && \
        break
    sleep 1
done

if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only |\
    jq -s -e '([ .[0].items["1"] | map(.osd_set)[][] ] | sort | unique == ["1","2","3"])'); then
    format_error "FAILED: OSD NOT REMOVED FROM DISTRIBUTION"
fi

wait_finish_rebalance 60

format_green OK
