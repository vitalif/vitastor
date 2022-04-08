#!/bin/bash -ex

. `dirname $0`/common.sh

if [ "$EC" != "" ]; then
    POOLCFG='"scheme":"xor","pg_size":3,"pg_minsize":2,"parity_chunks":1'
    NOBJ=512
else
    POOLCFG='"scheme":"replicated","pg_size":2,"pg_minsize":2'
    NOBJ=1024
fi

OSD_SIZE=1024
OSD_COUNT=6
OSD_ARGS=
for i in $(seq 1 $OSD_COUNT); do
    dd if=/dev/zero of=./testdata/test_osd$i.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))
    build/src/vitastor-osd --osd_num $i --bind_address 127.0.0.1 $OSD_ARGS --etcd_address $ETCD_URL $(build/src/vitastor-cli simple-offsets --format options ./testdata/test_osd$i.bin 2>/dev/null) &>./testdata/osd$i.log &
    eval OSD${i}_PID=$!
done

cd mon
npm install
cd ..
node mon/mon-main.js --etcd_url $ETCD_URL --etcd_prefix "/vitastor" --verbose 1 &>./testdata/mon.log &
MON_PID=$!

$ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool",'$POOLCFG',"pg_count":16,"failure_domain":"osd"}}'

sleep 2

if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only | jq -s -e '(.[0].items["1"] | map((.osd_set | select(. > 0)) | length == 2) | length) == 16'); then
    format_error "FAILED: 16 PGS NOT CONFIGURED"
fi

if ! ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == 16'); then
    format_error "FAILED: 16 PGS NOT UP"
fi

LD_PRELOAD=libasan.so.5 \
fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write \
    -etcd=$ETCD_URL -pool=1 -inode=2 -size=128M -cluster_log_level=10

try_change()
{
    n=$1

    for i in {1..6}; do
        echo --- Change PG count to $n --- >>testdata/osd$i.log
    done

    $ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool",'$POOLCFG',"pg_count":'$n',"failure_domain":"osd"}}'

    for i in {1..10}; do
        ($ETCDCTL get /vitastor/config/pgs --print-value-only | jq -s -e '(.[0].items["1"] | map((.osd_set | select(. > 0)) | length == 2) | length) == '$n) && \
            ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"] or .state == ["active", "has_misplaced"]) ] | length) == '$n'') && \
            break
        sleep 1
    done

    # Wait for the rebalance to finish
    for i in {1..60}; do
        ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$n'') && \
            break
        sleep 1
    done

    if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only | jq -s -e '(.[0].items["1"] | map((.osd_set | select(. > 0)) | length == 2) | length) == '$n); then
        $ETCDCTL get /vitastor/config/pgs
        $ETCDCTL get --prefix /vitastor/pg/state/
        format_error "FAILED: $n PGS NOT CONFIGURED"
    fi

    if ! ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$n); then
        $ETCDCTL get /vitastor/config/pgs
        $ETCDCTL get --prefix /vitastor/pg/state/
        format_error "FAILED: $n PGS NOT UP"
    fi

    # Check that no objects are lost !
    # But note that reporting this information may take up to <etcd_report_interval+1> seconds
    nobj=0
    waittime=0
    while [[ $nobj -ne $NOBJ && $waittime -lt 7 ]]; do
        nobj=`$ETCDCTL get --prefix '/vitastor/pg/stats' --print-value-only | jq -s '[ .[].object_count ] | reduce .[] as $num (0; .+$num)'`
        if [[ $nobj -ne $NOBJ ]]; then
            waittime=$((waittime+1))
            sleep 1
        fi
    done
    if [ "$nobj" -ne $NOBJ ]; then
        format_error "Data lost after changing PG count to $n: $NOBJ objects expected, but got $nobj"
    fi
}

# 16 -> 32

try_change 32

# 32 -> 16

try_change 16

# 16 -> 25

try_change 25

# 25 -> 17

try_change 17

# 17 -> 16

try_change 16

# Monitor should report non-zero overall statistics at least once

if ! (grep /vitastor/stats ./testdata/mon.log | jq -s -e '[ .[] | select((.kv.value.op_stats.primary_write.count | tonumber) > 0) ] | length > 0'); then
    format_error "FAILED: monitor doesn't aggregate stats"
fi

format_green OK
