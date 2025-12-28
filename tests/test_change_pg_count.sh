#!/bin/bash -ex

OSD_COUNT=${OSD_COUNT:-6}
PG_COUNT=16

. `dirname $0`/run_3osds.sh

NOBJ=$(((128*8+PG_DATA_SIZE-1)/PG_DATA_SIZE))

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write \
    -pool=1 -inode=2 -size=128M -cluster_log_level=10

try_change()
{
    n=$1

    for i in {1..6}; do
        echo --- Change PG count to $n --- >>testdata/osd$i.log
    done
    echo --- Change PG count to $n --- >>testdata/mon.log

    $ETCDCTL put /vitastor/config/pools '{"1":{'$POOLCFG',"pg_size":'$PG_SIZE',"pg_minsize":'$PG_MINSIZE',"pg_count":'$n'}}'

    for i in {1..60}; do
        ($ETCDCTL get /vitastor/pg/config --print-value-only | jq -s -e '(.[0].items["1"] | map((.osd_set | select(. > 0)) | length == 2) | length) == '$n) && \
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

    if ! ($ETCDCTL get /vitastor/pg/config --print-value-only | jq -s -e '(.[0].items["1"] | map((.osd_set | select(. > 0)) | length == 2) | length) == '$n); then
        $ETCDCTL get /vitastor/pg/config
        $ETCDCTL get --prefix /vitastor/pg/state/
        format_error "FAILED: $n PGS NOT CONFIGURED"
    fi

    if ! ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$n); then
        $ETCDCTL get /vitastor/pg/config
        $ETCDCTL get --prefix /vitastor/pg/state/
        format_error "FAILED: $n PGS NOT UP"
    fi

    # Check that no objects are lost !
    # But note that reporting this information may take up to <etcd_report_interval+1> seconds
    nobj=0
    waittime=0
    while [[ $nobj -ne $NOBJ && $waittime -lt 7 ]]; do
        nobj=`$ETCDCTL get --prefix '/vitastor/pgstats' --print-value-only | jq -s '[ .[].object_count ] | reduce .[] as $num (0; .+$num)'`
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

if ! (grep /vitastor/stats ./testdata/mon.log | jq -s -e '[ .[] | select((.kv.value.op_stats.primary_write.count // 0 | tonumber) > 0) ] | length > 0'); then
    format_error "FAILED: monitor doesn't aggregate stats"
fi

format_green OK
