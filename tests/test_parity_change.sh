#!/bin/bash -ex
# Test changing EC 4+1 into EC 4+3

OSD_COUNT=7
PG_COUNT=16
SCHEME=ec
PG_SIZE=5
PG_DATA_SIZE=4
PG_MINSIZE=5

. `dirname $0`/run_3osds.sh

try_change()
{
    n=$1
    s=$2

    for i in {1..10}; do
        ($ETCDCTL get /vitastor/pg/config --print-value-only |\
            jq -s -e '(.[0].items["1"] | map(  ([ .osd_set[] | select(. != 0) ] | length) == '$s'  ) | length == '$n')
                and ([ .[0].items["1"] | map(.osd_set)[][] ] | sort | unique == ["1","2","3","4","5","6","7"])') && \
            ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$n'') && \
            break
        sleep 1
    done

    if ! ($ETCDCTL get /vitastor/pg/config --print-value-only |\
        jq -s -e '(.[0].items["1"] | map(  ([ .osd_set[] | select(. != 0) ] | length) == '$s'  ) | length == '$n')
            and ([ .[0].items["1"] | map(.osd_set)[][] ] | sort | unique == ["1","2","3","4","5","6","7"])'); then
        $ETCDCTL get /vitastor/pg/config
        $ETCDCTL get --prefix /vitastor/pg/state/
        format_error "FAILED: PG SIZE NOT CHANGED OR SOME OSDS DO NOT HAVE PGS"
    fi

    if ! ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$n); then
        $ETCDCTL get /vitastor/pg/config
        $ETCDCTL get --prefix /vitastor/pg/state/
        format_error "FAILED: PGS NOT UP AFTER PG SIZE CHANGE"
    fi
}

$VITASTOR_FIO -bs=1M -direct=1 -iodepth=4 -rw=write -pool=1 -inode=1 -size=128M -runtime=10

PG_SIZE=7
POOLCFG='"name":"testpool","failure_domain":"osd","scheme":"ec","parity_chunks":'$((PG_SIZE-PG_DATA_SIZE))
$ETCDCTL put /vitastor/config/pools '{"1":{'$POOLCFG',"pg_size":'$PG_SIZE',"pg_minsize":'$PG_MINSIZE',"pg_count":'$PG_COUNT'}}'

sleep 2

try_change 16 7

format_green OK
