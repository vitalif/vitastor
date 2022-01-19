#!/bin/bash -ex

. `dirname $0`/common.sh

OSD_SIZE=1024
OSD_COUNT=3
OSD_ARGS=
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

$ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"replicated","pg_size":3,"pg_minsize":2,"pg_count":16,"failure_domain":"osd"}}'

sleep 2

if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only | jq -s -e '(.[0].items["1"] | map((.osd_set | sort) == ["1","2","3"]) | length) == 16'); then
    format_error "FAILED: 16 PGS NOT CONFIGURED"
fi

if ! ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == 16'); then
    format_error "FAILED: 16 PGS NOT UP"
fi

try_change()
{
    n=$1
    s=$2

    $ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"replicated","pg_size":'$s',"pg_minsize":2,"pg_count":'$n',"failure_domain":"osd"}}'

    for i in {1..10}; do
        ($ETCDCTL get /vitastor/config/pgs --print-value-only |\
            jq -s -e '(.[0].items["1"] | map(  ([ .osd_set[] | select(. != 0) ] | length) == '$s'  ) | length == '$n')
                and ([ .[0].items["1"] | map(.osd_set)[][] ] | sort | unique == ["1","2","3"])') && \
            ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$n'') && \
            break
        sleep 1
    done

    if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only |\
        jq -s -e '(.[0].items["1"] | map(  ([ .osd_set[] | select(. != 0) ] | length) == '$s'  ) | length == '$n')
            and ([ .[0].items["1"] | map(.osd_set)[][] ] | sort | unique == ["1","2","3"])'); then
        $ETCDCTL get /vitastor/config/pgs
        $ETCDCTL get --prefix /vitastor/pg/state/
        format_error "FAILED: PG SIZE NOT CHANGED OR SOME OSDS DO NOT HAVE PGS"
    fi

    if ! ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$n); then
        $ETCDCTL get /vitastor/config/pgs
        $ETCDCTL get --prefix /vitastor/pg/state/
        format_error "FAILED: PGS NOT UP AFTER PG SIZE CHANGE"
    fi
}

try_change 16 2

format_green OK
