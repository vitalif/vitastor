#!/bin/bash

. `dirname $0`/common.sh

if [ "$IMMEDIATE_COMMIT" != "" ]; then
    NO_SAME="--journal_no_same_sector_overwrites true --journal_sector_buffer_count 1024 --disable_data_fsync 1 --immediate_commit all --log_level 1"
    $ETCDCTL put /vitastor/config/global '{"recovery_queue_depth":1,"osd_out_time":5,"immediate_commit":"all"}'
else
    NO_SAME="--journal_sector_buffer_count 1024 --log_level 1"
    $ETCDCTL put /vitastor/config/global '{"recovery_queue_depth":1,"osd_out_time":5}'
fi

OSD_SIZE=1024
OSD_COUNT=7
OSD_ARGS=
for i in $(seq 1 $OSD_COUNT); do
    dd if=/dev/zero of=./testdata/test_osd$i.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))
    build/src/vitastor-osd --osd_num $i --bind_address 127.0.0.1 $NO_SAME $OSD_ARGS --etcd_address $ETCD_URL $(build/src/vitastor-cli simple-offsets --format options ./testdata/test_osd$i.bin 2>/dev/null) &>./testdata/osd$i.log &
    eval OSD${i}_PID=$!
done

cd mon
npm install
cd ..
node mon/mon-main.js --etcd_url $ETCD_URL --etcd_prefix "/vitastor" --verbose 1 &>./testdata/mon.log &
MON_PID=$!

if [ "$EC" != "" ]; then
    POOLCFG='"scheme":"xor","pg_size":3,"pg_minsize":2,"parity_chunks":1'
    PG_SIZE=3
else
    POOLCFG='"scheme":"replicated","pg_size":2,"pg_minsize":2'
    PG_SIZE=2
fi
$ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool",'$POOLCFG',"pg_count":32,"failure_domain":"osd"}}'

sleep 2

if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only | jq -s -e '(.[0].items["1"] | map((.osd_set | select(. > 0)) | length == '$PG_SIZE') | length) == 32'); then
    format_error "FAILED: 32 PGS NOT CONFIGURED"
fi

if ! ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == 32'); then
    format_error "FAILED: 32 PGS NOT UP"
fi

try_reweight()
{
    osd=$1
    w=$2
    $ETCDCTL put /vitastor/config/osd/$osd '{"reweight":'$w'}'
    sleep 3
}

wait_finish_rebalance()
{
    sec=$1
    i=0
    while [[ $i -lt $sec ]]; do
        ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == 32') && \
            break
        if [ $i -eq 60 ]; then
            format_error "Rebalance couldn't finish in $sec seconds"
        fi
        sleep 1
        i=$((i+1))
    done
}
