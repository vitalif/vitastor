#!/bin/bash

. `dirname $0`/common.sh

OSD_SIZE=${OSD_SIZE:-1024}
PG_COUNT=${PG_COUNT:-1}
# OSD_COUNT
SCHEME=${SCHEME:-replicated}
# OSD_ARGS
# PG_SIZE
# PG_MINSIZE

if [ "$SCHEME" = "ec" ]; then
    OSD_COUNT=${OSD_COUNT:-5}
else
    OSD_COUNT=${OSD_COUNT:-3}
fi

if [ "$IMMEDIATE_COMMIT" != "" ]; then
    NO_SAME="--journal_no_same_sector_overwrites true --journal_sector_buffer_count 1024 --disable_data_fsync 1 --immediate_commit all --log_level 10"
    $ETCDCTL put /vitastor/config/global '{"recovery_queue_depth":1,"osd_out_time":1,"immediate_commit":"all"}'
else
    NO_SAME="--journal_sector_buffer_count 1024 --log_level 10"
    $ETCDCTL put /vitastor/config/global '{"recovery_queue_depth":1,"osd_out_time":1}'
fi

start_osd()
{
    local i=$1
    build/src/vitastor-osd --osd_num $i --bind_address 127.0.0.1 $NO_SAME $OSD_ARGS --etcd_address $ETCD_URL $(build/src/vitastor-disk simple-offsets --format options ./testdata/test_osd$i.bin 2>/dev/null) >>./testdata/osd$i.log 2>&1 &
    eval OSD${i}_PID=$!
}

for i in $(seq 1 $OSD_COUNT); do
    dd if=/dev/zero of=./testdata/test_osd$i.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))
    start_osd $i
done

cd mon
npm install
cd ..
node mon/mon-main.js --etcd_url $ETCD_URL --etcd_prefix "/vitastor" --verbose 1 &>./testdata/mon.log &
MON_PID=$!

if [ "$SCHEME" = "ec" ]; then
    PG_SIZE=${PG_SIZE:-5}
    PG_MINSIZE=${PG_MINSIZE:-3}
    PG_DATA_SIZE=$PG_MINSIZE
    POOLCFG='"scheme":"ec","parity_chunks":'$((PG_SIZE-PG_MINSIZE))
elif [ "$SCHEME" = "xor" ]; then
    PG_SIZE=${PG_SIZE:-3}
    PG_MINSIZE=${PG_MINSIZE:-2}
    PG_DATA_SIZE=$PG_MINSIZE
    POOLCFG='"scheme":"xor","parity_chunks":'$((PG_SIZE-PG_MINSIZE))
else
    PG_SIZE=${PG_SIZE:-2}
    PG_MINSIZE=${PG_MINSIZE:-2}
    PG_DATA_SIZE=1
    POOLCFG='"scheme":"replicated"'
fi
POOLCFG='"name":"testpool","failure_domain":"osd",'$POOLCFG
$ETCDCTL put /vitastor/config/pools '{"1":{'$POOLCFG',"pg_size":'$PG_SIZE',"pg_minsize":'$PG_MINSIZE',"pg_count":'$PG_COUNT'}}'

wait_up()
{
    local sec=$1
    local i=0
    local configured=0
    while [[ $i -lt $sec ]]; do
        if $ETCDCTL get /vitastor/config/pgs --print-value-only | jq -s -e '(. | length) != 0 and ([ .[0].items["1"][] |
            select(((.osd_set | select(. != 0) | sort | unique) | length) == '$PG_SIZE') ] | length) == '$PG_COUNT; then
            configured=1
            if $ETCDCTL get /vitastor/pg/state/1/ --prefix --print-value-only | jq -s -e '[ .[] | select(.state == ["active"]) ] | length == '$PG_COUNT; then
                break
            fi
        fi
        sleep 1
        i=$((i+1))
        if [ $i -eq $sec ]; then
            if [[ $configured -ne 0 ]]; then
                format_error "FAILED: $PG_COUNT PG(s) NOT CONFIGURED"
            fi
            format_error "FAILED: $PG_COUNT PG(s) NOT UP"
        fi
    done
}

wait_up 60

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

check_qemu()
{
    if ! cmp build/src/block-vitastor.so /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so; then
        sudo rm -f /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so
        sudo ln -s "$(realpath .)/build/src/block-vitastor.so" /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so
    fi
}
