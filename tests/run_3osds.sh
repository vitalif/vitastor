#!/bin/bash

. `dirname $0`/common.sh

OSD_SIZE=${OSD_SIZE:-1024}
PG_COUNT=${PG_COUNT:-1}
# OSD_COUNT
SCHEME=${SCHEME:-replicated}
# OSD_ARGS
# OFFSET_ARGS
# PG_SIZE
# PG_MINSIZE
# GLOBAL_CONFIG

if [ "$SCHEME" = "ec" ]; then
    OSD_COUNT=${OSD_COUNT:-5}
else
    OSD_COUNT=${OSD_COUNT:-3}
fi

if [ "$IMMEDIATE_COMMIT" != "" ]; then
    NO_SAME="--journal_no_same_sector_overwrites true --journal_sector_buffer_count 1024 --disable_data_fsync 1 --immediate_commit all --log_level 10 --etcd_stats_interval 5"
    $ETCDCTL put /vitastor/config/global '{"recovery_queue_depth":1,"recovery_tune_util_low":1,"immediate_commit":"all","client_enable_writeback":true,"client_max_writeback_iodepth":32'$GLOBAL_CONFIG'}'
else
    NO_SAME="--journal_sector_buffer_count 1024 --log_level 10 --etcd_stats_interval 5"
    $ETCDCTL put /vitastor/config/global '{"recovery_queue_depth":1,"recovery_tune_util_low":1,"client_enable_writeback":true,"client_max_writeback_iodepth":32'$GLOBAL_CONFIG'}'
fi

start_osd_on()
{
    local i=$1
    local dev=$2
    build/src/vitastor-osd --osd_num $i --bind_address $ETCD_IP $NO_SAME $OSD_ARGS --etcd_address $ETCD_URL \
        $(build/src/vitastor-disk simple-offsets --format options $OFFSET_ARGS $dev $OFFSET_ARGS 2>/dev/null) \
        >>./testdata/osd$i.log 2>&1 &
    eval OSD${i}_PID=$!
}

if ! type -t osd_dev; then
    osd_dev()
    {
        local i=$1
        [[ -f ./testdata/test_osd$i.bin ]] || dd if=/dev/zero of=./testdata/test_osd$i.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))
        echo ./testdata/test_osd$i.bin
    }
fi

start_osd()
{
    start_osd_on $1 $(osd_dev $1)
}

for i in $(seq 1 $OSD_COUNT); do
    start_osd $i
done

(while true; do set +e; node mon/mon-main.js --etcd_address $ETCD_URL --etcd_prefix "/vitastor" --verbose 1; if [[ $? -ne 2 ]]; then break; fi; done) >>./testdata/mon.log 2>&1 &
MON_PID=$!

if [ "$SCHEME" = "ec" ]; then
    PG_SIZE=${PG_SIZE:-5}
    PG_MINSIZE=${PG_MINSIZE:-4}
    PG_DATA_SIZE=${PG_DATA_SIZE:-3}
    POOLCFG='"scheme":"ec","parity_chunks":'$((PG_SIZE-PG_DATA_SIZE))
elif [ "$SCHEME" = "xor" ]; then
    PG_SIZE=${PG_SIZE:-3}
    PG_MINSIZE=${PG_MINSIZE:-3}
    PG_DATA_SIZE=$((PG_SIZE-1))
    POOLCFG='"scheme":"xor","parity_chunks":1'
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

if [[ $OSD_COUNT -gt 0 ]]; then
    wait_up 120
fi

try_reweight()
{
    osd=$1
    w=$2
    $ETCDCTL put /vitastor/config/osd/$osd '{"reweight":'$w'}'
    sleep 3
}

wait_condition()
{
    sec=$1
    check=$2
    proc=$3
    i=0
    while [[ $i -lt $sec ]]; do
        eval "$check" && break
        if [ $i -eq $sec ]; then
            format_error "$proc couldn't finish in $sec seconds"
        fi
        sleep 1
        i=$((i+1))
    done
}

wait_finish_rebalance()
{
    sec=$1
    check=$2
    check=${check:-'.state == ["active"] or .state == ["active", "left_on_dead"]'}
    check="$ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select($check) ] | length) == $PG_COUNT'"
    wait_condition "$sec" "$check" Rebalance
}

check_qemu()
{
    if ! cmp build/src/block-vitastor.so /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so; then
        sudo rm -f /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so
        sudo ln -s "$(realpath .)/build/src/block-vitastor.so" /usr/lib/x86_64-linux-gnu/qemu/block-vitastor.so
    fi
}

check_nbd()
{
    if [[ -d /sys/module/nbd && ! -e /dev/nbd0 ]]; then
        max_part=$(cat /sys/module/nbd/parameters/max_part)
        nbds_max=$(cat /sys/module/nbd/parameters/nbds_max)
        for i in $(seq 1 $nbds_max); do
            mknod /dev/nbd$((i-1)) b 43 $(((i-1)*(max_part+1)))
        done
    fi
}
