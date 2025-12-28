#!/bin/bash -ex

# Kill OSDs while writing

PG_SIZE=${PG_SIZE:-3}
if [[ "$SCHEME" = "ec" ]]; then
    PG_DATA_SIZE=${PG_DATA_SIZE:-2}
    PG_MINSIZE=${PG_MINSIZE:-3}
fi
OSD_COUNT=${OSD_COUNT:-7}
PG_COUNT=32
GLOBAL_CONFIG=',"osd_out_time":1'
. `dirname $0`/run_3osds.sh
check_qemu

# FIXME: Fix space rebalance priorities :)
IMG_SIZE=960

$ETCDCTL put /vitastor/config/inode/1/1 '{"name":"testimg","size":'$((IMG_SIZE*1024*1024))'}'

$VITASTOR_FIO -bs=4M -direct=1 -iodepth=1 -fsync=1 -rw=write \
    -mirror_file=./testdata/bin/mirror.bin -image=testimg -cluster_log_level=10

kill_osds()
{
    sleep 5

    echo Killing OSD 1
    kill -9 $OSD1_PID
    $ETCDCTL del /vitastor/osd/state/1

    for kill_osd in $(seq 2 $OSD_COUNT); do
        sleep 15
        # Wait for all PGs to clear has_degraded - all data will be at least in 2 copies
        wait_condition 600 "$ETCDCTL get /vitastor/pg/state/1/ --prefix --print-value-only |\
            jq -s -e '[ .[] | select(.state | contains(["'"'"active"'"'"])) | select(.state | contains(["'"'"has_degraded"'"'"]) | not) ] | length == '$PG_COUNT"
        echo Killing OSD $kill_osd and starting OSD $((kill_osd-1))
        p=OSD${kill_osd}_PID
        kill -9 ${!p}
        $ETCDCTL del /vitastor/osd/state/$kill_osd
        start_osd $((kill_osd-1))
    done

    sleep 5
    echo Starting OSD $OSD_COUNT
    start_osd $OSD_COUNT

    sleep 5
}

kill_osds &

$VITASTOR_FIO -bsrange=4k-128k -blockalign=4k -direct=1 -iodepth=32 -fsync=256 -rw=randrw \
    -serialize_overlap=1 -randrepeat=0 -refill_buffers=1 -mirror_file=./testdata/bin/mirror.bin -image=testimg -loops=10 -runtime=120

qemu-img convert -S 4096 -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:image=testimg" \
    -O raw ./testdata/bin/read.bin

if ! diff -q ./testdata/bin/read.bin ./testdata/bin/mirror.bin; then
    build/src/test/bindiff ./testdata/bin/read.bin ./testdata/bin/mirror.bin
    format_error Data lost during self-heal
fi

if grep -qP 'Checksum mismatch|BUG' ./testdata/osd*.log; then
    format_error Checksum mismatches or BUGs detected during test
fi

format_green OK
