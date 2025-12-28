#!/bin/bash -ex
# Test for scrub without checksums

ZERO_OSD=${ZERO_OSD:-1}

if [[ ("$SCHEME" = "" || "$SCHEME" = "replicated") && ("$PG_SIZE" = "" || "$PG_SIZE" = 2) ]]; then
    OSD_COUNT=2
fi

IMG_SIZE=128

# OSD uses BIG_INTENTs, so we set journal_trim_interval exactly equal to the object count
# to force it do it just 1 trim_lsn().
OSD_ARGS="--scrub_list_limit 1000 --journal_trim_interval $((IMG_SIZE*8)) $OSD_ARGS"

. `dirname $0`/run_3osds.sh

check_qemu

$ETCDCTL put /vitastor/config/inode/1/1 '{"name":"testimg","size":'$((IMG_SIZE*1024*1024))'}'

# Write
$VITASTOR_FIO -bs=1M -direct=1 -iodepth=4 \
    -mirror_file=./testdata/bin/mirror.bin -end_fsync=1 -rw=write -image=testimg

sleep 1

# Save PG primary
primary=$($ETCDCTL get --print-value-only /vitastor/pg/config | jq -r '.items["1"]["1"].primary')

# Intentionally corrupt OSD data and restart it
zero_osd_pid=OSD${ZERO_OSD}_PID
kill ${!zero_osd_pid}
sleep 1
kill -9 ${!zero_osd_pid} || true
data_offset=$(build/src/disk_tool/vitastor-disk simple-offsets ./testdata/bin/test_osd$ZERO_OSD.bin $OFFSET_ARGS | grep data_offset | awk '{print $2}')
truncate -s $data_offset ./testdata/bin/test_osd$ZERO_OSD.bin
dd if=/dev/zero of=./testdata/bin/test_osd$ZERO_OSD.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))
$ETCDCTL del /vitastor/osd/state/$ZERO_OSD
mv ./testdata/osd$ZERO_OSD.log ./testdata/osd${ZERO_OSD}_pre.log
start_osd $ZERO_OSD

# Wait until start
wait_up 10

# Wait until PG is back on the same primary
wait_condition 10 "$ETCDCTL"$' get --print-value-only /vitastor/pg/config | jq -s -e \'.[0].items["1"]["1"].primary == "'$primary'"'"'"

# Trigger scrub
$ETCDCTL put /vitastor/pg/history/1/1 `$ETCDCTL get --print-value-only /vitastor/pg/history/1/1 | jq -s -c '(.[0] // {}) + {"next_scrub":1}'`

# Wait for scrub to finish
wait_condition 300 "$ETCDCTL get --prefix /vitastor/pg/history/ --print-value-only | jq -s -e '([ .[] | select(.next_scrub == 0 or .next_scrub == null) ] | length) == $PG_COUNT'" Scrubbing

if [[ ($SCHEME = replicated && $PG_SIZE < 3) || ($SCHEME != replicated && $((PG_SIZE-PG_DATA_SIZE)) < 2) ]]; then
    # Check that objects are marked as inconsistent if 2 replicas or EC/XOR 2+1
    $VITASTOR_CLI describe --json &>./testdata/describe.log
    $VITASTOR_CLI describe --json | jq -e '[ .[] | select(.inconsistent) ] | length == '$((IMG_SIZE * 8 * PG_SIZE / (SCHEME = replicated ? 1 : PG_DATA_SIZE)))

    # Fix objects using vitastor-cli fix
    $VITASTOR_CLI describe --json | \
        jq -s '[ .[0][] | select(.inconsistent and .osd_num == '$ZERO_OSD') ]' | \
        $VITASTOR_CLI fix --bad_osds $ZERO_OSD
elif [[ ($SCHEME = replicated && $PG_SIZE > 2) || ($SCHEME != replicated && $((PG_SIZE-PG_DATA_SIZE)) > 1) ]]; then
    # Check that everything heals
    wait_finish_rebalance 300

    $VITASTOR_CLI describe --json | jq -e '. | length == 0'
fi

# Read everything back
qemu-img convert -S 4096 -p \
    -f raw "vitastor:config_path=$VITASTOR_CFG:image=testimg" \
    -O raw ./testdata/bin/read.bin

diff ./testdata/bin/read.bin ./testdata/bin/mirror.bin

format_green OK
