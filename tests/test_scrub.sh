#!/bin/bash -ex
# Test for scrub without checksums

ZERO_OSD=${ZERO_OSD:-1}

if [[ ("$SCHEME" = "" || "$SCHEME" = "replicated") && ("$PG_SIZE" = "" || "$PG_SIZE" = 2) ]]; then
    OSD_COUNT=2
fi

. `dirname $0`/run_3osds.sh

check_qemu

IMG_SIZE=128

$ETCDCTL put /vitastor/config/inode/1/1 '{"name":"testimg","size":'$((IMG_SIZE*1024*1024))'}'

# Write
LD_PRELOAD="build/src/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=1M -direct=1 -iodepth=4 \
        -mirror_file=./testdata/mirror.bin -end_fsync=1 -rw=write -etcd=$ETCD_URL -image=testimg

# Save PG primary
primary=$($ETCDCTL get --print-value-only /vitastor/config/pgs | jq -r '.items["1"]["1"].primary')

# Intentionally corrupt OSD data and restart it
zero_osd_pid=OSD${ZERO_OSD}_PID
kill ${!zero_osd_pid}
sleep 1
kill -9 ${!zero_osd_pid} || true
data_offset=$(build/src/vitastor-disk simple-offsets ./testdata/test_osd$ZERO_OSD.bin $OFFSET_ARGS | grep data_offset | awk '{print $2}')
truncate -s $data_offset ./testdata/test_osd$ZERO_OSD.bin
dd if=/dev/zero of=./testdata/test_osd$ZERO_OSD.bin bs=1024 count=1 seek=$((OSD_SIZE*1024-1))
$ETCDCTL del /vitastor/osd/state/$ZERO_OSD
start_osd $ZERO_OSD

# Wait until start
wait_up 10

# Wait until PG is back on the same primary
wait_condition 10 "$ETCDCTL"$' get --print-value-only /vitastor/config/pgs | jq -s -e \'.[0].items["1"]["1"].primary == "'$primary'"'"'"

# Trigger scrub
$ETCDCTL put /vitastor/pg/history/1/1 `$ETCDCTL get --print-value-only /vitastor/pg/history/1/1 | jq -s -c '(.[0] // {}) + {"next_scrub":1}'`

# Wait for scrub to finish
wait_condition 300 "$ETCDCTL get --prefix /vitastor/pg/history/ --print-value-only | jq -s -e '([ .[] | select(.next_scrub == 0 or .next_scrub == null) ] | length) == $PG_COUNT'" Scrubbing

if [[ ($SCHEME = replicated && $PG_SIZE < 3) || ($SCHEME != replicated && $((PG_SIZE-PG_DATA_SIZE)) < 2) ]]; then
    # Check that objects are marked as inconsistent if 2 replicas or EC/XOR 2+1
    build/src/vitastor-cli describe --etcd_address $ETCD_URL --json | jq -e '[ .[] | select(.inconsistent) ] | length == '$((IMG_SIZE * 8 * PG_SIZE / (SCHEME = replicated ? 1 : PG_DATA_SIZE)))

    # Fix objects using vitastor-cli fix
    build/src/vitastor-cli describe --etcd_address $ETCD_URL --json | \
        jq -s '[ .[0][] | select(.inconsistent and .osd_num == '$ZERO_OSD') ]' | \
        build/src/vitastor-cli fix --etcd_address $ETCD_URL --bad_osds $ZERO_OSD
elif [[ ($SCHEME = replicated && $PG_SIZE > 2) || ($SCHEME != replicated && $((PG_SIZE-PG_DATA_SIZE)) > 1) ]]; then
    # Check that everything heals
    wait_finish_rebalance 300

    build/src/vitastor-cli describe --etcd_address $ETCD_URL --json | jq -e '. | length == 0'
fi

# Read everything back
qemu-img convert -S 4096 -p \
    -f raw "vitastor:etcd_host=127.0.0.1\:$ETCD_PORT/v3:image=testimg" \
    -O raw ./testdata/read.bin

diff ./testdata/read.bin ./testdata/mirror.bin

format_green OK
