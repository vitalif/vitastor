#!/bin/bash -ex

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
    build/src/vitastor-osd --osd_num $i --bind_address 127.0.0.1 $OSD_ARGS --etcd_address $ETCD_URL $(build/src/vitastor-cli simple-offsets --format options ./testdata/test_osd$i.bin 2>/dev/null) &>./testdata/osd$i.log &
    eval OSD${i}_PID=$!
done

cd mon
npm install
cd ..
node mon/mon-main.js --etcd_url http://$ETCD_URL --etcd_prefix "/vitastor" --verbose 1 &>./testdata/mon.log &
MON_PID=$!

$ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"replicated","pg_size":2,"pg_minsize":1,"pg_count":32,"failure_domain":"osd"}}'

sleep 2

if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only | jq -s -e '(.[0].items["1"] | map((.osd_set | select(. > 0)) | length == 2) | length) == 32'); then
    format_error "FAILED: 32 PGS NOT CONFIGURED"
fi

if ! ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == 32'); then
    format_error "FAILED: 32 PGS NOT UP"
fi

IMG_SIZE=960

LD_PRELOAD=libasan.so.5 \
fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4M -direct=1 -iodepth=16 -fsync=16 -rw=write \
    -etcd=$ETCD_URL -pool=1 -inode=2 -size=${IMG_SIZE}M -cluster_log_level=10

try_reweight()
{
    osd=$1
    w=$2
    $ETCDCTL put /vitastor/config/osd/$osd '{"reweight":'$w'}'
    sleep 3
}

try_reweight 1 0

try_reweight 2 0

try_reweight 3 0

try_reweight 4 0

try_reweight 5 0

try_reweight 1 1

try_reweight 2 1

try_reweight 3 1

try_reweight 4 1

try_reweight 5 1

# Wait for the rebalance to finish
for i in {1..60}; do
    ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == 32') && \
        break
    if [ $i -eq 60 ]; then
        format_error "Rebalance couldn't finish in 60 seconds"
    fi
    sleep 1
done

# Check that PGs never had degraded objects !
if grep has_degraded ./testdata/mon.log; then
    format_error "Some copies of objects were lost during interrupted rebalancings"
fi

# Check that no objects are lost !
nobj=`$ETCDCTL get --prefix '/vitastor/pg/stats' --print-value-only | jq -s '[ .[].object_count ] | reduce .[] as $num (0; .+$num)'`
if [ "$nobj" -ne $((IMG_SIZE*8)) ]; then
    format_error "Data lost after multiple interrupted rebalancings"
fi

format_green OK
