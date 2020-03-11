#!/bin/bash -ex

. `dirname $0`/common.sh

if [ "$IMMEDIATE_COMMIT" != "" ]; then
    NO_SAME="--journal_no_same_sector_overwrites true --journal_sector_buffer_count 1024 --disable_data_fsync 1 --immediate_commit all"
    $ETCDCTL put /vitastor/config/global '{"recovery_queue_depth":1,"osd_out_time":5,"immediate_commit":"all"}'
else
    NO_SAME="--journal_sector_buffer_count 1024"
    $ETCDCTL put /vitastor/config/global '{"recovery_queue_depth":1,"osd_out_time":5}'
fi

dd if=/dev/zero of=./testdata/test_osd1.bin bs=1024 count=1 seek=$((1024*1024-1))
dd if=/dev/zero of=./testdata/test_osd2.bin bs=1024 count=1 seek=$((1024*1024-1))
dd if=/dev/zero of=./testdata/test_osd3.bin bs=1024 count=1 seek=$((1024*1024-1))
dd if=/dev/zero of=./testdata/test_osd4.bin bs=1024 count=1 seek=$((1024*1024-1))
dd if=/dev/zero of=./testdata/test_osd5.bin bs=1024 count=1 seek=$((1024*1024-1))
dd if=/dev/zero of=./testdata/test_osd6.bin bs=1024 count=1 seek=$((1024*1024-1))
dd if=/dev/zero of=./testdata/test_osd7.bin bs=1024 count=1 seek=$((1024*1024-1))

build/src/vitastor-osd --osd_num 1 --bind_address 127.0.0.1 $NO_SAME --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd1.bin 2>/dev/null) 2>&1 >>./testdata/osd1.log &
OSD1_PID=$!
build/src/vitastor-osd --osd_num 2 --bind_address 127.0.0.1 $NO_SAME --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd2.bin 2>/dev/null) 2>&1 >>./testdata/osd2.log &
OSD2_PID=$!
build/src/vitastor-osd --osd_num 3 --bind_address 127.0.0.1 $NO_SAME --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd3.bin 2>/dev/null) 2>&1 >>./testdata/osd3.log &
OSD3_PID=$!
build/src/vitastor-osd --osd_num 4 --bind_address 127.0.0.1 $NO_SAME --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd4.bin 2>/dev/null) 2>&1 >>./testdata/osd4.log &
OSD4_PID=$!
build/src/vitastor-osd --osd_num 5 --bind_address 127.0.0.1 $NO_SAME --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd5.bin 2>/dev/null) 2>&1 >>./testdata/osd5.log &
OSD5_PID=$!
build/src/vitastor-osd --osd_num 6 --bind_address 127.0.0.1 $NO_SAME --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd6.bin 2>/dev/null) 2>&1 >>./testdata/osd6.log &
OSD6_PID=$!
build/src/vitastor-osd --osd_num 7 --bind_address 127.0.0.1 $NO_SAME --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd7.bin 2>/dev/null) 2>&1 >>./testdata/osd7.log &
OSD7_PID=$!

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

LD_PRELOAD=libasan.so.5 \
fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4M -direct=1 -iodepth=16 -fsync=16 -rw=write \
    -etcd=$ETCD_URL -pool=1 -inode=2 -size=1024M -cluster_log_level=10

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

# Check that PGs never has degraded objects !
if grep has_degraded ./testdata/mon.log; then
    format_error "Some copies of objects were lost during interrupted rebalancings"
fi

# Check that no objects are lost !
nobj=`$ETCDCTL get --prefix '/vitastor/pg/stats' --print-value-only | jq -s '[ .[].object_count ] | reduce .[] as $num (0; .+$num)'`
if [ "$nobj" -ne 8192 ]; then
    format_error "Data lost after multiple interrupted rebalancings"
fi

format_green OK
