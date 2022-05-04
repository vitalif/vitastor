#!/bin/bash -ex

OSD_COUNT=2
PG_SIZE=2
PG_MINSIZE=1
SCHEME=replicated

. `dirname $0`/run_3osds.sh

# Kill OSD 1

kill $OSD1_PID
sleep 2

# Write

LD_PRELOAD="build/src/libfio_vitastor.so" \
    fio -thread -name=test -ioengine=build/src/libfio_vitastor.so -bs=4k -direct=1 -iodepth=1 -fsync=1 \
        -rw=randwrite -etcd=$ETCD_URL -pool=1 -inode=1 -size=128M -runtime=10 -number_ios=100

# Kill OSD 2, start OSD 1

kill $OSD2_PID
build/src/vitastor-osd --osd_num 1 --bind_address 127.0.0.1 $OSD_ARGS --etcd_address $ETCD_URL $(build/src/vitastor-cli simple-offsets --format options --device ./testdata/test_osd2.bin 2>/dev/null) >>./testdata/osd2.log 2>&1 &
sleep 2

# Check PG state - it should NOT become active

if ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | grep -q active); then
    format_error "FAILED: PG STILL ACTIVE AFTER SPLITBRAIN"
fi

format_green OK
