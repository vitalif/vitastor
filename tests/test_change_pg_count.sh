#!/bin/bash -ex

. `dirname $0`/common.sh

dd if=/dev/zero of=./testdata/test_osd1.bin bs=1024 count=1 seek=$((1024*1024-1))
dd if=/dev/zero of=./testdata/test_osd2.bin bs=1024 count=1 seek=$((1024*1024-1))
dd if=/dev/zero of=./testdata/test_osd3.bin bs=1024 count=1 seek=$((1024*1024-1))

build/src/vitastor-osd --osd_num 1 --bind_address 127.0.0.1 --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd1.bin 2>/dev/null) &>./testdata/osd1.log &
OSD1_PID=$!
build/src/vitastor-osd --osd_num 2 --bind_address 127.0.0.1 --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd2.bin 2>/dev/null) &>./testdata/osd2.log &
OSD2_PID=$!
build/src/vitastor-osd --osd_num 3 --bind_address 127.0.0.1 --etcd_address $ETCD_URL $(node mon/simple-offsets.js --format options --device ./testdata/test_osd3.bin 2>/dev/null) &>./testdata/osd3.log &
OSD3_PID=$!

cd mon
npm install
cd ..
node mon/mon-main.js --etcd_url http://$ETCD_URL --etcd_prefix "/vitastor" &>./testdata/mon.log &
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

    $ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"replicated","pg_size":3,"pg_minsize":2,"pg_count":'$n',"failure_domain":"osd"}}'

    for i in {1..10}; do
        ($ETCDCTL get /vitastor/config/pgs --print-value-only | jq -s -e '(.[0].items["1"] | map((.osd_set | sort) == ["1","2","3"]) | length) == '$n) && \
            ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$n'') && \
            break
        sleep 1
    done

    if ! ($ETCDCTL get /vitastor/config/pgs --print-value-only | jq -s -e '(.[0].items["1"] | map((.osd_set | sort) == ["1","2","3"]) | length) == '$n); then
        $ETCDCTL get /vitastor/config/pgs
        $ETCDCTL get --prefix /vitastor/pg/state/
        format_error "FAILED: $n PGS NOT CONFIGURED"
    fi

    if ! ($ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '([ .[] | select(.state == ["active"]) ] | length) == '$n); then
        $ETCDCTL get /vitastor/config/pgs
        $ETCDCTL get --prefix /vitastor/pg/state/
        format_error "FAILED: $n PGS NOT UP"
    fi
}

# 16 -> 32

try_change 32

# 32 -> 16

try_change 16

# 16 -> 25

try_change 25

# 25 -> 17

try_change 17

# 17 -> 16

try_change 16

format_green OK
