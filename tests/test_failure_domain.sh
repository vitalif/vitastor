#!/bin/bash -ex

. `dirname $0`/common.sh

node mon/mon-main.js $MON_PARAMS --etcd_address $ETCD_URL --etcd_prefix "/vitastor" >>./testdata/mon.log 2>&1 &
MON_PID=$!
wait_etcd

TIME=$(date '+%s')
$ETCDCTL put /vitastor/config/global '{"placement_levels":{"rack":1,"host":2,"osd":3},"immediate_commit":"none"}'
$ETCDCTL put /vitastor/config/node_placement '{"rack1":{"level":"rack"},"rack2":{"level":"rack"},"host1":{"level":"host","parent":"rack1"},"host2":{"level":"host","parent":"rack1"},"host3":{"level":"host","parent":"rack2"},"host4":{"level":"host","parent":"rack2"}}'
$ETCDCTL put /vitastor/osd/stats/1 '{"host":"host1","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/2 '{"host":"host1","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/3 '{"host":"host2","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/4 '{"host":"host2","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/5 '{"host":"host3","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/6 '{"host":"host3","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/7 '{"host":"host4","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/8 '{"host":"host4","size":1073741824,"time":"'$TIME'"}'
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL create-pool testpool --ec 3+2 -n 32 --failure_domain rack --force
$ETCDCTL get --print-value-only /vitastor/config/pools | jq -s -e '. == [{"1": {"failure_domain": "rack", "name": "testpool", "parity_chunks": 2, "pg_count": 32, "pg_minsize": 4, "pg_size": 5, "scheme": "ec"}}]'
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL modify-pool testpool --ec 3+3 --failure_domain host
$ETCDCTL get --print-value-only /vitastor/config/pools | jq -s -e '. == [{"1": {"failure_domain": "host", "name": "testpool", "parity_chunks": 3, "pg_count": 32, "pg_minsize": 4, "pg_size": 6, "scheme": "ec"}}]'
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL rm-pool testpool
$ETCDCTL get --print-value-only /vitastor/config/pools | jq -s -e '. == [{}]'
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL create-pool testpool -s 2 -n 4 --failure_domain rack --force
$ETCDCTL get --print-value-only /vitastor/config/pools | jq -s -e '. == [{"1":{"name":"testpool","scheme":"replicated","pg_size":2,"pg_minsize":1,"pg_count":4,"failure_domain":"rack"}}]'

sleep 2

$ETCDCTL get --prefix /vitastor/pg/config --print-value-only | \
    jq -s -e '([ .[0].items["1"] | .[].osd_set | map_values(. | tonumber) | select((.[0] <= 4) != (.[1] <= 4)) ] | length) == 4'

# test pool with size 1
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL create-pool size1pool -s 1 -n 1 --force
wait_condition 10 "$ETCDCTL get --prefix /vitastor/pg/config --print-value-only | jq -s -e '.[0].items["'"'"2"'"'"]'"

format_green OK
