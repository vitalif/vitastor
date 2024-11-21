#!/bin/bash -ex

. `dirname $0`/common.sh

node mon/mon-main.js $MON_PARAMS --etcd_address $ETCD_URL --etcd_prefix "/vitastor" >>./testdata/mon.log 2>&1 &
MON_PID=$!
wait_etcd

TIME=$(date '+%s')
$ETCDCTL put /vitastor/config/global '{"placement_levels":{"dc":10,"host":100,"half":105,"osd":110}}'
$ETCDCTL put /vitastor/config/node_placement '{
  "h11":{"level":"half","parent":"host1"},
  "h12":{"level":"half","parent":"host1"},
  "h21":{"level":"half","parent":"host2"},
  "h22":{"level":"half","parent":"host2"},
  "h31":{"level":"half","parent":"host3"},
  "h32":{"level":"half","parent":"host3"},
  "1":{"parent":"h11"},
  "2":{"parent":"h12"},
  "3":{"parent":"h21"},
  "4":{"parent":"h22"},
  "5":{"parent":"h31"},
  "6":{"parent":"h32"}
}'
$ETCDCTL put /vitastor/osd/stats/1 '{"host":"host1","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/2 '{"host":"host1","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/3 '{"host":"host2","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/4 '{"host":"host2","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/5 '{"host":"host3","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/6 '{"host":"host3","size":1073741824,"time":"'$TIME'"}'
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL osd-tree
# check that it doesn't fail
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL create-pool testpool --ec 2+1 -n 32

format_green OK
