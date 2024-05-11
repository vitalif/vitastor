#!/bin/bash -ex

. `dirname $0`/common.sh

node mon/mon-main.js $MON_PARAMS --etcd_address $ETCD_URL --etcd_prefix "/vitastor" >>./testdata/mon.log 2>&1 &
MON_PID=$!
wait_etcd

TIME=$(date '+%s')
$ETCDCTL put /vitastor/config/global '{"placement_levels":{"rack":100,"host":101,"osd":102},"immediate_commit":"none"}'
$ETCDCTL put /vitastor/config/node_placement '{"rack1":{"level":"rack"},"rack2":{"level":"rack"},"stor1":{"level":"host","parent":"rack1"},"stor2":{"level":"host","parent":"rack1"},"stor3":{"level":"host","parent":"rack2"},"stor4":{"level":"host","parent":"rack2"}}'
$ETCDCTL put /vitastor/osd/stats/1 '{"host":"stor1","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/2 '{"host":"stor1","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/3 '{"host":"stor2","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/4 '{"host":"stor2","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/5 '{"host":"stor3","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/6 '{"host":"stor3","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/7 '{"host":"stor4","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/8 '{"host":"stor4","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"replicated","pg_size":2,"pg_minsize":2,"pg_count":16,"failure_domain":"host","root_node":"rack1"}}'

sleep 2

$ETCDCTL get --prefix /vitastor/config/pgs --print-value-only

if ! ($ETCDCTL get --prefix /vitastor/config/pgs --print-value-only | \
    jq -s -e '[ [ .[0].items["1"] | .[].osd_set | map(. | select(. != "" and (.|tonumber) < 5)) ][] | select((. | length) == 2) ] | length == 16'); then
    format_error "Some PGs missing replicas"
fi

format_green OK
