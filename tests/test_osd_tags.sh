#!/bin/bash -ex

. `dirname $0`/common.sh

TIME=$(date '+%s')
$ETCDCTL put /vitastor/config/osd/1 '{"tags":["a"]}'
$ETCDCTL put /vitastor/config/osd/2 '{"tags":["a"]}'
$ETCDCTL put /vitastor/config/osd/3 '{"tags":["a"]}'
$ETCDCTL put /vitastor/config/osd/4 '{"tags":["a"]}'
$ETCDCTL put /vitastor/config/osd/5 '{"tags":["b"]}'
$ETCDCTL put /vitastor/config/osd/6 '{"tags":["b"]}'
$ETCDCTL put /vitastor/config/osd/7 '{"tags":["b"]}'
$ETCDCTL put /vitastor/config/osd/8 '{"tags":["b"]}'
$ETCDCTL put /vitastor/osd/stats/1 '{"host":"stor1","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/2 '{"host":"stor1","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/3 '{"host":"stor2","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/4 '{"host":"stor2","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/5 '{"host":"stor3","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/6 '{"host":"stor3","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/7 '{"host":"stor4","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/8 '{"host":"stor4","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"replicated","pg_size":2,"pg_minsize":2,"pg_count":16,"failure_domain":"host","osd_tags":["a"]}}'

node mon/mon-main.js --etcd_address $ETCD_URL --etcd_prefix "/vitastor" >>./testdata/mon.log 2>&1 &
MON_PID=$!

sleep 2

etcdctl --endpoints=http://localhost:12379 get --prefix /vitastor/config/pgs --print-value-only

if ! (etcdctl --endpoints=http://localhost:12379 get --prefix /vitastor/config/pgs --print-value-only | \
    jq -s -e '[ [ .[0].items["1"] | .[].osd_set | map(. | select(. != "" and (.|tonumber) < 5)) ][] | select((. | length) == 2) ] | length == 16'); then
    format_error "Some PGs missing replicas"
fi

format_green OK
