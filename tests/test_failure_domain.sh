#!/bin/bash -ex

. `dirname $0`/common.sh

TIME=$(date '+%s')
$ETCDCTL put /vitastor/config/global '{"placement_levels":{"rack":1,"host":2,"osd":3}}'
$ETCDCTL put /vitastor/config/node_placement '{"rack1":{"level":"rack"},"rack2":{"level":"rack"},"host1":{"level":"host","parent":"rack1"},"host2":{"level":"host","parent":"rack1"},"host3":{"level":"host","parent":"rack2"},"host4":{"level":"host","parent":"rack2"}}'
$ETCDCTL put /vitastor/osd/stats/1 '{"host":"host1","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/2 '{"host":"host1","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/3 '{"host":"host2","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/4 '{"host":"host2","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/5 '{"host":"host3","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/6 '{"host":"host3","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/7 '{"host":"host4","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/8 '{"host":"host4","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"replicated","pg_size":2,"pg_minsize":1,"pg_count":4,"failure_domain":"rack"}}'

cd mon
npm install
cd ..
node mon/mon-main.js --etcd_url http://$ETCD_URL --etcd_prefix "/vitastor" &>./testdata/mon.log &
MON_PID=$!

sleep 2

etcdctl --endpoints=http://localhost:12379 get --prefix /vitastor/config/pgs --print-value-only | \
    jq -s -e '([ .[0].items["1"] | .[].osd_set | map_values(. | tonumber) | select((.[0] <= 4) != (.[1] <= 4)) ] | length) == 4'

format_green OK
