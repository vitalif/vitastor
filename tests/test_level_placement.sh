#!/bin/bash -ex

. `dirname $0`/common.sh

node mon/mon-main.js $MON_PARAMS --etcd_address $ETCD_URL --etcd_prefix "/vitastor" >>./testdata/mon.log 2>&1 &
MON_PID=$!
wait_etcd

TIME=$(date '+%s')
$ETCDCTL put /vitastor/config/global '{"placement_levels":{"rack":1,"host":2,"osd":3},"immediate_commit":"none"}'
$ETCDCTL put /vitastor/config/node_placement '{"rack1":{"level":"rack"},"rack2":{"level":"rack"},"rack3":{"level":"rack"},"rack4":{"level":"rack"},
    "host1":{"level":"host","parent":"rack1"},"host2":{"level":"host","parent":"rack1"},
    "host3":{"level":"host","parent":"rack2"},"host4":{"level":"host","parent":"rack2"},
    "host5":{"level":"host","parent":"rack3"},"host6":{"level":"host","parent":"rack3"},
    "host7":{"level":"host","parent":"rack4"},"host8":{"level":"host","parent":"rack4"}}'
$ETCDCTL put /vitastor/osd/stats/1 '{"host":"host1","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/2 '{"host":"host1","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/3 '{"host":"host2","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/4 '{"host":"host2","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/5 '{"host":"host3","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/6 '{"host":"host3","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/7 '{"host":"host4","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/8 '{"host":"host4","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/9 '{"host":"host5","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/10 '{"host":"host5","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/11 '{"host":"host6","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/12 '{"host":"host6","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/13 '{"host":"host7","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/14 '{"host":"host7","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/15 '{"host":"host8","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/16 '{"host":"host8","size":1073741824,"time":"'$TIME'"}'
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL create-pool testpool --failure_domain host --level_placement rack=112233 --ec 4+2 -n 32
sleep 2
$ETCDCTL get --prefix /vitastor/pg/config --print-value-only | \
    jq -s -e '([ .[0].items["1"] | .[].osd_set | map_values(. | tonumber) | select(
        (. | map_values((. - 1) / 2 | floor) | unique | length) == 6 and
        (. | map_values((. - 1) / 4 | floor) | unique | length) == 3) ] | length) == 32'

format_green OK
