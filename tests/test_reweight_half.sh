#!/bin/bash -ex

. `dirname $0`/common.sh

node mon/mon-main.js $MON_PARAMS --etcd_address $ETCD_URL --etcd_prefix "/vitastor" >>./testdata/mon.log 2>&1 &
MON_PID=$!
wait_etcd

TIME=$(date '+%s')
$ETCDCTL put /vitastor/osd/stats/1 '{"host":"host1","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/2 '{"host":"host1","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/3 '{"host":"host2","size":1073741824,"time":"'$TIME'"}'
$ETCDCTL put /vitastor/osd/stats/4 '{"host":"host2","size":1073741824,"time":"'$TIME'"}'
build/src/cmd/vitastor-cli --etcd_address $ETCD_URL create-pool testpool -s 2 -n 16 --force

sleep 2

# check that all OSDs have 8 PGs
$ETCDCTL get /vitastor/pg/config --print-value-only | \
    jq -s -e '([ .[0].items["1"] | .[].osd_set | map_values(. | tonumber) | select(.[0] == 1 or .[1] == 1) ] | length) == 8'
$ETCDCTL get /vitastor/pg/config --print-value-only | \
    jq -s -e '([ .[0].items["1"] | .[].osd_set | map_values(. | tonumber) | select(.[0] == 2 or .[1] == 2) ] | length) == 8'
$ETCDCTL get /vitastor/pg/config --print-value-only | \
    jq -s -e '([ .[0].items["1"] | .[].osd_set | map_values(. | tonumber) | select(.[0] == 3 or .[1] == 3) ] | length) == 8'
$ETCDCTL get /vitastor/pg/config --print-value-only | \
    jq -s -e '([ .[0].items["1"] | .[].osd_set | map_values(. | tonumber) | select(.[0] == 4 or .[1] == 4) ] | length) == 8'

build/src/cmd/vitastor-cli --etcd_address $ETCD_URL modify-osd --reweight 0.5 3

sleep 2

$ETCDCTL get /vitastor/pg/config --print-value-only | \
    jq -s -e '([ .[0].items["1"] | .[].osd_set | map_values(. | tonumber) | select(.[0] == 1 or .[1] == 1) ] | length) == 8'
$ETCDCTL get /vitastor/pg/config --print-value-only | \
    jq -s -e '([ .[0].items["1"] | .[].osd_set | map_values(. | tonumber) | select(.[0] == 2 or .[1] == 2) ] | length) == 8'
$ETCDCTL get /vitastor/pg/config --print-value-only | \
    jq -s -e '([ .[0].items["1"] | .[].osd_set | map_values(. | tonumber) | select(.[0] == 3 or .[1] == 3) ] | length) <= 6'
$ETCDCTL get /vitastor/pg/config --print-value-only | \
    jq -s -e '([ .[0].items["1"] | .[].osd_set | map_values(. | tonumber) | select(.[0] == 4 or .[1] == 4) ] | length) >= 10'

format_green OK
