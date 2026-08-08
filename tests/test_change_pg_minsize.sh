#!/bin/bash -ex

OSD_TLS=0
PG_COUNT=8
OSD_COUNT=3
PG_SIZE=3
PG_MINSIZE=3
SCHEME=replicated

. `dirname $0`/run_3osds.sh

kill $OSD1_PID

wait_condition 10 "$ETCDCTL get /vitastor/pg/state/1/ --prefix --print-value-only |\
    jq -s -e '[ .[] | select(.state | contains(["'"'"incomplete"'"'"])) ] | length == '$PG_COUNT" "wait for incomplete"

PG_MINSIZE=2
$ETCDCTL put /vitastor/config/pools '{"1":{'$POOLCFG',"pg_size":'$PG_SIZE',"pg_minsize":'$PG_MINSIZE',"pg_count":'$PG_COUNT'}}'

wait_condition 10 "$ETCDCTL get /vitastor/pg/state/1/ --prefix --print-value-only |\
    jq -s -e '[ .[] | select(.state | contains(["'"'"active"'"'"])) ] | length == '$PG_COUNT" "wait for active"

format_green OK
