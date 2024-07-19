#!/bin/bash -ex

. `dirname $0`/run_3osds.sh

primary=$($ETCDCTL get --print-value-only /vitastor/pg/config | jq -r '.items["1"]["1"].primary')
primary_pid=OSD${primary}_PID
kill -9 ${!primary_pid}

sleep 15
wait_condition 10 "$ETCDCTL get --print-value-only /vitastor/pg/config | jq -s -e '.[0].items[\"1\"][\"1\"].primary != \"$primary\"'"

newprim=$($ETCDCTL get --print-value-only /vitastor/pg/config | jq -r '.items["1"]["1"].primary')

if [ "$newprim" = "$primary" ]; then
    format_error Primary not switched
fi

format_green OK
