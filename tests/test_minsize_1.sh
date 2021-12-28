#!/bin/bash -ex

PG_MINSIZE=1
SCHEME=replicated

. `dirname $0`/run_3osds.sh

kill -INT $OSD1_PID
kill -INT $OSD2_PID

sleep 5

if ! ($ETCDCTL get /vitastor/pg/state/1/ --prefix --print-value-only | jq -s -e '[ .[] | select(.state == ["active", "degraded"]) ] | length == '$PG_COUNT); then
    format_error "FAILED: $PG_COUNT PG(s) NOT ACTIVE+DEGRADED"
fi

format_green OK
