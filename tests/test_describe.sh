#!/bin/bash -ex

# Test vitastor-cli describe on degraded objects

PG_COUNT=4
PG_SIZE=3
PG_MINSIZE=2
# Keep degraded objects until the checks are done
GLOBAL_CONFIG=',"no_recovery":true'
. `dirname $0`/run_3osds.sh

$VITASTOR_CLI create -s 32M testimg

# Data written while OSD 3 is down is degraded when it comes back
kill -9 $OSD3_PID
$ETCDCTL del /vitastor/osd/state/3
wait_condition 60 "$ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '[ .[] | select(.state | index(\"degraded\")) ] | length == $PG_COUNT'" "PG peering without OSD 3"
$VITASTOR_CLI dd if=/dev/urandom oimg=testimg bs=1M count=32
start_osd 3
wait_condition 60 "$ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '[ .[] | select((.state | index(\"active\")) and (.state | index(\"has_degraded\"))) ] | length == $PG_COUNT'" "PG peering with OSD 3"

INODE_ID=$($VITASTOR_CLI ls --json | jq -r '.[] | select(.name == "testimg") | .inode_id')
INODE=$(printf '0x%x' $INODE_ID)
INODE_NUM=$((INODE_ID & 0xffffffffffff))

# --inode lists the whole inode, not only its first object
all=$($VITASTOR_CLI describe --json | jq -c '[ .[] | select(.inode == "'$INODE'") | .stripe ] | unique')
if [[ $(echo "$all" | jq length) -le 1 ]]; then
    format_error "describe should list more than one degraded object of testimg: $all"
fi
one=$($VITASTOR_CLI describe --pool 1 --inode $INODE_NUM --json | jq -c '[ .[].stripe ] | unique')
if [[ "$one" != "$all" ]]; then
    format_error "describe --inode lists objects $one instead of $all"
fi

# --min-offset and --max-offset narrow --inode, and --max-inode includes its whole inode
second=$(echo "$all" | jq -r '.[1]')
part=$($VITASTOR_CLI describe --pool 1 --inode $INODE_NUM --min-offset $second --max-offset $second --json | jq -c '[ .[].stripe ] | unique')
if [[ "$part" != "[\"$second\"]" ]]; then
    format_error "describe --inode --min-offset $second --max-offset $second lists objects $part"
fi
range=$($VITASTOR_CLI describe --min-inode $INODE --max-inode $INODE --json | jq -c '[ .[].stripe ] | unique')
if [[ "$range" != "$all" ]]; then
    format_error "describe --min-inode $INODE --max-inode $INODE lists objects $range instead of $all"
fi

format_green OK
