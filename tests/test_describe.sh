#!/bin/bash -ex

# Test vitastor-cli describe and fix on degraded objects and with down OSDs

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

# describe and fix must not wait for an OSD which is down.
# Stop the monitor so that PGs keep the down OSD as their primary,
# and stop a primary OSD which has the latest data, so other PGs stay degraded.
VICTIM=$($VITASTOR_CLI ls-pgs --json | jq -r '[ .[].cur_primary | select(. == 1 or . == 2) ] | first')
VICTIM_PG=$($VITASTOR_CLI ls-pgs --json | jq -r '[ .[] | select(.cur_primary == '$VICTIM') | .pg_num ] | first')
OTHER_PG=$($VITASTOR_CLI ls-pgs --json | jq -r '[ .[] | select(.cur_primary != '$VICTIM') | .pg_num ] | first')
OTHER_PG_COUNT=$($VITASTOR_CLI ls-pgs --json | jq '[ .[] | select(.cur_primary != '$VICTIM') ] | length')
if [[ "$VICTIM" = null || "$OTHER_PG_COUNT" = 0 ]]; then
    format_error "Expected PGs with primary OSD 1 or 2 and PGs with other primary OSDs"
fi
VICTIM_OBJ=$($VITASTOR_CLI describe --pool 1 --pg $VICTIM_PG --json | jq -r '.[0] | .inode+":"+.stripe')
OTHER_OBJ=$($VITASTOR_CLI describe --pool 1 --pg $OTHER_PG --json | jq -r '.[0] | .inode+":"+.stripe')

kill -9 $MON_PID
eval kill -9 \$OSD${VICTIM}_PID
$ETCDCTL del /vitastor/osd/state/$VICTIM
wait_condition 60 "$ETCDCTL get --prefix /vitastor/pg/state/ --print-value-only | jq -s -e '[ .[] | select(.primary != $VICTIM and (.state | index(\"degraded\"))) ] | length == $OTHER_PG_COUNT'" "PG peering without OSD $VICTIM"
# Keep PG 1/$VICTIM_PG active with the down primary, as it is until the OSD's etcd lease expires
$ETCDCTL put /vitastor/pg/state/1/$VICTIM_PG '{"peers":[1,2,3],"primary":'$VICTIM',"state":["active"]}'

# A listing with skipped OSDs or PGs is incomplete: EIO, but still valid JSON on stdout
for args in "" "--osds 1,2,3" "--pool 1 --pg $VICTIM_PG"; do
    rc=0
    timeout 30 $VITASTOR_CLI describe $args --json > ./testdata/describe.json 2> ./testdata/describe.err || rc=$?
    [[ $rc == 5 ]] || format_error "describe $args exited with $rc instead of EIO with OSD $VICTIM down"
    grep -q "OSD $VICTIM is down, skipping \(it\|PG 1/$VICTIM_PG\)" ./testdata/describe.err || \
        format_error "describe $args should warn about OSD $VICTIM: $(cat ./testdata/describe.err)"
    grep -q "The listing is incomplete" ./testdata/describe.err || \
        format_error "describe $args should say that the listing is incomplete: $(cat ./testdata/describe.err)"
    count=$(jq length ./testdata/describe.json) || format_error "describe $args printed invalid JSON"
    if [[ "$args" == --pool* ]]; then
        [[ $count == 0 ]] || format_error "describe $args should list nothing"
    else
        [[ $count -gt 0 ]] || format_error "describe $args should still list objects from the other OSDs"
    fi
done

if timeout 30 $VITASTOR_CLI fix --bad-osds $VICTIM --objects $OTHER_OBJ 2> ./testdata/fix.err; then
    format_error "fix should refuse to work with bad OSD $VICTIM down"
fi
grep -q "Bad OSD(s) $VICTIM are down" ./testdata/fix.err || format_error "fix printed: $(cat ./testdata/fix.err)"

# An object which is skipped is not fixed: EIO
rc=0
timeout 30 $VITASTOR_CLI fix --bad-osds 3 --objects $VICTIM_OBJ 2> ./testdata/fix.err || rc=$?
[[ $rc == 5 ]] || format_error "fix exited with $rc instead of EIO on an object from PG 1/$VICTIM_PG with primary OSD $VICTIM down"
grep -q "is from PG 1/$VICTIM_PG whose primary OSD $VICTIM is down" ./testdata/fix.err || \
    format_error "fix should skip the object from PG 1/$VICTIM_PG: $(cat ./testdata/fix.err)"
grep -q "Some objects were not fixed" ./testdata/fix.err || \
    format_error "fix should say that it skipped objects: $(cat ./testdata/fix.err)"

format_green OK
