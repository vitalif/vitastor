#!/bin/bash -ex

# Test that modify-pool --name refuses to take the name of another pool

. `dirname $0`/common.sh

wait_etcd

POOL='"scheme":"replicated","pg_size":2,"pg_minsize":1,"pg_count":1,"failure_domain":"osd"'
$ETCDCTL put /vitastor/config/pools '{"1":{"name":"pool1",'$POOL'},"2":{"name":"pool2",'$POOL'}}'

for p in pool2 2; do
    r=0
    $VITASTOR_CLI modify-pool $p --name pool1 || r=$?
    if [[ $r != 17 ]]; then
        format_error "modify-pool $p --name pool1 should fail with EEXIST (17), but exited with $r"
    fi
done

names=$($ETCDCTL get --print-value-only /vitastor/config/pools | jq -c '[ .[].name ]')
if [[ "$names" != '["pool1","pool2"]' ]]; then
    format_error "Pool names should stay pool1, pool2, but they are $names"
fi

# A free name and the pool's own name are still accepted
$VITASTOR_CLI modify-pool pool2 --name pool3
$VITASTOR_CLI modify-pool pool3 --name pool3 --pg_count 2

cfg=$($ETCDCTL get --print-value-only /vitastor/config/pools | jq -c '[ .[] | [ .name, .pg_count ] ]')
if [[ "$cfg" != '[["pool1",1],["pool3",2]]' ]]; then
    format_error "Pools should be pool1 (1 PG) and pool3 (2 PGs), but they are $cfg"
fi

# Test that create-pool and modify-pool refuse pg_size > 64, which OSDs and clients don't support
$ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"replicated","pg_size":2,"pg_minsize":1,"pg_count":1,"failure_domain":"osd"}}'

for args in "create-pool bigpool -s 65 -n 1 --force" "create-pool bigpool --ec 62+3 -n 1 --force" "modify-pool testpool -s 65"; do
    r=0
    $VITASTOR_CLI $args || r=$?
    if [[ $r != 22 ]]; then
        format_error "$args should fail with EINVAL (22), but exited with $r"
    fi
done

$ETCDCTL get --print-value-only /vitastor/config/pools | jq -e 'keys == ["1"] and .["1"].pg_size == 2' || \
    format_error "Pool configuration should not change after refused commands"

# 64 is still accepted
$VITASTOR_CLI modify-pool testpool -s 64
$ETCDCTL get --print-value-only /vitastor/config/pools | jq -e '.["1"].pg_size == 64' || \
    format_error "modify-pool -s 64 should set pg_size to 64"

format_green OK
