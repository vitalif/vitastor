#!/bin/bash -ex

# Test that create-pool and modify-pool refuse pg_size > 64, which OSDs and clients don't support

. `dirname $0`/common.sh

wait_etcd

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
