#!/bin/bash -ex

# Test that modify refuses an invalid --resize value instead of ignoring it

. `dirname $0`/common.sh

wait_etcd

$ETCDCTL put /vitastor/config/pools '{"1":{"name":"testpool","scheme":"replicated","pg_size":2,"pg_minsize":1,"pg_count":1,"failure_domain":"osd"}}'

$VITASTOR_CLI create -s 1G --pool testpool testimg

for size in 1.5G 10X abc ''; do
    r=0
    $VITASTOR_CLI modify testimg --resize "$size" || r=$?
    if [[ $r != 22 ]]; then
        format_error "modify --resize '$size' should fail with EINVAL (22), but exited with $r"
    fi
done

$ETCDCTL get --print-value-only /vitastor/config/inode/1/1 | jq -e '.size == 1073741824' || \
    format_error "Image size should stay 1G after refused resizes"

$VITASTOR_CLI modify testimg --resize 2G
$ETCDCTL get --print-value-only /vitastor/config/inode/1/1 | jq -e '.size == 2147483648' || \
    format_error "Image size should be 2G after --resize 2G"

format_green OK
