#!/bin/bash -ex

# Test vitastor-cli create when /index/maxid is out of sync

. `dirname $0`/run_3osds.sh

$ETCDCTL put /vitastor/config/inode/1/120 '{"name":"testimg","size":'$((1024*1024*1024))'}'

build/src/vitastor-cli create --etcd_address $ETCD_URL -s 1G testimg2

t=$($ETCDCTL get --print-value-only /vitastor/config/inode/1/121 | jq -r .name)
if [[ "$t" != "testimg2" ]]; then
    format_error "testimg2 should've been created as inode 121"
fi

t=$($ETCDCTL get --print-value-only /vitastor/index/maxid/1)
if [[ "$t" != 121 ]]; then
    format_error "/index/maxid should've been set to 121"
fi

format_green OK
