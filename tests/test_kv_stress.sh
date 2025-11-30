#!/bin/bash -ex

PG_COUNT=16
. `dirname $0`/run_3osds.sh

build/src/kv/vitastor-kv-stress --etcd_address $ETCD_URL --pool_id 1 --inode_id 1 --runtime 30

format_green OK
