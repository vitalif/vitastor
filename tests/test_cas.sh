#!/bin/bash -ex

. `dirname $0`/run_3osds.sh

build/src/test/test_cas --pool_id 1 --inode_id 1 --etcd_address $ETCD_URL

format_green OK
