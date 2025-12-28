#!/bin/bash -ex

. `dirname $0`/run_3osds.sh

build/src/test/test_cas --pool_id 1 --inode_id 1 --config_path $VITASTOR_CFG

format_green OK
