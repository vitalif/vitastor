#!/bin/bash -ex
# Run all possible tests

cd $(dirname $0)

./test_add_osd.sh

./test_cas.sh

./test_change_pg_count.sh
EC=1 ./test_change_pg_count.sh

./test_change_pg_size.sh

./test_etcd_fail.sh

./test_failure_domain.sh

./test_interrupted_rebalance.sh
IMMEDIATE_COMMIT=1 ./test_interrupted_rebalance.sh
EC=1 ./test_interrupted_rebalance.sh
EC=1 IMMEDIATE_COMMIT=1 ./test_interrupted_rebalance.sh

./test_minsize_1.sh

./test_move_reappear.sh

./test_rebalance_verify.sh
IMMEDIATE_COMMIT=1 ./test_rebalance_verify.sh
EC=1 ./test_rebalance_verify.sh
EC=1 IMMEDIATE_COMMIT=1 ./test_rebalance_verify.sh

./test_rm.sh

./test_snapshot.sh
SCHEME=replicated ./test_snapshot.sh

./test_splitbrain.sh

./test_write.sh
SCHEME=replicated ./test_write.sh

./test_write_no_same.sh
