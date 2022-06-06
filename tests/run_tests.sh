#!/bin/bash -ex
# Run all possible tests

cd $(dirname $0)

./test_add_osd.sh

./test_cas.sh

./test_change_pg_count.sh
SCHEME=ec ./test_change_pg_count.sh

./test_change_pg_size.sh

./test_create_nomaxid.sh

./test_etcd_fail.sh

./test_failure_domain.sh

./test_interrupted_rebalance.sh
IMMEDIATE_COMMIT=1 ./test_interrupted_rebalance.sh
SCHEME=ec ./test_interrupted_rebalance.sh
SCHEME=ec IMMEDIATE_COMMIT=1 ./test_interrupted_rebalance.sh

./test_minsize_1.sh

./test_move_reappear.sh

./test_rebalance_verify.sh
IMMEDIATE_COMMIT=1 ./test_rebalance_verify.sh
SCHEME=ec ./test_rebalance_verify.sh
SCHEME=ec IMMEDIATE_COMMIT=1 ./test_rebalance_verify.sh

./test_rm.sh

./test_snapshot.sh
SCHEME=ec ./test_snapshot.sh

./test_splitbrain.sh

./test_write.sh
SCHEME=xor ./test_write.sh

./test_write_no_same.sh
