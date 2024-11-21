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
ANTIETCD=1 ./test_etcd_fail.sh

./test_interrupted_rebalance.sh
IMMEDIATE_COMMIT=1 ./test_interrupted_rebalance.sh

SCHEME=ec ./test_interrupted_rebalance.sh
SCHEME=ec IMMEDIATE_COMMIT=1 ./test_interrupted_rebalance.sh

./test_create_halfhost.sh

./test_failure_domain.sh

./test_snapshot.sh
SCHEME=ec ./test_snapshot.sh

./test_minsize_1.sh

./test_move_reappear.sh

./test_rm.sh

./test_snapshot_chain.sh
SCHEME=ec ./test_snapshot_chain.sh

./test_snapshot_down.sh
SCHEME=ec ./test_snapshot_down.sh

./test_splitbrain.sh

./test_rebalance_verify.sh
IMMEDIATE_COMMIT=1 ./test_rebalance_verify.sh
SCHEME=ec ./test_rebalance_verify.sh
SCHEME=ec IMMEDIATE_COMMIT=1 ./test_rebalance_verify.sh

./test_dd.sh

./test_root_node.sh

./test_switch_primary.sh

./test_write.sh
SCHEME=xor ./test_write.sh

./test_write_no_same.sh

PG_SIZE=2 ./test_heal.sh
SCHEME=ec ./test_heal.sh
ANTIETCD=1 ./test_heal.sh

TEST_NAME=csum_32k_dmj OSD_ARGS="--data_csum_type crc32c --csum_block_size 32k --inmemory_metadata false --inmemory_journal false" OFFSET_ARGS=$OSD_ARGS ./test_heal.sh
TEST_NAME=csum_32k_dj  OSD_ARGS="--data_csum_type crc32c --csum_block_size 32k --inmemory_journal false" OFFSET_ARGS=$OSD_ARGS ./test_heal.sh
TEST_NAME=csum_32k     OSD_ARGS="--data_csum_type crc32c --csum_block_size 32k" OFFSET_ARGS=$OSD_ARGS ./test_heal.sh
TEST_NAME=csum_4k_dmj  OSD_ARGS="--data_csum_type crc32c --inmemory_metadata false --inmemory_journal false" OFFSET_ARGS=$OSD_ARGS ./test_heal.sh
TEST_NAME=csum_4k_dj   OSD_ARGS="--data_csum_type crc32c --inmemory_journal false" OFFSET_ARGS=$OSD_ARGS ./test_heal.sh
TEST_NAME=csum_4k      OSD_ARGS="--data_csum_type crc32c" OFFSET_ARGS=$OSD_ARGS ./test_heal.sh

./test_resize.sh
./test_resize_auto.sh

./test_snapshot_pool2.sh

./test_osd_tags.sh

./test_enospc.sh
SCHEME=xor ./test_enospc.sh
IMMEDIATE_COMMIT=1 ./test_enospc.sh
IMMEDIATE_COMMIT=1 SCHEME=xor ./test_enospc.sh

./test_scrub.sh
ZERO_OSD=2 ./test_scrub.sh
SCHEME=xor ./test_scrub.sh
PG_SIZE=3 ./test_scrub.sh
PG_SIZE=6 PG_MINSIZE=4 OSD_COUNT=6 SCHEME=ec ./test_scrub.sh
SCHEME=ec ./test_scrub.sh

./test_nfs.sh
