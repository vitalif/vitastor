// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include "blockstore.h"

class blockstore_mock_t: public blockstore_i
{
public:
    std::map<uint64_t, uint64_t> inode_space;

    blockstore_mock_t(const blockstore_config_t & config);
    void parse_config(blockstore_config_t & config) override;
    void* reshard_start(pool_id_t pool, uint32_t pg_count, uint32_t pg_stripe_size, uint64_t chunk_limit) override;
    bool reshard_continue(void *reshard_state, uint64_t chunk_limit) override;
    void loop() override;
    bool is_started() override;
    bool is_stalled() override;
    bool is_safe_to_stop() override;
    void enqueue_op(blockstore_op_t *op) override;
    int read_bitmap(object_id oid, uint64_t target_version, void *bitmap, uint64_t *result_version = NULL) override;
    const std::map<uint64_t, uint64_t> & get_inode_space_stats() override;
    void set_no_inode_stats(const std::vector<uint64_t> & pool_ids) override;
    void dump_diagnostics() override;
    std::string get_op_diag(blockstore_op_t *op) override;
    uint32_t get_block_size() override;
    uint64_t get_block_count() override;
    uint64_t get_free_block_count() override;
    uint64_t get_journal_size() override;
    uint32_t get_bitmap_granularity() override;
    uint64_t get_live_entries() override;
    uint64_t get_live_memory() override;
    uint64_t get_garbage_entries() override;
    uint64_t get_garbage_memory() override;
};
