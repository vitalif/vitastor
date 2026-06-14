// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_mock.h"

blockstore_mock_t::blockstore_mock_t(const blockstore_config_t & config)
{
}

void blockstore_mock_t::parse_config(blockstore_config_t & config)
{
}

void* blockstore_mock_t::reshard_start(pool_id_t pool, uint32_t pg_count, uint32_t pg_stripe_size, uint64_t chunk_limit)
{
    return NULL;
}

bool blockstore_mock_t::reshard_continue(void *reshard_state, uint64_t chunk_limit)
{
    return true;
}

void blockstore_mock_t::loop()
{
}

bool blockstore_mock_t::is_started()
{
    return true;
}

bool blockstore_mock_t::is_stalled()
{
    return false;
}

bool blockstore_mock_t::is_safe_to_stop()
{
    return true;
}

void blockstore_mock_t::enqueue_op(blockstore_op_t *op)
{
}

int blockstore_mock_t::read_bitmap(object_id oid, uint64_t target_version, void *bitmap, uint64_t *result_version)
{
    return -EIO;
}

const std::map<uint64_t, uint64_t> & blockstore_mock_t::get_inode_space_stats()
{
    return inode_space;
}

void blockstore_mock_t::set_no_inode_stats(const std::vector<uint64_t> & pool_ids)
{
}

void blockstore_mock_t::dump_diagnostics()
{
}

std::string blockstore_mock_t::get_op_diag(blockstore_op_t *op)
{
    return "";
}

uint32_t blockstore_mock_t::get_block_size()
{
    return block_size;
}

uint64_t blockstore_mock_t::get_block_count()
{
    return block_count;
}

uint64_t blockstore_mock_t::get_free_block_count()
{
    return block_count;
}

uint64_t blockstore_mock_t::get_journal_size()
{
    return 32*1024*1024;
}

uint32_t blockstore_mock_t::get_bitmap_granularity()
{
    return bitmap_granularity;
}

uint64_t blockstore_mock_t::get_live_entries()
{
    return 0;
}

uint64_t blockstore_mock_t::get_live_memory()
{
    return 0;
}

uint64_t blockstore_mock_t::get_garbage_entries()
{
    return 0;
}

uint64_t blockstore_mock_t::get_garbage_memory()
{
    return 0;
}
