// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_impl.h"

blockstore_t::blockstore_t(blockstore_config_t & config, ring_loop_t *ringloop, timerfd_manager_t *tfd)
{
    impl = new blockstore_impl_t(config, ringloop, tfd);
}

blockstore_t::~blockstore_t()
{
    delete impl;
}

void blockstore_t::loop()
{
    impl->loop();
}

bool blockstore_t::is_started()
{
    return impl->is_started();
}

bool blockstore_t::is_stalled()
{
    return impl->is_stalled();
}

bool blockstore_t::is_safe_to_stop()
{
    return impl->is_safe_to_stop();
}

void blockstore_t::enqueue_op(blockstore_op_t *op)
{
    impl->enqueue_op(op);
}

int blockstore_t::read_bitmap(object_id oid, uint64_t target_version, void *bitmap, uint64_t *result_version)
{
    return impl->read_bitmap(oid, target_version, bitmap, result_version);
}

std::unordered_map<object_id, uint64_t> & blockstore_t::get_unstable_writes()
{
    return impl->unstable_writes;
}

std::map<uint64_t, uint64_t> & blockstore_t::get_inode_space_stats()
{
    return impl->inode_space_stats;
}

uint32_t blockstore_t::get_block_size()
{
    return impl->get_block_size();
}

uint64_t blockstore_t::get_block_count()
{
    return impl->get_block_count();
}

uint64_t blockstore_t::get_free_block_count()
{
    return impl->get_free_block_count();
}

uint32_t blockstore_t::get_bitmap_granularity()
{
    return impl->get_bitmap_granularity();
}
