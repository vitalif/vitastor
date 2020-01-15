#include "blockstore_impl.h"

blockstore_t::blockstore_t(blockstore_config_t & config, ring_loop_t *ringloop)
{
    impl = new blockstore_impl_t(config, ringloop);
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
    impl->enqueue_op(op, false);
}

void blockstore_t::enqueue_op_first(blockstore_op_t *op)
{
    impl->enqueue_op(op, true);
}

std::map<object_id, uint64_t> & blockstore_t::get_unstable_writes()
{
    return impl->unstable_writes;
}

uint32_t blockstore_t::get_block_size()
{
    return impl->get_block_size();
}

uint64_t blockstore_t::get_block_count()
{
    return impl->get_block_count();
}
