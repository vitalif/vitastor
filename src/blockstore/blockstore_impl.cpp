// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <stdexcept>

#include "blockstore_impl.h"
#include "blockstore_internal.h"
#include "crc32c.h"

blockstore_impl_t::blockstore_impl_t(blockstore_config_t & config, ring_loop_i *ringloop, timerfd_manager_t *tfd, bool mock_mode)
{
    assert(sizeof(blockstore_op_private_t) <= BS_OP_PRIVATE_DATA_SIZE);
    this->tfd = tfd;
    this->ringloop = ringloop;
    dsk.mock_mode = mock_mode;
    ring_consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(&ring_consumer);
    initialized = 0;
    parse_config(config, true);
    try
    {
        dsk.open_data();
        dsk.open_meta();
        dsk.open_journal();
        dsk.calc_lengths();
        zero_object = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, dsk.data_block_size);
    }
    catch (std::exception & e)
    {
        dsk.close_all();
        throw;
    }
    memset(zero_object, 0, dsk.data_block_size);
    meta_superblock = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, dsk.meta_block_size);
    memset(meta_superblock, 0, dsk.meta_block_size);
}

void blockstore_impl_t::init()
{
    flusher = new journal_flusher_t(this);
    if (dsk.inmemory_journal)
    {
        buffer_area = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, dsk.journal_len);
    }
    heap = new blockstore_heap_t(&dsk, buffer_area, log_level);
}

blockstore_impl_t::~blockstore_impl_t()
{
    if (flusher)
        delete flusher;
    if (heap)
        delete heap;
    if (buffer_area)
        free(buffer_area);
    if (meta_superblock)
        free(meta_superblock);
    if (zero_object)
        free(zero_object);
    ringloop->unregister_consumer(&ring_consumer);
    dsk.close_all();
}

bool blockstore_impl_t::is_started()
{
    return initialized == 10;
}

bool blockstore_impl_t::is_stalled()
{
    return queue_stall;
}

// main event loop - produce requests
void blockstore_impl_t::loop()
{
    if (initialized != 10)
    {
        // read metadata
        if (initialized == 0)
        {
            metadata_init_reader = new blockstore_init_meta(this);
            initialized = 1;
        }
        if (initialized == 1)
        {
            int res = metadata_init_reader->loop();
            if (!res)
            {
                delete metadata_init_reader;
                metadata_init_reader = NULL;
                initialized = 3;
            }
        }
        if (initialized == 3)
        {
            if (!readonly && dsk.discard_on_start)
            {
                dsk.trim_data([this](uint64_t block_num){ return heap->is_data_used(block_num * dsk.data_block_size); });
            }
            initialized = 10;
        }
    }
    else
    {
        // try to submit ops
        unsigned initial_ring_space = ringloop->space_left();
        int op_idx = 0, new_idx = 0;
        bool has_unfinished_writes = false;
        for (; op_idx < submit_queue.size(); op_idx++, new_idx++)
        {
            auto op = submit_queue[op_idx];
            submit_queue[new_idx] = op;
            if (PRIV(op)->wait_for)
            {
                check_wait(op);
                if (PRIV(op)->wait_for == WAIT_SQE)
                {
                    // ring is full, stop submission
                    break;
                }
                else if (PRIV(op)->wait_for)
                {
                    has_unfinished_writes = has_unfinished_writes || op->opcode == BS_OP_WRITE ||
                        op->opcode == BS_OP_WRITE_STABLE || op->opcode == BS_OP_DELETE ||
                        op->opcode == BS_OP_STABLE || op->opcode == BS_OP_ROLLBACK;
                    continue;
                }
            }
            unsigned prev_sqe_pos = ringloop->save();
            // 0 = can't submit
            // 1 = in progress
            // 2 = can be removed from queue
            int wr_st = 0;
            if (op->opcode == BS_OP_READ)
            {
                wr_st = dequeue_read(op);
            }
            else if (op->opcode == BS_OP_WRITE || op->opcode == BS_OP_WRITE_STABLE || op->opcode == BS_OP_DELETE)
            {
                wr_st = dequeue_write(op);
                has_unfinished_writes = has_unfinished_writes || (wr_st != 2);
            }
            else if (op->opcode == BS_OP_SYNC)
            {
                // syncs only completed writes, so doesn't have to be blocked by anything
                wr_st = continue_sync(op);
            }
            else if (op->opcode == BS_OP_STABLE || op->opcode == BS_OP_ROLLBACK)
            {
                wr_st = dequeue_stable(op);
                has_unfinished_writes = has_unfinished_writes || (wr_st != 2);
            }
            else if (op->opcode == BS_OP_LIST)
            {
                // LIST has to be blocked by previous writes and commits/rollbacks
                if (!has_unfinished_writes)
                {
                    process_list(op);
                    wr_st = 2;
                }
                else
                {
                    wr_st = 0;
                }
            }
            if (wr_st == 2)
            {
                submit_queue[op_idx] = NULL;
                new_idx--;
            }
            if (wr_st == 0)
            {
                PRIV(op)->pending_ops = 0;
                ringloop->restore(prev_sqe_pos);
                if (PRIV(op)->wait_for == WAIT_SQE)
                {
                    // ring is full, stop submission
                    break;
                }
            }
        }
        if (op_idx != new_idx)
        {
            while (op_idx < submit_queue.size())
            {
                submit_queue[new_idx++] = submit_queue[op_idx++];
            }
            submit_queue.resize(new_idx);
        }
        if (!readonly)
        {
            flusher->loop();
        }
        int ret = ringloop->submit();
        if (ret < 0)
        {
            throw std::runtime_error(std::string("io_uring_submit: ") + strerror(-ret));
        }
        for (auto & block_num: pending_modified_blocks)
        {
            heap->start_block_write(block_num);
            modified_blocks.insert(block_num);
        }
        pending_modified_blocks.clear();
        if ((initial_ring_space - ringloop->space_left()) > 0)
        {
            live = true;
        }
        queue_stall = !live && !ringloop->has_work();
        live = false;
    }
}

bool blockstore_impl_t::is_safe_to_stop()
{
    // It's safe to stop blockstore when there are no in-flight operations,
    // no in-progress syncs and flusher isn't doing anything
    if (submit_queue.size() > 0 || !readonly && flusher->is_active())
    {
        return false;
    }
    if (unsynced_big_write_count > 0 || unsynced_small_write_count > 0)
    {
        if (!readonly && !stop_sync_submitted)
        {
            // We should sync the blockstore before unmounting
            blockstore_op_t *op = new blockstore_op_t;
            op->opcode = BS_OP_SYNC;
            op->buf = NULL;
            op->callback = [](blockstore_op_t *op)
            {
                delete op;
            };
            enqueue_op(op);
            stop_sync_submitted = true;
        }
        return false;
    }
    return true;
}

void blockstore_impl_t::check_wait(blockstore_op_t *op)
{
    if (PRIV(op)->wait_for == WAIT_SQE)
    {
        if (ringloop->space_left() < PRIV(op)->wait_detail)
        {
            // stop submission if there's still no free space
#ifdef BLOCKSTORE_DEBUG
            printf("Still waiting for %ju SQE(s)\n", PRIV(op)->wait_detail);
#endif
            return;
        }
        PRIV(op)->wait_for = 0;
    }
    else if (PRIV(op)->wait_for == WAIT_COMPACTION)
    {
        if (flusher->get_compact_counter() <= PRIV(op)->wait_detail)
        {
            // do not submit
#ifdef BLOCKSTORE_DEBUG
            printf("Still waiting for more flushes\n");
#endif
            return;
        }
        flusher->release_trim();
        PRIV(op)->wait_for = 0;
    }
    else
    {
        throw std::runtime_error("BUG: op->wait_for value is unexpected");
    }
}

void blockstore_impl_t::enqueue_op(blockstore_op_t *op)
{
    if (op->opcode < BS_OP_MIN || op->opcode > BS_OP_MAX ||
        ((op->opcode == BS_OP_READ || op->opcode == BS_OP_WRITE || op->opcode == BS_OP_WRITE_STABLE) && (
            op->offset >= dsk.data_block_size ||
            op->len > dsk.data_block_size-op->offset ||
            (op->len % dsk.disk_alignment)
        )) ||
        readonly && op->opcode != BS_OP_READ && op->opcode != BS_OP_LIST)
    {
        // Basic verification not passed
        op->retval = -EINVAL;
        ringloop->set_immediate([op]() { std::function<void (blockstore_op_t*)>(op->callback)(op); });
        return;
    }
    if ((op->opcode == BS_OP_WRITE || op->opcode == BS_OP_WRITE_STABLE || op->opcode == BS_OP_DELETE) && !enqueue_write(op))
    {
        ringloop->set_immediate([op]() { std::function<void (blockstore_op_t*)>(op->callback)(op); });
        return;
    }
    if (op->opcode == BS_OP_SYNC)
    {
        unsynced_queued_ops = 0;
    }
    init_op(op);
    submit_queue.push_back(op);
    ringloop->wakeup();
}

void blockstore_impl_t::init_op(blockstore_op_t *op)
{
    // Call constructor without allocating memory. We'll call destructor before returning op back
    new ((void*)op->private_data) blockstore_op_private_t;
    PRIV(op)->wait_for = 0;
    PRIV(op)->op_state = 0;
    PRIV(op)->pending_ops = 0;
}

void blockstore_impl_t::process_list(blockstore_op_t *op)
{
    uint32_t list_pg = op->pg_number+1;
    uint32_t pg_count = op->pg_count;
    uint64_t pg_stripe_size = op->pg_alignment;
    uint64_t min_inode = op->min_oid.inode;
    uint64_t max_inode = op->max_oid.inode;
    // Check PG
    if (!pg_count || (pg_stripe_size < MIN_DATA_BLOCK_SIZE || list_pg > pg_count) ||
        !INODE_POOL(min_inode) || INODE_POOL(min_inode) != INODE_POOL(max_inode))
    {
        op->retval = -EINVAL;
        FINISH_OP(op);
        return;
    }
    // Check if the DB needs resharding
    // (we don't know about PGs from the beginning, we only create "shards" here)
    heap->reshard(INODE_POOL(min_inode), pg_count, pg_stripe_size);
    obj_ver_id *result = NULL;
    size_t stable_count = 0, unstable_count = 0;
    int res = heap->list_objects(list_pg, op->min_oid, op->max_oid, &result, &stable_count, &unstable_count);
    if (op->list_stable_limit)
    {
        // Ordered result is expected - used by scrub
        // We use an unordered map
        std::sort(result, result + stable_count);
        if (stable_count > op->list_stable_limit)
        {
            memmove(result + op->list_stable_limit, result + stable_count, unstable_count);
            stable_count = op->list_stable_limit;
        }
    }
    op->version = stable_count;
    op->retval = res == 0 ? stable_count+unstable_count : -res;
    op->buf = (uint8_t*)result;
    FINISH_OP(op);
}

void blockstore_impl_t::set_no_inode_stats(const std::vector<uint64_t> & pool_ids)
{
}

void blockstore_impl_t::dump_diagnostics()
{
    flusher->dump_diagnostics();
}

void blockstore_meta_header_v3_t::set_crc32c()
{
    header_csum = 0;
    uint32_t calc = crc32c(0, this, version == BLOCKSTORE_META_FORMAT_HEAP
        ? sizeof(blockstore_meta_header_v3_t) : sizeof(blockstore_meta_header_v2_t));
    header_csum = calc;
}

void blockstore_impl_t::disk_error_abort(const char *op, int retval, int expected)
{
    if (retval == -EAGAIN)
    {
        fprintf(stderr, "EAGAIN error received from a disk %s during flush."
            " It must never happen with io_uring and indicates a kernel bug."
            " Please upgrade your kernel. Aborting.\n", op);
        exit(1);
    }
    fprintf(stderr, "Disk %s failed: result is %d, expected %d. Can't continue, sorry :-(\n", op, retval, expected);
    exit(1);
}

uint64_t blockstore_impl_t::get_free_block_count()
{
    return dsk.block_count - heap->get_data_used_space()/dsk.data_block_size;
}

std::string blockstore_impl_t::get_op_diag(blockstore_op_t *op)
{
    char buf[256];
    auto priv = PRIV(op);
    if (priv->wait_for)
        snprintf(buf, sizeof(buf), "state=%d wait=%d (detail=%ju)", priv->op_state, priv->wait_for, priv->wait_detail);
    else
        snprintf(buf, sizeof(buf), "state=%d", priv->op_state);
    return std::string(buf);
}
