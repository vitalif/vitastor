#include "blockstore.h"

blockstore::blockstore(blockstore_config_t & config, ring_loop_t *ringloop)
{
    this->ringloop = ringloop;
    ring_consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(ring_consumer);
    initialized = 0;
    block_order = strtoull(config["block_size_order"].c_str(), NULL, 10);
    if (block_order == 0)
    {
        block_order = DEFAULT_ORDER;
    }
    block_size = 1 << block_order;
    if (block_size < MIN_BLOCK_SIZE || block_size >= MAX_BLOCK_SIZE)
    {
        throw std::runtime_error("Bad block size");
    }
    zero_object = (uint8_t*)memalign(DISK_ALIGNMENT, block_size);
    data_fd = meta_fd = journal.fd = -1;
    try
    {
        open_data(config);
        open_meta(config);
        open_journal(config);
        calc_lengths(config);
        data_alloc = new allocator(block_count);
    }
    catch (std::exception & e)
    {
        if (data_fd >= 0)
            close(data_fd);
        if (meta_fd >= 0 && meta_fd != data_fd)
            close(meta_fd);
        if (journal.fd >= 0 && journal.fd != meta_fd)
            close(journal.fd);
        throw;
    }
    int flusher_count = strtoull(config["flusher_count"].c_str(), NULL, 10);
    if (!flusher_count)
        flusher_count = 32;
    flusher = new journal_flusher_t(flusher_count, this);
}

blockstore::~blockstore()
{
    delete data_alloc;
    delete flusher;
    free(zero_object);
    ringloop->unregister_consumer(ring_consumer);
    if (data_fd >= 0)
        close(data_fd);
    if (meta_fd >= 0 && meta_fd != data_fd)
        close(meta_fd);
    if (journal.fd >= 0 && journal.fd != meta_fd)
        close(journal.fd);
    if (metadata_buffer)
        free(metadata_buffer);
}

bool blockstore::is_started()
{
    return initialized == 10;
}

// main event loop - produce requests
void blockstore::loop()
{
    if (initialized != 10)
    {
        // read metadata, then journal
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
                journal_init_reader = new blockstore_init_journal(this);
                initialized = 2;
            }
        }
        if (initialized == 2)
        {
            int res = journal_init_reader->loop();
            if (!res)
            {
                delete journal_init_reader;
                journal_init_reader = NULL;
                initialized = 10;
            }
        }
    }
    else
    {
        // try to submit ops
        auto cur_sync = in_progress_syncs.begin();
        while (cur_sync != in_progress_syncs.end())
        {
            continue_sync(*cur_sync++);
        }
        auto cur = submit_queue.begin();
        int has_writes = 0;
        while (cur != submit_queue.end())
        {
            auto op_ptr = cur;
            auto op = *(cur++);
            // FIXME: This needs some simplification
            // Writes should not block reads if the ring is not full and if reads don't depend on them
            // In all other cases we should stop submission
            if (op->wait_for)
            {
                check_wait(op);
                if (op->wait_for == WAIT_SQE)
                {
                    break;
                }
                else if (op->wait_for)
                {
                    if ((op->flags & OP_TYPE_MASK) == OP_WRITE ||
                        (op->flags & OP_TYPE_MASK) == OP_DELETE)
                    {
                        has_writes = 2;
                    }
                    continue;
                }
            }
            unsigned ring_space = io_uring_sq_space_left(&ringloop->ring);
            unsigned prev_sqe_pos = ringloop->ring.sq.sqe_tail;
            int dequeue_op = 0;
            if ((op->flags & OP_TYPE_MASK) == OP_READ)
            {
                dequeue_op = dequeue_read(op);
            }
            else if ((op->flags & OP_TYPE_MASK) == OP_WRITE ||
                (op->flags & OP_TYPE_MASK) == OP_DELETE)
            {
                if (has_writes == 2)
                {
                    // Some writes could not be submitted
                    break;
                }
                dequeue_op = dequeue_write(op);
                has_writes = dequeue_op ? 1 : 2;
            }
            else if ((op->flags & OP_TYPE_MASK) == OP_SYNC)
            {
                // wait for all small writes to be submitted
                // wait for all big writes to complete, submit data device fsync
                // wait for the data device fsync to complete, then submit journal writes for big writes
                // then submit an fsync operation
                if (has_writes)
                {
                    // Can't submit SYNC before previous writes
                    continue;
                }
                dequeue_op = dequeue_sync(op);
            }
            else if ((op->flags & OP_TYPE_MASK) == OP_STABLE)
            {
                dequeue_op = dequeue_stable(op);
            }
            if (dequeue_op)
            {
                submit_queue.erase(op_ptr);
            }
            else
            {
                ringloop->ring.sq.sqe_tail = prev_sqe_pos;
                if (op->wait_for == WAIT_SQE)
                {
                    op->wait_detail = 1 + ring_space;
                    // ring is full, stop submission
                    break;
                }
            }
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
    }
}

bool blockstore::is_safe_to_stop()
{
    // It's safe to stop blockstore when there are no in-flight operations,
    // no in-progress syncs and flusher isn't doing anything
    if (submit_queue.size() > 0 || in_progress_syncs.size() > 0 || !readonly && flusher->is_active())
    {
        return false;
    }
    if (unsynced_big_writes.size() > 0 || unsynced_small_writes.size() > 0)
    {
        if (!readonly && !stop_sync_submitted)
        {
            // We should sync the blockstore before unmounting
            blockstore_op_t *op = new blockstore_op_t;
            op->flags = OP_SYNC;
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

void blockstore::check_wait(blockstore_op_t *op)
{
    if (op->wait_for == WAIT_SQE)
    {
        if (io_uring_sq_space_left(&ringloop->ring) < op->wait_detail)
        {
            // stop submission if there's still no free space
            return;
        }
        op->wait_for = 0;
    }
    else if (op->wait_for == WAIT_IN_FLIGHT)
    {
        auto dirty_it = dirty_db.find((obj_ver_id){
            .oid = op->oid,
            .version = op->wait_detail,
        });
        if (dirty_it != dirty_db.end() && IS_IN_FLIGHT(dirty_it->second.state))
        {
            // do not submit
            return;
        }
        op->wait_for = 0;
    }
    else if (op->wait_for == WAIT_JOURNAL)
    {
        if (journal.used_start == op->wait_detail)
        {
            // do not submit
            return;
        }
        op->wait_for = 0;
    }
    else if (op->wait_for == WAIT_JOURNAL_BUFFER)
    {
        if (journal.sector_info[((journal.cur_sector + 1) % journal.sector_count)].usage_count > 0)
        {
            // do not submit
            return;
        }
        op->wait_for = 0;
    }
    else if (op->wait_for == WAIT_FREE)
    {
        if (!data_alloc->get_free_count() && !flusher->is_active())
        {
            return;
        }
        op->wait_for = 0;
    }
    else
    {
        throw std::runtime_error("BUG: op->wait_for value is unexpected");
    }
}

void blockstore::enqueue_op(blockstore_op_t *op)
{
    int type = op->flags & OP_TYPE_MASK;
    if (type < OP_READ || type > OP_DELETE || (type == OP_READ || type == OP_WRITE) &&
        (op->offset >= block_size || op->len > block_size-op->offset || (op->len % DISK_ALIGNMENT)) ||
        readonly && type != OP_READ)
    {
        // Basic verification not passed
        op->retval = -EINVAL;
        op->callback(op);
        return;
    }
    op->wait_for = 0;
    op->sync_state = 0;
    op->pending_ops = 0;
    submit_queue.push_back(op);
    if ((op->flags & OP_TYPE_MASK) == OP_WRITE)
    {
        enqueue_write(op);
    }
    ringloop->wakeup();
}
