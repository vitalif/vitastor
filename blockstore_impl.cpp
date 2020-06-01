#include "blockstore_impl.h"

blockstore_impl_t::blockstore_impl_t(blockstore_config_t & config, ring_loop_t *ringloop)
{
    assert(sizeof(blockstore_op_private_t) <= BS_OP_PRIVATE_DATA_SIZE);
    this->ringloop = ringloop;
    ring_consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(&ring_consumer);
    initialized = 0;
    zero_object = (uint8_t*)memalign(MEM_ALIGNMENT, block_size);
    data_fd = meta_fd = journal.fd = -1;
    parse_config(config);
    try
    {
        open_data();
        open_meta();
        open_journal();
        calc_lengths();
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
    flusher = new journal_flusher_t(flusher_count, this);
}

blockstore_impl_t::~blockstore_impl_t()
{
    delete data_alloc;
    delete flusher;
    free(zero_object);
    ringloop->unregister_consumer(&ring_consumer);
    if (data_fd >= 0)
        close(data_fd);
    if (meta_fd >= 0 && meta_fd != data_fd)
        close(meta_fd);
    if (journal.fd >= 0 && journal.fd != meta_fd)
        close(journal.fd);
    if (metadata_buffer)
        free(metadata_buffer);
    if (clean_bitmap)
        free(clean_bitmap);
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
    // FIXME: initialized == 10 is ugly
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
                ringloop->wakeup();
            }
        }
    }
    else
    {
        // try to submit ops
        unsigned initial_ring_space = ringloop->space_left();
        // FIXME: rework this "sync polling"
        auto cur_sync = in_progress_syncs.begin();
        while (cur_sync != in_progress_syncs.end())
        {
            if (continue_sync(*cur_sync) != 2)
            {
                // List is unmodified
                cur_sync++;
            }
            else
            {
                cur_sync = in_progress_syncs.begin();
            }
        }
        auto cur = submit_queue.begin();
        int has_writes = 0;
        while (cur != submit_queue.end())
        {
            auto op_ptr = cur;
            auto op = *(cur++);
            // FIXME: This needs some simplification
            // Writes should not block reads if the ring is not full and reads don't depend on them
            // In all other cases we should stop submission
            if (PRIV(op)->wait_for)
            {
                check_wait(op);
                if (PRIV(op)->wait_for == WAIT_SQE)
                {
                    break;
                }
                else if (PRIV(op)->wait_for)
                {
                    if (op->opcode == BS_OP_WRITE || op->opcode == BS_OP_DELETE)
                    {
                        has_writes = 2;
                    }
                    continue;
                }
            }
            unsigned ring_space = ringloop->space_left();
            unsigned prev_sqe_pos = ringloop->save();
            bool dequeue_op = false;
            if (op->opcode == BS_OP_READ)
            {
                dequeue_op = dequeue_read(op);
            }
            else if (op->opcode == BS_OP_WRITE || op->opcode == BS_OP_DELETE)
            {
                if (has_writes == 2)
                {
                    // Some writes could not be submitted
                    break;
                }
                dequeue_op = dequeue_write(op);
                has_writes = dequeue_op ? 1 : 2;
            }
            else if (op->opcode == BS_OP_SYNC)
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
            else if (op->opcode == BS_OP_STABLE)
            {
                if (has_writes == 2)
                {
                    // Don't submit additional flushes before completing previous LISTs
                    break;
                }
                dequeue_op = dequeue_stable(op);
            }
            else if (op->opcode == BS_OP_ROLLBACK)
            {
                if (has_writes == 2)
                {
                    // Don't submit additional flushes before completing previous LISTs
                    break;
                }
                dequeue_op = dequeue_rollback(op);
            }
            else if (op->opcode == BS_OP_LIST)
            {
                // Block LIST operation by previous modifications,
                // so it always returns a consistent state snapshot
                if (has_writes == 2 || inflight_writes > 0)
                    has_writes = 2;
                else
                {
                    process_list(op);
                    dequeue_op = true;
                }
            }
            if (dequeue_op)
            {
                submit_queue.erase(op_ptr);
            }
            else
            {
                ringloop->restore(prev_sqe_pos);
                if (PRIV(op)->wait_for == WAIT_SQE)
                {
                    PRIV(op)->wait_detail = 1 + ring_space;
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
            printf("Still waiting for %lu SQE(s)\n", PRIV(op)->wait_detail);
#endif
            return;
        }
        PRIV(op)->wait_for = 0;
    }
    else if (PRIV(op)->wait_for == WAIT_JOURNAL)
    {
        if (journal.used_start == PRIV(op)->wait_detail)
        {
            // do not submit
#ifdef BLOCKSTORE_DEBUG
            printf("Still waiting to flush journal offset %08lx\n", PRIV(op)->wait_detail);
#endif
            return;
        }
        flusher->release_trim();
        PRIV(op)->wait_for = 0;
    }
    else if (PRIV(op)->wait_for == WAIT_JOURNAL_BUFFER)
    {
        int next = ((journal.cur_sector + 1) % journal.sector_count);
        if (journal.sector_info[next].usage_count > 0 ||
            journal.sector_info[next].dirty)
        {
            // do not submit
#ifdef BLOCKSTORE_DEBUG
            printf("Still waiting for a journal buffer\n");
#endif
            return;
        }
        PRIV(op)->wait_for = 0;
    }
    else if (PRIV(op)->wait_for == WAIT_FREE)
    {
        if (!data_alloc->get_free_count() && !flusher->is_active())
        {
#ifdef BLOCKSTORE_DEBUG
            printf("Still waiting for free space on the data device\n");
#endif
            return;
        }
        PRIV(op)->wait_for = 0;
    }
    else
    {
        throw std::runtime_error("BUG: op->wait_for value is unexpected");
    }
}

void blockstore_impl_t::enqueue_op(blockstore_op_t *op, bool first)
{
    if (op->opcode < BS_OP_MIN || op->opcode > BS_OP_MAX ||
        ((op->opcode == BS_OP_READ || op->opcode == BS_OP_WRITE) && (
            op->offset >= block_size ||
            op->len > block_size-op->offset ||
            (op->len % disk_alignment)
        )) ||
        readonly && op->opcode != BS_OP_READ && op->opcode != BS_OP_LIST ||
        first && op->opcode == BS_OP_WRITE)
    {
        // Basic verification not passed
        op->retval = -EINVAL;
        std::function<void (blockstore_op_t*)>(op->callback)(op);
        return;
    }
    if (op->opcode == BS_OP_SYNC_STAB_ALL)
    {
        std::function<void(blockstore_op_t*)> *old_callback = new std::function<void(blockstore_op_t*)>(op->callback);
        op->opcode = BS_OP_SYNC;
        op->callback = [this, old_callback](blockstore_op_t *op)
        {
            if (op->retval >= 0 && unstable_writes.size() > 0)
            {
                op->opcode = BS_OP_STABLE;
                op->len = unstable_writes.size();
                obj_ver_id *vers = new obj_ver_id[op->len];
                op->buf = vers;
                int i = 0;
                for (auto it = unstable_writes.begin(); it != unstable_writes.end(); it++, i++)
                {
                    vers[i] = {
                        .oid = it->first,
                        .version = it->second,
                    };
                }
                unstable_writes.clear();
                op->callback = [this, old_callback](blockstore_op_t *op)
                {
                    obj_ver_id *vers = (obj_ver_id*)op->buf;
                    delete[] vers;
                    op->buf = NULL;
                    (*old_callback)(op);
                    delete old_callback;
                };
                this->enqueue_op(op);
            }
            else
            {
                (*old_callback)(op);
                delete old_callback;
            }
        };
    }
    if (op->opcode == BS_OP_WRITE && !enqueue_write(op))
    {
        std::function<void (blockstore_op_t*)>(op->callback)(op);
        return;
    }
    if (op->opcode == BS_OP_SYNC && immediate_commit == IMMEDIATE_ALL)
    {
        op->retval = 0;
        std::function<void (blockstore_op_t*)>(op->callback)(op);
        return;
    }
    // Call constructor without allocating memory. We'll call destructor before returning op back
    new ((void*)op->private_data) blockstore_op_private_t;
    PRIV(op)->wait_for = 0;
    PRIV(op)->op_state = 0;
    PRIV(op)->pending_ops = 0;
    if (!first)
    {
        submit_queue.push_back(op);
    }
    else
    {
        submit_queue.push_front(op);
    }
    ringloop->wakeup();
}

void blockstore_impl_t::process_list(blockstore_op_t *op)
{
    // Count objects
    uint32_t list_pg = op->offset;
    uint32_t pg_count = op->len;
    uint64_t pg_stripe_size = op->oid.stripe;
    if (pg_count != 0 && (pg_stripe_size < MIN_BLOCK_SIZE || list_pg >= pg_count))
    {
        op->retval = -EINVAL;
        FINISH_OP(op);
        return;
    }
    uint64_t stable_count = 0;
    if (pg_count > 0)
    {
        for (auto it = clean_db.begin(); it != clean_db.end(); it++)
        {
            uint32_t pg = (it->first.inode + it->first.stripe / pg_stripe_size) % pg_count;
            if (pg == list_pg)
            {
                stable_count++;
            }
        }
    }
    else
    {
        stable_count = clean_db.size();
    }
    uint64_t total_count = stable_count;
    for (auto it = dirty_db.begin(); it != dirty_db.end(); it++)
    {
        if (!pg_count || ((it->first.oid.inode + it->first.oid.stripe / pg_stripe_size) % pg_count) == list_pg)
        {
            if (IS_STABLE(it->second.state))
            {
                stable_count++;
            }
            total_count++;
        }
    }
    // Allocate memory
    op->version = stable_count;
    op->retval = total_count;
    op->buf = malloc(sizeof(obj_ver_id) * total_count);
    if (!op->buf)
    {
        op->retval = -ENOMEM;
        FINISH_OP(op);
        return;
    }
    obj_ver_id *vers = (obj_ver_id*)op->buf;
    int i = 0;
    for (auto it = clean_db.begin(); it != clean_db.end(); it++)
    {
        if (!pg_count || ((it->first.inode + it->first.stripe / pg_stripe_size) % pg_count) == list_pg)
        {
            vers[i++] = {
                .oid = it->first,
                .version = it->second.version,
            };
        }
    }
    int j = stable_count;
    for (auto it = dirty_db.begin(); it != dirty_db.end(); it++)
    {
        if (!pg_count || ((it->first.oid.inode + it->first.oid.stripe / pg_stripe_size) % pg_count) == list_pg)
        {
            if (IS_STABLE(it->second.state))
            {
                vers[i++] = it->first;
            }
            else
            {
                vers[j++] = it->first;
            }
        }
    }
    FINISH_OP(op);
}
