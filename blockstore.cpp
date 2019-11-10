#include "blockstore.h"

blockstore::blockstore(spp::sparse_hash_map<std::string, std::string> & config, ring_loop_t *ringloop)
{
    this->ringloop = ringloop;
    ring_consumer.handle_event = [this](ring_data_t *d) { handle_event(d); };
    ring_consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(ring_consumer);
    initialized = 0;
    block_order = stoull(config["block_size_order"]);
    block_size = 1 << block_order;
    if (block_size <= 1 || block_size >= MAX_BLOCK_SIZE)
    {
        throw new std::runtime_error("Bad block size");
    }
    data_fd = meta_fd = journal.fd = -1;
    try
    {
        open_data(config);
        open_meta(config);
        open_journal(config);
        calc_lengths(config);
        data_alloc = allocator_create(block_count);
        if (!data_alloc)
            throw new std::bad_alloc();
    }
    catch (std::exception & e)
    {
        if (data_fd >= 0)
            close(data_fd);
        if (meta_fd >= 0 && meta_fd != data_fd)
            close(meta_fd);
        if (journal.fd >= 0 && journal.fd != meta_fd)
            close(journal.fd);
        throw e;
    }
}

blockstore::~blockstore()
{
    ringloop->unregister_consumer(ring_consumer.number);
    if (data_fd >= 0)
        close(data_fd);
    if (meta_fd >= 0 && meta_fd != data_fd)
        close(meta_fd);
    if (journal.fd >= 0 && journal.fd != meta_fd)
        close(journal.fd);
    free(journal.sector_buf);
    free(journal.sector_info);
}

// main event loop - handle requests
void blockstore::handle_event(ring_data_t *data)
{
    if (initialized != 10)
    {
        if (metadata_init_reader)
        {
            metadata_init_reader->handle_event(data);
        }
        else if (journal_init_reader)
        {
            journal_init_reader->handle_event(data);
        }
    }
    else
    {
        struct blockstore_operation* op = (struct blockstore_operation*)data->op;
        if ((op->flags & OP_TYPE_MASK) == OP_READ)
        {
            handle_read_event(data, op);
        }
        else if ((op->flags & OP_TYPE_MASK) == OP_WRITE ||
            (op->flags & OP_TYPE_MASK) == OP_DELETE)
        {
            handle_write_event(data, op);
        }
        else if ((op->flags & OP_TYPE_MASK) == OP_SYNC)
        {
            handle_sync_event(data, op);
        }
        else if ((op->flags & OP_TYPE_MASK) == OP_STABLE)
        {
            handle_stable_event(data, op);
        }
    }
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
        else if (initialized == 1)
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
        else if (initialized == 2)
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
        bool has_writes = false;
        while (cur != submit_queue.end())
        {
            auto op_ptr = cur;
            auto op = *(cur++);
            if (op->wait_for)
            {
                check_wait(op);
                if (op->wait_for == WAIT_SQE)
                    break;
                else if (op->wait_for)
                    continue;
            }
            unsigned ring_space = io_uring_sq_space_left(ringloop->ring);
            unsigned prev_sqe_pos = ringloop->ring->sq.sqe_tail;
            int dequeue_op = 0;
            if ((op->flags & OP_TYPE_MASK) == OP_READ)
            {
                dequeue_op = dequeue_read(op);
            }
            else if ((op->flags & OP_TYPE_MASK) == OP_WRITE ||
                (op->flags & OP_TYPE_MASK) == OP_DELETE)
            {
                has_writes = true;
                dequeue_op = dequeue_write(op);
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
                int ret = ringloop->submit();
                if (ret < 0)
                {
                    throw new std::runtime_error(std::string("io_uring_submit: ") + strerror(-ret));
                }
                submit_queue.erase(op_ptr);
            }
            else
            {
                ringloop->ring->sq.sqe_tail = prev_sqe_pos;
                if (op->wait_for == WAIT_SQE)
                {
                    op->wait_detail = 1 + ring_space;
                    // ring is full, stop submission
                    break;
                }
            }
        }
    }
}

void blockstore::check_wait(blockstore_operation *op)
{
    if (op->wait_for == WAIT_SQE)
    {
        if (io_uring_sq_space_left(ringloop->ring) < op->wait_detail)
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
        if (journal.used_start < op->wait_detail)
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
    else
    {
        throw new std::runtime_error("BUG: op->wait_for value is unexpected");
    }
}

int blockstore::enqueue_op(blockstore_operation *op)
{
    if (op->offset >= block_size || op->len >= block_size-op->offset ||
        (op->len % DISK_ALIGNMENT) ||
        (op->flags & OP_TYPE_MASK) < OP_READ || (op->flags & OP_TYPE_MASK) > OP_DELETE)
    {
        // Basic verification not passed
        return -EINVAL;
    }
    op->wait_for = 0;
    op->sync_state = 0;
    submit_queue.push_back(op);
    if ((op->flags & OP_TYPE_MASK) == OP_WRITE)
    {
        enqueue_write(op);
    }
    return 0;
}
