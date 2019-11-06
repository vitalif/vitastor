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
    data_fd = meta_fd = journal_fd = -1;
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
        if (journal_fd >= 0 && journal_fd != meta_fd)
            close(journal_fd);
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
    if (journal_fd >= 0 && journal_fd != meta_fd)
        close(journal_fd);
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
        if ((op->flags & OP_TYPE_MASK) == OP_READ_DIRTY ||
            (op->flags & OP_TYPE_MASK) == OP_READ)
        {
            op->pending_ops--;
            if (data->res < 0)
            {
                // read error
                op->retval = data->res;
            }
            if (op->pending_ops == 0)
            {
                if (op->retval == 0)
                    op->retval = op->len;
                op->callback(op);
                in_process_ops.erase(op);
            }
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
        auto op = submit_queue.begin();
        while (op != submit_queue.end())
        {
            auto cur = op++;
            if (((*cur)->flags & OP_TYPE_MASK) == OP_READ_DIRTY ||
                ((*cur)->flags & OP_TYPE_MASK) == OP_READ)
            {
                int dequeue_op = dequeue_read(*cur);
                if (dequeue_op)
                {
                    submit_queue.erase(cur);
                }
                else if ((*cur)->wait_for == WAIT_SQE)
                {
                    // ring is full, stop submission
                    break;
                }
            }
        }
    }
}

int blockstore::enqueue_op(blockstore_operation *op)
{
    if (op->offset >= block_size || op->len >= block_size-op->offset)
    {
        return -EINVAL;
    }
    submit_queue.push_back(op);
    if ((op->flags & OP_TYPE_MASK) == OP_WRITE)
    {
        // Assign version number
        auto dirty_it = dirty_queue.find(op->oid);
        if (dirty_it != dirty_queue.end())
        {
            op->version = (*dirty_it).back().version + 1;
        }
        else
        {
            auto clean_it = object_db.find(op->oid);
            if (clean_it != object_db.end())
            {
                op->version = (*clean_it).version + 1;
            }
            else
            {
                op->version = 1;
            }
            dirty_it = dirty_queue.emplace(op->oid, dirty_list()).first;
        }
        // Immediately add the operation into the dirty queue, so subsequent reads could see it
        (*dirty_it).push_back((dirty_entry){
            .version = op->version,
            .state = ST_IN_FLIGHT,
            .flags = 0,
            .location = 0,
            .offset = op->offset,
            .size = op->len,
        });
    }
    return 0;
}
