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
    if (initialized != 0)
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
        
    }
}
