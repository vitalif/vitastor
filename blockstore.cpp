#include "blockstore.h"

blockstore::blockstore(spp::sparse_hash_map<std::string, std::string> & config, io_uring *ring)
{
    this->ring = ring;
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
    if (data_fd >= 0)
        close(data_fd);
    if (meta_fd >= 0 && meta_fd != data_fd)
        close(meta_fd);
    if (journal_fd >= 0 && journal_fd != meta_fd)
        close(journal_fd);
}

// must be called in the event loop until it returns 0
int blockstore::init_loop()
{
    // read metadata, then journal
    if (initialized)
    {
        return 0;
    }
    if (!metadata_init_reader)
    {
        metadata_init_reader = new blockstore_init_meta(this);
    }
    if (metadata_init_reader->read_loop())
    {
        return 1;
    }
    if (!journal_init_reader)
    {
        journal_init_reader = new blockstore_init_journal(this);
    }
    if (journal_init_reader->read_loop())
    {
        return 1;
    }
    initialized = true;
    delete metadata_init_reader;
    delete journal_init_reader;
    metadata_init_reader = NULL;
    journal_init_reader = NULL;
    return 0;
}
