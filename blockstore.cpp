#include "blockstore.h"

blockstore::blockstore(spp::sparse_hash_map<std::string, std::string> & config, io_uring *ring)
{
    this->ring = ring;
    ring_data = (struct ring_data_t*)malloc(sizeof(ring_data_t) * ring->sq.ring_sz);
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
    free(ring_data);
    if (data_fd >= 0)
        close(data_fd);
    if (meta_fd >= 0 && meta_fd != data_fd)
        close(meta_fd);
    if (journal_fd >= 0 && journal_fd != meta_fd)
        close(journal_fd);
}

struct io_uring_sqe* blockstore::get_sqe()
{
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (sqe)
    {
        io_uring_sqe_set_data(sqe, ring_data + (sqe - ring->sq.sqes));
    }
    return sqe;
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

// main event loop
int blockstore::main_loop()
{
    while (true)
    {
        struct io_uring_cqe *cqe;
        io_uring_peek_cqe(ring, &cqe);
        if (cqe)
        {
            struct ring_data *d = cqe->user_data;
            if (d->source == SRC_BLOCKSTORE)
            {
                handle_event();
            }
            else
            {
                // someone else
            }
            io_uring_cqe_seen(ring, cqe);
        }
    }
    return 0;
}
