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

blockstore_init_meta::blockstore_init_meta(blockstore *bs)
{
    this->bs = bs;
}

int blockstore_init_meta::read_loop()
{
    if (metadata_read >= bs->meta_len)
    {
        return 0;
    }
    if (!metadata_buffer)
    {
        metadata_buffer = new uint8_t[2*bs->metadata_buf_size];
    }
    if (submitted)
    {
        struct io_uring_cqe *cqe;
        io_uring_peek_cqe(bs->ring, &cqe);
        if (cqe)
        {
            if (cqe->res < 0)
            {
                throw new std::runtime_error(
                    std::string("read metadata failed at offset ") + std::to_string(metadata_read) +
                    std::string(": ") + strerror(-cqe->res)
                );
            }
            prev_done = cqe->res > 0 ? submitted : 0;
            done_len = cqe->res;
            metadata_read += cqe->res;
            submitted = 0;
        }
    }
    if (!submitted)
    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(bs->ring);
        if (!sqe)
        {
            throw new std::runtime_error("io_uring is full while trying to read metadata");
        }
        submit_iov = {
            metadata_buffer + (prev == 1 ? bs->metadata_buf_size : 0),
            bs->meta_len - metadata_read > bs->metadata_buf_size ? bs->metadata_buf_size : bs->meta_len - metadata_read,
        };
        io_uring_prep_readv(sqe, bs->meta_fd, &submit_iov, 1, bs->meta_offset + metadata_read);
        io_uring_submit(bs->ring);
        submitted = (prev == 1 ? 2 : 1);
        prev = submitted;
    }
    if (prev_done)
    {
        assert(!(done_len % sizeof(clean_disk_entry)));
        int count = done_len / sizeof(clean_disk_entry);
        struct clean_disk_entry *entries = (struct clean_disk_entry*)(metadata_buffer + (prev_done == 1 ? bs->metadata_buf_size : 0));
        // handle <count> entries
        handle_entries(entries, count);
        done_cnt += count;
        prev_done = 0;
        done_len = 0;
    }
    if (metadata_read >= bs->meta_len)
    {
        // metadata read finished
        delete[] metadata_buffer;
        metadata_buffer = NULL;
        return 0;
    }
    return 1;
}

void blockstore_init_meta::handle_entries(struct clean_disk_entry* entries, int count)
{
    for (unsigned i = 0; i < count; i++)
    {
        if (entries[i].oid.inode > 0)
        {
            allocator_set(bs->data_alloc, done_cnt+i, true);
            bs->object_db[entries[i].oid] = (struct clean_entry){
                entries[i].version,
                (uint32_t)(entries[i].flags ? ST_CURRENT : ST_D_META_SYNCED),
                done_cnt+i
            };
        }
    }
}

blockstore_init_journal::blockstore_init_journal(blockstore *bs)
{
    this->bs = bs;
}

int blockstore_init_journal::read_loop()
{
    return 0;
}
