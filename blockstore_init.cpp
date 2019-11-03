#include "blockstore.h"

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
        metadata_buffer = (uint8_t*)memalign(512, 2*bs->metadata_buf_size);
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
        free(metadata_buffer);
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
    if (!journal_buffer)
    {
        journal_buffer = new uint8_t[4*1024*1024];
    }
    
    return 0;
}
