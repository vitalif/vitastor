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
            io_uring_cqe_seen(bs->ring, cqe);
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

bool iszero(uint64_t *buf, int len)
{
    for (int i = 0; i < len; i++)
        if (buf[i] != 0)
            return false;
    return true;
}

int blockstore_init_journal::read_loop()
{
    if (step == 100)
    {
        return 0;
    }
    if (!journal_buffer)
    {
        journal_buffer = (uint8_t*)memalign(DISK_ALIGNMENT, 2*JOURNAL_BUFFER_SIZE);
    }
    if (step == 0)
    {
        // Step 1: Read first block of the journal
        struct io_uring_sqe *sqe = io_uring_get_sqe(bs->ring);
        if (!sqe)
        {
            throw new std::runtime_error("io_uring is full while trying to read journal");
        }
        submit_iov = { journal_buffer, 512 };
        io_uring_prep_readv(sqe, bs->journal_fd, &submit_iov, 1, bs->journal_offset);
        io_uring_submit(bs->ring);
        step = 1;
    }
    if (step == 1)
    {
        // Step 2: Get the completion event and check the beginning for <START> entry
        struct io_uring_cqe *cqe;
        io_uring_peek_cqe(bs->ring, &cqe);
        if (cqe)
        {
            if (cqe->res < 0)
            {
                throw new std::runtime_error(
                    std::string("read journal failed at offset ") + std::to_string(0) +
                    std::string(": ") + strerror(-cqe->res)
                );
            }
            if (iszero((uint64_t*)journal_buffer, 3))
            {
                // Journal is empty
                bs->journal_start = 512;
                bs->journal_end = 512;
                step = 99;
            }
            else
            {
                // First block always contains a single JE_START entry
                journal_entry_start *je = (journal_entry_start*)journal_buffer;
                if (je->magic != JOURNAL_MAGIC ||
                    je->type != JE_START ||
                    je->size != sizeof(journal_entry_start) ||
                    je_crc32((journal_entry*)je) != je->crc32)
                {
                    // Entry is corrupt
                    throw new std::runtime_error("first entry of the journal is corrupt");
                }
                journal_pos = bs->journal_start = je->journal_start;
                crc32_last = je->crc32_replaced;
                step = 2;
            }
            io_uring_cqe_seen(bs->ring, cqe);
        }
    }
    if (step == 2 || step == 3)
    {
        // Step 3: Read journal
        if (submitted)
        {
            struct io_uring_cqe *cqe;
            io_uring_peek_cqe(bs->ring, &cqe);
            if (cqe)
            {
                if (cqe->res < 0)
                {
                    throw new std::runtime_error(
                        std::string("read journal failed at offset ") + std::to_string(journal_pos) +
                        std::string(": ") + strerror(-cqe->res)
                    );
                }
                done_pos = journal_pos;
                done_buf = submitted;
                done_len = cqe->res;
                journal_pos += cqe->res;
                if (journal_pos >= bs->journal_len)
                {
                    // Continue from the beginning
                    journal_pos = 512;
                    wrapped = true;
                }
                submitted = 0;
                io_uring_cqe_seen(bs->ring, cqe);
            }
        }
        if (!submitted)
        {
            if (step != 3)
            {
                if (journal_pos == bs->journal_start && wrapped)
                {
                    step = 3;
                }
                else
                {
                    struct io_uring_sqe *sqe = io_uring_get_sqe(bs->ring);
                    if (!sqe)
                    {
                        throw new std::runtime_error("io_uring is full while trying to read journal");
                    }
                    uint64_t end = bs->journal_len;
                    if (journal_pos < bs->journal_start)
                    {
                        end = bs->journal_start;
                    }
                    submit_iov = {
                        journal_buffer + (done_buf == 1 ? JOURNAL_BUFFER_SIZE : 0),
                        end - journal_pos < JOURNAL_BUFFER_SIZE ? end - journal_pos : JOURNAL_BUFFER_SIZE,
                    };
                    io_uring_prep_readv(sqe, bs->journal_fd, &submit_iov, 1, bs->journal_offset + journal_pos);
                    io_uring_submit(bs->ring);
                    submitted = done_buf == 1 ? 2 : 1;
                }
            }
            else
            {
                step = 99;
            }
        }
        if (done_buf && step != 3)
        {
            // handle journal entries
            if (handle_journal_part(journal_buffer + (done_buf == 1 ? 0 : JOURNAL_BUFFER_SIZE), done_len) == 0)
            {
                // journal ended. wait for the next read to complete, then stop
                step = 3;
            }
            done_buf = 0;
        }
    }
    if (step == 99)
    {
        free(journal_buffer);
        bs->journal_crc32_last = crc32_last;
        journal_buffer = NULL;
        step = 100;
    }
    return 1;
}

int blockstore_init_journal::handle_journal_part(void *buf, uint64_t len)
{
    uint64_t total_pos = 0;
    if (cur_skip >= 0)
    {
        total_pos = cur_skip;
        cur_skip = 0;
    }
    while (total_pos < len)
    {
        total_pos += 512;
        uint64_t pos = 0;
        while (pos < 512)
        {
            journal_entry *je = (journal_entry*)((uint8_t*)buf + total_pos + pos);
            if (je->magic != JOURNAL_MAGIC || je_crc32(je) != je->crc32 ||
                je->type < JE_SMALL_WRITE || je->type > JE_DELETE || je->crc32_prev != crc32_last)
            {
                if (pos == 0)
                {
                    // invalid entry in the beginning, this is definitely the end of the journal
                    bs->journal_end = done_pos + total_pos + pos;
                    return 0;
                }
                else
                {
                    // allow partially filled sectors
                    break;
                }
            }
            pos += je->size;
            crc32_last = je->crc32;
            if (je->type == JE_SMALL_WRITE)
            {
                // oid, version, offset, len
                uint64_t location;
                if (cur_skip > 0 || done_pos + total_pos + je->small_write.len > bs->journal_len)
                {
                    // data continues from the beginning of the journal
                    location = 512 + cur_skip;
                    cur_skip += je->small_write.len;
                }
                else
                {
                    // data is right next
                    location = done_pos + total_pos;
                    total_pos += je->small_write.len;
                }
                bs->dirty_queue[je->small_write.oid].push_back((dirty_entry){
                    .version = je->small_write.version,
                    .state = ST_J_SYNCED,
                    .flags = 0,
                    .location = location,
                    .offset = je->small_write.offset,
                    .size = je->small_write.len,
                });
            }
            else if (je->type == JE_BIG_WRITE)
            {
                // oid, version, block
                bs->dirty_queue[je->big_write.oid].push_back((dirty_entry){
                    .version = je->big_write.version,
                    .state = ST_D_META_SYNCED,
                    .flags = 0,
                    .location = je->big_write.block,
                    .offset = 0,
                    .size = bs->block_size,
                });
            }
            else if (je->type == JE_STABLE)
            {
                // oid, version
                auto it = bs->dirty_queue.find(je->stable.oid);
                if (it == bs->dirty_queue.end())
                {
                    // journal contains a legitimate STABLE entry for a non-existing dirty write
                    // this probably means that journal was trimmed between WRITTEN and STABLE entries
                    // skip for now. but FIXME: maybe warn about it in the future
                }
                else
                {
                    auto & lst = it->second;
                    int i;
                    for (i = 0; i < lst.size(); i++)
                    {
                        if (lst[i].version == je->stable.version)
                        {
                            lst[i].state = (lst[i].state == ST_D_META_SYNCED
                                ? ST_D_STABLE
                                : (lst[i].state == ST_DEL_SYNCED ? ST_DEL_STABLE : ST_J_STABLE));
                            break;
                        }
                    }
                    if (i >= lst.size())
                    {
                        // same. STABLE entry for a missing object version
                    }
                }
            }
            else if (je->type == JE_DELETE)
            {
                // oid, version
                bs->dirty_queue[je->small_write.oid].push_back((dirty_entry){
                    .version = je->small_write.version,
                    .state = ST_DEL_SYNCED,
                    .flags = 0,
                    .location = 0,
                    .offset = 0,
                    .size = 0,
                });
            }
        }
    }
    if (cur_skip == 0 && total_pos > len)
    {
        cur_skip = total_pos - len;
    }
    return 1;
}
