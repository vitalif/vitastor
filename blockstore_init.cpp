#include "blockstore.h"

blockstore_init_meta::blockstore_init_meta(blockstore *bs)
{
    this->bs = bs;
}

void blockstore_init_meta::handle_event(ring_data_t *data)
{
    if (data->res <= 0)
    {
        throw std::runtime_error(
            std::string("read metadata failed at offset ") + std::to_string(metadata_read) +
            std::string(": ") + strerror(-data->res)
        );
    }
    prev_done = data->res > 0 ? submitted : 0;
    done_len = data->res;
    metadata_read += data->res;
    submitted = 0;
}

int blockstore_init_meta::loop()
{
    if (metadata_read >= bs->meta_len)
    {
        return 0;
    }
    if (!metadata_buffer)
    {
        metadata_buffer = (uint8_t*)memalign(512, 2*bs->metadata_buf_size);
        if (!metadata_buffer)
            throw std::bad_alloc();
    }
    if (!submitted)
    {
        struct io_uring_sqe *sqe = bs->get_sqe();
        if (!sqe)
        {
            throw std::runtime_error("io_uring is full while trying to read metadata");
        }
        struct ring_data_t *data = ((ring_data_t*)sqe->user_data);
        data->iov = {
            metadata_buffer + (prev == 1 ? bs->metadata_buf_size : 0),
            bs->meta_len - metadata_read > bs->metadata_buf_size ? bs->metadata_buf_size : bs->meta_len - metadata_read,
        };
        data->callback = [this](ring_data_t *data) { handle_event(data); };
        my_uring_prep_readv(sqe, bs->meta_fd, &data->iov, 1, bs->meta_offset + metadata_read);
        bs->ringloop->submit();
        submitted = (prev == 1 ? 2 : 1);
        prev = submitted;
    }
    if (prev_done)
    {
        int count = 512 / sizeof(clean_disk_entry);
        for (int sector = 0; sector < done_len; sector += 512)
        {
            clean_disk_entry *entries = (clean_disk_entry*)(metadata_buffer + (prev_done == 1 ? bs->metadata_buf_size : 0) + sector);
            // handle <count> entries
            handle_entries(entries, count, bs->block_order);
            done_cnt += count;
        }
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

void blockstore_init_meta::handle_entries(struct clean_disk_entry* entries, int count, int block_order)
{
    auto end = bs->clean_db.end();
    for (unsigned i = 0; i < count; i++)
    {
        if (entries[i].oid.inode > 0)
        {
            auto clean_it = bs->clean_db.find(entries[i].oid);
            if (clean_it == end || clean_it->second.version < entries[i].version)
            {
                if (clean_it != end)
                {
                    // free the previous block
                    allocator_set(bs->data_alloc, clean_it->second.version >> block_order, false);
                }
                allocator_set(bs->data_alloc, done_cnt+i, true);
                bs->clean_db[entries[i].oid] = (struct clean_entry){
                    .version = entries[i].version,
                    .location = (done_cnt+i) << block_order,
                };
            }
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

void blockstore_init_journal::handle_event(ring_data_t *data)
{
    if (step == 1)
    {
        // Step 1: Read first block of the journal
        if (data->res < 0)
        {
            throw std::runtime_error(
                std::string("read journal failed at offset ") + std::to_string(0) +
                std::string(": ") + strerror(-data->res)
            );
        }
        if (iszero((uint64_t*)journal_buffer, 3))
        {
            // Journal is empty
            // FIXME handle this wrapping to 512 better
            bs->journal.used_start = 512;
            bs->journal.next_free = 512;
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
                throw std::runtime_error("first entry of the journal is corrupt");
            }
            journal_pos = bs->journal.used_start = je->journal_start;
            crc32_last = 0;
            step = 2;
            started = false;
        }
    }
    else if (step == 2 || step == 3)
    {
        // Step 3: Read journal
        if (data->res < 0)
        {
            throw std::runtime_error(
                std::string("read journal failed at offset ") + std::to_string(journal_pos) +
                std::string(": ") + strerror(-data->res)
            );
        }
        done_pos = journal_pos;
        done_buf = submitted;
        done_len = data->res;
        journal_pos += data->res;
        if (journal_pos >= bs->journal.len)
        {
            // Continue from the beginning
            journal_pos = 512;
            wrapped = true;
        }
        submitted = 0;
    }
}

int blockstore_init_journal::loop()
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
        struct io_uring_sqe *sqe = bs->get_sqe();
        if (!sqe)
        {
            throw std::runtime_error("io_uring is full while trying to read journal");
        }
        struct ring_data_t *data = ((ring_data_t*)sqe->user_data);
        data->iov = { journal_buffer, 512 };
        data->callback = [this](ring_data_t *data) { handle_event(data); };
        my_uring_prep_readv(sqe, bs->journal.fd, &data->iov, 1, bs->journal.offset);
        bs->ringloop->submit();
        step = 1;
    }
    if (step == 2 || step == 3)
    {
        // Step 3: Read journal
        if (!submitted)
        {
            if (step != 3)
            {
                if (journal_pos == bs->journal.used_start && wrapped)
                {
                    step = 3;
                }
                else
                {
                    struct io_uring_sqe *sqe = bs->get_sqe();
                    if (!sqe)
                    {
                        throw std::runtime_error("io_uring is full while trying to read journal");
                    }
                    struct ring_data_t *data = ((ring_data_t*)sqe->user_data);
                    uint64_t end = bs->journal.len;
                    if (journal_pos < bs->journal.used_start)
                    {
                        end = bs->journal.used_start;
                    }
                    data->iov = {
                        journal_buffer + (done_buf == 1 ? JOURNAL_BUFFER_SIZE : 0),
                        end - journal_pos < JOURNAL_BUFFER_SIZE ? end - journal_pos : JOURNAL_BUFFER_SIZE,
                    };
                    data->callback = [this](ring_data_t *data) { handle_event(data); };
                    my_uring_prep_readv(sqe, bs->journal.fd, &data->iov, 1, bs->journal.offset + journal_pos);
                    bs->ringloop->submit();
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
        bs->journal.crc32_last = crc32_last;
        journal_buffer = NULL;
        step = 100;
        return 0;
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
                je->type < JE_SMALL_WRITE || je->type > JE_DELETE || started && je->crc32_prev != crc32_last)
            {
                if (pos == 0)
                {
                    // invalid entry in the beginning, this is definitely the end of the journal
                    // FIXME handle the edge case when the journal is full
                    bs->journal.next_free = done_pos + total_pos;
                    return 0;
                }
                else
                {
                    // allow partially filled sectors
                    break;
                }
            }
            started = true;
            pos += je->size;
            crc32_last = je->crc32;
            if (je->type == JE_SMALL_WRITE)
            {
                // oid, version, offset, len
                uint64_t location;
                if (cur_skip > 0 || done_pos + total_pos + je->small_write.len > bs->journal.len)
                {
                    // data continues from the beginning of the journal
                    location = 512 + cur_skip;
                    cur_skip += je->small_write.len;
                }
                else
                {
                    // data is right next
                    location = done_pos + total_pos;
                    // FIXME: OOPS. Please don't modify total_pos here
                    total_pos += je->small_write.len;
                }
                obj_ver_id ov = {
                    .oid = je->small_write.oid,
                    .version = je->small_write.version,
                };
                bs->dirty_db.emplace(ov, (dirty_entry){
                    .state = ST_J_SYNCED,
                    .flags = 0,
                    .location = location,
                    .offset = je->small_write.offset,
                    .len = je->small_write.len,
                    .journal_sector = total_pos,
                });
                bs->journal.used_sectors[total_pos]++;
                bs->flusher->queue_flush(ov);
            }
            else if (je->type == JE_BIG_WRITE)
            {
                // oid, version, block
                obj_ver_id ov = {
                    .oid = je->big_write.oid,
                    .version = je->big_write.version,
                };
                bs->dirty_db.emplace(ov, (dirty_entry){
                    .state = ST_D_META_SYNCED,
                    .flags = 0,
                    .location = je->big_write.location,
                    .offset = 0,
                    .len = bs->block_size,
                    .journal_sector = total_pos,
                });
                bs->journal.used_sectors[total_pos]++;
                bs->flusher->queue_flush(ov);
            }
            else if (je->type == JE_STABLE)
            {
                // oid, version
                obj_ver_id ov = {
                    .oid = je->stable.oid,
                    .version = je->stable.version,
                };
                auto it = bs->dirty_db.find(ov);
                if (it == bs->dirty_db.end())
                {
                    // journal contains a legitimate STABLE entry for a non-existing dirty write
                    // this probably means that journal was trimmed between WRITTEN and STABLE entries
                    // skip for now. but FIXME: maybe warn about it in the future
                }
                else
                {
                    it->second.state = (it->second.state == ST_D_META_SYNCED
                        ? ST_D_STABLE
                        : (it->second.state == ST_DEL_SYNCED ? ST_DEL_STABLE : ST_J_STABLE));
                }
            }
            else if (je->type == JE_DELETE)
            {
                // oid, version
                obj_ver_id ov = {
                    .oid = je->del.oid,
                    .version = je->del.version,
                };
                bs->dirty_db.emplace(ov, (dirty_entry){
                    .state = ST_DEL_SYNCED,
                    .flags = 0,
                    .location = 0,
                    .offset = 0,
                    .len = 0,
                    .journal_sector = total_pos,
                });
                bs->journal.used_sectors[total_pos]++;
                bs->flusher->queue_flush(ov);
            }
        }
    }
    if (cur_skip == 0 && total_pos > len)
    {
        cur_skip = total_pos - len;
    }
    return 1;
}
