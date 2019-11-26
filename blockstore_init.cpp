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
    if (wait_state == 1)
        goto resume_1;
    metadata_buffer = (uint8_t*)memalign(512, 2*bs->metadata_buf_size);
    if (!metadata_buffer)
        throw std::bad_alloc();
    while (1)
    {
    resume_1:
        if (submitted)
        {
            wait_state = 1;
            return 1;
        }
        if (metadata_read < bs->meta_len)
        {
            sqe = bs->get_sqe();
            if (!sqe)
            {
                throw std::runtime_error("io_uring is full while trying to read metadata");
            }
            data = ((ring_data_t*)sqe->user_data);
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
                clean_disk_entry *entries = (clean_disk_entry*)(metadata_buffer + (prev_done == 2 ? bs->metadata_buf_size : 0) + sector);
                // handle <count> entries
                handle_entries(entries, count, bs->block_order);
                done_cnt += count;
            }
            prev_done = 0;
            done_len = 0;
        }
        if (!submitted)
        {
            break;
        }
    }
    // metadata read finished
    printf("Metadata entries loaded: %d\n", entries_loaded);
    free(metadata_buffer);
    metadata_buffer = NULL;
    return 0;
}

void blockstore_init_meta::handle_entries(struct clean_disk_entry* entries, int count, int block_order)
{
    for (unsigned i = 0; i < count; i++)
    {
        if (entries[i].oid.inode > 0)
        {
            auto clean_it = bs->clean_db.find(entries[i].oid);
            if (clean_it == bs->clean_db.end() || clean_it->second.version < entries[i].version)
            {
                if (clean_it != bs->clean_db.end())
                {
                    // free the previous block
                    bs->data_alloc->set(clean_it->second.version >> block_order, false);
                }
                else
                    entries_loaded++;
                bs->data_alloc->set(done_cnt+i, true);
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
    simple_callback = [this](ring_data_t *data1)
    {
        if (data1->res != data1->iov.iov_len)
        {
            throw std::runtime_error(std::string("I/O operation failed while reading journal: ") + strerror(-data1->res));
        }
        wait_count--;
    };
}

bool iszero(uint64_t *buf, int len)
{
    for (int i = 0; i < len; i++)
        if (buf[i] != 0)
            return false;
    return true;
}

void blockstore_init_journal::handle_event(ring_data_t *data1)
{
    // Step 3: Read journal
    if (data1->res < 0)
    {
        throw std::runtime_error(
            std::string("read journal failed at offset ") + std::to_string(journal_pos) +
            std::string(": ") + strerror(-data1->res)
        );
    }
    done_pos = journal_pos;
    done_buf = submitted;
    done_len = data1->res;
    journal_pos += data1->res;
    if (journal_pos >= bs->journal.len)
    {
        // Continue from the beginning
        journal_pos = 512;
        wrapped = true;
    }
    submitted = 0;
}

#define GET_SQE() \
    sqe = bs->get_sqe();\
    if (!sqe)\
        throw std::runtime_error("io_uring is full while trying to read journal");\
    data = ((ring_data_t*)sqe->user_data)

int blockstore_init_journal::loop()
{
    if (wait_state == 1)
        goto resume_1;
    else if (wait_state == 2)
        goto resume_2;
    else if (wait_state == 3)
        goto resume_3;
    else if (wait_state == 4)
        goto resume_4;
    journal_buffer = (uint8_t*)memalign(DISK_ALIGNMENT, 2*JOURNAL_BUFFER_SIZE);
    // Read first block of the journal
    sqe = bs->get_sqe();
    if (!sqe)
        throw std::runtime_error("io_uring is full while trying to read journal");
    data = ((ring_data_t*)sqe->user_data);
    data->iov = { journal_buffer, 512 };
    data->callback = simple_callback;
    my_uring_prep_readv(sqe, bs->journal.fd, &data->iov, 1, bs->journal.offset);
    bs->ringloop->submit();
    wait_count = 1;
resume_1:
    if (wait_count > 0)
    {
        wait_state = 1;
        return 1;
    }
    if (iszero((uint64_t*)journal_buffer, 3))
    {
        // Journal is empty
        // FIXME handle this wrapping to 512 better
        bs->journal.used_start = 512;
        bs->journal.next_free = 512;
        // Initialize journal "superblock" and the first block
        // Cool effect. Same operations result in journal replay.
        // FIXME: Randomize initial crc32. Track crc32 when trimming.
        GET_SQE();
        memset(journal_buffer, 0, 1024);
        *((journal_entry_start*)journal_buffer) = {
            .crc32 = 0,
            .magic = JOURNAL_MAGIC,
            .type = JE_START,
            .size = sizeof(journal_entry_start),
            .reserved = 0,
            .journal_start = 512,
        };
        ((journal_entry_start*)journal_buffer)->crc32 = je_crc32((journal_entry*)journal_buffer);
        data->iov = (struct iovec){ journal_buffer, 1024 };
        data->callback = simple_callback;
        my_uring_prep_writev(sqe, bs->journal.fd, &data->iov, 1, bs->journal.offset);
        wait_count++;
        GET_SQE();
        my_uring_prep_fsync(sqe, bs->journal.fd, 0);
        data->iov = { 0 };
        data->callback = simple_callback;
        wait_count++;
        printf("Resetting journal\n");
        bs->ringloop->submit();
    resume_4:
        if (wait_count > 0)
        {
            wait_state = 4;
            return 1;
        }
    }
    else
    {
        // First block always contains a single JE_START entry
        je_start = (journal_entry_start*)journal_buffer;
        if (je_start->magic != JOURNAL_MAGIC ||
            je_start->type != JE_START ||
            je_start->size != sizeof(journal_entry_start) ||
            je_crc32((journal_entry*)je_start) != je_start->crc32)
        {
            // Entry is corrupt
            throw std::runtime_error("first entry of the journal is corrupt");
        }
        journal_pos = bs->journal.used_start = je_start->journal_start;
        crc32_last = 0;
        // Read journal
        while (1)
        {
        resume_2:
            if (submitted)
            {
                wait_state = 2;
                return 1;
            }
            if (!wrapped || journal_pos < bs->journal.used_start)
            {
                GET_SQE();
                uint64_t end = bs->journal.len;
                if (journal_pos < bs->journal.used_start)
                {
                    end = bs->journal.used_start;
                }
                data->iov = {
                    journal_buffer + (done_buf == 1 ? JOURNAL_BUFFER_SIZE : 0),
                    end - journal_pos < JOURNAL_BUFFER_SIZE ? end - journal_pos : JOURNAL_BUFFER_SIZE,
                };
                data->callback = [this](ring_data_t *data1) { handle_event(data1); };
                my_uring_prep_readv(sqe, bs->journal.fd, &data->iov, 1, bs->journal.offset + journal_pos);
                bs->ringloop->submit();
                submitted = done_buf == 1 ? 2 : 1;
            }
            if (done_buf && handle_journal_part(journal_buffer + (done_buf == 1 ? 0 : JOURNAL_BUFFER_SIZE), done_len) == 0)
            {
                // journal ended. wait for the next read to complete, then stop
            resume_3:
                if (submitted)
                {
                    wait_state = 3;
                    return 1;
                }
            }
            if (!submitted)
            {
                break;
            }
        }
    }
    printf("Journal entries loaded: %d\n", entries_loaded);
    free(journal_buffer);
    bs->journal.crc32_last = crc32_last;
    journal_buffer = NULL;
    return 0;
}

int blockstore_init_journal::handle_journal_part(void *buf, uint64_t len)
{
    uint64_t buf_pos = 0;
    if (cur_skip >= 0)
    {
        buf_pos = cur_skip;
        cur_skip = 0;
    }
    while (buf_pos < len)
    {
        uint64_t proc_pos = buf_pos, pos = 0;
        buf_pos += 512;
        while (pos < 512)
        {
            journal_entry *je = (journal_entry*)((uint8_t*)buf + proc_pos + pos);
            if (je->magic != JOURNAL_MAGIC || je_crc32(je) != je->crc32 ||
                je->type < JE_SMALL_WRITE || je->type > JE_DELETE || started && je->crc32_prev != crc32_last)
            {
                if (pos == 0)
                {
                    // invalid entry in the beginning, this is definitely the end of the journal
                    bs->journal.next_free = done_pos + proc_pos;
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
                if (cur_skip > 0 || done_pos + buf_pos + je->small_write.len > bs->journal.len)
                {
                    // data continues from the beginning of the journal
                    if (buf_pos > len)
                    {
                        // if something is already skipped, skip everything until the end of the journal
                        buf_pos = bs->journal.len-done_pos;
                    }
                    location = 512 + cur_skip;
                    cur_skip += je->small_write.len;
                }
                else
                {
                    // data is right next
                    location = done_pos + buf_pos;
                    buf_pos += je->small_write.len;
                }
                if (location != je->small_write.data_offset)
                {
                    throw new std::runtime_error("BUG: calculated journal data offset != stored journal data offset");
                }
                obj_ver_id ov = {
                    .oid = je->small_write.oid,
                    .version = je->small_write.version,
                };
                printf("je_small_write oid=%lu:%lu ver=%lu offset=%u len=%u\n", ov.oid.inode, ov.oid.stripe, ov.version, je->small_write.offset, je->small_write.len);
                bs->dirty_db.emplace(ov, (dirty_entry){
                    .state = ST_J_SYNCED,
                    .flags = 0,
                    .location = location,
                    .offset = je->small_write.offset,
                    .len = je->small_write.len,
                    .journal_sector = proc_pos,
                });
                bs->journal.used_sectors[proc_pos]++;
                auto & unstab = bs->unstable_writes[ov.oid];
                unstab = !unstab || unstab > ov.version ? ov.version : unstab;
            }
            else if (je->type == JE_BIG_WRITE)
            {
                // oid, version, block
                obj_ver_id ov = {
                    .oid = je->big_write.oid,
                    .version = je->big_write.version,
                };
                printf("je_big_write oid=%lu:%lu ver=%lu\n", ov.oid.inode, ov.oid.stripe, ov.version);
                bs->dirty_db.emplace(ov, (dirty_entry){
                    .state = ST_D_META_SYNCED,
                    .flags = 0,
                    .location = je->big_write.location,
                    .offset = 0,
                    .len = bs->block_size,
                    .journal_sector = proc_pos,
                });
                bs->journal.used_sectors[proc_pos]++;
                auto & unstab = bs->unstable_writes[ov.oid];
                unstab = !unstab || unstab > ov.version ? ov.version : unstab;
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
                    printf("je_stable oid=%lu:%lu ver=%lu\n", ov.oid.inode, ov.oid.stripe, ov.version);
                    while (1)
                    {
                        it->second.state = (it->second.state == ST_D_META_SYNCED
                            ? ST_D_STABLE
                            : (it->second.state == ST_DEL_SYNCED ? ST_DEL_STABLE : ST_J_STABLE));
                        if (it == bs->dirty_db.begin())
                            break;
                        it--;
                        if (it->first.oid != ov.oid || IS_STABLE(it->second.state))
                            break;
                    }
                    bs->flusher->queue_flush(ov);
                }
                auto unstab_it = bs->unstable_writes.find(ov.oid);
                if (unstab_it != bs->unstable_writes.end() && unstab_it->second <= ov.version)
                {
                    bs->unstable_writes.erase(unstab_it);
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
                    .journal_sector = proc_pos,
                });
                bs->journal.used_sectors[proc_pos]++;
            }
            entries_loaded++;
        }
    }
    if (buf_pos > len)
    {
        cur_skip = buf_pos - len;
    }
    return 1;
}
