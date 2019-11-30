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
    printf("Reading blockstore metadata\n");
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
    printf("Metadata entries loaded: %lu, free blocks: %lu / %lu\n", entries_loaded, bs->data_alloc->get_free_count(), bs->block_count);
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
#ifdef BLOCKSTORE_DEBUG
                    printf("Free block %lu\n", clean_it->second.location >> bs->block_order);
#endif
                    bs->data_alloc->set(clean_it->second.location >> block_order, false);
                }
                entries_loaded++;
#ifdef BLOCKSTORE_DEBUG
                printf("Allocate block (clean entry) %lu: %lu:%lu v%lu\n", done_cnt+i, entries[i].oid.inode, entries[i].oid.stripe, entries[i].version);
#endif
                bs->data_alloc->set(done_cnt+i, true);
                bs->clean_db[entries[i].oid] = (struct clean_entry){
                    .version = entries[i].version,
                    .location = (done_cnt+i) << block_order,
                };
            }
#ifdef BLOCKSTORE_DEBUG
            else
                printf("Old clean entry %lu: %lu:%lu v%lu\n", done_cnt+i, entries[i].oid.inode, entries[i].oid.stripe, entries[i].version);
#endif
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
    if (data1->res <= 0)
    {
        throw std::runtime_error(
            std::string("read journal failed at offset ") + std::to_string(journal_pos) +
            std::string(": ") + strerror(-data1->res)
        );
    }
    done.push_back({
        .buf = submitted_buf,
        .pos = journal_pos,
        .len = (uint64_t)data1->res,
    });
    journal_pos += data1->res;
    if (journal_pos >= bs->journal.len)
    {
        // Continue from the beginning
        journal_pos = 512;
        wrapped = true;
    }
    submitted_buf = NULL;
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
    else if (wait_state == 5)
        goto resume_5;
    printf("Reading blockstore journal\n");
    if (!bs->journal.inmemory)
    {
        submitted_buf = memalign(512, 1024);
        if (!submitted_buf)
            throw std::bad_alloc();
    }
    else
        submitted_buf = bs->journal.buffer;
    // Read first block of the journal
    sqe = bs->get_sqe();
    if (!sqe)
        throw std::runtime_error("io_uring is full while trying to read journal");
    data = ((ring_data_t*)sqe->user_data);
    data->iov = { submitted_buf, 512 };
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
    if (iszero((uint64_t*)submitted_buf, 3))
    {
        // Journal is empty
        // FIXME handle this wrapping to 512 better
        bs->journal.used_start = 512;
        bs->journal.next_free = 512;
        // Initialize journal "superblock" and the first block
        memset(submitted_buf, 0, 1024);
        *((journal_entry_start*)submitted_buf) = {
            .crc32 = 0,
            .magic = JOURNAL_MAGIC,
            .type = JE_START,
            .size = sizeof(journal_entry_start),
            .reserved = 0,
            .journal_start = 512,
        };
        ((journal_entry_start*)submitted_buf)->crc32 = je_crc32((journal_entry*)submitted_buf);
        if (bs->readonly)
        {
            printf("Skipping journal initialization because blockstore is readonly\n");
        }
        else
        {
            // Cool effect. Same operations result in journal replay.
            // FIXME: Randomize initial crc32. Track crc32 when trimming.
            GET_SQE();
            data->iov = (struct iovec){ submitted_buf, 1024 };
            data->callback = simple_callback;
            my_uring_prep_writev(sqe, bs->journal.fd, &data->iov, 1, bs->journal.offset);
            wait_count++;
            GET_SQE();
            my_uring_prep_fsync(sqe, bs->journal.fd, IORING_FSYNC_DATASYNC);
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
        if (!bs->journal.inmemory)
        {
            free(submitted_buf);
        }
    }
    else
    {
        // First block always contains a single JE_START entry
        je_start = (journal_entry_start*)submitted_buf;
        if (je_start->magic != JOURNAL_MAGIC ||
            je_start->type != JE_START ||
            je_start->size != sizeof(journal_entry_start) ||
            je_crc32((journal_entry*)je_start) != je_start->crc32)
        {
            // Entry is corrupt
            throw std::runtime_error("first entry of the journal is corrupt");
        }
        next_free = journal_pos = bs->journal.used_start = je_start->journal_start;
        if (!bs->journal.inmemory)
            free(submitted_buf);
        submitted_buf = NULL;
        crc32_last = 0;
        // Read journal
        while (1)
        {
        resume_2:
            if (submitted_buf)
            {
                wait_state = 2;
                return 1;
            }
            if (!wrapped || journal_pos < bs->journal.used_start)
            {
                GET_SQE();
                uint64_t end = bs->journal.len;
                if (journal_pos < bs->journal.used_start)
                    end = bs->journal.used_start;
                if (!bs->journal.inmemory)
                    submitted_buf = memalign(512, JOURNAL_BUFFER_SIZE);
                else
                    submitted_buf = bs->journal.buffer + journal_pos;
                data->iov = {
                    submitted_buf,
                    end - journal_pos < JOURNAL_BUFFER_SIZE ? end - journal_pos : JOURNAL_BUFFER_SIZE,
                };
                data->callback = [this](ring_data_t *data1) { handle_event(data1); };
                my_uring_prep_readv(sqe, bs->journal.fd, &data->iov, 1, bs->journal.offset + journal_pos);
                bs->ringloop->submit();
            }
            while (done.size() > 0)
            {
                handle_res = handle_journal_part(done[0].buf, done[0].pos, done[0].len);
                if (handle_res == 0)
                {
                    // journal ended
                    // zero out corrupted entry, if required
                    if (init_write_buf && !bs->readonly)
                    {
                        GET_SQE();
                        data->iov = { init_write_buf, 512 };
                        data->callback = simple_callback;
                        wait_count++;
                        my_uring_prep_writev(sqe, bs->journal.fd, &data->iov, 1, bs->journal.offset + init_write_sector);
                        GET_SQE();
                        data->iov = { 0 };
                        data->callback = simple_callback;
                        wait_count++;
                        my_uring_prep_fsync(sqe, bs->journal.fd, IORING_FSYNC_DATASYNC);
                        bs->ringloop->submit();
                    resume_5:
                        if (wait_count > 0)
                        {
                            wait_state = 5;
                            return 1;
                        }
                    }
                    // wait for the next read to complete, then stop
                resume_3:
                    if (submitted_buf)
                    {
                        wait_state = 3;
                        return 1;
                    }
                    // free buffers
                    if (!bs->journal.inmemory)
                        for (auto & e: done)
                            free(e.buf);
                    done.clear();
                    break;
                }
                else if (handle_res == 1)
                {
                    // OK, remove it
                    if (!bs->journal.inmemory)
                    {
                        free(done[0].buf);
                    }
                    done.erase(done.begin());
                }
                else if (handle_res == 2)
                {
                    // Need to wait for more reads
                    break;
                }
            }
            if (!submitted_buf)
            {
                break;
            }
        }
    }
    // Trim journal on start so we don't stall when all entries are older
    bs->journal.trim();
    printf("Journal entries loaded: %lu, free blocks: %lu / %lu\n", entries_loaded, bs->data_alloc->get_free_count(), bs->block_count);
    bs->journal.crc32_last = crc32_last;
    return 0;
}

int blockstore_init_journal::handle_journal_part(void *buf, uint64_t done_pos, uint64_t len)
{
    uint64_t proc_pos, pos;
    if (continue_pos != 0)
    {
        proc_pos = (continue_pos / 512) * 512;
        pos = continue_pos % 512;
        continue_pos = 0;
        goto resume;
    }
    while (next_free >= done_pos && next_free < done_pos+len)
    {
        proc_pos = next_free;
        pos = 0;
        next_free += 512;
        if (next_free >= bs->journal.len)
        {
            next_free = 512;
        }
    resume:
        while (pos < 512)
        {
            journal_entry *je = (journal_entry*)((uint8_t*)buf + proc_pos - done_pos + pos);
            if (je->magic != JOURNAL_MAGIC || je_crc32(je) != je->crc32 ||
                je->type < JE_SMALL_WRITE || je->type > JE_DELETE || started && je->crc32_prev != crc32_last)
            {
                if (pos == 0)
                {
                    // invalid entry in the beginning, this is definitely the end of the journal
                    bs->journal.next_free = next_free;
                    return 0;
                }
                else
                {
                    // allow partially filled sectors
                    break;
                }
            }
            if (je->type == JE_SMALL_WRITE)
            {
#ifdef BLOCKSTORE_DEBUG
                printf("je_small_write oid=%lu:%lu ver=%lu offset=%u len=%u\n", je->small_write.oid.inode, je->small_write.oid.stripe, je->small_write.version, je->small_write.offset, je->small_write.len);
#endif
                // oid, version, offset, len
                uint64_t prev_free = next_free;
                if (next_free + je->small_write.len > bs->journal.len)
                {
                    // data continues from the beginning of the journal
                    next_free = 512;
                }
                uint64_t location = next_free;
                next_free += je->small_write.len;
                if (next_free >= bs->journal.len)
                {
                    next_free = 512;
                }
                if (location != je->small_write.data_offset)
                {
                    char err[1024];
                    snprintf(err, 1024, "BUG: calculated journal data offset (%lu) != stored journal data offset (%lu)", location, je->small_write.data_offset);
                    throw std::runtime_error(err);
                }
                uint32_t data_crc32 = 0;
                if (location >= done_pos && location+je->small_write.len <= done_pos+len)
                {
                    // data is within this buffer
                    data_crc32 = crc32c(0, buf + location - done_pos, je->small_write.len);
                }
                else
                {
                    // this case is even more interesting because we must carry data crc32 check to next buffer(s)
                    uint64_t covered = 0;
                    for (int i = 0; i < done.size(); i++)
                    {
                        if (location+je->small_write.len > done[i].pos &&
                            location < done[i].pos+done[i].len)
                        {
                            uint64_t part_end = (location+je->small_write.len < done[i].pos+done[i].len
                                ? location+je->small_write.len : done[i].pos+done[i].len);
                            uint64_t part_begin = (location < done[i].pos ? done[i].pos : location);
                            covered += part_end - part_begin;
                            data_crc32 = crc32c(data_crc32, done[i].buf + part_begin - done[i].pos, part_end - part_begin);
                        }
                    }
                    if (covered < je->small_write.len)
                    {
                        continue_pos = proc_pos+pos;
                        next_free = prev_free;
                        return 2;
                    }
                }
                if (data_crc32 != je->small_write.crc32_data)
                {
                    // journal entry is corrupt, stop here
                    // interesting thing is that we must clear the corrupt entry if we're not readonly
                    memset(buf + proc_pos - done_pos + pos, 0, 512 - pos);
                    bs->journal.next_free = prev_free;
                    init_write_buf = buf + proc_pos - done_pos;
                    init_write_sector = proc_pos;
                    return 0;
                }
                auto clean_it = bs->clean_db.find(je->small_write.oid);
                if (clean_it == bs->clean_db.end() ||
                    clean_it->second.version < je->big_write.version)
                {
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
                        .journal_sector = proc_pos,
                    });
                    bs->journal.used_sectors[proc_pos]++;
#ifdef BLOCKSTORE_DEBUG
                    printf("journal offset %lu is used by %lu:%lu v%lu\n", proc_pos, ov.oid.inode, ov.oid.stripe, ov.version);
#endif
                    auto & unstab = bs->unstable_writes[ov.oid];
                    unstab = unstab < ov.version ? ov.version : unstab;
                }
            }
            else if (je->type == JE_BIG_WRITE)
            {
#ifdef BLOCKSTORE_DEBUG
                printf("je_big_write oid=%lu:%lu ver=%lu\n", je->big_write.oid.inode, je->big_write.oid.stripe, je->big_write.version);
#endif
                auto clean_it = bs->clean_db.find(je->big_write.oid);
                if (clean_it == bs->clean_db.end() ||
                    clean_it->second.version < je->big_write.version)
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
                        .journal_sector = proc_pos,
                    });
#ifdef BLOCKSTORE_DEBUG
                    printf("Allocate block %lu\n", je->big_write.location >> bs->block_order);
#endif
                    bs->data_alloc->set(je->big_write.location >> bs->block_order, true);
                    bs->journal.used_sectors[proc_pos]++;
                    auto & unstab = bs->unstable_writes[ov.oid];
                    unstab = unstab < ov.version ? ov.version : unstab;
                }
            }
            else if (je->type == JE_STABLE)
            {
#ifdef BLOCKSTORE_DEBUG
                printf("je_stable oid=%lu:%lu ver=%lu\n", je->stable.oid.inode, je->stable.oid.stripe, je->stable.version);
#endif
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
                    // skip it
                }
                else
                {
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
                    bs->flusher->enqueue_flush(ov);
                }
                auto unstab_it = bs->unstable_writes.find(ov.oid);
                if (unstab_it != bs->unstable_writes.end() && unstab_it->second <= ov.version)
                {
                    bs->unstable_writes.erase(unstab_it);
                }
            }
            else if (je->type == JE_DELETE)
            {
#ifdef BLOCKSTORE_DEBUG
                printf("je_delete oid=%lu:%lu ver=%lu\n", je->del.oid.inode, je->del.oid.stripe, je->del.version);
#endif
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
            started = true;
            pos += je->size;
            crc32_last = je->crc32;
            entries_loaded++;
        }
    }
    bs->journal.next_free = next_free;
    return 1;
}
