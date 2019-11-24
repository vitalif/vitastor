#include "blockstore.h"

journal_flusher_t::journal_flusher_t(int flusher_count, blockstore *bs)
{
    this->bs = bs;
    this->flusher_count = flusher_count;
    active_flushers = 0;
    active_until_sync = 0;
    sync_required = true;
    sync_threshold = flusher_count == 1 ? 1 : flusher_count/2;
    journal_trim_interval = 1;//sync_threshold; //FIXME
    journal_trim_counter = 0;
    journal_superblock = (uint8_t*)memalign(512, 512);
    co = new journal_flusher_co[flusher_count];
    for (int i = 0; i < flusher_count; i++)
    {
        co[i].bs = bs;
        co[i].flusher = this;
    }
}

journal_flusher_co::journal_flusher_co()
{
    wait_state = 0;
    simple_callback = [this](ring_data_t* data)
    {
        if (data->res != data->iov.iov_len)
        {
            throw std::runtime_error(
                "write operation failed ("+std::to_string(data->res)+" != "+std::to_string(data->iov.iov_len)+
                "). in-memory state is corrupted. AAAAAAAaaaaaaaaa!!!111"
            );
        }
        wait_count--;
    };
}

journal_flusher_t::~journal_flusher_t()
{
    free(journal_superblock);
    delete[] co;
}

bool journal_flusher_t::is_active()
{
    return active_flushers > 0 || flush_queue.size() > 0;
}

void journal_flusher_t::loop()
{
    for (int i = 0; i < flusher_count; i++)
    {
        if (!active_flushers && !flush_queue.size())
        {
            return;
        }
        co[i].loop();
    }
}

void journal_flusher_t::queue_flush(obj_ver_id ov)
{
    auto it = flush_versions.find(ov.oid);
    if (it != flush_versions.end())
    {
        it->second = ov.version;
    }
    else
    {
        flush_versions[ov.oid] = ov.version;
        flush_queue.push_back(ov.oid);
    }
}

void journal_flusher_t::unshift_flush(obj_ver_id ov)
{
    auto it = flush_versions.find(ov.oid);
    if (it != flush_versions.end())
    {
        it->second = ov.version;
    }
    else
    {
        flush_versions[ov.oid] = ov.version;
        flush_queue.push_front(ov.oid);
    }
}

#define await_sqe(label) \
    resume_##label:\
        sqe = bs->get_sqe();\
        if (!sqe)\
        {\
            wait_state = label;\
            return;\
        }\
        data = ((ring_data_t*)sqe->user_data);

void journal_flusher_co::loop()
{
    // This is much better than implementing the whole function as an FSM
    // Maybe I should consider a coroutine library like https://github.com/hnes/libaco ...
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
    else if (wait_state == 6)
        goto resume_6;
    else if (wait_state == 7)
        goto resume_7;
    else if (wait_state == 8)
        goto resume_8;
    else if (wait_state == 9)
        goto resume_9;
    else if (wait_state == 10)
        goto resume_10;
    else if (wait_state == 11)
        goto resume_11;
    else if (wait_state == 12)
        goto resume_12;
    else if (wait_state == 13)
        goto resume_13;
resume_0:
    if (!flusher->flush_queue.size())
        return;
    cur.oid = flusher->flush_queue.front();
    cur.version = flusher->flush_versions[cur.oid];
    flusher->flush_queue.pop_front();
    flusher->flush_versions.erase(cur.oid);
    dirty_end = bs->dirty_db.find(cur);
    if (dirty_end != bs->dirty_db.end())
    {
        repeat_it = flusher->sync_to_repeat.find(cur.oid);
        if (repeat_it != flusher->sync_to_repeat.end())
        {
            // We don't flush different parts of history of the same object in parallel
            // So we check if someone is already flushing this object
            // In that case we set sync_to_repeat to 2 and pick another object
            // Another coroutine will see this "2" and re-queue the object after it finishes
            repeat_it->second = cur.version;
            wait_state = 0;
            goto resume_0;
        }
        else
            repeat_it->second = 0;
        dirty_it = dirty_end;
        flusher->active_flushers++;
        flusher->active_until_sync++;
        v.clear();
        wait_count = 0;
        clean_loc = UINT64_MAX;
        skip_copy = false;
        while (1)
        {
            if (dirty_it->second.state == ST_J_STABLE && !skip_copy)
            {
                // First we submit all reads
                offset = dirty_it->second.offset;
                len = dirty_it->second.len;
                it = v.begin();
                while (1)
                {
                    for (; it != v.end(); it++)
                        if (it->offset >= offset)
                            break;
                    if (it == v.end() || it->offset > offset)
                    {
                        submit_len = it == v.end() || it->offset >= offset+len ? len : it->offset-offset;
                        await_sqe(1);
                        it = v.insert(it, (copy_buffer_t){ .offset = offset, .len = submit_len, .buf = memalign(512, submit_len) });
                        data->iov = (struct iovec){ v.back().buf, (size_t)submit_len };
                        data->callback = simple_callback;
                        my_uring_prep_readv(
                            sqe, bs->journal.fd, &data->iov, 1, bs->journal.offset + dirty_it->second.location + offset - dirty_it->second.offset
                        );
                        wait_count++;
                    }
                    if (it == v.end() || it->offset+it->len >= offset+len)
                    {
                        break;
                    }
                }
            }
            else if (dirty_it->second.state == ST_D_STABLE)
            {
                // There is an unflushed big write. Copy small writes in its position
                if (!skip_copy)
                {
                    clean_loc = dirty_it->second.location;
                }
                skip_copy = true;
            }
            else if (!IS_STABLE(dirty_it->second.state))
            {
                throw std::runtime_error("BUG: Unexpected dirty_entry state during flush: " + std::to_string(dirty_it->second.state));
            }
            if (dirty_it == bs->dirty_db.begin())
            {
                break;
            }
            dirty_it--;
            if (dirty_it->first.oid != cur.oid)
            {
                break;
            }
        }
        if (wait_count == 0 && clean_loc == UINT64_MAX)
        {
            // Nothing to flush
            flusher->active_flushers--;
            flusher->active_until_sync--;
            repeat_it = flusher->sync_to_repeat.find(cur.oid);
            if (repeat_it->second != 0)
            {
                // Requeue version
                flusher->unshift_flush({ .oid = cur.oid, .version = repeat_it->second });
            }
            flusher->sync_to_repeat.erase(repeat_it);
            wait_state = 0;
            goto resume_0;
        }
        if (clean_loc == UINT64_MAX)
        {
            // Find it in clean_db
            clean_it = bs->clean_db.find(cur.oid);
            if (clean_it == bs->clean_db.end())
            {
                // Object not present at all. This is a bug.
                throw std::runtime_error("BUG: Object we are trying to flush is not allocated on the data device");
            }
            else
                clean_loc = clean_it->second.location;
        }
        else
            clean_it = bs->clean_db.end();
        // Also we need to submit the metadata read. We do a read-modify-write for every operation.
        // But we must check if the same sector is already in memory.
        // Another option is to keep all raw metadata in memory all the time. Maybe I'll do it sometime...
        // And yet another option is to use LSM trees for metadata, but it sophisticates everything a lot,
        // so I'll avoid it as long as I can.
        meta_sector = ((clean_loc >> bs->block_order) / (512 / sizeof(clean_disk_entry))) * 512;
        meta_pos = ((clean_loc >> bs->block_order) % (512 / sizeof(clean_disk_entry)));
        meta_it = flusher->meta_sectors.find(meta_sector);
        if (meta_it == flusher->meta_sectors.end())
        {
            // Not in memory yet, read it
            meta_it = flusher->meta_sectors.emplace(meta_sector, (meta_sector_t){
                .offset = meta_sector,
                .len = 512,
                .state = 0, // 0 = not read yet
                .buf = memalign(512, 512),
                .usage_count = 1,
            }).first;
            await_sqe(2);
            data->iov = (struct iovec){ meta_it->second.buf, 512 };
            data->callback = [this](ring_data_t* data)
            {
                if (data->res != data->iov.iov_len)
                {
                    throw std::runtime_error(
                        "write operation failed ("+std::to_string(data->res)+" != "+std::to_string(data->iov.iov_len)+
                        "). in-memory state is corrupted. AAAAAAAaaaaaaaaa!!!111"
                    );
                }
                meta_it->second.state = 1;
                wait_count--;
            };
            my_uring_prep_readv(
                sqe, bs->meta_fd, &data->iov, 1, bs->meta_offset + meta_sector
            );
            wait_count++;
        }
        else
            meta_it->second.usage_count++;
    resume_3:
        if (wait_count > 0)
        {
            wait_state = 3;
            return;
        }
        // Reads completed, submit writes
        for (it = v.begin(); it != v.end(); it++)
        {
            await_sqe(4);
            data->iov = (struct iovec){ it->buf, (size_t)it->len };
            data->callback = simple_callback;
            my_uring_prep_writev(
                sqe, bs->data_fd, &data->iov, 1, bs->data_offset + clean_loc + it->offset
            );
            wait_count++;
        }
        // And a metadata write
    resume_5:
        if (meta_it->second.state == 0)
        {
            // metadata sector is still being read, wait for it
            wait_state = 5;
            return;
        }
        ((clean_disk_entry*)meta_it->second.buf)[meta_pos] = {
            .oid = cur.oid,
            .version = cur.version,
        };
        // I consider unordered writes to data & metadata safe here
        // BUT it requires that journal entries even older than clean_db are replayed after restart
        await_sqe(6);
        data->iov = (struct iovec){ meta_it->second.buf, 512 };
        data->callback = simple_callback;
        my_uring_prep_writev(
            sqe, bs->meta_fd, &data->iov, 1, bs->meta_offset + meta_sector
        );
        wait_count++;
    resume_7:
        if (wait_count > 0)
        {
            wait_state = 7;
            return;
        }
        // Done, free all buffers
        meta_it->second.usage_count--;
        if (meta_it->second.usage_count == 0)
        {
            free(meta_it->second.buf);
            flusher->meta_sectors.erase(meta_it);
        }
        for (it = v.begin(); it != v.end(); it++)
        {
            free(it->buf);
        }
        v.clear();
        flusher->active_until_sync--;
        if (flusher->sync_required)
        {
            // And sync everything (in batches - not per each operation!)
            cur_sync = flusher->syncs.end();
            if (cur_sync == flusher->syncs.begin())
                cur_sync = flusher->syncs.emplace(flusher->syncs.end(), (flusher_sync_t){ .ready_count = 0, .state = 0 });
            else
                cur_sync--;
            cur_sync->ready_count++;
            if (cur_sync->ready_count >= flusher->sync_threshold ||
                !flusher->active_until_sync && !flusher->flush_queue.size())
            {
                // Sync batch is ready. Do it.
                await_sqe(9);
                data->callback = simple_callback;
                data->iov = { 0 };
                my_uring_prep_fsync(sqe, bs->data_fd, 0);
                wait_count++;
                if (bs->meta_fd != bs->data_fd)
                {
                    await_sqe(10);
                    data->callback = simple_callback;
                    data->iov = { 0 };
                    my_uring_prep_fsync(sqe, bs->meta_fd, 0);
                    wait_count++;
                }
                wait_state = 11;
            resume_11:
                if (wait_count > 0)
                    return;
                // Sync completed. All previous coroutines waiting for it must be resumed
                cur_sync->state = 1;
            }
            // Wait until someone else sends and completes a sync.
        resume_8:
            if (!cur_sync->state)
            {
                wait_state = 8;
                return;
            }
            cur_sync->ready_count--;
            if (cur_sync->ready_count == 0)
            {
                flusher->syncs.erase(cur_sync);
            }
        }
        // Update clean_db and dirty_db, free old data locations
        if (clean_it != bs->clean_db.end() && clean_it->second.location != clean_loc)
        {
            allocator_set(bs->data_alloc, clean_it->second.location >> bs->block_order, false);
        }
        bs->clean_db[cur.oid] = {
            .version = cur.version,
            .location = clean_loc,
        };
        dirty_it = dirty_end;
        while (1)
        {
            if (IS_BIG_WRITE(dirty_it->second.state) && dirty_it->second.location != clean_loc)
            {
                allocator_set(bs->data_alloc, dirty_it->second.location >> bs->block_order, false);
            }
            int used = --bs->journal.used_sectors[dirty_it->second.journal_sector];
            if (used == 0)
            {
                bs->journal.used_sectors.erase(dirty_it->second.journal_sector);
            }
            if (dirty_it == bs->dirty_db.begin())
            {
                break;
            }
            dirty_it--;
            if (dirty_it->first.oid != cur.oid)
            {
                break;
            }
        }
        // Then, basically, remove everything up to the current version from dirty_db...
        if (dirty_it->first.oid != cur.oid)
            dirty_it++;
        bs->dirty_db.erase(dirty_it, std::next(dirty_end));
        // Clear unused part of the journal every <journal_trim_interval> flushes
        if (!((++flusher->journal_trim_counter) % flusher->journal_trim_interval))
        {
            flusher->journal_trim_counter = 0;
            journal_used_it = bs->journal.used_sectors.lower_bound(bs->journal.used_start);
            if (journal_used_it == bs->journal.used_sectors.end())
            {
                // Journal is cleared to its end, restart from the beginning
                journal_used_it = bs->journal.used_sectors.begin();
                if (journal_used_it == bs->journal.used_sectors.end())
                {
                    // Journal is empty
                    bs->journal.used_start = bs->journal.next_free;
                }
                else
                {
                    bs->journal.used_start = journal_used_it->first;
                    // next_free does not need updating here
                }
            }
            else if (journal_used_it->first > bs->journal.used_start)
            {
                // Journal is cleared up to <journal_used_it>
                bs->journal.used_start = journal_used_it->first;
            }
            else
            {
                // Can't trim journal
                goto do_not_trim;
            }
            // Update journal "superblock"
            await_sqe(12);
            data->callback = simple_callback;
            *((journal_entry_start*)flusher->journal_superblock) = {
                .crc32 = 0,
                .magic = JOURNAL_MAGIC,
                .type = JE_START,
                .size = sizeof(journal_entry_start),
                .reserved = 0,
                .journal_start = bs->journal.used_start,
            };
            ((journal_entry_start*)flusher->journal_superblock)->crc32 = je_crc32((journal_entry*)flusher->journal_superblock);
            data->iov = (struct iovec){ flusher->journal_superblock, 512 };
            my_uring_prep_writev(sqe, bs->journal.fd, &data->iov, 1, bs->journal.offset);
            wait_count++;
        resume_13:
            if (wait_count > 0)
            {
                wait_state = 13;
                return;
            }
        }
    do_not_trim:
        // All done
        wait_state = 0;
        flusher->active_flushers--;
        repeat_it = flusher->sync_to_repeat.find(cur.oid);
        if (repeat_it->second != 0)
        {
            // Requeue version
            flusher->unshift_flush({ .oid = cur.oid, .version = repeat_it->second });
            flusher->sync_to_repeat.erase(repeat_it);
        }
        goto resume_0;
    }
}
