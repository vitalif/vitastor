#include "blockstore.h"

journal_flusher_t::journal_flusher_t(int flusher_count, blockstore *bs)
{
    this->bs = bs;
    this->flusher_count = flusher_count;
    this->active_flushers = 0;
    this->active_until_sync = 0;
    this->sync_required = true;
    this->sync_threshold = flusher_count == 1 ? 1 : flusher_count/2;
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
        if (data->res < 0)
        {
            throw new std::runtime_error("write operation failed. in-memory state is corrupted. AAAAAAAaaaaaaaaa!!!111");
        }
        wait_count--;
    };
}

journal_flusher_t::~journal_flusher_t()
{
    delete[] co;
}

void journal_flusher_t::loop()
{
    if (!active_flushers && !flush_queue.size())
    {
        return;
    }
    for (int i = 0; i < flusher_count; i++)
    {
        co[i].loop();
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
resume_0:
    if (!flusher->flush_queue.size())
        return;
    cur = flusher->flush_queue.front();
    flusher->flush_queue.pop_front();
    dirty_it = bs->dirty_db.find(cur);
    if (dirty_it != bs->dirty_db.end())
    {
        flusher->active_flushers++;
        flusher->active_until_sync++;
        v.clear();
        wait_count = 0;
        clean_loc = UINT64_MAX;
        skip_copy = false;
        do
        {
            if (dirty_it->second.state == ST_J_STABLE)
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
                        submit_len = it->offset >= offset+len ? len : it->offset-offset;
                        await_sqe(1);
                        v.insert(it, (copy_buffer_t){ .offset = offset, .len = submit_len, .buf = memalign(512, submit_len) });
                        data->iov = (struct iovec){ v.back().buf, (size_t)submit_len };
                        data->callback = simple_callback;
                        io_uring_prep_readv(
                            sqe, bs->journal.fd, &data->iov, 1, bs->journal.offset + dirty_it->second.location + offset
                        );
                        wait_count++;
                    }
                    if (it == v.end() || it->offset+it->len >= offset+len)
                    {
                        break;
                    }
                }
                // So subsequent stabilizers don't flush the entry again
                dirty_it->second.state = ST_J_MOVE_READ_SUBMITTED;
            }
            else if (dirty_it->second.state == ST_D_STABLE)
            {
                // Copy last STABLE entry metadata
                if (!skip_copy)
                {
                    clean_loc = dirty_it->second.location;
                }
                skip_copy = true;
            }
            else if (IS_STABLE(dirty_it->second.state))
            {
                break;
            }
            dirty_it--;
        } while (dirty_it != bs->dirty_db.begin() && dirty_it->first.oid == cur.oid);
        if (clean_loc == UINT64_MAX)
        {
            // Find it in clean_db
            auto clean_it = bs->clean_db.find(cur.oid);
            if (clean_it == bs->clean_db.end())
            {
                // Object not present at all. This is a bug.
                throw new std::runtime_error("BUG: Object we are trying to flush not allocated on the data device");
            }
            else
                clean_loc = clean_it->second.location;
        }
        // Also we need to submit the metadata read. We do a read-modify-write for every operation.
        // But we must check if the same sector is already in memory.
        // Another option is to keep all raw metadata in memory all the time. Maybe I'll do it sometime...
        // And yet another option is to use LSM trees for metadata, but it sophisticates everything a lot,
        // so I'll avoid it as long as I can.
        meta_sector = (clean_loc / (512 / sizeof(clean_disk_entry))) * 512;
        meta_pos = (clean_loc % (512 / sizeof(clean_disk_entry)));
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
                if (data->res < 0)
                {
                    throw new std::runtime_error("write operation failed. in-memory state is corrupted. AAAAAAAaaaaaaaaa!!!111");
                }
                meta_it->second.state = 1;
                wait_count--;
            };
            io_uring_prep_writev(
                sqe, bs->meta_fd, &data->iov, 1, bs->meta_offset + meta_sector
            );
            wait_count++;
        }
        else
            meta_it->second.usage_count++;
        wait_state = 3;
    resume_3:
        if (wait_count > 0)
            return;
        // Reads completed, submit writes
        for (it = v.begin(); it != v.end(); it++)
        {
            await_sqe(4);
            data->iov = (struct iovec){ it->buf, (size_t)it->len };
            data->callback = simple_callback;
            io_uring_prep_writev(
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
        *((clean_disk_entry*)meta_it->second.buf + meta_pos) = {
            .oid = cur.oid,
            .version = cur.version,
        };
        // I consider unordered writes to data & metadata safe here, because
        // "dirty" entries always override "clean" entries in our case
        await_sqe(6);
        data->iov = (struct iovec){ meta_it->second.buf, 512 };
        data->callback = simple_callback;
        io_uring_prep_writev(
            sqe, bs->meta_fd, &data->iov, 1, bs->meta_offset + meta_sector
        );
        wait_count++;
        wait_state = 7;
    resume_7:
        if (wait_count > 0)
            return;
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
                io_uring_prep_fsync(sqe, bs->data_fd, 0);
                wait_count++;
                if (bs->meta_fd != bs->data_fd)
                {
                    await_sqe(10);
                    data->callback = simple_callback;
                    io_uring_prep_fsync(sqe, bs->meta_fd, 0);
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
        wait_state = 0;
        flusher->active_flushers--;
        goto resume_0;
    }
}
