// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_impl.h"
#include "blockstore_internal.h"
#include "crc32c.h"
#include "allocator.h"

#define META_BLOCK_UNREAD 0
#define META_BLOCK_READ 1

// FIXME rename to compactor_t
journal_flusher_t::journal_flusher_t(blockstore_impl_t *bs)
{
    this->bs = bs;
    this->max_flusher_count = bs->max_flusher_count;
    this->min_flusher_count = bs->min_flusher_count;
    this->cur_flusher_count = bs->min_flusher_count;
    this->target_flusher_count = bs->min_flusher_count;
    active_flushers = 0;
    syncing_flushers = 0;
    advance_lsn_counter = 0;
    co = new journal_flusher_co[max_flusher_count];
    for (int i = 0; i < max_flusher_count; i++)
    {
        co[i].co_id = i;
        co[i].bs = bs;
        co[i].flusher = this;
        if (bs->dsk.csum_block_size)
            co[i].csum_buf = (uint8_t*)malloc_or_die(bs->dsk.data_block_size/bs->dsk.csum_block_size * (bs->dsk.data_csum_type & 0xFF));
    }
}

journal_flusher_co::journal_flusher_co()
{
    wait_state = 0;
    simple_callback_r = [this](ring_data_t* data)
    {
        bs->live = true;
        if (data->res != data->iov.iov_len)
            bs->disk_error_abort("read operation during flush", data->res, data->iov.iov_len);
        wait_count--;
    };
    simple_callback_w = [this](ring_data_t* data)
    {
        bs->live = true;
        if (data->res != data->iov.iov_len)
            bs->disk_error_abort("write operation during flush", data->res, data->iov.iov_len);
        wait_count--;
    };
}

journal_flusher_t::~journal_flusher_t()
{
    delete[] co;
}

journal_flusher_co::~journal_flusher_co()
{
    if (csum_buf)
    {
        free(csum_buf);
    }
}

int journal_flusher_t::get_active()
{
    return active_flushers;
}

uint64_t journal_flusher_t::get_counter()
{
    return compact_counter;
}

bool journal_flusher_t::is_active()
{
    return active_flushers > 0 || bs->heap->get_compact_queue_size() > (force_start > 0 ? 0 : bs->flusher_start_threshold);
}

void journal_flusher_t::request_trim()
{
    force_start++;
    bs->ringloop->wakeup();
}

void journal_flusher_t::release_trim()
{
    force_start--;
}

void journal_flusher_t::dump_diagnostics()
{
    printf(
        "Compaction queue: %u items, data: %ju/%ju blocks used, meta: %ju/%ju bytes used, %u/%ju blocks nearfull\n",
        bs->heap->get_compact_queue_size(),
        bs->heap->get_data_used_space()/bs->dsk.data_block_size, bs->dsk.block_count,
        bs->heap->get_meta_used_space(), bs->heap->get_meta_total_space(),
        bs->heap->get_meta_nearfull_blocks(), bs->dsk.meta_area_size/bs->dsk.meta_block_size-1
    );
}

void journal_flusher_t::loop()
{
    target_flusher_count = bs->write_iodepth*2;
    if (target_flusher_count < min_flusher_count)
        target_flusher_count = min_flusher_count;
    else if (target_flusher_count > max_flusher_count)
        target_flusher_count = max_flusher_count;
    if (target_flusher_count > cur_flusher_count)
        cur_flusher_count = target_flusher_count;
    else if (target_flusher_count < cur_flusher_count)
    {
        while (target_flusher_count < cur_flusher_count)
        {
            if (co[cur_flusher_count-1].wait_state)
                break;
            cur_flusher_count--;
        }
    }
    int prev_active = active_flushers;
    for (int i = 0; is_active() && i < cur_flusher_count; i++)
        co[i].loop();
    if (prev_active && !active_flushers && force_start > 0)
        bs->ringloop->wakeup();
}

#define await_sqe(label) \
    resume_##label:\
        sqe = bs->get_sqe();\
        if (!sqe)\
        {\
            wait_state = wait_base+label;\
            return false;\
        }\
        data = ((ring_data_t*)sqe->user_data);

bool journal_flusher_co::loop()
{
    int wait_base = 0;
    // This is much better than implementing the whole function as an FSM
    // Maybe I should consider a coroutine library like https://github.com/hnes/libaco ...
    // Or just C++ coroutines, but they require some wrappers
    if (wait_state == 1)       goto resume_1;
    else if (wait_state == 2)  goto resume_2;
    else if (wait_state == 3)  goto resume_3;
    else if (wait_state == 4)  goto resume_4;
    else if (wait_state == 5)  goto resume_5;
    else if (wait_state == 6)  goto resume_6;
    else if (wait_state == 7)  goto resume_7;
    else if (wait_state == 8)  goto resume_8;
    else if (wait_state == 9)  goto resume_9;
    else if (wait_state == 10) goto resume_10;
    else if (wait_state == 11) goto resume_11;
    else if (wait_state == 12) goto resume_12;
    else if (wait_state == 13) goto resume_13;
    else if (wait_state == 14) goto resume_14;
    else if (wait_state == 15) goto resume_15;
    else if (wait_state == 16) goto resume_16;
    else if (wait_state == 17) goto resume_17;
resume_0:
    res = bs->heap->get_next_compact(cur_oid);
    if (res == ENOENT)
    {
        cur_oid = {};
        wait_state = 0;
        return true;
    }
    for (int i = 0; i < flusher->cur_flusher_count; i++)
    {
        if (i != co_id && flusher->co[i].cur_oid == cur_oid)
        {
            // Already flushing it
            flusher->co[i].should_repeat = true;
            goto resume_0;
        }
    }
resume_1:
    should_repeat = false;
    cur_obj = bs->heap->lock_and_read_entry(cur_oid, copy_id);
    if (!cur_obj)
    {
        // Object does not exist
        goto resume_0;
    }
    cur_version = cur_obj->get_writes()->version;
    // Find the range to compact
    compact_lsn = bs->heap->get_completed_lsn();
    bs->heap->get_compact_range(cur_obj, compact_lsn, &begin_wr, &end_wr);
    if (!begin_wr)
    {
        // Nothing to flush
        bs->heap->unlock_entry(cur_oid, copy_id);
        goto resume_0;
    }
    assert(!end_wr->next() && end_wr->flags == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE));
    clean_loc = end_wr->location;
#ifdef BLOCKSTORE_DEBUG
    printf("Flushing %jx:%jx v%ju .. v%ju\n", cur_oid.inode, cur_oid.stripe, end_wr->version, begin_wr->version);
#endif
    flusher->active_flushers++;
    // Scan versions to flush
    read_vec.clear();
    for (auto wr = begin_wr; wr != end_wr; wr = wr->next())
    {
        min_compact_lsn = wr->lsn;
        bs->prepare_read(read_vec, cur_obj, wr, 0, bs->dsk.data_block_size);
    }
    overwrite_start = overwrite_end = 0;
    if (read_vec.size() > 0)
    {
        overwrite_start = read_vec[0].offset;
        overwrite_end = read_vec[read_vec.size()-1].offset + read_vec[read_vec.size()-1].len;
    }
    read_to_fill_incomplete = false;
    if (bs->dsk.csum_block_size > bs->dsk.bitmap_granularity && end_wr->next())
    {
        // Read original checksum blocks to calculate padded checksums if required
        fill_partial_checksum_blocks();
    }
    // Read buffered data
    cur_obj = NULL;
    begin_wr = end_wr = NULL;
resume_2:
resume_3:
    if (!read_buffered(2))
        return false;
    // Now, if csum_block_size is > bitmap_granularity and if we are doing partial checksum block updates,
    // perform a trick: clear bitmap bits in the metadata entry and recalculate block checksum with zeros
    // in place of overwritten parts. Then, even if the actual partial update fully or partially fails,
    // we'll have a correct checksum because it won't include overwritten parts!
    // The same thing actually happens even when csum_block_size == bitmap_granularity, but in that case
    // we never need to read (and thus verify) overwritten parts from the data device.
    res = check_and_punch_checksums();
    if (res == EBUSY)
    {
resume_4:
resume_5:
        if (!write_meta_block(4))
            return false;
resume_6:
resume_7:
resume_8:
        if (!fsync_batch(true, 6)) // FIXME: is it correct to batch here
            return false;
    }
    else if (res == ENOENT || res == EDOM)
    {
        // Abort compaction
        goto release_oid;
    }
    assert(res == 0);
    // Submit data writes
    copy_count = 0;
    for (i = 0; i < read_vec.size(); i++)
    {
        if (read_vec[i].copy_flags & COPY_BUF_JOURNAL)
        {
            assert(read_vec[i].buf);
            await_sqe(9);
            data->iov = (struct iovec){ read_vec[i].buf, (size_t)read_vec[i].len };
            data->callback = simple_callback_w;
            io_uring_prep_writev(sqe, bs->dsk.data_fd, &data->iov, 1, bs->dsk.data_offset + clean_loc + read_vec[i].offset);
            wait_count++;
            copy_count++;
        }
    }
resume_10:
    if (wait_count > 0)
    {
        wait_state = 10;
        return false;
    }
    // Sync data before modifying metadata
resume_11:
resume_12:
resume_13:
    if (copy_count && !fsync_batch(false, 11))
        return false;
    bs->heap->unlock_entry(cur_oid, copy_id);
    // Modify the metadata entry; don't write anything. Metadata block will be written on the next write
    calc_block_checksums();
    bs->heap->compact_object(cur_oid, compact_lsn, new_data_csums);
    // Done, free all buffers
    free_buffers();
#ifdef BLOCKSTORE_DEBUG
    printf("Compacted %jx:%jx v%ju (%d writes)\n", cur_oid.inode, cur_oid.stripe, cur_version, copy_count);
#endif
    // Advance compacted_lsn every <journal_trim_interval> objects
    bs->heap->set_compacted_lsn(min_compact_lsn);
    if (bs->journal_trim_interval && !((++flusher->advance_lsn_counter) % bs->journal_trim_interval))
    {
resume_14:
resume_15:
resume_16:
resume_17:
        if (!trim_lsn(14))
            return false;
    }
release_oid:
    flusher->active_flushers--;
    if (should_repeat)
    {
        // Flush the same object again
        goto resume_1;
    }
    // All done
    flusher->compact_counter++;
    wait_state = 0;
    goto resume_0;
}

void journal_flusher_co::iterate_partial_overwrites(std::function<int(int, uint32_t, uint32_t)> cb)
{
    int prev = 0;
    uint32_t prev_begin = 0, prev_end = 0;
    for (int i = 0; i < read_vec.size() && !(read_vec[i].copy_flags & COPY_BUF_CSUM_FILL); i++)
    {
        if (read_vec[i].copy_flags != (COPY_BUF_JOURNAL|COPY_BUF_COALESCED))
        {
            if (read_vec[i].offset > prev_end)
            {
                i += cb(prev, prev_begin, prev_end);
                prev = i;
                prev_begin = read_vec[i].offset;
            }
            prev_end = read_vec[i].offset + read_vec[i].len;
        }
    }
    if (prev_end > prev_begin)
    {
        cb(prev, prev_begin, prev_end);
    }
}

void journal_flusher_co::iterate_checksum_holes(std::function<void(int, uint32_t, uint32_t)> cb)
{
    iterate_partial_overwrites([&](int pos, uint32_t prev_begin, uint32_t prev_end)
    {
        int r = 0;
        if ((prev_begin % bs->dsk.csum_block_size) &&
            (prev_begin / bs->dsk.csum_block_size) != (prev_end / bs->dsk.csum_block_size))
        {
            cb(pos, prev_begin, prev_begin + bs->dsk.csum_block_size - prev_begin%bs->dsk.csum_block_size);
            r++;
        }
        if ((prev_end % bs->dsk.csum_block_size) ||
            (prev_begin % bs->dsk.csum_block_size) &&
            (prev_end / bs->dsk.csum_block_size) == (prev_begin / bs->dsk.csum_block_size))
        {
            cb(i, prev_end - (prev_end % bs->dsk.csum_block_size ? (prev_end % bs->dsk.csum_block_size) : bs->dsk.csum_block_size), prev_end);
            r++;
        }
        return r;
    });
}

void journal_flusher_co::fill_partial_checksum_blocks()
{
    iterate_checksum_holes([&](int vec_pos, uint32_t hole_start, uint32_t hole_end)
    {
        read_to_fill_incomplete = true;
        int pos = read_vec.size();
        bs->prepare_disk_read(read_vec, pos, cur_obj, end_wr,
            hole_start - hole_start % bs->dsk.csum_block_size, hole_start - hole_start % bs->dsk.csum_block_size + bs->dsk.csum_block_size,
            hole_start - hole_start % bs->dsk.csum_block_size, hole_start - hole_start % bs->dsk.csum_block_size + bs->dsk.csum_block_size);
        pos--;
        read_vec[pos].copy_flags |= COPY_BUF_CSUM_FILL;
        read_vec.insert(read_vec.begin()+vec_pos, (copy_buffer_t){
            .copy_flags = COPY_BUF_JOURNAL|COPY_BUF_COALESCED,
            .offset = hole_start,
            .len = hole_end-hole_start,
            .buf = read_vec[pos].buf + hole_start - read_vec[pos].offset,
        });
    });
}

void journal_flusher_co::free_buffers()
{
    for (auto it = read_vec.begin(); it != read_vec.end(); it++)
    {
        // Free it if it's not taken from the journal
        if (it->buf && !(it->copy_flags & COPY_BUF_COALESCED) &&
            (!bs->dsk.inmemory_journal || it->buf < bs->buffer_area || it->buf >= (uint8_t*)bs->buffer_area + bs->dsk.journal_len))
        {
            free(it->buf);
        }
    }
    read_vec.clear();
}

// FIXME: Write tests for it
int journal_flusher_co::check_and_punch_checksums()
{
    if (!bs->dsk.csum_block_size)
    {
        // Nothing to do
        return 0;
    }
    // Verify data checksums
    cur_obj = bs->heap->read_locked_entry(cur_oid, copy_id);
    bool csum_ok = true;
    for (int i = 0; i < read_vec.size(); i++)
    {
        auto & vec = read_vec[i];
        if (!(vec.copy_flags & (COPY_BUF_COALESCED|COPY_BUF_ZERO)))
        {
            heap_write_t *wr = cur_obj->get_writes();
            while (wr && wr->lsn != vec.wr_lsn)
                wr = wr->next();
            assert(wr);
            bs->heap->calc_block_checksums(
                (uint32_t*)wr->get_checksums(bs->heap), vec.buf, wr->get_int_bitmap(bs->heap), vec.offset, vec.offset+vec.len, false,
                [&](uint32_t mismatch_pos, uint32_t expected_csum, uint32_t real_csum)
                {
                    printf("Checksum mismatch in object %jx:%jx v%ju in %s area at offset 0x%jx: got %08x, expected %08x\n",
                        cur_oid.inode, cur_oid.stripe, wr->version,
                        (vec.copy_flags & COPY_BUF_JOURNAL ? "buffer" : "data"),
                        vec.disk_offset, real_csum, expected_csum);
                    csum_ok = false;
                }
            );
        }
    }
    if (!csum_ok)
    {
        // Checksum error, abort compaction
        // FIXME: Report the corrupted object to the upper layer
        return EDOM;
    }
    if (!read_to_fill_incomplete)
    {
        // Nothing to do
        return 0;
    }
    cur_obj = bs->heap->read_entry(cur_oid, &modified_block, true);
    if (!cur_obj)
    {
        // Object is deleted, abort compaction
        return ENOENT;
    }
    bs->heap->get_compact_range(cur_obj, compact_lsn, &begin_wr, &end_wr);
    if (!begin_wr || begin_wr->lsn != compact_lsn)
    {
        // Object is overwritten, abort compaction
        return ENOENT;
    }
    uint8_t *bmp = end_wr->get_int_bitmap(bs->heap);
    uint8_t *csums = end_wr->get_checksums(bs->heap);
    // Clear bits
    iterate_partial_overwrites([&](int pos, uint32_t start, uint32_t end)
    {
        bitmap_clear(bmp, start, end-start, bs->dsk.bitmap_granularity);
        return 0;
    });
    // Update partial block checksums
    for (auto & vec: read_vec)
    {
        if (vec.copy_flags & COPY_BUF_CSUM_FILL)
        {
            uint32_t csum_off = (vec.offset/bs->dsk.csum_block_size - end_wr->offset/bs->dsk.csum_block_size) * (bs->dsk.data_csum_type & 0xFF);
            bs->heap->calc_block_checksums((uint32_t*)(csums+csum_off), vec.buf, bmp, vec.offset, vec.offset+vec.len, true, NULL);
        }
    }
    cur_obj->crc32c = cur_obj->calc_crc32c();
    if (res == ENOENT)
    {
        // Object is deleted, abort compaction
        return ENOENT;
    }
    // Modified, we should write the block to disk
    assert(!res);
    return EBUSY;
}

void journal_flusher_co::calc_block_checksums()
{
    new_data_csums = NULL;
    if (bs->dsk.csum_block_size <= bs->dsk.bitmap_granularity)
        return;
    new_data_csums = csum_buf + overwrite_start/bs->dsk.csum_block_size * (bs->dsk.data_csum_type & 0xFF);
    cur_obj = bs->heap->read_locked_entry(cur_oid, copy_id);
    uint64_t block_offset = 0;
    uint32_t block_done = 0;
    uint32_t block_csum = 0;
    for (auto it = read_vec.begin(); it != read_vec.end(); it++)
    {
        if (it->copy_flags & COPY_BUF_CSUM_FILL)
            break;
        if (block_done == 0)
        {
            // `read_vec` should contain aligned items, possibly split into pieces
            assert(!(it->offset % bs->dsk.csum_block_size));
            block_offset = it->offset;
        }
        bool zero = (it->copy_flags & COPY_BUF_ZERO);
        auto len = it->len;
        while ((block_done+len) >= bs->dsk.csum_block_size)
        {
            if (!block_done && it->wr_lsn)
            {
                // We may take existing checksums if an overwrite contains a full block
                heap_write_t *wr = cur_obj->get_writes();
                while (wr && wr->lsn != it->wr_lsn)
                    wr = wr->next();
                assert(wr);
                assert(!(it->offset % bs->dsk.csum_block_size));
                assert(!(wr->offset % bs->dsk.csum_block_size));
                auto full_csum_offset = (it->offset - wr->offset) / bs->dsk.csum_block_size;
                auto full_csum_count = len/bs->dsk.csum_block_size;
                memcpy(new_data_csums + block_offset/bs->dsk.csum_block_size,
                    wr->get_checksums(bs->heap) + full_csum_offset*4, full_csum_count*4);
                len -= full_csum_count*bs->dsk.csum_block_size;
                block_offset += full_csum_count*bs->dsk.csum_block_size;
            }
            else
            {
                auto cur_len = bs->dsk.csum_block_size-block_done;
                block_csum = zero
                    ? crc32c_pad(block_csum, NULL, 0, cur_len, 0)
                    : crc32c(block_csum, (uint8_t*)it->buf+(it->len-len), cur_len);
                new_data_csums[block_offset / bs->dsk.csum_block_size] = block_csum;
                block_csum = 0;
                block_done = 0;
                block_offset += bs->dsk.csum_block_size;
                len -= cur_len;
            }
        }
        if (len > 0)
        {
            block_csum = zero
                ? crc32c_pad(block_csum, NULL, 0, len, 0)
                : crc32c(block_csum, (uint8_t*)it->buf+(it->len-len), len);
            block_done += len;
        }
    }
    // `read_vec` should contain aligned items, possibly split into pieces
    assert(!block_done);
}

bool journal_flusher_co::write_meta_block(int wait_base)
{
    if (wait_state == wait_base)
        goto resume_0;
    else if (wait_state == wait_base+1)
        goto resume_1;
    await_sqe(0);
    data->iov = (struct iovec){ bs->heap->get_meta_block(modified_block), (size_t)bs->dsk.meta_block_size };
    data->callback = simple_callback_w;
    io_uring_prep_writev(sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset + (modified_block+1)*bs->dsk.meta_block_size);
    wait_count++;
resume_1:
    if (wait_count > 0)
    {
        wait_state = wait_base+1;
        return false;
    }
    return true;
}

bool journal_flusher_co::read_buffered(int wait_base)
{
    if (wait_state == wait_base)
        goto resume_0;
    else if (wait_state == wait_base+1)
        goto resume_1;
    wait_count = 0;
    if (bs->dsk.inmemory_journal && !read_to_fill_incomplete)
    {
        // Happy path: nothing to read :)
        return true;
    }
    for (i = 0; i < read_vec.size(); i++)
    {
        if (read_vec[i].copy_flags == COPY_BUF_JOURNAL && !bs->dsk.inmemory_journal ||
            (read_vec[i].copy_flags & COPY_BUF_DATA) && !(read_vec[i].copy_flags & COPY_BUF_COALESCED))
        {
            await_sqe(0);
            auto & vec = read_vec[i];
            data->iov = (struct iovec){ vec.buf, (size_t)vec.disk_len };
            wait_count++;
            io_uring_prep_readv(
                sqe,
                (vec.copy_flags & COPY_BUF_JOURNAL) ? bs->dsk.journal_fd : bs->dsk.data_fd,
                &data->iov, 1,
                ((vec.copy_flags & COPY_BUF_JOURNAL) ? bs->dsk.journal_offset : bs->dsk.data_offset) + vec.disk_offset
            );
            data->callback = simple_callback_r;
        }
    }
    // Wait for reads/writes if the journal is not inmemory
resume_1:
    if (wait_count > 0)
    {
        wait_state = wait_base+1;
        return false;
    }
    return true;
}

bool journal_flusher_co::fsync_batch(bool fsync_meta, int wait_base)
{
    if (wait_state == wait_base)        goto resume_0;
    else if (wait_state == wait_base+1) goto resume_1;
    else if (wait_state == wait_base+2) goto resume_2;
    if (!(fsync_meta ? bs->disable_meta_fsync : bs->disable_data_fsync))
    {
        cur_sync = flusher->syncs.end();
        while (cur_sync != flusher->syncs.begin())
        {
            cur_sync--;
            if (cur_sync->fsync_meta == fsync_meta && cur_sync->state == 0)
            {
                goto sync_found;
            }
        }
        cur_sync = flusher->syncs.emplace(flusher->syncs.end(), (flusher_sync_t){
            .fsync_meta = fsync_meta,
            .ready_count = 0,
            .state = 0,
        });
    sync_found:
        cur_sync->ready_count++;
        flusher->syncing_flushers++;
    resume_1:
        if (!cur_sync->state)
        {
            if (flusher->syncing_flushers >= flusher->active_flushers || true /*FIXME*/)
            {
                // Sync batch is ready. Do it.
                await_sqe(0);
                data->iov = { 0 };
                data->callback = simple_callback_w;
                io_uring_prep_fsync(sqe, fsync_meta ? bs->dsk.meta_fd : bs->dsk.data_fd, IORING_FSYNC_DATASYNC);
                cur_sync->state = 1;
                wait_count++;
            resume_2:
                if (wait_count > 0)
                {
                    wait_state = wait_base+2;
                    return false;
                }
                // Sync completed. All previous coroutines waiting for it must be resumed
                cur_sync->state = 2;
                bs->ringloop->wakeup();
            }
            else
            {
                // Wait until someone else sends and completes a sync.
                wait_state = wait_base+1;
                return false;
            }
        }
        flusher->syncing_flushers--;
        cur_sync->ready_count--;
        if (cur_sync->ready_count == 0)
        {
            flusher->syncs.erase(cur_sync);
        }
    }
    return true;
}

bool journal_flusher_co::trim_lsn(int wait_base)
{
    if (wait_state == wait_base)        goto resume_0;
    else if (wait_state == wait_base+1) goto resume_1;
    else if (wait_state == wait_base+2) goto resume_2;
    else if (wait_state == wait_base+3) goto resume_3;
    if (!bs->disable_meta_fsync)
    {
        await_sqe(0);
        data->iov = { 0 };
        data->callback = simple_callback_w;
        io_uring_prep_fsync(sqe, bs->dsk.meta_fd, IORING_FSYNC_DATASYNC);
        wait_count++;
resume_1:
        if (wait_count > 0)
        {
            wait_state = wait_base+1;
            return false;
        }
    }
    ((blockstore_meta_header_v3_t*)bs->meta_superblock)->compacted_lsn = bs->heap->get_compacted_lsn();
    ((blockstore_meta_header_v3_t*)bs->meta_superblock)->set_crc32c();
    await_sqe(2);
    data->iov = (struct iovec){ bs->meta_superblock, (size_t)bs->dsk.meta_block_size };
    data->callback = simple_callback_w;
    io_uring_prep_writev(sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset);
    wait_count++;
resume_3:
    if (wait_count > 0)
    {
        wait_state = wait_base+3;
        return false;
    }
    flusher->advance_lsn_counter = 0;
    return true;
}
