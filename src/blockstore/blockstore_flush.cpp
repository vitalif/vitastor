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
    co = new journal_flusher_co[max_flusher_count];
    for (int i = 0; i < max_flusher_count; i++)
    {
        co[i].co_id = i;
        co[i].bs = bs;
        co[i].new_bmp = (uint8_t*)malloc_or_die(3*bs->dsk.clean_entry_bitmap_size);
        co[i].new_ext_bmp = co[i].new_bmp + bs->dsk.clean_entry_bitmap_size;
        co[i].punch_bmp = co[i].new_bmp + 2*bs->dsk.clean_entry_bitmap_size;
        if (bs->dsk.csum_block_size > 0)
        {
            co[i].new_csums = (uint8_t*)malloc_or_die(bs->dsk.data_block_size / bs->dsk.csum_block_size * (bs->dsk.data_csum_type & 0xFF));
        }
        co[i].flusher = this;
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
    if (new_csums)
    {
        free(new_csums);
        new_csums = NULL;
    }
    if (new_bmp)
    {
        free(new_bmp);
        new_bmp = NULL;
    }
    new_ext_bmp = NULL;
    punch_bmp = NULL;
    free_buffers();
}

int journal_flusher_t::get_syncing_buffer()
{
    return syncing_buffer;
}

uint64_t journal_flusher_t::get_compact_counter()
{
    return compact_counter;
}

bool journal_flusher_t::is_active()
{
    return active_flushers > 0;
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
        "Compaction queue: %u/%u items, data: %ju/%ju blocks used, buffer: %ju/%ju bytes used, meta: %ju/%ju bytes used, %u/%ju blocks nearfull\n",
        bs->heap->get_compact_queue_size(), bs->heap->get_to_compact_count(),
        bs->heap->get_data_used_space()/bs->dsk.data_block_size, bs->dsk.block_count,
        bs->heap->get_buffer_area_used_space(), bs->dsk.journal_len,
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
    for (int i = 0; (active_flushers > 0 || force_start > 0 ||
        bs->heap->get_to_compact_count() > bs->flusher_start_threshold ||
        i == 0 && bs->intent_write_counter >= bs->journal_trim_interval) && i < cur_flusher_count; i++)
    {
        co[i].loop();
    }
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
    else if (wait_state == 18) goto resume_18;
resume_0:
    wait_state = 0;
    wait_count = 0;
    cur_oid = {};
    res = bs->heap->get_next_compact(cur_oid);
    // Advance fsynced_lsn every <journal_trim_interval> intent writes
    if ((bs->intent_write_counter >= bs->journal_trim_interval) && co_id == 0)
    {
        bs->intent_write_counter = 0;
resume_17:
resume_18:
        if (!trim_lsn(17))
            return false;
    }
    if (res == ENOENT && flusher->force_start > 0 && co_id == 0 &&
        (!bs->dsk.disable_journal_fsync || !bs->dsk.disable_meta_fsync))
    {
        flusher->active_flushers++;
resume_14:
resume_15:
resume_16:
        if (!fsync_buffer(14))
        {
            return false;
        }
        flusher->active_flushers--;
        res = (res == 2 ? bs->heap->get_next_compact(cur_oid) : ENOENT);
    }
    if (res == ENOENT)
    {
        cur_oid = {};
        wait_state = 0;
        return true;
    }
    if (flusher->flushing.find(cur_oid) != flusher->flushing.end())
    {
        for (int i = 0; i < flusher->cur_flusher_count; i++)
        {
            if (i != co_id && flusher->co[i].cur_oid == cur_oid)
            {
                // Already flushing it
                flusher->co[i].should_repeat = true;
                goto resume_0;
            }
        }
        assert(false);
    }
    flusher->flushing.insert(cur_oid);
resume_1:
    wait_state = 1;
    should_repeat = false;
    cur_obj = bs->heap->lock_and_read_entry(cur_oid);
    if (!cur_obj)
    {
        // Object does not exist
        flusher->flushing.erase(cur_oid);
        goto resume_0;
    }
    // Scan versions to flush
    free_buffers();
    copy_count = 0;
    fsynced_lsn = bs->heap->get_fsynced_lsn();
    bitmap_copied = false;
    memset(new_bmp, 0, bs->dsk.clean_entry_bitmap_size);
    csum_copy.clear();
    compact_info = bs->heap->iterate_compaction(cur_obj, fsynced_lsn, flusher->force_start, [&](heap_entry_t *wr)
    {
        if (!bitmap_copied)
        {
            memcpy(new_ext_bmp, wr->get_ext_bitmap(bs->heap), bs->dsk.clean_entry_bitmap_size);
            bitmap_copied = true;
        }
        bitmap_set(new_bmp, wr->small().offset, wr->small().len, bs->dsk.bitmap_granularity);
        if (bs->dsk.csum_block_size && bs->dsk.csum_block_size <= bs->dsk.bitmap_granularity)
        {
            csum_copy.push_back(wr);
        }
        if (wr->type() == BS_HEAP_SMALL_WRITE ||
            wr->type() == BS_HEAP_INTENT_WRITE && bs->dsk.csum_block_size > bs->dsk.bitmap_granularity)
        {
            bs->prepare_read(read_vec, cur_obj, wr, 0, bs->dsk.data_block_size);
            copy_count++;
        }
    });
    if (!compact_info.compact_lsn)
    {
        // Flushing is aborted
        flusher->flushing.erase(cur_oid);
        bs->heap->unlock_entry(cur_oid);
        goto resume_0;
    }
    mem_or(new_bmp, compact_info.clean_wr->get_int_bitmap(bs->heap), bs->dsk.clean_entry_bitmap_size);
    if (!bitmap_copied)
    {
        memcpy(new_ext_bmp, compact_info.clean_wr->get_ext_bitmap(bs->heap), bs->dsk.clean_entry_bitmap_size);
        bitmap_copied = true;
    }
    if (bs->dsk.csum_block_size && bs->dsk.csum_block_size <= bs->dsk.bitmap_granularity)
    {
        memcpy(new_csums, compact_info.clean_wr->get_checksums(bs->heap), bs->dsk.data_block_size/bs->dsk.csum_block_size * (bs->dsk.data_csum_type & 0xFF));
        for (size_t i = csum_copy.size(); i > 0; i--)
        {
            auto wr = csum_copy[i-1];
            memcpy(new_csums + wr->small().offset/bs->dsk.csum_block_size*(bs->dsk.data_csum_type & 0xFF),
                wr->get_checksums(bs->heap), wr->small().len/bs->dsk.csum_block_size*(bs->dsk.data_csum_type & 0xFF));
        }
        csum_copy.clear();
    }
    clean_loc = compact_info.clean_wr->big_location(bs->heap);
    flusher->active_flushers++;
    if (bs->log_level > 10)
    {
        printf("Compacting %jx:%jx v%ju..v%ju / l%ju..l%ju\n", cur_oid.inode, cur_oid.stripe,
            compact_info.clean_wr->version, compact_info.compact_version,
            compact_info.clean_wr->lsn, compact_info.compact_lsn);
    }
    overwrite_start = overwrite_end = 0;
    if (read_vec.size() > 0)
    {
        overwrite_start = read_vec[0].offset;
        overwrite_end = read_vec[read_vec.size()-1].offset + read_vec[read_vec.size()-1].len;
    }
    read_to_fill_incomplete = false;
    if (bs->dsk.csum_block_size > bs->dsk.bitmap_granularity)
    {
        // Read original checksum blocks to calculate padded checksums if required
        fill_partial_checksum_blocks();
        if (read_to_fill_incomplete && bs->perfect_csum_update)
        {
            flusher->wanting_meta_fsync++;
        }
    }
    // Read buffered data
    cur_obj = NULL;
resume_2:
resume_3:
    if (!read_buffered(2))
    {
        return false;
    }
    // Now, if csum_block_size is > bitmap_granularity and if we are doing partial checksum block updates,
    // perform a trick: clear bitmap bits in the metadata entry and recalculate block checksum with zeros
    // in place of overwritten parts. Then, even if the actual partial update fully or partially fails,
    // we'll have a correct checksum because it won't include overwritten parts!
    // The same thing actually happens even when csum_block_size == bitmap_granularity, but in that case
    // we never need to read (and thus verify) overwritten parts from the data device.
    if (read_to_fill_incomplete && bs->perfect_csum_update)
    {
        flusher->wanting_meta_fsync--;
    }
    res = check_and_punch_checksums();
    if (res == ENOENT || res == EDOM)
    {
        // Abort compaction
        flusher->flushing.erase(cur_oid);
        bs->heap->unlock_entry(cur_oid);
        flusher->active_flushers--;
        goto resume_0;
    }
    if (res == EBUSY)
    {
resume_4:
        modified_block = UINT32_MAX;
        res = bs->heap->punch_holes(compact_info.clean_wr, punch_bmp, new_csums, &modified_block);
        if (res == ENOENT)
        {
            // Abort compaction
            flusher->flushing.erase(cur_oid);
            bs->heap->unlock_entry(cur_oid);
            flusher->active_flushers--;
            goto resume_0;
        }
        if (res == EAGAIN)
        {
            // Retry, block is busy
            wait_state = 4;
            return false;
        }
        assert(res == 0);
resume_5:
resume_6:
        if (!write_meta_block(5))
        {
            return false;
        }
resume_7:
resume_8:
resume_9:
        if (!fsync_meta(7))
        {
            return false;
        }
        res = 0;
    }
    assert(res == 0);
    // Submit data writes
    for (i = 0; i < read_vec.size(); i++)
    {
        if ((read_vec[i].copy_flags & COPY_BUF_JOURNAL) &&
            !(read_vec[i].copy_flags & COPY_BUF_COALESCED) ||
            (read_vec[i].copy_flags & COPY_BUF_PADDED)) // FIXME Shit, simplify these flags
        {
            assert(read_vec[i].buf);
            await_sqe(10);
            data->iov = (struct iovec){ read_vec[i].buf + (read_vec[i].copy_flags & COPY_BUF_PADDED
                ? read_vec[i].offset - read_vec[i].disk_offset : 0), (size_t)read_vec[i].len };
            data->callback = simple_callback_w;
            io_uring_prep_writev(sqe, bs->dsk.data_fd, &data->iov, 1, bs->dsk.data_offset + clean_loc + read_vec[i].offset);
            wait_count++;
        }
    }
resume_11:
    if (wait_count > 0)
    {
        wait_state = 11;
        return false;
    }
    // Lock is only needed to prevent freeing the big_write because we overwrite it...
    bs->heap->unlock_entry(cur_oid);
    // Mark the object compacted, but don't free and remove small_writes
    // We'll free and remove them only when trimming
    // The only thing we modify here are big_write block checksums if >4k block is used
    cur_obj = bs->heap->read_entry(cur_oid);
    if (!cur_obj)
    {
        // Abort compaction
        flusher->flushing.erase(cur_oid);
        goto resume_0;
    }
    if (!calc_block_checksums())
    {
        // Abort compaction
        flusher->flushing.erase(cur_oid);
        goto resume_0;
    }
    bs->heap->add_compact(cur_obj, compact_info.compact_version, compact_info.compact_lsn, clean_loc,
        compact_info.do_delete, &modified_block, new_bmp, new_ext_bmp, new_csums);
resume_12:
resume_13:
    if (!write_meta_block(12))
    {
        return false;
    }
    // Done
    if (bs->log_level > 10)
    {
        printf("Compacted %jx:%jx l%ju (%d writes)\n", cur_oid.inode, cur_oid.stripe, compact_info.compact_lsn, copy_count);
    }
    flusher->compact_counter++;
    flusher->active_flushers--;
    if (should_repeat)
    {
        // Flush the same object again
        goto resume_1;
    }
    flusher->flushing.erase(cur_oid);
    // All done
    goto resume_0;
}

void journal_flusher_co::iterate_checksum_holes(std::function<void(int & pos, uint32_t hole_start, uint32_t hole_end)> cb)
{
    bs->find_holes(read_vec, 0, bs->dsk.data_block_size, [&](int & pos, uint32_t hole_start, uint32_t hole_end)
    {
        if (hole_start % bs->dsk.csum_block_size)
        {
            uint32_t blk_end = hole_start - (hole_start % bs->dsk.csum_block_size) + bs->dsk.csum_block_size;
            cb(pos, hole_start, hole_end < blk_end ? hole_end : blk_end);
        }
        if ((hole_end % bs->dsk.csum_block_size) &&
            (!(hole_start % bs->dsk.csum_block_size) || (hole_end / bs->dsk.csum_block_size) != (hole_start / bs->dsk.csum_block_size)))
        {
            cb(pos, hole_end - (hole_end % bs->dsk.csum_block_size), hole_end);
        }
    });
}

void journal_flusher_co::fill_partial_checksum_blocks()
{
    iterate_checksum_holes([&](int & vec_pos, uint32_t hole_start, uint32_t hole_end)
    {
        read_to_fill_incomplete = true;
        uint32_t blk_begin = (hole_start - hole_start % bs->dsk.csum_block_size);
        uint32_t blk_end = (blk_begin + bs->dsk.csum_block_size);
        uint32_t copy_flags = COPY_BUF_CSUM_FILL | (bs->perfect_csum_update ? 0 : COPY_BUF_SKIP_CSUM);
        if (!read_vec.size() || read_vec.back().copy_flags != copy_flags ||
            read_vec.back().offset != blk_begin || read_vec.back().len != blk_end-blk_begin)
        {
            read_vec.push_back((copy_buffer_t){
                .copy_flags = COPY_BUF_DATA | copy_flags,
                .offset = blk_begin,
                .len = blk_end - blk_begin,
                .disk_loc = clean_loc,
                .disk_offset = blk_begin,
                .disk_len = blk_end - blk_begin,
                .buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, blk_end - blk_begin),
                .wr = compact_info.clean_wr,
            });
        }
        auto & vec = read_vec[read_vec.size()-1];
        read_vec.insert(read_vec.begin()+vec_pos, (copy_buffer_t){
            .copy_flags = COPY_BUF_JOURNAL|COPY_BUF_COALESCED,
            .offset = hole_start,
            .len = hole_end - hole_start,
            .disk_offset = hole_start,
            .disk_len = hole_end - hole_start,
            .buf = vec.buf + hole_start - vec.offset,
        });
        vec_pos++;
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

int journal_flusher_co::check_and_punch_checksums()
{
    if (!bs->dsk.csum_block_size)
    {
        // Nothing to do
        return 0;
    }
    // Verify data checksums
    cur_obj = bs->heap->read_entry(cur_oid);
    if (!cur_obj)
    {
        // Object is deleted, abort compaction
        return ENOENT;
    }
    bool csum_ok = true;
    for (int i = 0; i < read_vec.size(); i++)
    {
        auto & vec = read_vec[i];
        if (!(vec.copy_flags & (COPY_BUF_COALESCED|COPY_BUF_ZERO|COPY_BUF_SKIP_CSUM)))
        {
            uint32_t *csums = (uint32_t*)(vec.wr->get_checksums(bs->heap)
                + (vec.disk_offset/bs->dsk.csum_block_size)*(bs->dsk.data_csum_type & 0xFF)
                - ((vec.wr->type() == BS_HEAP_BIG_WRITE || vec.wr->type() == BS_HEAP_BIG_INTENT)
                    ? 0 : (vec.wr->small().offset/bs->dsk.csum_block_size)*(bs->dsk.data_csum_type & 0xFF)));
            bs->heap->calc_block_checksums(
                csums, vec.buf, vec.wr->get_int_bitmap(bs->heap), vec.disk_offset, vec.disk_offset+vec.disk_len, false,
                [&](uint32_t mismatch_pos, uint32_t expected_csum, uint32_t real_csum)
                {
                    printf("Checksum mismatch during compaction in object %jx:%jx v%ju, offset 0x%x in %s area at offset 0x%jx: got %08x, expected %08x\n",
                        cur_oid.inode, cur_oid.stripe, vec.wr->version, mismatch_pos,
                        (vec.copy_flags & COPY_BUF_JOURNAL ? "buffer" : "data"),
                        vec.disk_loc+vec.disk_offset, real_csum, expected_csum);
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
    if (!read_to_fill_incomplete || !bs->perfect_csum_update)
    {
        // Nothing to do
        return 0;
    }
    memcpy(punch_bmp, compact_info.clean_wr->get_int_bitmap(bs->heap), bs->dsk.clean_entry_bitmap_size);
    memcpy(new_csums, compact_info.clean_wr->get_checksums(bs->heap), bs->dsk.data_block_size/bs->dsk.csum_block_size * (bs->dsk.data_csum_type & 0xFF));
    // Clear bits
    for (auto & vec: read_vec)
    {
        if (vec.copy_flags & COPY_BUF_CSUM_FILL)
        {
            break;
        }
        if (!(vec.copy_flags & COPY_BUF_COALESCED) &&
            ((vec.offset % bs->dsk.csum_block_size) || (vec.len % bs->dsk.csum_block_size)))
        {
            bitmap_clear(punch_bmp, vec.offset, vec.len, bs->dsk.bitmap_granularity);
        }
    }
    // Update partial block checksums
    for (auto & vec: read_vec)
    {
        if (vec.copy_flags & COPY_BUF_CSUM_FILL)
        {
            uint32_t csum_off = vec.offset/bs->dsk.csum_block_size * (bs->dsk.data_csum_type & 0xFF);
            bs->heap->calc_block_checksums((uint32_t*)(new_csums+csum_off), vec.buf, punch_bmp, vec.offset, vec.offset+vec.len, true, NULL);
        }
    }
    // Modified, we should add_punch_holes and then write the block to disk
    return EBUSY;
}

bool journal_flusher_co::calc_block_checksums()
{
    if (bs->dsk.csum_block_size <= bs->dsk.bitmap_granularity || !read_vec.size())
    {
        return true;
    }
    memcpy(new_csums, compact_info.clean_wr->get_checksums(bs->heap), bs->dsk.data_block_size/bs->dsk.csum_block_size * (bs->dsk.data_csum_type & 0xFF));
    // Update block checksums
    size_t i = 0;
    while (i < read_vec.size() && !(read_vec[i].copy_flags & COPY_BUF_CSUM_FILL))
    {
        uint32_t start = read_vec[i].offset;
        uint32_t end = read_vec[i].offset+read_vec[i].len;
        i++;
        while (i < read_vec.size() && !(read_vec[i].copy_flags & COPY_BUF_CSUM_FILL) &&
            read_vec[i].offset == end)
        {
            end = read_vec[i].offset+read_vec[i].len;
            i++;
        }
        // `read_vec` should contain aligned items, possibly split into pieces
        assert(!(start % bs->dsk.csum_block_size));
        assert(!(end % bs->dsk.csum_block_size));
        uint32_t csum_off = start/bs->dsk.csum_block_size * (bs->dsk.data_csum_type & 0xFF);
        bs->heap->calc_block_checksums(
            (uint32_t*)(new_csums+csum_off), new_bmp, start, end,
            [&](uint32_t start, uint32_t & len)
            {
                // O(n^2) search, may be fixed later :-p
                for (size_t i = 0; i < read_vec.size(); i++)
                {
                    assert(read_vec[i].offset <= start);
                    if (read_vec[i].offset+read_vec[i].len > start)
                    {
                        len = read_vec[i].offset+read_vec[i].len-start;
                        return read_vec[i].buf + start-read_vec[i].disk_offset;
                    }
                }
                return (uint8_t*)NULL;
            }, true, NULL
        );
    }
    return true;
}

bool journal_flusher_co::write_meta_block(int wait_base)
{
    if (wait_state == wait_base)
        goto resume_0;
    else if (wait_state == wait_base+1)
        goto resume_1;
resume_0:
    if (bs->ringloop->space_left() < 1)
    {
        wait_state = wait_base+0;
        return 0;
    }
    bs->prepare_meta_block_write(modified_block);
resume_1:
    if (bs->meta_block_is_pending(modified_block))
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
        if (!(read_vec[i].copy_flags & COPY_BUF_COALESCED) &&
            ((read_vec[i].copy_flags & COPY_BUF_JOURNAL) && !bs->dsk.inmemory_journal || (read_vec[i].copy_flags & COPY_BUF_DATA)))
        {
            await_sqe(0);
            auto & vec = read_vec[i];
            if (!vec.buf)
                vec.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, vec.disk_len);
            data->iov = (struct iovec){ vec.buf, (size_t)vec.disk_len };
            wait_count++;
            io_uring_prep_readv(
                sqe,
                (vec.copy_flags & COPY_BUF_JOURNAL) ? bs->dsk.journal_fd : bs->dsk.data_fd,
                &data->iov, 1,
                ((vec.copy_flags & COPY_BUF_JOURNAL) ? bs->dsk.journal_offset : bs->dsk.data_offset) + vec.disk_loc + vec.disk_offset
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

bool journal_flusher_co::fsync_meta(int wait_base)
{
    if (wait_state == wait_base)        goto resume_0;
    else if (wait_state == wait_base+1) goto resume_1;
    else if (wait_state == wait_base+2) goto resume_2;
    if (bs->dsk.disable_meta_fsync)
    {
        return true;
    }
resume_0:
    if (flusher->wanting_meta_fsync || flusher->fsyncing_meta > 0)
    {
        wait_state = wait_base;
        return false;
    }
    flusher->fsyncing_meta = true;
    // Sync batch is ready. Do it.
    await_sqe(1);
    data->iov = { 0 };
    data->callback = simple_callback_w;
    io_uring_prep_fsync(sqe, bs->dsk.meta_fd, IORING_FSYNC_DATASYNC);
    wait_count++;
resume_2:
    if (wait_count > 0)
    {
        wait_state = wait_base+2;
        return false;
    }
    // Sync completed. All previous coroutines waiting for it must be resumed
    flusher->fsyncing_meta = false;
    bs->ringloop->wakeup();
    return true;
}

bool journal_flusher_co::fsync_buffer(int wait_base)
{
    if (wait_state == wait_base)        goto resume_0;
    else if (wait_state == wait_base+1) goto resume_1;
    else if (wait_state == wait_base+2) goto resume_2;
    if (bs->dsk.disable_journal_fsync && bs->dsk.disable_meta_fsync && bs->dsk.disable_data_fsync ||
        !bs->unsynced_big_write_count && !bs->unsynced_small_write_count && !bs->unsynced_meta_write_count)
    {
        return true;
    }
resume_0:
    if (flusher->syncing_buffer)
    {
        wait_state = wait_base+0;
        return false;
    }
    flusher->active_flushers++;
    flusher->syncing_buffer++;
resume_1:
    assert(!wait_count);
    fsynced_lsn = bs->heap->get_completed_lsn();
    if (!bs->submit_fsyncs(wait_count))
    {
        wait_state = wait_base+1;
        return false;
    }
resume_2:
    if (wait_count > 0)
    {
        wait_state = wait_base+2;
        return false;
    }
    bs->heap->mark_lsn_fsynced(fsynced_lsn);
    flusher->active_flushers--;
    flusher->syncing_buffer--;
    return true;
}

bool journal_flusher_co::trim_lsn(int wait_base)
{
    if (wait_state == wait_base)        goto resume_0;
    else if (wait_state == wait_base+1) goto resume_1;
    fsynced_lsn = bs->heap->get_fsynced_lsn();
    if (((blockstore_meta_header_v3_t*)bs->meta_superblock)->completed_lsn == fsynced_lsn)
    {
        return true;
    }
    flusher->active_flushers++;
    ((blockstore_meta_header_v3_t*)bs->meta_superblock)->completed_lsn = fsynced_lsn;
    ((blockstore_meta_header_v3_t*)bs->meta_superblock)->set_crc32c();
    await_sqe(0);
    data->iov = (struct iovec){ bs->meta_superblock, (size_t)bs->dsk.meta_block_size };
    data->callback = simple_callback_w;
    io_uring_prep_writev(sqe, bs->dsk.meta_fd, &data->iov, 1, bs->dsk.meta_offset);
    // Update superblock with datasync
    sqe->rw_flags = RWF_DSYNC;
    wait_count++;
resume_1:
    if (wait_count > 0)
    {
        wait_state = wait_base+1;
        return false;
    }
    flusher->compact_counter++;
    flusher->active_flushers--;
    return true;
}
