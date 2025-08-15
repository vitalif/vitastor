// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <limits.h>
#include "blockstore_impl.h"

int blockstore_impl_t::fulfill_read_push(blockstore_op_t *op, void *buf, uint64_t offset, uint64_t len,
    uint32_t item_state, uint64_t item_version)
{
    if (!len)
    {
        // Zero-length read
        return 1;
    }
    else if (IS_DELETE(item_state))
    {
        // item is unallocated - return zeroes
        memset(buf, 0, len);
        return 1;
    }
    assert(!IS_IN_FLIGHT(item_state));
    if (journal.inmemory && IS_JOURNAL(item_state))
    {
        memcpy(buf, (uint8_t*)journal.buffer + offset, len);
        return 1;
    }
    BS_SUBMIT_GET_SQE(sqe, data);
    data->iov = (struct iovec){ buf, (size_t)len };
    PRIV(op)->pending_ops++;
    io_uring_prep_readv(
        sqe,
        IS_JOURNAL(item_state) ? dsk.journal_fd : dsk.data_fd,
        &data->iov, 1,
        (IS_JOURNAL(item_state) ? dsk.journal_offset : dsk.data_offset) + offset
    );
    data->callback = [this, op](ring_data_t *data) { handle_read_event(data, op); };
    return 1;
}

void blockstore_impl_t::find_holes(std::vector<copy_buffer_t> & read_vec,
    uint32_t item_start, uint32_t item_end,
    std::function<int(int, bool, uint32_t, uint32_t)> callback)
{
    auto cur_start = item_start;
    int i = 0;
    while (cur_start < item_end)
    {
        // COPY_BUF_CSUM_FILL items are fake items inserted in the end, their offsets aren't in order
        if (i >= read_vec.size() || read_vec[i].copy_flags & COPY_BUF_CSUM_FILL || read_vec[i].offset >= item_end)
        {
            // Hole (at end): cur_start .. item_end
            i += callback(i, false, cur_start, item_end);
            break;
        }
        else if (read_vec[i].offset > cur_start)
        {
            // Hole: cur_start .. min(read_vec[i].offset, item_end)
            auto cur_end = read_vec[i].offset > item_end ? item_end : read_vec[i].offset;
            i += callback(i, false, cur_start, cur_end);
            cur_start = cur_end;
        }
        else if (read_vec[i].offset + read_vec[i].len > cur_start)
        {
            // Allocated: cur_start .. min(read_vec[i].offset + read_vec[i].len, item_end)
            auto cur_end = read_vec[i].offset + read_vec[i].len;
            cur_end = cur_end > item_end ? item_end : cur_end;
            i += callback(i, true, cur_start, cur_end);
            cur_start = cur_end;
            i++;
        }
        else
            i++;
    }
}

int blockstore_impl_t::fulfill_read(blockstore_op_t *read_op,
    uint64_t &fulfilled, uint32_t item_start, uint32_t item_end, // FIXME: Rename item_* to dirty_*
    uint32_t item_state, uint64_t item_version, uint64_t item_location,
    uint64_t journal_sector, uint8_t *csum, int *dyn_data)
{
    int r = 1;
    if (item_start < read_op->offset + read_op->len && item_end > read_op->offset)
    {
        auto & rv = PRIV(read_op)->read_vec;
        auto rd_start = item_start < read_op->offset ? read_op->offset : item_start;
        auto rd_end = item_end > read_op->offset + read_op->len ? read_op->offset + read_op->len : item_end;
        find_holes(rv, rd_start, rd_end, [&](int pos, bool alloc, uint32_t start, uint32_t end)
        {
            if (!r || alloc)
                return 0;
            if (!journal.inmemory && dsk.csum_block_size > dsk.bitmap_granularity && IS_JOURNAL(item_state) && !IS_DELETE(item_state))
            {
                uint32_t blk_begin = (start/dsk.csum_block_size) * dsk.csum_block_size;
                blk_begin = blk_begin < item_start ? item_start : blk_begin;
                uint32_t blk_end = ((end-1) / dsk.csum_block_size + 1) * dsk.csum_block_size;
                blk_end = blk_end > item_end ? item_end : blk_end;
                rv.push_back((copy_buffer_t){
                    .copy_flags = COPY_BUF_JOURNAL|COPY_BUF_CSUM_FILL,
                    .offset = blk_begin,
                    .len = blk_end-blk_begin,
                    .csum_buf = (csum + (blk_begin/dsk.csum_block_size -
                        item_start/dsk.csum_block_size) * (dsk.data_csum_type & 0xFF)),
                    .dyn_data = dyn_data,
                });
                if (dyn_data)
                {
                    (*dyn_data)++;
                }
                // Submit the journal checksum block read
                if (!read_checksum_block(read_op, 1, fulfilled, item_location - item_start))
                {
                    r = 0;
                }
                return 0;
            }
            copy_buffer_t el = {
                .copy_flags = (IS_JOURNAL(item_state) ? COPY_BUF_JOURNAL : COPY_BUF_DATA),
                .offset = start,
                .len = end-start,
                .disk_offset = item_location + start - item_start,
                .journal_sector = (IS_JOURNAL(item_state) ? journal_sector : 0),
                .csum_buf = !csum ? NULL : (csum + (start - item_start) / dsk.csum_block_size * (dsk.data_csum_type & 0xFF)),
                .dyn_data = dyn_data,
            };
            if (dyn_data)
            {
                (*dyn_data)++;
            }
            if (IS_BIG_WRITE(item_state))
            {
                // If we don't track it then we may IN THEORY read another object's data:
                // submit read -> remove the object -> flush remove -> overwrite with another object -> finish read
                // Very improbable, but possible
                PRIV(read_op)->clean_block_used = 1;
            }
            rv.insert(rv.begin() + pos, el);
            fulfilled += el.len;
            if (!fulfill_read_push(read_op,
                (uint8_t*)read_op->buf + el.offset - read_op->offset,
                item_location + el.offset - item_start,
                el.len, item_state, item_version))
            {
                r = 0;
            }
            return 1;
        });
    }
    return r;
}

uint8_t* blockstore_impl_t::get_clean_entry_bitmap(uint64_t block_loc, int offset)
{
    uint8_t *clean_entry_bitmap;
    uint64_t meta_loc = block_loc >> dsk.block_order;
    if (inmemory_meta)
    {
        uint64_t sector = (meta_loc / (dsk.meta_block_size / dsk.clean_entry_size)) * dsk.meta_block_size;
        uint64_t pos = (meta_loc % (dsk.meta_block_size / dsk.clean_entry_size));
        clean_entry_bitmap = ((uint8_t*)metadata_buffer + sector + pos*dsk.clean_entry_size + sizeof(clean_disk_entry) + offset);
    }
    else
        clean_entry_bitmap = (uint8_t*)(clean_bitmaps + meta_loc*2*dsk.clean_entry_bitmap_size + offset);
    return clean_entry_bitmap;
}

int blockstore_impl_t::fill_partial_checksum_blocks(std::vector<copy_buffer_t> & rv, uint64_t & fulfilled,
    uint8_t *clean_entry_bitmap, int *dyn_data, bool from_journal, uint8_t *read_buf, uint64_t read_offset, uint64_t read_end)
{
    if (read_end == read_offset)
        return 0;
    int required = 0;
    read_buf -= read_offset;
    uint32_t last_block = (read_end-1)/dsk.csum_block_size;
    uint32_t start_block = read_offset/dsk.csum_block_size;
    uint32_t end_block = 0;
    while (start_block <= last_block)
    {
        if (read_range_fulfilled(rv, fulfilled, read_buf, clean_entry_bitmap,
            start_block*dsk.csum_block_size < read_offset ? read_offset : start_block*dsk.csum_block_size,
            (start_block+1)*dsk.csum_block_size > read_end ? read_end : (start_block+1)*dsk.csum_block_size))
        {
            // read_range_fulfilled() also adds zero-filled areas
            start_block++;
        }
        else
        {
            // Find a sequence of checksum blocks required to be read
            end_block = start_block;
            while ((end_block+1)*dsk.csum_block_size < read_end &&
                !read_range_fulfilled(rv, fulfilled, read_buf, clean_entry_bitmap,
                    (end_block+1)*dsk.csum_block_size < read_offset ? read_offset : (end_block+1)*dsk.csum_block_size,
                    (end_block+2)*dsk.csum_block_size > read_end ? read_end : (end_block+2)*dsk.csum_block_size))
            {
                end_block++;
            }
            end_block++;
            // OK, mark this range as required
            rv.push_back((copy_buffer_t){
                .copy_flags = COPY_BUF_CSUM_FILL | (from_journal ? COPY_BUF_JOURNALED_BIG : 0),
                .offset = start_block*dsk.csum_block_size,
                .len = (end_block-start_block)*dsk.csum_block_size,
                // save clean_entry_bitmap if we're reading clean data from the journal
                .csum_buf = from_journal ? clean_entry_bitmap : NULL,
                .dyn_data = dyn_data,
            });
            if (dyn_data)
            {
                (*dyn_data)++;
            }
            start_block = end_block;
            required++;
        }
    }
    return required;
}

// read_buf should be == op->buf - op->offset
bool blockstore_impl_t::read_range_fulfilled(std::vector<copy_buffer_t> & rv, uint64_t & fulfilled, uint8_t *read_buf,
    uint8_t *clean_entry_bitmap, uint32_t item_start, uint32_t item_end)
{
    bool all_done = true;
    find_holes(rv, item_start, item_end, [&](int pos, bool alloc, uint32_t cur_start, uint32_t cur_end)
    {
        if (alloc)
            return 0;
        int diff = 0;
        uint32_t bmp_start = cur_start/dsk.bitmap_granularity;
        uint32_t bmp_end = cur_end/dsk.bitmap_granularity;
        uint32_t bmp_pos = bmp_start;
        while (bmp_pos < bmp_end)
        {
            while (bmp_pos < bmp_end && !(clean_entry_bitmap[bmp_pos >> 3] & (1 << (bmp_pos & 0x7))))
                bmp_pos++;
            if (bmp_pos > bmp_start)
            {
                // zero fill
                copy_buffer_t el = {
                    .copy_flags = COPY_BUF_ZERO,
                    .offset = bmp_start*dsk.bitmap_granularity,
                    .len = (bmp_pos-bmp_start)*dsk.bitmap_granularity,
                };
                rv.insert(rv.begin() + pos, el);
                if (read_buf)
                    memset(read_buf + el.offset, 0, el.len);
                fulfilled += el.len;
                diff++;
            }
            bmp_start = bmp_pos;
            while (bmp_pos < bmp_end && (clean_entry_bitmap[bmp_pos >> 3] & (1 << (bmp_pos & 0x7))))
                bmp_pos++;
            if (bmp_pos > bmp_start)
            {
                // something is to be read
                all_done = false;
            }
            bmp_start = bmp_pos;
        }
        return diff;
    });
    return all_done;
}

bool blockstore_impl_t::read_checksum_block(blockstore_op_t *op, int rv_pos, uint64_t &fulfilled, uint64_t clean_loc)
{
    auto & rv = PRIV(op)->read_vec;
    auto *vi = &rv[rv.size()-rv_pos];
    uint32_t item_start = vi->offset, item_end = vi->offset+vi->len;
    uint32_t fill_size = 0;
    int n_iov = 0;
    find_holes(rv, item_start, item_end, [&](int pos, bool alloc, uint32_t cur_start, uint32_t cur_end)
    {
        if (alloc)
        {
            fill_size += cur_end-cur_start;
            n_iov++;
        }
        else
        {
            if (cur_start < op->offset)
            {
                fill_size += op->offset-cur_start;
                n_iov++;
                cur_start = op->offset;
            }
            if (cur_end > op->offset+op->len)
            {
                fill_size += cur_end-(op->offset+op->len);
                n_iov++;
                cur_end = op->offset+op->len;
            }
            if (cur_end > cur_start)
            {
                n_iov++;
            }
        }
        return 0;
    });
    void *buf = memalign_or_die(MEM_ALIGNMENT, fill_size + n_iov*sizeof(struct iovec));
    iovec *iov = (struct iovec*)((uint8_t*)buf+fill_size);
    n_iov = 0;
    fill_size = 0;
    find_holes(rv, item_start, item_end, [&](int pos, bool alloc, uint32_t cur_start, uint32_t cur_end)
    {
        int res = 0;
        if (alloc)
        {
            iov[n_iov++] = (struct iovec){ (uint8_t*)buf+fill_size, cur_end-cur_start };
            fill_size += cur_end-cur_start;
        }
        else
        {
            if (cur_start < op->offset)
            {
                iov[n_iov++] = (struct iovec){ (uint8_t*)buf+fill_size, op->offset-cur_start };
                fill_size += op->offset-cur_start;
                cur_start = op->offset;
            }
            auto lim_end = cur_end > op->offset+op->len ? op->offset+op->len : cur_end;
            if (lim_end > cur_start)
            {
                iov[n_iov++] = (struct iovec){ (uint8_t*)op->buf+cur_start-op->offset, lim_end-cur_start };
                rv.insert(rv.begin() + pos, (copy_buffer_t){
                    .copy_flags = COPY_BUF_DATA,
                    .offset = cur_start,
                    .len = lim_end-cur_start,
                });
                fulfilled += lim_end-cur_start;
                res++;
            }
            if (cur_end > op->offset+op->len)
            {
                iov[n_iov++] = (struct iovec){ (uint8_t*)buf+fill_size, cur_end - (op->offset+op->len) };
                fill_size += cur_end - (op->offset+op->len);
                cur_end = op->offset+op->len;
            }
        }
        return res;
    });
    vi = &rv[rv.size()-rv_pos];
    // Save buf into read_vec too but in a creepy way
    // FIXME: Shit, something else should be invented %)
    *vi = (copy_buffer_t){
        .copy_flags = vi->copy_flags,
        .offset = vi->offset,
        .len = ((uint64_t)n_iov << 32) | fill_size,
        .disk_offset = clean_loc + item_start,
        .buf = (uint8_t*)buf,
        .csum_buf = vi->csum_buf,
        .dyn_data = vi->dyn_data,
    };
    int submit_fd = (vi->copy_flags & COPY_BUF_JOURNAL ? dsk.journal_fd : dsk.data_fd);
    uint64_t submit_offset = (vi->copy_flags & COPY_BUF_JOURNAL ? journal.offset : dsk.data_offset);
    uint32_t d_pos = 0;
    for (int n_pos = 0; n_pos < n_iov; n_pos += IOV_MAX)
    {
        int n_cur = n_iov-n_pos < IOV_MAX ? n_iov-n_pos : IOV_MAX;
        BS_SUBMIT_GET_SQE(sqe, data);
        PRIV(op)->pending_ops++;
        io_uring_prep_readv(sqe, submit_fd, iov + n_pos, n_cur, submit_offset + clean_loc + item_start + d_pos);
        data->callback = [this, op](ring_data_t *data) { handle_read_event(data, op); };
        if (n_pos > 0 || n_pos + IOV_MAX < n_iov)
        {
            uint32_t d_len = 0;
            for (int i = 0; i < IOV_MAX; i++)
                d_len += iov[n_pos+i].iov_len;
            data->iov.iov_len = d_len;
            d_pos += d_len;
        }
        else
            data->iov.iov_len = item_end-item_start;
    }
    if (!(vi->copy_flags & COPY_BUF_JOURNAL))
    {
        // Reads running parallel to flushes of the same clean block may read
        // a mixture of old and new data. So we don't verify checksums for such blocks.
        PRIV(op)->clean_block_used = 1;
    }
    return true;
}

int blockstore_impl_t::dequeue_read(blockstore_op_t *read_op)
{
    auto & clean_db = clean_db_shard(read_op->oid);
    auto clean_it = clean_db.find(read_op->oid);
    auto dirty_it = dirty_db.upper_bound((obj_ver_id){
        .oid = read_op->oid,
        .version = UINT64_MAX,
    });
    if (dirty_it != dirty_db.begin())
        dirty_it--;
    bool clean_found = clean_it != clean_db.end();
    bool dirty_found = (dirty_it != dirty_db.end() && dirty_it->first.oid == read_op->oid);
    if (!clean_found && !dirty_found)
    {
        read_op->version = 0;
        read_op->retval = -ENOENT;
        FINISH_OP(read_op);
        return 2;
    }
    uint64_t fulfilled = 0;
    PRIV(read_op)->pending_ops = 0;
    PRIV(read_op)->clean_block_used = 0;
    auto & rv = PRIV(read_op)->read_vec;
    uint64_t result_version = 0;
    if (dirty_found)
    {
        while (dirty_it->first.oid == read_op->oid)
        {
            dirty_entry& dirty = dirty_it->second;
            bool version_ok = !IS_IN_FLIGHT(dirty.state) && read_op->version >= dirty_it->first.version;
            if (version_ok)
            {
                if (IS_DELETE(dirty.state))
                {
                    assert(!result_version);
                    read_op->version = 0;
                    read_op->retval = -ENOENT;
                    FINISH_OP(read_op);
                    return 2;
                }
                int *dyn_data = (int*)(dsk.csum_block_size > 0 && alloc_dyn_data ? dirty.dyn_data : NULL);
                uint8_t *bmp_ptr = (alloc_dyn_data
                    ? (uint8_t*)dirty.dyn_data + sizeof(int) : (uint8_t*)&dirty.dyn_data);
                if (!result_version)
                {
                    result_version = dirty_it->first.version;
                    if (read_op->bitmap)
                    {
                        memcpy(read_op->bitmap, bmp_ptr, dsk.clean_entry_bitmap_size);
                    }
                }
                // If inmemory_journal is false, journal trim will have to wait until the read is completed
                if (!IS_JOURNAL(dirty.state))
                {
                    // Read from data disk, possibly checking checksums
                    if (!fulfill_clean_read(read_op, fulfilled, bmp_ptr, dyn_data,
                        dirty.offset, dirty.offset+dirty.len, dirty.location, dirty_it->first.version))
                    {
                        goto undo_read;
                    }
                }
                else
                {
                    // Copy from memory or read from journal, possibly checking checksums
                    if (!fulfill_read(read_op, fulfilled, dirty.offset, dirty.offset + dirty.len,
                        dirty.state, dirty_it->first.version, dirty.location, dirty.journal_sector+1,
                        journal.inmemory ? NULL : bmp_ptr+dsk.clean_entry_bitmap_size, dyn_data))
                    {
                        goto undo_read;
                    }
                }
            }
            if (fulfilled == read_op->len || dirty_it == dirty_db.begin())
            {
                break;
            }
            dirty_it--;
        }
    }
    if (clean_found)
    {
        if (!result_version)
        {
            result_version = clean_it->second.version;
            if (read_op->bitmap)
            {
                void *bmp_ptr = get_clean_entry_bitmap(clean_it->second.location, dsk.clean_entry_bitmap_size);
                memcpy(read_op->bitmap, bmp_ptr, dsk.clean_entry_bitmap_size);
            }
        }
        if (fulfilled < read_op->len)
        {
            if (!fulfill_clean_read(read_op, fulfilled, NULL, NULL, 0, dsk.data_block_size,
                clean_it->second.location, clean_it->second.version))
            {
                goto undo_read;
            }
        }
    }
    if (!result_version)
    {
        // May happen if there are entries in dirty_db but all of them are !version_ok
        read_op->version = 0;
        read_op->retval = -ENOENT;
        FINISH_OP(read_op);
        return 2;
    }
    assert(fulfilled == read_op->len);
    read_op->version = result_version;
    if (!PRIV(read_op)->pending_ops)
    {
        // everything is fulfilled from memory
        if (!PRIV(read_op)->read_vec.size())
        {
            // region is not allocated - return zeroes
            memset(read_op->buf, 0, read_op->len);
        }
        read_op->retval = read_op->len;
        FINISH_OP(read_op);
        return 2;
    }
    if (!journal.inmemory)
    {
        // Journal trim has to wait until the read is completed - record journal sector usage
        for (auto & rv: PRIV(read_op)->read_vec)
        {
            if (rv.journal_sector)
                journal.used_sectors.at(rv.journal_sector-1)++;
        }
    }
    read_op->retval = 0;
    return 2;
undo_read:
    // need to wait. undo added requests, don't dequeue op
    if (dsk.csum_block_size > dsk.bitmap_granularity)
    {
        for (auto & vec: rv)
        {
            if ((vec.copy_flags & COPY_BUF_CSUM_FILL) && vec.buf)
            {
                free(vec.buf);
                vec.buf = NULL;
            }
            if (vec.dyn_data && --(*vec.dyn_data) == 0) // refcount
            {
                free(vec.dyn_data);
                vec.dyn_data = NULL;
            }
        }
    }
    rv.clear();
    return 0;
}

int blockstore_impl_t::pad_journal_read(std::vector<copy_buffer_t> & rv, copy_buffer_t & cp,
    // FIXME Passing dirty_entry& would be nicer
    uint64_t dirty_offset, uint64_t dirty_end, uint64_t dirty_loc, uint8_t *csum_ptr, int *dyn_data,
    uint64_t offset, uint64_t submit_len, uint64_t & blk_begin, uint64_t & blk_end, uint8_t* & blk_buf)
{
    if (offset % dsk.csum_block_size || submit_len % dsk.csum_block_size)
    {
        if (offset < blk_end)
        {
            // Already being read as a part of the previous checksum block series
            cp.buf = blk_buf + offset - blk_begin;
            cp.copy_flags |= COPY_BUF_COALESCED;
            if (offset+submit_len > blk_end)
                cp.len = blk_end-offset;
            return 2;
        }
        else
        {
            // We don't use fill_partial_checksum_blocks for journal because journal writes never have holes (internal bitmap)
            blk_begin = (offset/dsk.csum_block_size) * dsk.csum_block_size;
            blk_begin = blk_begin < dirty_offset ? dirty_offset : blk_begin;
            blk_end = ((offset+submit_len-1)/dsk.csum_block_size + 1) * dsk.csum_block_size;
            blk_end = blk_end > dirty_end ? dirty_end : blk_end;
            if (blk_begin < offset || blk_end > offset+submit_len)
            {
                blk_buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, blk_end-blk_begin);
                cp.buf = blk_buf + offset - blk_begin;
                cp.copy_flags |= COPY_BUF_COALESCED;
                rv.push_back((copy_buffer_t){
                    .copy_flags = COPY_BUF_JOURNAL|COPY_BUF_CSUM_FILL,
                    .offset = blk_begin,
                    .len = blk_end-blk_begin,
                    .disk_offset = dirty_loc + blk_begin - dirty_offset,
                    .buf = blk_buf,
                    .csum_buf = (csum_ptr + (blk_begin/dsk.csum_block_size -
                        dirty_offset/dsk.csum_block_size) * (dsk.data_csum_type & 0xFF)),
                    .dyn_data = dyn_data,
                });
                if (dyn_data)
                {
                    (*dyn_data)++;
                }
                return 1;
            }
        }
    }
    return 0;
}

bool blockstore_impl_t::fulfill_clean_read(blockstore_op_t *read_op, uint64_t & fulfilled,
    uint8_t *clean_entry_bitmap, int *dyn_data, uint32_t item_start, uint32_t item_end, uint64_t clean_loc, uint64_t clean_ver)
{
    bool from_journal = clean_entry_bitmap != NULL;
    if (!clean_entry_bitmap)
    {
        // NULL clean_entry_bitmap means we're reading from data, not from the journal,
        // and the bitmap location is obvious
        clean_entry_bitmap = get_clean_entry_bitmap(clean_loc, 0);
    }
    if (dsk.csum_block_size > dsk.bitmap_granularity)
    {
        auto & rv = PRIV(read_op)->read_vec;
        int req = fill_partial_checksum_blocks(rv, fulfilled, clean_entry_bitmap, dyn_data, from_journal,
            (uint8_t*)read_op->buf, read_op->offset, read_op->offset+read_op->len);
        if (!inmemory_meta && !from_journal && req > 0)
        {
            // Read checksums from disk
            uint8_t *csum_buf = read_clean_meta_block(read_op, clean_loc, rv.size()-req);
            for (int i = req; i > 0; i--)
            {
                rv[rv.size()-i].csum_buf = csum_buf;
            }
        }
        for (int i = req; i > 0; i--)
        {
            if (!read_checksum_block(read_op, i, fulfilled, clean_loc))
            {
                return false;
            }
        }
        PRIV(read_op)->clean_block_used = req > 0;
    }
    else if (from_journal)
    {
        // Don't scan bitmap - journal writes don't have holes (internal bitmap)!
        uint8_t *csum = !dsk.csum_block_size ? 0 : (clean_entry_bitmap + dsk.clean_entry_bitmap_size +
            item_start/dsk.csum_block_size*(dsk.data_csum_type & 0xFF));
        if (!fulfill_read(read_op, fulfilled, item_start, item_end,
            (BS_ST_BIG_WRITE | BS_ST_STABLE), 0, clean_loc + item_start, 0, csum, dyn_data))
        {
            return false;
        }
        if (item_start > 0 && fulfilled < read_op->len)
        {
            // fill with zeroes
            assert(fulfill_read(read_op, fulfilled, 0, item_start, (BS_ST_DELETE | BS_ST_STABLE), 0, 0, 0, NULL, NULL));
        }
        if (item_end < dsk.data_block_size && fulfilled < read_op->len)
        {
            // fill with zeroes
            assert(fulfill_read(read_op, fulfilled, item_end, dsk.data_block_size, (BS_ST_DELETE | BS_ST_STABLE), 0, 0, 0, NULL, NULL));
        }
    }
    else
    {
        bool csum_done = !dsk.csum_block_size || inmemory_meta;
        uint8_t *csum_buf = clean_entry_bitmap;
        uint64_t bmp_start = 0, bmp_end = 0, bmp_size = dsk.data_block_size/dsk.bitmap_granularity;
        while (bmp_start < bmp_size)
        {
            while (!(clean_entry_bitmap[bmp_end >> 3] & (1 << (bmp_end & 0x7))) && bmp_end < bmp_size)
            {
                bmp_end++;
            }
            if (bmp_end > bmp_start)
            {
                // fill with zeroes
                assert(fulfill_read(read_op, fulfilled, bmp_start * dsk.bitmap_granularity,
                    bmp_end * dsk.bitmap_granularity, (BS_ST_DELETE | BS_ST_STABLE), 0, 0, 0, NULL, NULL));
            }
            bmp_start = bmp_end;
            while (clean_entry_bitmap[bmp_end >> 3] & (1 << (bmp_end & 0x7)) && bmp_end < bmp_size)
            {
                bmp_end++;
            }
            if (bmp_end > bmp_start)
            {
                if (!csum_done)
                {
                    // Read checksums from disk
                    csum_buf = read_clean_meta_block(read_op, clean_loc, PRIV(read_op)->read_vec.size());
                    csum_done = true;
                }
                uint8_t *csum = !dsk.csum_block_size ? 0 : (csum_buf + 2*dsk.clean_entry_bitmap_size + bmp_start*(dsk.data_csum_type & 0xFF));
                if (!fulfill_read(read_op, fulfilled, bmp_start * dsk.bitmap_granularity,
                    bmp_end * dsk.bitmap_granularity, (BS_ST_BIG_WRITE | BS_ST_STABLE), 0,
                    clean_loc + bmp_start * dsk.bitmap_granularity, 0, csum, dyn_data))
                {
                    return false;
                }
                bmp_start = bmp_end;
            }
        }
    }
    // Increment reference counter if clean data is being read from the disk
    if (PRIV(read_op)->clean_block_used)
    {
        auto & uo = used_clean_objects[clean_loc];
        uo.refs++;
        if (dsk.csum_block_size && flusher->is_mutated(clean_loc))
            uo.was_changed = true;
        PRIV(read_op)->clean_block_used = clean_loc;
    }
    return true;
}

uint8_t* blockstore_impl_t::read_clean_meta_block(blockstore_op_t *op, uint64_t clean_loc, int rv_pos)
{
    auto & rv = PRIV(op)->read_vec;
    auto sector = ((clean_loc >> dsk.block_order) / (dsk.meta_block_size / dsk.clean_entry_size)) * dsk.meta_block_size;
    auto pos = ((clean_loc >> dsk.block_order) % (dsk.meta_block_size / dsk.clean_entry_size)) * dsk.clean_entry_size;
    uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, dsk.meta_block_size);
    rv.insert(rv.begin()+rv_pos, (copy_buffer_t){
        .copy_flags = COPY_BUF_META_BLOCK|COPY_BUF_CSUM_FILL,
        .offset = pos,
        .buf = buf,
    });
    BS_SUBMIT_GET_SQE(sqe, data);
    data->iov = (struct iovec){ buf, (size_t)dsk.meta_block_size };
    PRIV(op)->pending_ops++;
    io_uring_prep_readv(sqe, dsk.meta_fd, &data->iov, 1, dsk.meta_offset + dsk.meta_block_size + sector);
    data->callback = [this, op](ring_data_t *data) { handle_read_event(data, op); };
    // return pointer to checksums + bitmap
    return buf + pos + sizeof(clean_disk_entry);
}

bool blockstore_impl_t::verify_padded_checksums(uint8_t *clean_entry_bitmap, uint8_t *csum_buf, uint32_t offset,
    iovec *iov, int n_iov, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb)
{
    assert(!(offset % dsk.csum_block_size));
    uint32_t *csums = (uint32_t*)csum_buf;
    uint32_t block_csum = 0;
    uint32_t block_done = 0;
    uint32_t block_num = clean_entry_bitmap ? offset/dsk.csum_block_size : 0;
    uint32_t bmp_pos = offset/dsk.bitmap_granularity;
    for (int i = 0; i < n_iov; i++)
    {
        uint32_t pos = 0;
        while (pos < iov[i].iov_len)
        {
            uint32_t start = pos;
            uint8_t bit = (clean_entry_bitmap[bmp_pos >> 3] >> (bmp_pos & 0x7)) & 1;
            while (pos < iov[i].iov_len && ((clean_entry_bitmap[bmp_pos >> 3] >> (bmp_pos & 0x7)) & 1) == bit)
            {
                pos += dsk.bitmap_granularity;
                bmp_pos++;
            }
            uint32_t len = pos-start;
            auto buf = (uint8_t*)iov[i].iov_base+start;
            while (block_done+len >= dsk.csum_block_size)
            {
                auto cur_len = dsk.csum_block_size-block_done;
                block_csum = crc32c_pad(block_csum, buf, bit ? cur_len : 0, bit ? 0 : cur_len, 0);
                if (block_csum != csums[block_num])
                {
                    if (bad_block_cb)
                        bad_block_cb(block_num*dsk.csum_block_size, block_csum, csums[block_num]);
                    else
                        return false;
                }
                block_num++;
                buf += cur_len;
                len -= cur_len;
                block_done = block_csum = 0;
            }
            if (len > 0)
            {
                block_csum = crc32c_pad(block_csum, buf, bit ? len : 0, bit ? 0 : len, 0);
                block_done += len;
            }
        }
    }
    assert(!block_done);
    return true;
}

bool blockstore_impl_t::verify_journal_checksums(uint8_t *csums, uint32_t offset,
    iovec *iov, int n_iov, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb)
{
    uint32_t block_csum = 0;
    uint32_t block_num = 0;
    uint32_t block_done = offset%dsk.csum_block_size;
    for (int i = 0; i < n_iov; i++)
    {
        uint32_t len = iov[i].iov_len;
        auto buf = (uint8_t*)iov[i].iov_base;
        while (block_done+len >= dsk.csum_block_size)
        {
            auto cur_len = dsk.csum_block_size-block_done;
            block_csum = crc32c(block_csum, buf, cur_len);
            if (block_csum != ((uint32_t*)csums)[block_num])
            {
                if (bad_block_cb)
                    bad_block_cb(block_num*dsk.csum_block_size, block_csum, ((uint32_t*)csums)[block_num]);
                else
                    return false;
            }
            block_num++;
            buf += cur_len;
            len -= cur_len;
            block_done = block_csum = 0;
        }
        if (len > 0)
        {
            block_csum = crc32c(block_csum, buf, len);
            block_done += len;
        }
    }
    if (block_done > 0 && block_csum != ((uint32_t*)csums)[block_num])
    {
        if (bad_block_cb)
            bad_block_cb(block_num*dsk.csum_block_size, block_csum, ((uint32_t*)csums)[block_num]);
        else
            return false;
    }
    return true;
}

bool blockstore_impl_t::verify_clean_padded_checksums(blockstore_op_t *op, uint64_t clean_loc, uint8_t *dyn_data, bool from_journal,
    iovec *iov, int n_iov, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb)
{
    uint32_t offset = clean_loc % dsk.data_block_size;
    if (from_journal)
        return verify_padded_checksums(dyn_data, dyn_data + dsk.clean_entry_bitmap_size, offset, iov, n_iov, bad_block_cb);
    clean_loc = (clean_loc >> dsk.block_order) << dsk.block_order;
    if (!dyn_data)
    {
        assert(inmemory_meta);
        dyn_data = get_clean_entry_bitmap(clean_loc, 0);
    }
    return verify_padded_checksums(dyn_data, dyn_data + 2*dsk.clean_entry_bitmap_size, offset, iov, n_iov, bad_block_cb);
}

void blockstore_impl_t::handle_read_event(ring_data_t *data, blockstore_op_t *op)
{
    live = true;
    PRIV(op)->pending_ops--;
    if (data->res != data->iov.iov_len)
    {
        // read error
        op->retval = data->res;
    }
    if (PRIV(op)->pending_ops == 0)
    {
        if (dsk.csum_block_size)
        {
            // verify checksums if required
            auto & rv = PRIV(op)->read_vec;
            void *meta_block = NULL;
            if (dsk.csum_block_size > dsk.bitmap_granularity)
            {
                for (int i = rv.size()-1; i >= 0 && (rv[i].copy_flags & COPY_BUF_CSUM_FILL); i--)
                {
                    if (rv[i].copy_flags & COPY_BUF_META_BLOCK)
                    {
                        // Metadata read. Skip
                        assert(!meta_block);
                        meta_block = rv[i].buf;
                        rv[i].buf = NULL;
                        continue;
                    }
                    struct iovec *iov = (struct iovec*)((uint8_t*)rv[i].buf + (rv[i].len & 0xFFFFFFFF));
                    int n_iov = rv[i].len >> 32;
                    bool ok = true;
                    if (rv[i].copy_flags & COPY_BUF_JOURNAL)
                    {
                        // SMALL_WRITE from journal
                        verify_journal_checksums(
                            rv[i].csum_buf, rv[i].offset, iov, n_iov,
                            [&](uint32_t bad_block, uint32_t calc_csum, uint32_t stored_csum)
                            {
                                ok = false;
                                printf(
                                    "Checksum mismatch in object %jx:%jx v%ju in journal at 0x%jx, checksum block #%u: got %08x, expected %08x\n",
                                    op->oid.inode, op->oid.stripe, op->version,
                                    rv[i].disk_offset, bad_block / dsk.csum_block_size, calc_csum, stored_csum
                                );
                            }
                        );
                    }
                    else
                    {
                        // BIG_WRITE from journal or clean data
                        // Do not verify checksums if the data location is/was mutated by flushers
                        auto & uo = used_clean_objects.at((rv[i].disk_offset >> dsk.block_order) << dsk.block_order);
                        if (!uo.was_changed)
                        {
                            verify_clean_padded_checksums(
                                op, rv[i].disk_offset, rv[i].csum_buf, (rv[i].copy_flags & COPY_BUF_JOURNALED_BIG), iov, n_iov,
                                [&](uint32_t bad_block, uint32_t calc_csum, uint32_t stored_csum)
                                {
                                    ok = false;
                                    printf(
                                        "Checksum mismatch in object %jx:%jx v%ju in %s data at 0x%jx, checksum block #%u: got %08x, expected %08x\n",
                                        op->oid.inode, op->oid.stripe, op->version,
                                        (rv[i].copy_flags & COPY_BUF_JOURNALED_BIG ? "redirect-write" : "clean"),
                                        rv[i].disk_offset, bad_block / dsk.csum_block_size, calc_csum, stored_csum
                                    );
                                }
                            );
                        }
                    }
                    if (!ok)
                    {
                        op->retval = -EDOM;
                    }
                    free(rv[i].buf);
                    rv[i].buf = NULL;
                    if (rv[i].dyn_data && --(*rv[i].dyn_data) == 0) // refcount
                    {
                        free(rv[i].dyn_data);
                        rv[i].dyn_data = NULL;
                    }
                }
            }
            else
            {
                for (auto & vec: rv)
                {
                    if (vec.copy_flags & COPY_BUF_META_BLOCK)
                    {
                        // Metadata read. Skip
                        assert(!meta_block);
                        meta_block = vec.buf;
                        vec.buf = NULL;
                        continue;
                    }
                    if (vec.csum_buf)
                    {
                        uint32_t *csum = (uint32_t*)vec.csum_buf;
                        for (size_t p = 0; p < vec.len; p += dsk.csum_block_size, csum++)
                        {
                            if (crc32c(0, (uint8_t*)op->buf + vec.offset - op->offset + p, dsk.csum_block_size) != *csum)
                            {
                                // checksum error
                                printf(
                                    "Checksum mismatch in object %jx:%jx v%ju in %s area at offset 0x%jx+0x%zx: %08x vs %08x\n",
                                    op->oid.inode, op->oid.stripe, op->version,
                                    (vec.copy_flags & COPY_BUF_JOURNAL) ? "journal" : "data", vec.disk_offset, p,
                                    crc32c(0, (uint8_t*)op->buf + vec.offset - op->offset + p, dsk.csum_block_size), *csum
                                );
                                op->retval = -EDOM;
                                break;
                            }
                        }
                    }
                    if (vec.dyn_data && --(*vec.dyn_data) == 0) // refcount
                    {
                        free(vec.dyn_data);
                        vec.dyn_data = NULL;
                    }
                }
            }
            if (meta_block)
            {
                // Free after checking
                free(meta_block);
                meta_block = NULL;
            }
        }
        if (PRIV(op)->clean_block_used)
        {
            // Release clean data block
            auto uo_it = used_clean_objects.find(PRIV(op)->clean_block_used);
            if (uo_it != used_clean_objects.end())
            {
                uo_it->second.refs--;
                if (uo_it->second.refs <= 0)
                {
                    if (uo_it->second.was_freed)
                    {
                        data_alloc->set(PRIV(op)->clean_block_used, false);
                    }
                    used_clean_objects.erase(uo_it);
                }
            }
        }
        if (!journal.inmemory)
        {
            // Release journal sector usage
            for (auto & rv: PRIV(op)->read_vec)
            {
                if (rv.journal_sector)
                {
                    auto used = --journal.used_sectors.at(rv.journal_sector-1);
                    if (used == 0)
                    {
                        journal.used_sectors.erase(rv.journal_sector-1);
                        flusher->mark_trim_possible();
                    }
                }
            }
        }
        if (op->retval == 0)
            op->retval = op->len;
        FINISH_OP(op);
    }
}

int blockstore_impl_t::read_bitmap(object_id oid, uint64_t target_version, void *bitmap, uint64_t *result_version)
{
    auto dirty_it = dirty_db.upper_bound((obj_ver_id){
        .oid = oid,
        .version = UINT64_MAX,
    });
    if (dirty_it != dirty_db.begin())
        dirty_it--;
    if (dirty_it != dirty_db.end())
    {
        while (dirty_it->first.oid == oid)
        {
            // Condition has to be the same as in dequeue_read()
            if (!IS_IN_FLIGHT(dirty_it->second.state) && target_version >= dirty_it->first.version)
            {
                if (result_version)
                    *result_version = dirty_it->first.version;
                if (bitmap)
                {
                    void *dyn_ptr = (alloc_dyn_data
                        ? (uint8_t*)dirty_it->second.dyn_data + sizeof(int) : (uint8_t*)&dirty_it->second.dyn_data);
                    memcpy(bitmap, dyn_ptr, dsk.clean_entry_bitmap_size);
                }
                return 0;
            }
            if (dirty_it == dirty_db.begin())
                break;
            dirty_it--;
        }
    }
    auto & clean_db = clean_db_shard(oid);
    auto clean_it = clean_db.find(oid);
    if (clean_it != clean_db.end())
    {
        if (result_version)
            *result_version = clean_it->second.version;
        if (bitmap)
        {
            void *bmp_ptr = get_clean_entry_bitmap(clean_it->second.location, dsk.clean_entry_bitmap_size);
            memcpy(bitmap, bmp_ptr, dsk.clean_entry_bitmap_size);
        }
        return 0;
    }
    if (result_version)
        *result_version = 0;
    if (bitmap)
        memset(bitmap, 0, dsk.clean_entry_bitmap_size);
    return -ENOENT;
}
