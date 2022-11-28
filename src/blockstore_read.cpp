// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <limits.h>
#include "blockstore_impl.h"

int blockstore_impl_t::fulfill_read_push(blockstore_op_t *op, void *buf, uint64_t offset, uint64_t len,
    uint32_t item_state, uint64_t item_version)
{
    if (!len)
    {
        // Zero-length version - skip
        return 1;
    }
    else if (IS_IN_FLIGHT(item_state))
    {
        // Write not finished yet - skip
        return 1;
    }
    else if (IS_DELETE(item_state))
    {
        // item is unallocated - return zeroes
        memset(buf, 0, len);
        return 1;
    }
    if (journal.inmemory && IS_JOURNAL(item_state))
    {
        memcpy(buf, (uint8_t*)journal.buffer + offset, len);
        return 1;
    }
    BS_SUBMIT_GET_SQE(sqe, data);
    data->iov = (struct iovec){ buf, len };
    PRIV(op)->pending_ops++;
    my_uring_prep_readv(
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
    auto alloc_start = item_start;
    int i = 0;
    while (1)
    {
        // COPY_BUF_CSUM_FILL items are fake items inserted in the end, their offsets aren't in order
        for (; i < read_vec.size() && !(read_vec[i].copy_flags & COPY_BUF_CSUM_FILL); i++)
        {
            if (read_vec[i].offset >= cur_start)
                break;
            else if (read_vec[i].offset + read_vec[i].len > cur_start)
            {
                // Allocated: cur_start .. read_vec[i].offset + read_vec[i].len
                cur_start = read_vec[i].offset + read_vec[i].len;
                if (cur_start >= item_end)
                    goto endwhile;
            }
        }
        if (i < read_vec.size() && !(read_vec[i].copy_flags & COPY_BUF_CSUM_FILL) && read_vec[i].offset == cur_start)
        {
            // Allocated - don't move alloc_start
        }
        else
        {
            // Hole
            uint32_t cur_end = (i == read_vec.size() || (read_vec[i].copy_flags & COPY_BUF_CSUM_FILL) || read_vec[i].offset >= item_end
                ? item_end : read_vec[i].offset);
            if (alloc_start < cur_start)
                i += callback(i, true, alloc_start, cur_start);
            i += callback(i, false, cur_start, cur_end);
            alloc_start = cur_end;
        }
        if (i >= read_vec.size() || (read_vec[i].copy_flags & COPY_BUF_CSUM_FILL))
            break;
        cur_start = read_vec[i].offset + read_vec[i].len;
        if (cur_start >= item_end)
            break;
    }
endwhile:
    if (alloc_start < cur_start)
        i += callback(i, true, alloc_start, cur_start);
}

int blockstore_impl_t::fulfill_read(blockstore_op_t *read_op,
    uint64_t &fulfilled, uint32_t item_start, uint32_t item_end,
    uint32_t item_state, uint64_t item_version, uint64_t item_location,
    uint64_t journal_sector, uint8_t *csum)
{
    int r = 1;
    uint32_t cur_start = item_start;
    if (cur_start < read_op->offset + read_op->len && item_end > read_op->offset)
    {
        cur_start = cur_start < read_op->offset ? read_op->offset : cur_start;
        item_end = item_end > read_op->offset + read_op->len ? read_op->offset + read_op->len : item_end;
        find_holes(PRIV(read_op)->read_vec, cur_start, item_end, [&](int pos, bool alloc, uint32_t start, uint32_t end)
        {
            if (alloc)
                return 0;
            copy_buffer_t el = {
                .copy_flags = (IS_JOURNAL(item_state) ? COPY_BUF_JOURNAL : COPY_BUF_DATA),
                .offset = start,
                .len = end-start,
                .disk_offset = item_location + el.offset - item_start,
                .journal_sector = journal_sector,
                .csum_buf = !csum ? NULL : (csum + (cur_start - item_start) / dsk.csum_block_size * (dsk.data_csum_type & 0xFF)),
            };
            if (IS_BIG_WRITE(item_state))
            {
                // If we don't track it then we may IN THEORY read another object's data:
                // submit read -> remove the object -> flush remove -> overwrite with another object -> finish read
                // Very improbable, but possible
                PRIV(read_op)->clean_version_used = 1;
            }
            PRIV(read_op)->read_vec.insert(PRIV(read_op)->read_vec.begin() + pos, el);
            if (!fulfill_read_push(read_op,
                (uint8_t*)read_op->buf + el.offset - read_op->offset,
                item_location + el.offset - item_start,
                el.len, item_state, item_version))
            {
                PRIV(read_op)->read_vec.clear();
                r = 0;
                return 0;
            }
            fulfilled += el.len;
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
        clean_entry_bitmap = (uint8_t*)(clean_dyn_data + meta_loc*dsk.clean_dyn_size + offset);
    return clean_entry_bitmap;
}

int blockstore_impl_t::fill_partial_checksum_blocks(std::vector<copy_buffer_t> & rv, uint64_t & fulfilled,
    uint8_t *clean_entry_bitmap, uint8_t *read_buf, uint64_t read_offset, uint64_t read_end)
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
                .copy_flags = COPY_BUF_CSUM_FILL,
                .offset = start_block*dsk.csum_block_size,
                .len = (end_block-start_block)*dsk.csum_block_size,
            });
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

bool blockstore_impl_t::read_clean_checksum_block(blockstore_op_t *op, int rv_pos,
    uint64_t &fulfilled, uint64_t clean_loc, uint32_t item_start, uint32_t item_end)
{
    auto & rv = PRIV(op)->read_vec;
    uint32_t fill_size = 0;
    int n_iov = 0;
    find_holes(rv, item_start, item_end, [&](int pos, bool alloc, uint32_t cur_start, uint32_t cur_end)
    {
        if (alloc)
            fill_size += cur_end-cur_start;
        n_iov++;
        return 0;
    });
    void *buf = memalign_or_die(MEM_ALIGNMENT, fill_size + n_iov*sizeof(struct iovec));
    iovec *iov = (struct iovec*)((uint8_t*)buf+fill_size);
    n_iov = 0;
    fill_size = 0;
    find_holes(rv, item_start, item_end, [&](int pos, bool alloc, uint32_t cur_start, uint32_t cur_end)
    {
        if (alloc)
        {
            iov[n_iov++] = (struct iovec){ (uint8_t*)buf+fill_size, cur_end-cur_start };
            fill_size += cur_end-cur_start;
        }
        else
        {
            iov[n_iov++] = (struct iovec){ (uint8_t*)op->buf+cur_start-op->offset, cur_end-cur_start };
            rv.insert(rv.begin() + pos, (copy_buffer_t){
                .copy_flags = COPY_BUF_DATA,
                .offset = cur_start,
                .len = cur_end-cur_start,
                .disk_offset = clean_loc,
            });
            fulfilled += cur_end-cur_start;
            return 1;
        }
        return 0;
    });
    // Save buf into read_vec too but in a creepy way
    // FIXME: Shit, something else should be invented %)
    rv[rv.size()-rv_pos] = (copy_buffer_t){
        .copy_flags = COPY_BUF_CSUM_FILL,
        .offset = 0xffffffff,
        .len = ((uint64_t)n_iov << 32) | fill_size,
        .disk_offset = clean_loc + item_start,
        .csum_buf = (uint8_t*)buf,
    };
    uint32_t d_pos = 0;
    for (int n_pos = 0; n_pos < n_iov; n_pos += IOV_MAX)
    {
        int n_cur = n_iov-n_pos < IOV_MAX ? n_iov-n_pos : IOV_MAX;
        BS_SUBMIT_GET_SQE(sqe, data);
        PRIV(op)->pending_ops++;
        my_uring_prep_readv(
            sqe, dsk.data_fd, iov + n_pos, n_cur, dsk.data_offset + clean_loc + d_pos
        );
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
    // Reading may race with flushing.
    // - Flushing happens in 3 steps: (2) punch holes in meta -> (4) update data -> (6) update meta
    // - Reading may start/end at: 1/3, 1/5, 1/7, 3/5, 3/7, 5/7
    // - 1/3, 1/5, 3/5 are not a problem because we'll check data using punched bitmap and CRCs
    // - For 1/7, 3/7 and 5/7 to finish correctly we need a copy of punched metadata
    //   otherwise the checksum may not match
    // So flushers save a copy of punched metadata if the object is being read during (6).
    PRIV(op)->clean_version_used = 1;
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
    PRIV(read_op)->clean_version_used = 0;
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
                size_t dyn_size = dsk.dirty_dyn_size(dirty.offset, dirty.len);
                uint8_t *bmp_ptr = (uint8_t*)(dyn_size > sizeof(void*) ? dirty.dyn_data : &dirty.dyn_data);
                if (!result_version)
                {
                    result_version = dirty_it->first.version;
                    if (read_op->bitmap)
                    {
                        memcpy(read_op->bitmap, bmp_ptr, dsk.clean_entry_bitmap_size);
                    }
                }
                // If inmemory_journal is false, journal trim will have to wait until the read is completed
                // FIXME: Verify checksums when reading from journal disk
                if (!fulfill_read(read_op, fulfilled, dirty.offset, dirty.offset + dirty.len,
                    dirty.state, dirty_it->first.version, dirty.location + (IS_JOURNAL(dirty.state) ? 0 : dirty.offset),
                    (IS_JOURNAL(dirty.state) ? dirty.journal_sector+1 : 0),
                    journal.inmemory ? NULL : bmp_ptr+dsk.clean_entry_bitmap_size))
                {
                    // need to wait. undo added requests, don't dequeue op
                    PRIV(read_op)->read_vec.clear();
                    return 0;
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
            uint8_t *clean_entry_bitmap = get_clean_entry_bitmap(clean_it->second.location, 0);
            if (!dsk.clean_entry_bitmap_size)
            {
                if (!fulfill_read(read_op, fulfilled, 0, dsk.data_block_size,
                    (BS_ST_BIG_WRITE | BS_ST_STABLE), 0, clean_it->second.location, 0,
                    clean_entry_bitmap + 2*dsk.clean_entry_bitmap_size))
                {
                    // need to wait. undo added requests, don't dequeue op
                    PRIV(read_op)->read_vec.clear();
                    return 0;
                }
            }
            else if (dsk.csum_block_size > dsk.bitmap_granularity)
            {
                auto & rv = PRIV(read_op)->read_vec;
                int req = fill_partial_checksum_blocks(rv, fulfilled, clean_entry_bitmap,
                    (uint8_t*)read_op->buf, read_op->offset, read_op->offset+read_op->len);
                for (int i = req; i > 0; i--)
                {
                    auto & vi = rv[rv.size()-i];
                    if (!read_clean_checksum_block(read_op, i, fulfilled, clean_it->second.location, vi.offset, vi.offset+vi.len))
                    {
                        // need to wait. undo added requests, don't dequeue op
                        for (auto & vec: rv)
                        {
                            if (vec.copy_flags == COPY_BUF_CSUM_FILL && vec.csum_buf)
                            {
                                free(vec.csum_buf);
                                vec.csum_buf = NULL;
                            }
                        }
                        rv.clear();
                        return 0;
                    }
                }
            }
            else
            {
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
                            bmp_end * dsk.bitmap_granularity, (BS_ST_DELETE | BS_ST_STABLE), 0, 0, 0, NULL));
                    }
                    bmp_start = bmp_end;
                    while (clean_entry_bitmap[bmp_end >> 3] & (1 << (bmp_end & 0x7)) && bmp_end < bmp_size)
                    {
                        bmp_end++;
                    }
                    if (bmp_end > bmp_start)
                    {
                        uint8_t *csum = !dsk.csum_block_size ? 0 : (clean_entry_bitmap +
                            2*dsk.clean_entry_bitmap_size +
                            bmp_start*dsk.bitmap_granularity/dsk.csum_block_size*(dsk.data_csum_type & 0xFF));
                        if (!fulfill_read(read_op, fulfilled, bmp_start * dsk.bitmap_granularity,
                            bmp_end * dsk.bitmap_granularity, (BS_ST_BIG_WRITE | BS_ST_STABLE), 0,
                            clean_it->second.location + bmp_start * dsk.bitmap_granularity, 0, csum))
                        {
                            // need to wait. undo added requests, don't dequeue op
                            PRIV(read_op)->read_vec.clear();
                            return 0;
                        }
                        bmp_start = bmp_end;
                    }
                }
            }
            // Increment counter if clean data is being read from the disk
            if (PRIV(read_op)->clean_version_used)
            {
                obj_ver_id ov = { .oid = read_op->oid, .version = clean_it->second.version };
                used_clean_objects[ov].refs++;
                PRIV(read_op)->clean_version_used = ov.version;
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
    if (fulfilled < read_op->len)
    {
        assert(fulfill_read(read_op, fulfilled, 0, dsk.data_block_size, (BS_ST_DELETE | BS_ST_STABLE), 0, 0, 0, NULL));
        assert(fulfilled == read_op->len);
    }
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
                journal.used_sectors[rv.journal_sector-1]++;
        }
    }
    read_op->retval = 0;
    return 2;
}

bool blockstore_impl_t::verify_padded_checksums(uint8_t *clean_entry_bitmap, uint32_t offset,
    iovec *iov, int n_iov, std::function<void(uint32_t, uint32_t, uint32_t)> bad_block_cb)
{
    assert(!(offset % dsk.csum_block_size));
    uint32_t *csums = (uint32_t*)(clean_entry_bitmap + 2*dsk.clean_entry_bitmap_size);
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
                    bad_block_cb(block_num*dsk.csum_block_size - (offset%dsk.csum_block_size), block_csum, ((uint32_t*)csums)[block_num]);
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
            bad_block_cb(block_num*dsk.csum_block_size - (offset%dsk.csum_block_size), block_csum, ((uint32_t*)csums)[block_num]);
        else
            return false;
    }
    return true;
}

bool blockstore_impl_t::verify_read_padded_checksums(blockstore_op_t *op, uint64_t clean_loc, iovec *iov, int n_iov)
{
    uint32_t offset = clean_loc % dsk.data_block_size;
    clean_loc = (clean_loc >> dsk.block_order) << dsk.block_order;
    // First verify against the newest checksum version
    uint8_t *clean_entry_bitmap = get_clean_entry_bitmap(clean_loc, 0);
    if (verify_padded_checksums(clean_entry_bitmap, offset, iov, n_iov, NULL))
        return true;
    // Check through all relevant "metadata backups" possibly added by flushers
    auto mb_it = used_clean_objects.lower_bound((obj_ver_id){ .oid = op->oid, .version = PRIV(op)->clean_version_used });
    for (; mb_it != used_clean_objects.end() && mb_it->first.oid == op->oid; mb_it++)
        if (mb_it->second.meta != NULL && verify_padded_checksums(mb_it->second.meta, offset, iov, n_iov, NULL))
            return true;
    return false;
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
            if (dsk.csum_block_size > dsk.bitmap_granularity)
            {
                for (int i = rv.size()-1; i >= 0 && rv[i].copy_flags == COPY_BUF_CSUM_FILL; i--)
                {
                    struct iovec *iov = (struct iovec*)(rv[i].csum_buf + (rv[i].len & 0xFFFFFFFF));
                    if (!verify_read_padded_checksums(op, rv[i].disk_offset, iov, rv[i].len >> 32))
                        op->retval = -EDOM;
                    free(rv[i].csum_buf);
                    rv[i].csum_buf = NULL;
                }
            }
            else
            {
                for (auto & vec: rv)
                {
                    if (vec.csum_buf)
                    {
                        uint32_t *csum = (uint32_t*)vec.csum_buf;
                        for (size_t p = 0; p < data->iov.iov_len; p += dsk.csum_block_size, csum++)
                        {
                            if (crc32c(0, (uint8_t*)data->iov.iov_base + p, dsk.csum_block_size) != *csum)
                            {
                                // checksum error
                                printf(
                                    "Checksum mismatch in object %lx:%lx v%lu in %s area at offset 0x%lx: %08x vs %08x\n",
                                    op->oid.inode, op->oid.stripe, op->version,
                                    (vec.copy_flags & COPY_BUF_JOURNAL) ? "journal" : "data", vec.disk_offset,
                                    crc32c(0, (uint8_t*)data->iov.iov_base + p, dsk.csum_block_size), *csum
                                );
                                op->retval = -EDOM;
                                break;
                            }
                        }
                    }
                }
            }
        }
        if (PRIV(op)->clean_version_used)
        {
            // Release clean data block
            obj_ver_id ov = { .oid = op->oid, .version = PRIV(op)->clean_version_used };
            auto uo_it = used_clean_objects.find(ov);
            if (uo_it != used_clean_objects.end())
            {
                uo_it->second.refs--;
                if (uo_it->second.refs <= 0)
                {
                    // Check to the left - even older usage entries may exist
                    bool still_used = false;
                    while (uo_it != used_clean_objects.begin())
                    {
                        uo_it--;
                        if (uo_it->first.oid != op->oid)
                        {
                            uo_it++;
                            break;
                        }
                        if (uo_it->second.refs > 0)
                        {
                            still_used = true;
                            break;
                        }
                    }
                    // Free uo_it AND all following records with refs==0 too
                    if (!still_used)
                    {
                        while (uo_it != used_clean_objects.end() &&
                            uo_it->first.oid == op->oid &&
                            uo_it->second.refs == 0)
                        {
                            if (uo_it->second.freed_block > 0)
                            {
                                data_alloc->set(uo_it->second.freed_block-1, false);
                            }
                            if (uo_it->second.meta)
                            {
                                free(uo_it->second.meta);
                                uo_it->second.meta = NULL;
                            }
                            used_clean_objects.erase(uo_it++);
                        }
                    }
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
                    auto used = --journal.used_sectors[rv.journal_sector-1];
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
            if (target_version >= dirty_it->first.version)
            {
                if (result_version)
                    *result_version = dirty_it->first.version;
                if (bitmap)
                {
                    size_t dyn_size = dsk.dirty_dyn_size(dirty_it->second.offset, dirty_it->second.len);
                    void *dyn_ptr = (dyn_size > sizeof(void*) ? dirty_it->second.dyn_data : &dirty_it->second.dyn_data);
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
