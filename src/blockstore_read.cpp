// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

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

// FIXME I've seen a bug here so I want some tests
int blockstore_impl_t::fulfill_read(blockstore_op_t *read_op, uint64_t &fulfilled, uint32_t item_start, uint32_t item_end,
    uint32_t item_state, uint64_t item_version, uint64_t item_location, uint64_t journal_sector)
{
    uint32_t cur_start = item_start;
    if (cur_start < read_op->offset + read_op->len && item_end > read_op->offset)
    {
        cur_start = cur_start < read_op->offset ? read_op->offset : cur_start;
        item_end = item_end > read_op->offset + read_op->len ? read_op->offset + read_op->len : item_end;
        auto it = PRIV(read_op)->read_vec.begin();
        while (1)
        {
            for (; it != PRIV(read_op)->read_vec.end(); it++)
            {
                if (it->offset >= cur_start)
                {
                    break;
                }
                else if (it->offset + it->len > cur_start)
                {
                    cur_start = it->offset + it->len;
                    if (cur_start >= item_end)
                    {
                        goto endwhile;
                    }
                }
            }
            if (it == PRIV(read_op)->read_vec.end() || it->offset > cur_start)
            {
                fulfill_read_t el = {
                    .offset = cur_start,
                    .len = it == PRIV(read_op)->read_vec.end() || it->offset >= item_end ? item_end-cur_start : it->offset-cur_start,
                    .journal_sector = journal_sector,
                };
                it = PRIV(read_op)->read_vec.insert(it, el);
                if (!fulfill_read_push(read_op,
                    (uint8_t*)read_op->buf + el.offset - read_op->offset,
                    item_location + el.offset - item_start,
                    el.len, item_state, item_version))
                {
                    return 0;
                }
                fulfilled += el.len;
            }
            cur_start = it->offset + it->len;
            if (it == PRIV(read_op)->read_vec.end() || cur_start >= item_end)
            {
                break;
            }
        }
    }
endwhile:
    return 1;
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
        clean_entry_bitmap = (uint8_t*)(clean_bitmap + meta_loc*2*dsk.clean_entry_bitmap_size + offset);
    return clean_entry_bitmap;
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
        // region is not allocated - return zeroes
        memset(read_op->buf, 0, read_op->len);
        read_op->version = 0;
        read_op->retval = read_op->len;
        FINISH_OP(read_op);
        return 2;
    }
    uint64_t fulfilled = 0;
    PRIV(read_op)->pending_ops = 0;
    uint64_t result_version = 0;
    if (dirty_found)
    {
        while (dirty_it->first.oid == read_op->oid)
        {
            dirty_entry& dirty = dirty_it->second;
            bool version_ok = !IS_IN_FLIGHT(dirty.state) && read_op->version >= dirty_it->first.version;
            if (IS_SYNCED(dirty.state))
            {
                if (!version_ok && read_op->version != 0)
                    read_op->version = dirty_it->first.version;
                version_ok = true;
            }
            if (version_ok)
            {
                if (!result_version)
                {
                    result_version = dirty_it->first.version;
                    if (read_op->bitmap)
                    {
                        void *bmp_ptr = (dsk.clean_entry_bitmap_size > sizeof(void*) ? dirty_it->second.bitmap : &dirty_it->second.bitmap);
                        memcpy(read_op->bitmap, bmp_ptr, dsk.clean_entry_bitmap_size);
                    }
                }
                // If inmemory_journal is false, journal trim will have to wait until the read is completed
                if (!fulfill_read(read_op, fulfilled, dirty.offset, dirty.offset + dirty.len,
                    dirty.state, dirty_it->first.version, dirty.location + (IS_JOURNAL(dirty.state) ? 0 : dirty.offset),
                    (IS_JOURNAL(dirty.state) ? dirty.journal_sector+1 : 0)))
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
            if (!dsk.clean_entry_bitmap_size)
            {
                if (!fulfill_read(read_op, fulfilled, 0, dsk.data_block_size,
                    (BS_ST_BIG_WRITE | BS_ST_STABLE), 0, clean_it->second.location, 0))
                {
                    // need to wait. undo added requests, don't dequeue op
                    PRIV(read_op)->read_vec.clear();
                    return 0;
                }
            }
            else
            {
                uint8_t *clean_entry_bitmap = get_clean_entry_bitmap(clean_it->second.location, 0);
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
                            bmp_end * dsk.bitmap_granularity, (BS_ST_DELETE | BS_ST_STABLE), 0, 0, 0));
                    }
                    bmp_start = bmp_end;
                    while (clean_entry_bitmap[bmp_end >> 3] & (1 << (bmp_end & 0x7)) && bmp_end < bmp_size)
                    {
                        bmp_end++;
                    }
                    if (bmp_end > bmp_start)
                    {
                        if (!fulfill_read(read_op, fulfilled, bmp_start * dsk.bitmap_granularity,
                            bmp_end * dsk.bitmap_granularity, (BS_ST_BIG_WRITE | BS_ST_STABLE), 0,
                            clean_it->second.location + bmp_start * dsk.bitmap_granularity, 0))
                        {
                            // need to wait. undo added requests, don't dequeue op
                            PRIV(read_op)->read_vec.clear();
                            return 0;
                        }
                        bmp_start = bmp_end;
                    }
                }
            }
        }
    }
    else if (fulfilled < read_op->len)
    {
        // fill remaining parts with zeroes
        assert(fulfill_read(read_op, fulfilled, 0, dsk.data_block_size, (BS_ST_DELETE | BS_ST_STABLE), 0, 0, 0));
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
                journal.used_sectors[rv.journal_sector-1]++;
        }
    }
    read_op->retval = 0;
    return 2;
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
                    void *bmp_ptr = (dsk.clean_entry_bitmap_size > sizeof(void*) ? dirty_it->second.bitmap : &dirty_it->second.bitmap);
                    memcpy(bitmap, bmp_ptr, dsk.clean_entry_bitmap_size);
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
