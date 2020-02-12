#include "blockstore_impl.h"

int blockstore_impl_t::fulfill_read_push(blockstore_op_t *op, void *buf, uint64_t offset, uint64_t len,
    uint32_t item_state, uint64_t item_version)
{
    if (!len)
    {
        // Zero-length version - skip
        return 1;
    }
    if (IS_IN_FLIGHT(item_state))
    {
        // Pause until it's written somewhere
        PRIV(op)->wait_for = WAIT_IN_FLIGHT;
        PRIV(op)->wait_detail = item_version;
        return 0;
    }
    else if (IS_DELETE(item_state))
    {
        // item is unallocated - return zeroes
        memset(buf, 0, len);
        return 1;
    }
    if (journal.inmemory && IS_JOURNAL(item_state))
    {
        memcpy(buf, journal.buffer + offset, len);
        return 1;
    }
    BS_SUBMIT_GET_SQE(sqe, data);
    data->iov = (struct iovec){ buf, len };
    PRIV(op)->pending_ops++;
    my_uring_prep_readv(
        sqe,
        IS_JOURNAL(item_state) ? journal.fd : data_fd,
        &data->iov, 1,
        (IS_JOURNAL(item_state) ? journal.offset : data_offset) + offset
    );
    data->callback = [this, op](ring_data_t *data) { handle_read_event(data, op); };
    return 1;
}

int blockstore_impl_t::fulfill_read(blockstore_op_t *read_op, uint64_t &fulfilled, uint32_t item_start, uint32_t item_end,
    uint32_t item_state, uint64_t item_version, uint64_t item_location)
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
                if (it->offset >= cur_start)
                    break;
            if (it == PRIV(read_op)->read_vec.end() || it->offset > cur_start)
            {
                fulfill_read_t el = {
                    .offset = cur_start,
                    .len = it == PRIV(read_op)->read_vec.end() || it->offset >= item_end ? item_end-cur_start : it->offset-cur_start,
                };
                it = PRIV(read_op)->read_vec.insert(it, el);
                if (!fulfill_read_push(read_op,
                    read_op->buf + el.offset - read_op->offset,
                    item_location + el.offset - item_start,
                    el.len, item_state, item_version))
                {
                    return 0;
                }
                fulfilled += el.len;
            }
            cur_start = it->offset + it->len;
            if (it == PRIV(read_op)->read_vec.end() || cur_start >= item_end)
                break;
        }
    }
    return 1;
}

int blockstore_impl_t::dequeue_read(blockstore_op_t *read_op)
{
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
        read_op->callback(read_op);
        return 1;
    }
    uint64_t fulfilled = 0;
    PRIV(read_op)->pending_ops = 0;
    uint64_t result_version = 0;
    if (dirty_found)
    {
        while (dirty_it->first.oid == read_op->oid)
        {
            dirty_entry& dirty = dirty_it->second;
            bool version_ok = read_op->version >= dirty_it->first.version;
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
                }
                if (!fulfill_read(read_op, fulfilled, dirty.offset, dirty.offset + dirty.len,
                    dirty.state, dirty_it->first.version, dirty.location + (IS_JOURNAL(dirty.state) ? 0 : dirty.offset)))
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
    if (clean_it != clean_db.end() && fulfilled < read_op->len)
    {
        if (!result_version)
        {
            result_version = clean_it->second.version;
        }
        if (!clean_entry_bitmap_size)
        {
            if (!fulfill_read(read_op, fulfilled, 0, block_size, ST_CURRENT, 0, clean_it->second.location))
            {
                // need to wait. undo added requests, don't dequeue op
                PRIV(read_op)->read_vec.clear();
                return 0;
            }
        }
        else
        {
            uint64_t meta_loc = clean_it->second.location >> block_order;
            uint8_t *clean_entry_bitmap;
            if (inmemory_meta)
            {
                uint64_t sector = (meta_loc / (meta_block_size / clean_entry_size)) * meta_block_size;
                uint64_t pos = (meta_loc % (meta_block_size / clean_entry_size));
                clean_entry_bitmap = (uint8_t*)(metadata_buffer + sector + pos*clean_entry_size + sizeof(clean_disk_entry));
            }
            else
            {
                clean_entry_bitmap = (uint8_t*)(clean_bitmap + meta_loc*clean_entry_bitmap_size);
            }
            uint64_t bmp_start = 0, bmp_end = 0, bmp_size = block_size/bitmap_granularity;
            while (bmp_start < bmp_size)
            {
                while (!(clean_entry_bitmap[bmp_end >> 3] & (1 << (bmp_end & 0x7))) && bmp_end < bmp_size)
                {
                    bmp_end++;
                }
                if (bmp_end > bmp_start)
                {
                    // fill with zeroes
                    fulfill_read(read_op, fulfilled, bmp_start * bitmap_granularity,
                        bmp_end * bitmap_granularity, ST_DEL_STABLE, 0, 0);
                }
                bmp_start = bmp_end;
                while (clean_entry_bitmap[bmp_end >> 3] & (1 << (bmp_end & 0x7)) && bmp_end < bmp_size)
                {
                    bmp_end++;
                }
                if (bmp_end > bmp_start)
                {
                    if (!fulfill_read(read_op, fulfilled, bmp_start * bitmap_granularity,
                        bmp_end * bitmap_granularity, ST_CURRENT, 0, clean_it->second.location + bmp_start * bitmap_granularity))
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
    else if (fulfilled < read_op->len)
    {
        // fill remaining parts with zeroes
        fulfill_read(read_op, fulfilled, 0, block_size, ST_DEL_STABLE, 0, 0);
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
        read_op->callback(read_op);
        return 1;
    }
    read_op->retval = 0;
    return 1;
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
        if (op->retval == 0)
            op->retval = op->len;
        FINISH_OP(op);
    }
}
