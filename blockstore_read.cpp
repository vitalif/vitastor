#include "blockstore.h"

int blockstore::fulfill_read_push(blockstore_operation *op, uint64_t &fulfilled, uint32_t item_start,
    uint32_t item_state, uint64_t item_version, uint64_t item_location, uint32_t cur_start, uint32_t cur_end)
{
    if (cur_end > cur_start)
    {
        if (IS_IN_FLIGHT(item_state))
        {
            // Pause until it's written somewhere
            op->wait_for = WAIT_IN_FLIGHT;
            op->wait_detail = item_version;
            return 0;
        }
        else if (item_state == ST_DEL_WRITTEN || item_state == ST_DEL_SYNCED || item_state == ST_DEL_MOVED)
        {
            // item is unallocated - return zeroes
            memset(op->buf + cur_start - op->offset, 0, cur_end - cur_start);
            return 1;
        }
        BS_SUBMIT_GET_SQE(sqe, data);
        data->iov = (struct iovec){
            op->buf + cur_start - op->offset,
            cur_end - cur_start
        };
        // FIXME: use simple std::vector instead of map for read_vec
        op->read_vec[cur_start] = data->iov;
        io_uring_prep_readv(
            sqe,
            IS_JOURNAL(item_state) ? journal.fd : data_fd,
            &data->iov, 1,
            (IS_JOURNAL(item_state) ? journal.offset : data_offset) + item_location + cur_start - item_start
        );
        data->callback = [this, op](ring_data_t *data) { handle_read_event(data, op); };
        fulfilled += cur_end-cur_start;
    }
    return 1;
}

int blockstore::fulfill_read(blockstore_operation *read_op, uint64_t &fulfilled, uint32_t item_start, uint32_t item_end,
    uint32_t item_state, uint64_t item_version, uint64_t item_location)
{
    uint32_t cur_start = item_start;
    if (cur_start < read_op->offset + read_op->len && item_end > read_op->offset)
    {
        cur_start = cur_start < read_op->offset ? read_op->offset : cur_start;
        item_end = item_end > read_op->offset + read_op->len ? read_op->offset + read_op->len : item_end;
        auto fulfill_near = read_op->read_vec.lower_bound(cur_start);
        if (fulfill_near != read_op->read_vec.begin())
        {
            fulfill_near--;
            if (fulfill_near->first + fulfill_near->second.iov_len <= cur_start)
            {
                fulfill_near++;
            }
        }
        while (fulfill_near != read_op->read_vec.end() && fulfill_near->first < item_end)
        {
            if (!fulfill_read_push(read_op, fulfilled, item_start, item_state, item_version, item_location, cur_start, fulfill_near->first))
            {
                return 0;
            }
            cur_start = fulfill_near->first + fulfill_near->second.iov_len;
            fulfill_near++;
        }
        if (!fulfill_read_push(read_op, fulfilled, item_start, item_state, item_version, item_location, cur_start, item_end))
        {
            return 0;
        }
    }
    return 1;
}

int blockstore::dequeue_read(blockstore_operation *read_op)
{
    auto clean_it = clean_db.find(read_op->oid);
    auto dirty_it = dirty_db.upper_bound((obj_ver_id){
        .oid = read_op->oid,
        .version = UINT64_MAX,
    });
    dirty_it--;
    bool clean_found = clean_it != clean_db.end();
    bool dirty_found = (dirty_it != dirty_db.end() && dirty_it->first.oid == read_op->oid);
    if (!clean_found && !dirty_found)
    {
        // region is not allocated - return zeroes
        memset(read_op->buf, 0, read_op->len);
        read_op->retval = read_op->len;
        read_op->callback(read_op);
        return 1;
    }
    uint64_t fulfilled = 0;
    if (dirty_found)
    {
        while (dirty_it->first.oid == read_op->oid)
        {
            dirty_entry& dirty = dirty_it->second;
            bool version_ok = read_op->version >= dirty_it->first.version;
            if (IS_STABLE(dirty.state))
            {
                if (!version_ok && read_op->version != 0)
                    read_op->version = dirty_it->first.version;
                version_ok = true;
            }
            if (version_ok)
            {
                if (!fulfill_read(read_op, fulfilled, dirty.offset, dirty.offset + dirty.len,
                    dirty.state, dirty_it->first.version, dirty.location))
                {
                    // need to wait. undo added requests, don't dequeue op
                    read_op->read_vec.clear();
                    return 0;
                }
            }
            if (fulfilled == read_op->len)
            {
                break;
            }
            dirty_it--;
        }
    }
    if (clean_it != clean_db.end())
    {
        if (!fulfill_read(read_op, fulfilled, 0, block_size, ST_CURRENT, 0, clean_it->second.location))
        {
            // need to wait. undo added requests, don't dequeue op
            read_op->read_vec.clear();
            return 0;
        }
    }
    if (!read_op->read_vec.size())
    {
        // region is not allocated - return zeroes
        memset(read_op->buf, 0, read_op->len);
        read_op->retval = read_op->len;
        read_op->callback(read_op);
        return 1;
    }
    read_op->retval = 0;
    read_op->pending_ops = read_op->read_vec.size();
    return 1;
}

void blockstore::handle_read_event(ring_data_t *data, blockstore_operation *op)
{
    op->pending_ops--;
    if (data->res < 0)
    {
        // read error
        op->retval = data->res;
    }
    if (op->pending_ops == 0)
    {
        if (op->retval == 0)
            op->retval = op->len;
        op->callback(op);
    }
}
