// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <cassert>

#include "cluster_client_impl.h"

writeback_cache_t::~writeback_cache_t()
{
    for (auto & bp: dirty_buffers)
    {
        if (!--(*bp.second.refcnt))
        {
            free(bp.second.refcnt); // refcnt is allocated with the buffer
        }
    }
    dirty_buffers.clear();
}

dirty_buf_it_t writeback_cache_t::find_dirty(uint64_t inode, uint64_t offset)
{
    auto dirty_it = dirty_buffers.lower_bound((object_id){
        .inode = inode,
        .stripe = offset,
    });
    while (dirty_it != dirty_buffers.begin())
    {
        dirty_it--;
        if (dirty_it->first.inode != inode ||
            (dirty_it->first.stripe + dirty_it->second.len) <= offset)
        {
            dirty_it++;
            break;
        }
    }
    return dirty_it;
}

bool writeback_cache_t::is_left_merged(dirty_buf_it_t dirty_it)
{
    if (dirty_it != dirty_buffers.begin())
    {
        auto prev_it = dirty_it;
        prev_it--;
        if (prev_it->first.inode == dirty_it->first.inode &&
            prev_it->first.stripe+prev_it->second.len == dirty_it->first.stripe &&
            prev_it->second.state == CACHE_DIRTY)
        {
            return true;
        }
    }
    return false;
}

bool writeback_cache_t::is_right_merged(dirty_buf_it_t dirty_it)
{
    auto next_it = dirty_it;
    next_it++;
    if (next_it != dirty_buffers.end() &&
        next_it->first.inode == dirty_it->first.inode &&
        next_it->first.stripe == dirty_it->first.stripe+dirty_it->second.len &&
        next_it->second.state == CACHE_DIRTY)
    {
        return true;
    }
    return false;
}

bool writeback_cache_t::is_merged(const dirty_buf_it_t & dirty_it)
{
    return is_left_merged(dirty_it) || is_right_merged(dirty_it);
}

void writeback_cache_t::copy_write(cluster_op_t *op, int state)
{
    // Save operation for replay when one of PGs goes out of sync
    // (primary OSD drops our connection in this case)
    // ...or just save it for writeback if write buffering is enabled
    if (op->len == 0)
    {
        return;
    }
    auto dirty_it = find_dirty(op->inode, op->offset);
    auto new_end = op->offset + op->len;
    while (dirty_it != dirty_buffers.end() &&
        dirty_it->first.inode == op->inode &&
        dirty_it->first.stripe < op->offset+op->len)
    {
        assert(dirty_it->first.stripe + dirty_it->second.len > op->offset);
        // Remove overlapping part(s) of buffers
        auto old_end = dirty_it->first.stripe + dirty_it->second.len;
        if (dirty_it->first.stripe < op->offset)
        {
            if (old_end > new_end)
            {
                // Split into end and start
                dirty_it->second.len = op->offset - dirty_it->first.stripe;
                dirty_it = dirty_buffers.emplace_hint(dirty_it, (object_id){
                    .inode = op->inode,
                    .stripe = new_end,
                }, (cluster_buffer_t){
                    .buf = dirty_it->second.buf + new_end - dirty_it->first.stripe,
                    .len = old_end - new_end,
                    .state = dirty_it->second.state,
                    .flush_id = dirty_it->second.flush_id,
                    .refcnt = dirty_it->second.refcnt,
                });
                (*dirty_it->second.refcnt)++;
                if (dirty_it->second.state == CACHE_DIRTY)
                {
                    writeback_bytes -= op->len;
                    writeback_queue_size++;
                }
                break;
            }
            else
            {
                // Only leave the beginning
                if (dirty_it->second.state == CACHE_DIRTY)
                {
                    writeback_bytes -= old_end - op->offset;
                    if (is_left_merged(dirty_it) && !is_right_merged(dirty_it))
                    {
                        writeback_queue_size++;
                    }
                }
                dirty_it->second.len = op->offset - dirty_it->first.stripe;
                dirty_it++;
            }
        }
        else if (old_end > new_end)
        {
            // Only leave the end
            if (dirty_it->second.state == CACHE_DIRTY)
            {
                writeback_bytes -= new_end - dirty_it->first.stripe;
                if (!is_left_merged(dirty_it) && is_right_merged(dirty_it))
                {
                    writeback_queue_size++;
                }
            }
            auto new_dirty_it = dirty_buffers.emplace_hint(dirty_it, (object_id){
                .inode = op->inode,
                .stripe = new_end,
            }, (cluster_buffer_t){
                .buf = dirty_it->second.buf + new_end - dirty_it->first.stripe,
                .len = old_end - new_end,
                .state = dirty_it->second.state,
                .flush_id = dirty_it->second.flush_id,
                .refcnt = dirty_it->second.refcnt,
            });
            dirty_buffers.erase(dirty_it);
            dirty_it = new_dirty_it;
            break;
        }
        else
        {
            // Remove the whole buffer
            if (dirty_it->second.state == CACHE_DIRTY && !is_merged(dirty_it))
            {
                writeback_bytes -= dirty_it->second.len;
                assert(writeback_queue_size > 0);
                writeback_queue_size--;
            }
            if (!--(*dirty_it->second.refcnt))
            {
                free(dirty_it->second.refcnt);
            }
            dirty_buffers.erase(dirty_it++);
        }
    }
    // Overlapping buffers are removed, just insert the new one
    uint64_t *refcnt = (uint64_t*)malloc_or_die(sizeof(uint64_t) + op->len);
    uint8_t *buf = (uint8_t*)refcnt + sizeof(uint64_t);
    *refcnt = 1;
    dirty_it = dirty_buffers.emplace_hint(dirty_it, (object_id){
        .inode = op->inode,
        .stripe = op->offset,
    }, (cluster_buffer_t){
        .buf = buf,
        .len = op->len,
        .state = state,
        .refcnt = refcnt,
    });
    if (state == CACHE_DIRTY)
    {
        writeback_bytes += op->len;
        // Track consecutive write-back operations
        if (!is_merged(dirty_it))
        {
            // <writeback_queue> is OK to contain more than actual number of consecutive
            // requests as long as it doesn't miss anything. But <writeback_queue_size>
            // is always calculated correctly.
            writeback_queue_size++;
            writeback_queue.push_back((object_id){
                .inode = op->inode,
                .stripe = op->offset,
            });
        }
    }
    uint64_t pos = 0, len = op->len, iov_idx = 0;
    while (len > 0 && iov_idx < op->iov.count)
    {
        auto & iov = op->iov.buf[iov_idx];
        memcpy(buf + pos, iov.iov_base, iov.iov_len);
        pos += iov.iov_len;
        iov_idx++;
    }
}

int writeback_cache_t::repeat_ops_for(cluster_client_t *cli, osd_num_t peer_osd)
{
    int repeated = 0;
    if (dirty_buffers.size())
    {
        // peer_osd just dropped connection
        // determine WHICH dirty_buffers are now obsolete and repeat them
        for (auto wr_it = dirty_buffers.begin(), flush_it = wr_it, last_it = wr_it; ; )
        {
            bool end = wr_it == dirty_buffers.end();
            bool flush_this = !end && wr_it->second.state != CACHE_REPEATING &&
                cli->affects_osd(wr_it->first.inode, wr_it->first.stripe, wr_it->second.len, peer_osd);
            if (flush_it != wr_it && (end || !flush_this ||
                wr_it->first.inode != flush_it->first.inode ||
                wr_it->first.stripe != last_it->first.stripe+last_it->second.len))
            {
                repeated++;
                flush_buffers(cli, flush_it, wr_it);
                flush_it = wr_it;
            }
            if (end)
                break;
            last_it = wr_it;
            wr_it++;
            if (!flush_this)
                flush_it = wr_it;
        }
    }
    return repeated;
}

void writeback_cache_t::flush_buffers(cluster_client_t *cli, dirty_buf_it_t from_it, dirty_buf_it_t to_it)
{
    auto prev_it = to_it;
    prev_it--;
    bool is_writeback = from_it->second.state == CACHE_DIRTY;
    cluster_op_t *op = new cluster_op_t;
    op->flags = OSD_OP_IGNORE_READONLY|OP_FLUSH_BUFFER;
    op->opcode = OSD_OP_WRITE;
    op->cur_inode = op->inode = from_it->first.inode;
    op->offset = from_it->first.stripe;
    op->len = prev_it->first.stripe + prev_it->second.len - from_it->first.stripe;
    uint32_t calc_len = 0;
    uint64_t flush_id = ++last_flush_id;
    for (auto it = from_it; it != to_it; it++)
    {
        it->second.state = CACHE_REPEATING;
        it->second.flush_id = flush_id;
        (*it->second.refcnt)++;
        flushed_buffers.emplace(flush_id, it->second.refcnt);
        op->iov.push_back(it->second.buf, it->second.len);
        calc_len += it->second.len;
    }
    assert(calc_len == op->len);
    writebacks_active++;
    op->callback = [this, flush_id](cluster_op_t* op)
    {
        // Buffer flushes should be always retried, regardless of the error,
        // so they should never result in an error here
        assert(op->retval == op->len);
        for (auto fl_it = flushed_buffers.find(flush_id);
            fl_it != flushed_buffers.end() && fl_it->first == flush_id; )
        {
            if (!--(*fl_it->second)) // refcnt
            {
                free(fl_it->second);
            }
            flushed_buffers.erase(fl_it++);
        }
        for (auto dirty_it = find_dirty(op->inode, op->offset);
            dirty_it != dirty_buffers.end() && dirty_it->first.inode == op->inode &&
            dirty_it->first.stripe < op->offset+op->len; dirty_it++)
        {
            if (dirty_it->second.flush_id == flush_id && dirty_it->second.state == CACHE_REPEATING)
            {
                dirty_it->second.flush_id = 0;
                dirty_it->second.state = CACHE_WRITTEN;
            }
        }
        delete op;
        writebacks_active--;
        // We can't call execute_internal because it affects an invalid copy of the list here
        // (erase_op remembers `next` after writeback callback)
    };
    if (is_writeback)
    {
        cli->execute_internal(op);
    }
    else
    {
        // Insert repeated flushes into the beginning
        cli->unshift_op(op);
        cli->continue_rw(op);
    }
}

void writeback_cache_t::start_writebacks(cluster_client_t *cli, int count)
{
    if (!writeback_queue.size())
    {
        return;
    }
    std::vector<object_id> queue_copy;
    queue_copy.swap(writeback_queue);
    int started = 0, i = 0;
    for (i = 0; i < queue_copy.size() && (!count || started < count); i++)
    {
        object_id & req = queue_copy[i];
        auto dirty_it = find_dirty(req.inode, req.stripe);
        if (dirty_it == dirty_buffers.end() ||
            dirty_it->first.inode != req.inode ||
            dirty_it->second.state != CACHE_DIRTY)
        {
            continue;
        }
        auto from_it = dirty_it;
        uint64_t off = dirty_it->first.stripe;
        while (from_it != dirty_buffers.begin())
        {
            from_it--;
            if (from_it->second.state != CACHE_DIRTY ||
                from_it->first.inode != req.inode ||
                from_it->first.stripe+from_it->second.len != off)
            {
                from_it++;
                break;
            }
            off = from_it->first.stripe;
        }
        off = dirty_it->first.stripe + dirty_it->second.len;
        auto to_it = dirty_it;
        to_it++;
        while (to_it != dirty_buffers.end())
        {
            if (to_it->second.state != CACHE_DIRTY ||
                to_it->first.inode != req.inode ||
                to_it->first.stripe != off)
            {
                break;
            }
            off = to_it->first.stripe + to_it->second.len;
            to_it++;
        }
        started++;
        assert(writeback_queue_size > 0);
        writeback_queue_size--;
        writeback_bytes -= off - from_it->first.stripe;
        flush_buffers(cli, from_it, to_it);
    }
    queue_copy.erase(queue_copy.begin(), queue_copy.begin()+i);
    if (writeback_queue.size())
    {
        queue_copy.insert(queue_copy.end(), writeback_queue.begin(), writeback_queue.end());
    }
    queue_copy.swap(writeback_queue);
}

static void copy_to_op(cluster_op_t *op, uint64_t offset, uint8_t *buf, uint64_t len, uint32_t bitmap_granularity)
{
    if (op->opcode == OSD_OP_READ)
    {
        // Not OSD_OP_READ_BITMAP or OSD_OP_READ_CHAIN_BITMAP
        int iov_idx = 0;
        uint64_t cur_offset = op->offset;
        while (iov_idx < op->iov.count && cur_offset+op->iov.buf[iov_idx].iov_len <= offset)
        {
            cur_offset += op->iov.buf[iov_idx].iov_len;
            iov_idx++;
        }
        while (iov_idx < op->iov.count && cur_offset < offset+len)
        {
            auto & v = op->iov.buf[iov_idx];
            auto begin = (cur_offset < offset ? offset : cur_offset);
            auto end = (cur_offset+v.iov_len > offset+len ? offset+len : cur_offset+v.iov_len);
            memcpy(
                (uint8_t*)v.iov_base + begin - cur_offset,
                buf + (cur_offset <= offset ? 0 : cur_offset-offset),
                end - begin
            );
            cur_offset += v.iov_len;
            iov_idx++;
        }
    }
    // Set bitmap bits
    int start_bit = (offset-op->offset)/bitmap_granularity;
    int end_bit = (offset-op->offset+len)/bitmap_granularity;
    for (int bit = start_bit; bit < end_bit;)
    {
        if (!(bit%8) && bit <= end_bit-8)
        {
            ((uint8_t*)op->bitmap_buf)[bit/8] = 0xFF;
            bit += 8;
        }
        else
        {
            ((uint8_t*)op->bitmap_buf)[bit/8] |= (1 << (bit%8));
            bit++;
        }
    }
}

bool writeback_cache_t::read_from_cache(cluster_op_t *op, uint32_t bitmap_granularity)
{
    bool dirty_copied = false;
    if (dirty_buffers.size() && (op->opcode == OSD_OP_READ ||
        op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP))
    {
        // We also have to return reads from CACHE_REPEATING buffers - they are not
        // guaranteed to be present on target OSDs at the moment of repeating
        // And we're also free to return data from other cached buffers just
        // because it's faster
        auto dirty_it = find_dirty(op->cur_inode, op->offset);
        while (dirty_it != dirty_buffers.end() && dirty_it->first.inode == op->cur_inode &&
            dirty_it->first.stripe < op->offset+op->len)
        {
            uint64_t begin = dirty_it->first.stripe, end = dirty_it->first.stripe + dirty_it->second.len;
            if (begin < op->offset)
                begin = op->offset;
            if (end > op->offset+op->len)
                end = op->offset+op->len;
            bool skip_prev = true;
            uint64_t cur = begin, prev = begin;
            while (cur < end)
            {
                unsigned bmp_loc = (cur - op->offset)/bitmap_granularity;
                bool skip = (((*((uint8_t*)op->bitmap_buf + bmp_loc/8)) >> (bmp_loc%8)) & 0x1);
                if (skip_prev != skip)
                {
                    if (cur > prev && !skip)
                    {
                        // Copy data
                        dirty_copied = true;
                        copy_to_op(op, prev, dirty_it->second.buf + prev - dirty_it->first.stripe, cur-prev, bitmap_granularity);
                    }
                    skip_prev = skip;
                    prev = cur;
                }
                cur += bitmap_granularity;
            }
            assert(cur > prev);
            if (!skip_prev)
            {
                // Copy data
                dirty_copied = true;
                copy_to_op(op, prev, dirty_it->second.buf + prev - dirty_it->first.stripe, cur-prev, bitmap_granularity);
            }
            dirty_it++;
        }
    }
    return dirty_copied;
}

void writeback_cache_t::fsync_start()
{
    for (auto & prev_op: dirty_buffers)
    {
        if (prev_op.second.state == CACHE_WRITTEN)
        {
            prev_op.second.state = CACHE_FLUSHING;
        }
    }
}

void writeback_cache_t::fsync_error()
{
    for (auto & prev_op: dirty_buffers)
    {
        if (prev_op.second.state == CACHE_FLUSHING)
        {
            prev_op.second.state = CACHE_WRITTEN;
        }
    }
}

void writeback_cache_t::fsync_ok()
{
    for (auto uw_it = dirty_buffers.begin(); uw_it != dirty_buffers.end(); )
    {
        if (uw_it->second.state == CACHE_FLUSHING)
        {
            if (!--(*uw_it->second.refcnt))
                free(uw_it->second.refcnt);
            dirty_buffers.erase(uw_it++);
        }
        else
            uw_it++;
    }
}
