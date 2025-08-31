// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <limits.h>
#include "blockstore_impl.h"
#include "blockstore_internal.h"

int blockstore_impl_t::dequeue_read(blockstore_op_t *op)
{
    heap_object_t *obj = heap->lock_and_read_entry(op->oid, PRIV(op)->lsn);
    if (!obj)
    {
        op->version = 0;
        op->retval = -ENOENT;
        FINISH_OP(op);
        return 2;
    }
    uint32_t fulfilled = 0;
    PRIV(op)->pending_ops = 0;
    auto & rv = PRIV(op)->read_vec;
    uint64_t result_version = 0;
    bool found = false;
    for (auto wr = obj->get_writes(); wr; wr = wr->next())
    {
        if (op->version < wr->version)
        {
            continue;
        }
        if (!found)
        {
            found = true;
            result_version = wr->version;
            if (op->bitmap)
            {
                memcpy(op->bitmap, wr->get_ext_bitmap(heap), dsk.clean_entry_bitmap_size);
            }
        }
        fulfilled += prepare_read(PRIV(op)->read_vec, obj, wr, op->offset, op->offset+op->len);
        if (fulfilled == op->len ||
            wr->type() == BS_HEAP_BIG_WRITE ||
            wr->type() == BS_HEAP_TOMBSTONE)
        {
            break;
        }
    }
    if (!found)
    {
        // May happen if there are entries but all of them are > requested version
        heap->unlock_entry(op->oid, PRIV(op)->lsn);
        op->version = 0;
        op->retval = -ENOENT;
        FINISH_OP(op);
        return 2;
    }
    assert(fulfilled == op->len);
    if (!fulfill_read(op))
    {
        // Need to wait. undo added requests, unlock lsn
        heap->unlock_entry(op->oid, PRIV(op)->lsn);
        free_read_buffers(rv);
        rv.clear();
        return 0;
    }
    op->version = result_version;
    if (!PRIV(op)->pending_ops)
    {
        // everything is fulfilled from memory
        heap->unlock_entry(op->oid, PRIV(op)->lsn);
        op->retval = op->len;
        free_read_buffers(rv);
        FINISH_OP(op);
        return 2;
    }
    op->retval = 0;
    return 2;
}

int blockstore_impl_t::fulfill_read(blockstore_op_t *op)
{
    for (auto & vec: PRIV(op)->read_vec)
    {
        if (vec.copy_flags & (COPY_BUF_COALESCED|COPY_BUF_CSUM_FILL))
        {
            // This buffer references another one
        }
        else if (vec.copy_flags == COPY_BUF_ZERO)
        {
            memset(op->buf + vec.offset - op->offset, 0, vec.len);
        }
        else if ((vec.copy_flags & COPY_BUF_JOURNAL) && dsk.inmemory_journal)
        {
            memcpy(op->buf + vec.offset - op->offset, buffer_area + vec.disk_loc + vec.disk_offset, vec.len);
        }
        else
        {
            BS_SUBMIT_GET_SQE(sqe, data);
            data->iov = (struct iovec){ vec.buf ? vec.buf : (op->buf + vec.offset - op->offset), (size_t)vec.disk_len };
            PRIV(op)->pending_ops++;
            io_uring_prep_readv(
                sqe,
                (vec.copy_flags & COPY_BUF_JOURNAL) ? dsk.journal_fd : dsk.data_fd,
                &data->iov, 1,
                ((vec.copy_flags & COPY_BUF_JOURNAL) ? dsk.journal_offset : dsk.data_offset) + vec.disk_loc + vec.disk_offset
            );
            data->callback = [this, op](ring_data_t *data) { handle_read_event(data, op); };
        }
    }
    return 1;
}

uint32_t blockstore_impl_t::prepare_read(std::vector<copy_buffer_t> & read_vec, heap_object_t *obj, heap_write_t *wr, uint32_t start, uint32_t end)
{
    if (wr->type() == BS_HEAP_BIG_WRITE)
    {
        return prepare_read_with_bitmaps(read_vec, obj, wr, start, end);
    }
    if (wr->type() == BS_HEAP_TOMBSTONE)
    {
        return prepare_read_zero(read_vec, start, end);
    }
    return prepare_read_simple(read_vec, obj, wr, start, end);
}

uint32_t blockstore_impl_t::prepare_read_with_bitmaps(std::vector<copy_buffer_t> & read_vec, heap_object_t *obj, heap_write_t *wr, uint32_t start, uint32_t end)
{
    // BIG_WRITEs contain a bitmap and we have to handle its holes
    uint32_t res = 0;
    uint8_t *bmp = wr->get_int_bitmap(heap);
    uint32_t bmp_start = start/dsk.bitmap_granularity, bmp_end = bmp_start, bmp_size = end/dsk.bitmap_granularity;
    while (bmp_start < bmp_size)
    {
        while (bmp_end < bmp_size && !(bmp[bmp_end >> 3] & (1 << (bmp_end & 0x7))))
        {
            bmp_end++;
        }
        if (bmp_end > bmp_start)
        {
            res += prepare_read_zero(read_vec, bmp_start * dsk.bitmap_granularity, bmp_end * dsk.bitmap_granularity);
        }
        bmp_start = bmp_end;
        while (bmp_end < bmp_size && (bmp[bmp_end >> 3] & (1 << (bmp_end & 0x7))))
        {
            bmp_end++;
        }
        if (bmp_end > bmp_start)
        {
            res += prepare_read_simple(read_vec, obj, wr, bmp_start * dsk.bitmap_granularity, bmp_end * dsk.bitmap_granularity);
            bmp_start = bmp_end;
        }
    }
    return res;
}

uint32_t blockstore_impl_t::prepare_read_zero(std::vector<copy_buffer_t> & read_vec, uint32_t start, uint32_t end)
{
    uint32_t res = 0;
    find_holes(read_vec, start, end, [&](int & pos, uint32_t start, uint32_t end)
    {
        res += end-start;
        read_vec.insert(read_vec.begin() + (pos++), (copy_buffer_t){
            .copy_flags = COPY_BUF_ZERO,
            .offset = start,
            .len = end-start,
        });
    });
    return res;
}

uint32_t blockstore_impl_t::prepare_read_simple(std::vector<copy_buffer_t> & read_vec, heap_object_t *obj, heap_write_t *wr, uint32_t start, uint32_t end)
{
    uint32_t res = 0;
    if (wr->offset >= end || wr->offset+wr->len <= start)
    {
        return 0;
    }
    start = start < wr->offset ? wr->offset : start;
    end = end > wr->offset+wr->len ? wr->offset+wr->len : end;
    find_holes(read_vec, start, end, [&](int & pos, uint32_t start, uint32_t end)
    {
        res += end-start;
        if (wr->type() == BS_HEAP_SMALL_WRITE && dsk.inmemory_journal)
        {
            // read buffered data from memory
            read_vec.insert(read_vec.begin() + (pos++), (copy_buffer_t){
                .copy_flags = COPY_BUF_JOURNAL | COPY_BUF_SKIP_CSUM,
                .offset = start,
                .len = end-start,
                .disk_loc = wr->location - wr->offset,
                .disk_offset = start,
                .disk_len = end-start,
                .buf = buffer_area + wr->location + start - wr->offset,
                .wr_lsn = wr->lsn,
            });
        }
        else if (dsk.csum_block_size <= dsk.bitmap_granularity)
        {
            // simple disk read
            prepare_disk_read(read_vec, pos++, obj, wr, start, end, start, end, 0);
        }
        else
        {
            // the most complex case: read data from disk with padding
            uint32_t blk_start = start, blk_end = end;
            blk_start = (start/dsk.csum_block_size) * dsk.csum_block_size;
            blk_start = blk_start < wr->offset ? wr->offset : blk_start;
            blk_end = ((end-1) / dsk.csum_block_size + 1) * dsk.csum_block_size;
            blk_end = blk_end > wr->offset+wr->len ? wr->offset+wr->len : blk_end;
            uint32_t skip_csum = 0;
            if (!perfect_csum_update && wr->type() == BS_HEAP_BIG_WRITE)
            {
                for (auto owr = obj->get_writes(); owr && owr != wr; owr = owr->next())
                    if (owr->offset < blk_end && owr->offset+owr->len > blk_start)
                        skip_csum = COPY_BUF_SKIP_CSUM;
            }
            if ((blk_end-1)/dsk.csum_block_size == blk_start/dsk.csum_block_size ||
                blk_end/dsk.csum_block_size == blk_start/dsk.csum_block_size+1 && blk_end != end && blk_start != start ||
                blk_end == end && blk_start == start)
            {
                // single block, exactly two partial blocks, or any number of full blocks
                // i.e. [..X.] or [..XX][XX..] or [XXXX]..[XXXX]
                prepare_disk_read(read_vec, pos++, obj, wr, blk_start, blk_end, start, end, skip_csum);
            }
            else
            {
                // one or two partial blocks plus any number of full blocks
                // i.e. [..XX][XXXX][X...]
                uint32_t full_start = (blk_start != start ? (blk_start/dsk.csum_block_size+1)*dsk.csum_block_size : blk_start);
                uint32_t full_end = (blk_end != end ? (blk_end % dsk.csum_block_size ? blk_end-blk_end%dsk.csum_block_size : blk_end - dsk.csum_block_size) : blk_end);
                if (blk_start != start) // starting padded block
                    prepare_disk_read(read_vec, pos++, obj, wr, blk_start, full_start, start, full_start, skip_csum);
                if (full_end > full_start) // full non-padded blocks
                    prepare_disk_read(read_vec, pos++, obj, wr, full_start, full_end, full_start, full_end, skip_csum);
                if (blk_end != end) // ending padded block
                    prepare_disk_read(read_vec, pos++, obj, wr, full_end, blk_end, full_end, end, skip_csum);
            }
        }
    });
    return res;
}

void blockstore_impl_t::prepare_disk_read(std::vector<copy_buffer_t> & read_vec, int pos, heap_object_t *obj, heap_write_t *wr,
    uint32_t blk_start, uint32_t blk_end, uint32_t start, uint32_t end, uint32_t copy_flags)
{
    // Only one INTENT_WRITE is allowed at a time
    assert(wr->type() != BS_HEAP_INTENT_WRITE || wr->next()->type() == BS_HEAP_BIG_WRITE);
    copy_buffer_t vec = {
        .copy_flags = (wr->type() == BS_HEAP_SMALL_WRITE ? COPY_BUF_JOURNAL : COPY_BUF_DATA) | copy_flags,
        .offset = start,
        .len = end-start,
        .disk_loc = (wr->type() == BS_HEAP_INTENT_WRITE ? wr->next()->location : wr->location - (wr->type() == BS_HEAP_SMALL_WRITE ? wr->offset : 0)),
        .disk_offset = blk_start,
        .disk_len = blk_end - blk_start,
        .wr_lsn = wr->lsn,
    };
    if (blk_start != start || blk_end != end)
    {
        assert(!(copy_flags & COPY_BUF_CSUM_FILL));
        vec.copy_flags |= COPY_BUF_PADDED;
        if (pos > 0 && read_vec.size() >= pos &&
            read_vec[pos-1].copy_flags == vec.copy_flags &&
            read_vec[pos-1].wr_lsn == vec.wr_lsn &&
            read_vec[pos-1].disk_offset <= vec.disk_offset &&
            read_vec[pos-1].disk_offset+read_vec[pos-1].disk_len >= blk_end)
        {
            // This is the same block as the previous one, we can read it only once
            vec.copy_flags |= COPY_BUF_COALESCED;
            vec.buf = read_vec[pos-1].buf + vec.disk_offset - read_vec[pos-1].disk_offset;
        }
        else
        {
            vec.buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, vec.disk_len);
        }
    }
    read_vec.insert(read_vec.begin() + pos, vec);
}

void blockstore_impl_t::find_holes(std::vector<copy_buffer_t> & read_vec,
    uint32_t item_start, uint32_t item_end,
    std::function<void(int&, uint32_t, uint32_t)> callback)
{
    auto cur_start = item_start;
    int i = 0;
    while (cur_start < item_end)
    {
        // COPY_BUF_CSUM_FILL items are fake items inserted in the end, their offsets aren't in order
        if (i >= read_vec.size() || (read_vec[i].copy_flags & COPY_BUF_CSUM_FILL) || read_vec[i].offset >= item_end)
        {
            // Hole (at end): cur_start .. item_end
            callback(i, cur_start, item_end);
            break;
        }
        else if (read_vec[i].offset > cur_start)
        {
            // Hole: cur_start .. min(read_vec[i].offset, item_end)
            auto cur_end = read_vec[i].offset > item_end ? item_end : read_vec[i].offset;
            callback(i, cur_start, cur_end);
            cur_start = cur_end;
        }
        else if (read_vec[i].offset + read_vec[i].len > cur_start)
        {
            // Allocated: cur_start .. min(read_vec[i].offset + read_vec[i].len, item_end)
            auto cur_end = read_vec[i].offset + read_vec[i].len;
            cur_end = cur_end > item_end ? item_end : cur_end;
            //callback(i, true, cur_start, cur_end);
            cur_start = cur_end;
            i++;
        }
        else
            i++;
    }
}

void blockstore_impl_t::free_read_buffers(std::vector<copy_buffer_t> & rv)
{
    if (dsk.csum_block_size > dsk.bitmap_granularity)
    {
        for (auto & vec: rv)
        {
            if (!(vec.copy_flags & COPY_BUF_COALESCED) && vec.buf &&
                (!buffer_area || vec.buf < buffer_area || vec.buf >= (uint8_t*)buffer_area + dsk.journal_len))
            {
                free(vec.buf);
                vec.buf = NULL;
            }
        }
    }
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
        // verify checksums if required
        if (dsk.csum_block_size && !verify_read_checksums(op))
            op->retval = -EDOM;
        else if (op->retval == 0)
            op->retval = op->len;
        heap->unlock_entry(op->oid, PRIV(op)->lsn);
        free_read_buffers(PRIV(op)->read_vec);
        FINISH_OP(op);
    }
}

bool blockstore_impl_t::verify_read_checksums(blockstore_op_t *op)
{
    heap_object_t *obj = heap->read_locked_entry(op->oid, PRIV(op)->lsn);
    auto & rv = PRIV(op)->read_vec;
    for (auto & vec: rv)
    {
        if (vec.copy_flags & COPY_BUF_ZERO)
            continue;
        if (vec.copy_flags & COPY_BUF_PADDED)
            memcpy(op->buf + vec.offset - op->offset, vec.buf + vec.offset - vec.disk_offset, vec.len);
        if (vec.copy_flags & (COPY_BUF_COALESCED|COPY_BUF_SKIP_CSUM))
            continue;
        heap_write_t *wr = obj->get_writes();
        while (wr && wr->lsn != vec.wr_lsn)
            wr = wr->next();
        assert(wr);
        uint8_t *buf = vec.buf ? vec.buf : (op->buf + vec.offset - op->offset);
        uint32_t *csums = (uint32_t*)(wr->get_checksums(heap)
            + (vec.disk_offset/dsk.csum_block_size)*(dsk.data_csum_type & 0xFF)
            - ((wr->type() == BS_HEAP_BIG_WRITE) ? 0 : (wr->offset/dsk.csum_block_size)*(dsk.data_csum_type & 0xFF)));
        if (!heap->calc_block_checksums(csums, buf, wr->get_int_bitmap(heap),
            vec.disk_offset, vec.disk_offset+vec.disk_len, false, [&](uint32_t mismatch_pos, uint32_t expected_csum, uint32_t real_csum)
            {
                printf(
                    "Checksum mismatch in object %jx:%jx v%ju, offset 0x%x in %s area at offset 0x%jx: %08x expected vs %08x actual\n",
                    op->oid.inode, op->oid.stripe, op->version, mismatch_pos,
                    (vec.copy_flags & COPY_BUF_JOURNAL) ? "buffer" : "data", vec.disk_loc + vec.disk_offset,
                    expected_csum, real_csum
                );
            }))
        {
            return false;
        }
    }
    return true;
}

int blockstore_impl_t::read_bitmap(object_id oid, uint64_t target_version, void *bitmap, uint64_t *result_version)
{
    heap_object_t *obj = heap->read_entry(oid, NULL);
    if (obj)
    {
        for (auto wr = obj->get_writes(); wr; wr = wr->next())
        {
            if (target_version < wr->version)
            {
                continue;
            }
            if (result_version)
            {
                *result_version = wr->version;
            }
            if (bitmap)
            {
                memcpy(bitmap, wr->get_ext_bitmap(heap), dsk.clean_entry_bitmap_size);
            }
            return 0;
        }
    }
    if (result_version)
    {
        *result_version = 0;
    }
    if (bitmap)
    {
        memset(bitmap, 0, dsk.clean_entry_bitmap_size);
    }
    return -ENOENT;
}
