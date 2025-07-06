// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_impl.h"
#include "blockstore_internal.h"

bool blockstore_impl_t::enqueue_write(blockstore_op_t *op)
{
    clock_gettime(CLOCK_REALTIME, &PRIV(op)->tv_begin);
    return true;
}

void blockstore_impl_t::cancel_all_writes(blockstore_op_t *op, int retval)
{
    bool found = false;
    for (auto other_op: submit_queue)
    {
        if (!other_op)
        {
            // freed operations during submitting are zeroed
        }
        else if (other_op == op)
        {
            // <op> may be present in queue multiple times due to moving operations in submit_queue
            found = true;
        }
        else if (found && other_op->oid == op->oid &&
            (other_op->opcode == BS_OP_WRITE || other_op->opcode == BS_OP_WRITE_STABLE) &&
            !PRIV(other_op)->op_state)
        {
            // Mark operations to cancel them
            PRIV(other_op)->op_state = 100;
            other_op->retval = retval;
        }
    }
    op->retval = retval;
    FINISH_OP(op);
}

void blockstore_impl_t::prepare_meta_block_write(blockstore_op_t *op, uint64_t modified_block, io_uring_sqe *sqe)
{
    if (!sqe)
    {
        sqe = get_sqe();
        assert(sqe != NULL);
    }
    ring_data_t *data = ((ring_data_t*)sqe->user_data);
    data->iov = (struct iovec){ heap->get_meta_block(modified_block), (size_t)dsk.meta_block_size };
    data->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
    PRIV(op)->pending_ops++;
    io_uring_prep_writev(
        sqe, dsk.meta_fd, &data->iov, 1, dsk.meta_offset + (modified_block+1)*dsk.meta_block_size
    );
}

// First step of the write algorithm: dequeue operation and submit initial write(s)
int blockstore_impl_t::dequeue_write(blockstore_op_t *op)
{
    if (PRIV(op)->op_state == 100)
    {
        // This is the flag used to cancel ops
        FINISH_OP(op);
        return 2;
    }
    if (PRIV(op)->op_state)
    {
        return continue_write(op);
    }
    if (write_iodepth >= max_write_iodepth)
    {
        return 0;
    }
    PRIV(op)->is_big = false;
    heap_object_t *obj = heap->read_entry(op->oid, NULL);
    if (op->opcode == BS_OP_DELETE)
    {
        // Delete
        if (!obj)
        {
            // Already deleted
            op->retval = 0;
            FINISH_OP(op);
            return 2;
        }
        BS_SUBMIT_CHECK_SQES(1);
        uint32_t modified_block;
        int res = heap->post_delete(op->oid, &modified_block);
        assert(res == 0);
        prepare_meta_block_write(op, modified_block);
        PRIV(op)->pending_ops++;
        PRIV(op)->op_state = 5;
        write_iodepth++;
    }
    // FIXME: Allow to do initial writes as buffered, not redirected
    // FIXME: Allow to do direct writes over holes
    else if (!obj || (obj->get_writes()->flags & BS_HEAP_TYPE) == BS_HEAP_TOMBSTONE ||
        op->offset == 0 && op->len == dsk.data_block_size)
    {
        // Big (redirect) write
        PRIV(op)->is_big = true;
        uint32_t tmp_block;
        uint64_t loc = heap->find_free_data();
        if (loc == UINT64_MAX ||
            !obj && heap->get_block_for_new_object(tmp_block) != 0)
        {
            if (!heap->get_inflight_queue_size())
            {
                // no space
                cancel_all_writes(op, -ENOSPC);
                return 2;
            }
            PRIV(op)->wait_for = WAIT_COMPACTION;
            PRIV(op)->wait_detail = flusher->get_compact_counter();
            flusher->request_trim();
            return 0;
        }
        BS_SUBMIT_GET_SQE(sqe, data);
        PRIV(op)->location = loc;
#ifdef BLOCKSTORE_DEBUG
        printf(
            "Allocate offset %ju for %jx:%jx v%ju\n",
            loc, op->oid.inode, op->oid.stripe, op->version
        );
#endif
        heap->use_data(op->oid.inode, PRIV(op)->location);
        uint64_t stripe_offset = (op->offset % dsk.bitmap_granularity);
        uint64_t stripe_end = (op->offset + op->len) % dsk.bitmap_granularity;
        // Zero fill up to dsk.bitmap_granularity
        int vcnt = 0;
        if (stripe_offset)
        {
            PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ zero_object, (size_t)stripe_offset };
        }
        PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ op->buf, op->len };
        if (stripe_end)
        {
            stripe_end = dsk.bitmap_granularity - stripe_end;
            PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ zero_object, (size_t)stripe_end };
        }
        data->iov.iov_len = op->len + stripe_offset + stripe_end; // to check it in the callback
        data->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
        io_uring_prep_writev(
            sqe, dsk.data_fd, PRIV(op)->iov_zerofill, vcnt, dsk.data_offset + loc + op->offset - stripe_offset
        );
        PRIV(op)->pending_ops = 1;
        unsynced_big_write_count++;
        PRIV(op)->op_state = 1;
        write_iodepth++;
        inflight_big++;
    }
    // Only one INTENT_WRITE is allowed at a time, but in fact,
    // parallel writes to the same object are forbidden anyway
    else if (disable_data_fsync &&
        op->opcode == BS_OP_WRITE_STABLE &&
        op->len > 0 && op->len <= dsk.bitmap_granularity /* FIXME atomic_write_size */ &&
        (obj->get_writes()->flags == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE) ||
        obj->get_writes()->flags == (BS_HEAP_INTENT_WRITE|BS_HEAP_STABLE)))
    {
        // Direct intent-write
        BS_SUBMIT_CHECK_SQES(1);
        for (auto wr = obj->get_writes(); wr; wr = wr->next())
        {
            assert(wr->flags != BS_HEAP_BIG_WRITE);
            if (wr->flags == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE))
            {
                PRIV(op)->location = wr->location;
            }
        }
        uint8_t wr_buf[heap->get_max_write_entry_size()];
        heap_write_t *wr = (heap_write_t*)wr_buf;
        wr->version = op->version;
        wr->offset = op->offset;
        wr->len = op->len;
        wr->location = 0;
        wr->flags = BS_HEAP_INTENT_WRITE | BS_HEAP_STABLE;
        if (op->bitmap)
            memcpy(wr->get_ext_bitmap(heap), op->bitmap, dsk.clean_entry_bitmap_size);
        heap->calc_checksums(wr, (uint8_t*)op->buf, true);
        uint32_t modified_block;
        int res = heap->post_write(op->oid, wr, &modified_block);
        if (res == ENOSPC)
        {
            if (!heap->get_inflight_queue_size())
            {
                // no space
                cancel_all_writes(op, -ENOSPC);
                return 2;
            }
            PRIV(op)->wait_for = WAIT_COMPACTION;
            PRIV(op)->wait_detail = flusher->get_compact_counter();
            flusher->request_trim();
            return 0;
        }
        assert(res == 0);
        PRIV(op)->lsn = wr->lsn;
        prepare_meta_block_write(op, modified_block);
        unsynced_small_write_count++;
        PRIV(op)->op_state = 9;
        write_iodepth++;
    }
    else
    {
        // Small (buffered) overwrite
        // First check if there is free buffer space
        uint64_t loc = !op->len ? 0 : heap->find_free_buffer_area(op->len);
        if (loc == UINT64_MAX)
        {
            PRIV(op)->wait_for = WAIT_COMPACTION;
            PRIV(op)->wait_detail = flusher->get_compact_counter();
            flusher->request_trim();
            return 0;
        }
        // There is sufficient space. Check SQE(s)
        BS_SUBMIT_CHECK_SQES(1 + (op->len > 0 ? 1 : 0));
        uint8_t wr_buf[heap->get_max_write_entry_size()];
        heap_write_t *wr = (heap_write_t*)wr_buf;
        wr->version = op->version;
        wr->offset = op->offset;
        wr->len = op->len;
        wr->location = loc;
        PRIV(op)->location = loc;
        wr->flags = BS_HEAP_SMALL_WRITE | (op->opcode == BS_OP_WRITE_STABLE ? BS_HEAP_STABLE : 0);
        if (op->bitmap)
            memcpy(wr->get_ext_bitmap(heap), op->bitmap, dsk.clean_entry_bitmap_size);
        heap->calc_checksums(wr, (uint8_t*)op->buf, true);
        uint32_t modified_block;
        int res = heap->post_write(op->oid, wr, &modified_block);
        if (res == ENOSPC)
        {
            if (!heap->get_inflight_queue_size())
            {
                // no space
                cancel_all_writes(op, -ENOSPC);
                return 2;
            }
            PRIV(op)->wait_for = WAIT_COMPACTION;
            PRIV(op)->wait_detail = flusher->get_compact_counter();
            flusher->request_trim();
            return 0;
        }
        assert(res == 0);
        PRIV(op)->lsn = wr->lsn;
        heap->use_buffer_area(op->oid.inode, loc, op->len);
        prepare_meta_block_write(op, modified_block);
        if (op->len > 0)
        {
            // Prepare buffered data write
            assert(dsk.inmemory_journal);
            memcpy((uint8_t*)buffer_area + loc, op->buf, op->len);
            BS_SUBMIT_GET_SQE(sqe2, data2);
            data2->iov = (struct iovec){ op->buf, op->len };
            data2->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
            io_uring_prep_writev(sqe2, dsk.journal_fd, &data2->iov, 1, dsk.journal_offset + loc);
            PRIV(op)->pending_ops++;
        }
        else
        {
            // Zero-length overwrite. Allowed to bump object version in EC placement groups without actually writing data
        }
        unsynced_small_write_count++;
        assert(PRIV(op)->pending_ops);
        PRIV(op)->op_state = 5;
        write_iodepth++;
    }
    return 1;
}

int blockstore_impl_t::continue_write(blockstore_op_t *op)
{
    int op_state = PRIV(op)->op_state;
    if (op_state == 2)
        goto resume_2;
    else if (op_state == 4)
        goto resume_4;
    else if (op_state == 6)
        goto resume_6;
    else if (op_state == 8)
        goto resume_8;
    else if (op_state == 10)
        goto resume_10;
    else if (op_state == 11)
        goto resume_11;
    else if (op_state == 12)
        goto resume_12;
    else
    {
        // In progress
        return 1;
    }
resume_2:
    // We must fsync all big writes to avoid complex write workflows
    // It's OK for all HDDs and for server SSDs, but slightly worse for desktop SSDs
    // The other way is to add another type of MVCC to blockstore_heap: "forward" MVCC :)
    inflight_big--;
    if (!disable_data_fsync)
    {
        // fsync data in a batch
resume_11:
        if (inflight_big > 0)
        {
            PRIV(op)->op_state = 11;
            return 1;
        }
        if (fsyncing_data)
        {
resume_12:
            if (fsyncing_data)
            {
                PRIV(op)->op_state = 12;
                return 1;
            }
            goto resume_4;
        }
        fsyncing_data = true;
        BS_SUBMIT_GET_SQE(sqe, data);
        io_uring_prep_fsync(sqe, dsk.data_fd, IORING_FSYNC_DATASYNC);
        data->iov = { 0 };
        data->callback = [this, op](ring_data_t *data)
        {
            fsyncing_data = false;
            handle_write_event(data, op);
        };
        PRIV(op)->pending_ops++;
        PRIV(op)->op_state = 3;
        return 1;
    }
resume_4:
    {
        uint8_t wr_buf[heap->get_max_write_entry_size()];
        heap_write_t *wr = (heap_write_t*)wr_buf;
        wr->version = op->version;
        wr->offset = op->offset;
        wr->len = op->len;
        wr->location = PRIV(op)->location;
        wr->flags = BS_HEAP_BIG_WRITE | (op->opcode == BS_OP_WRITE_STABLE ? BS_HEAP_STABLE : 0);
        if (op->bitmap)
            memcpy(wr->get_ext_bitmap(heap), op->bitmap, dsk.clean_entry_bitmap_size);
        heap->calc_checksums(wr, (uint8_t*)op->buf, true);
        uint32_t modified_block;
        int res = heap->post_write(op->oid, wr, &modified_block);
        if (res == ENOSPC)
        {
            PRIV(op)->wait_for = WAIT_COMPACTION;
            PRIV(op)->wait_detail = flusher->get_compact_counter();
            return 1;
        }
        assert(res == 0);
        PRIV(op)->lsn = wr->lsn;
        prepare_meta_block_write(op, modified_block);
        PRIV(op)->op_state = 5;
        return 1;
    }
resume_6:
    {
#ifdef BLOCKSTORE_DEBUG
        printf("Ack write %jx:%jx v%ju\n", op->oid.inode, op->oid.stripe, op->version);
#endif
        // Apply throttling to not fill the journal too fast for the SSD+HDD case
        if (!PRIV(op)->is_big && throttle_small_writes)
        {
            // Apply throttling
            timespec tv_end;
            clock_gettime(CLOCK_REALTIME, &tv_end);
            uint64_t exec_us =
                (tv_end.tv_sec - PRIV(op)->tv_begin.tv_sec)*1000000 +
                (tv_end.tv_nsec - PRIV(op)->tv_begin.tv_nsec)/1000;
            // Compare with target execution time
            // 100% free -> target time = 0
            // 0% free -> target time = iodepth/parallelism * (iops + size/bw) / write per second
            uint64_t buffer_free_space = dsk.journal_len - heap->get_buffer_area_used_space();
            uint64_t ref_us =
                (write_iodepth <= throttle_target_parallelism ? 100 : 100*write_iodepth/throttle_target_parallelism)
                * (1000000/throttle_target_iops + op->len*1000000/throttle_target_mbs/1024/1024)
                / 100;
            ref_us -= ref_us * buffer_free_space / dsk.journal_len;
            if (ref_us > exec_us + throttle_threshold_us)
            {
                // Pause reply
                PRIV(op)->op_state = 7;
                // Remember that the timer can in theory be called right here
                tfd->set_timer_us(ref_us-exec_us, false, [this, op](int timer_id)
                {
                    PRIV(op)->op_state++;
                    ringloop->wakeup();
                });
                return 1;
            }
        }
    }
resume_8:
    // Acknowledge write
    op->retval = op->len;
    heap->mark_lsn_completed(PRIV(op)->lsn);
    write_iodepth--;
    FINISH_OP(op);
    return 2;
resume_10:
    // Direct intent-write
    heap_object_t *obj = heap->read_entry(op->oid, NULL);
    uint64_t loc = UINT64_MAX;
    for (auto wr = obj->get_writes(); wr; wr = wr->next())
    {
        if (wr->flags == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE))
            loc = wr->location;
    }
    if (loc != PRIV(op)->location)
    {
        goto resume_8;
    }
    BS_SUBMIT_GET_SQE(sqe, data);
    data->iov = (struct iovec){ op->buf, op->len };
    data->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
    io_uring_prep_writev(sqe, dsk.data_fd, &data->iov, 1, dsk.data_offset + loc + op->offset);
    PRIV(op)->pending_ops++;
    PRIV(op)->op_state = 7;
    return 1;
}

void blockstore_impl_t::handle_write_event(ring_data_t *data, blockstore_op_t *op)
{
    live = true;
    if (data->res != data->iov.iov_len)
    {
        // FIXME: our state becomes corrupted after a write error. maybe do something better than just die
        disk_error_abort("data write", data->res, data->iov.iov_len);
    }
    PRIV(op)->pending_ops--;
    assert(PRIV(op)->pending_ops >= 0);
    if (PRIV(op)->pending_ops == 0)
    {
        PRIV(op)->op_state++;
        ringloop->wakeup();
    }
}
