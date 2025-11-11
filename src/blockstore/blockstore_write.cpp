// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_impl.h"
#include "blockstore_internal.h"
#include "allocator.h"

#define _REDIRECT_INTENT 0x101

bool blockstore_impl_t::enqueue_write(blockstore_op_t *op)
{
    clock_gettime(CLOCK_REALTIME, &PRIV(op)->tv_begin);
    return true;
}

void blockstore_impl_t::prepare_meta_block_write(uint32_t modified_block)
{
    if (modified_blocks.find(modified_block) != modified_blocks.end())
        return;
    io_uring_sqe *sqe = get_sqe();
    assert(sqe != NULL);
    ring_data_t *data = ((ring_data_t*)sqe->user_data);
    uint8_t *buf = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, dsk.meta_block_size);
    data->iov = (struct iovec){ buf, (size_t)dsk.meta_block_size };
    data->callback = [this, modified_block, buf](ring_data_t *data)
    {
        free(buf);
        live = true;
        if (data->res != data->iov.iov_len)
        {
            // FIXME: our state becomes corrupted after a write error. maybe do something better than just die
            disk_error_abort("data write", data->res, data->iov.iov_len);
        }
        modified_blocks.erase(modified_block);
        heap->complete_block_write(modified_block);
        ringloop->wakeup();
    };
    io_uring_prep_writev(
        sqe, dsk.meta_fd, &data->iov, 1, dsk.meta_offset + (modified_block+1)*dsk.meta_block_size
    );
    unsynced_meta_write_count++;
    pending_modified_blocks.push_back(modified_block);
    modified_blocks[modified_block] = { .sent = false, .buf = buf };
}

bool blockstore_impl_t::meta_block_is_pending(uint32_t modified_block)
{
    auto mb_it = modified_blocks.find(modified_block);
    return mb_it != modified_blocks.end();
}

bool blockstore_impl_t::intent_write_allowed(blockstore_op_t *op, heap_entry_t *obj)
{
    // Parallel writes to the same object are forbidden so "one intent at a time" is fulfilled automatically
    // Intent writes are disabled when metadata fsync is enabled
    if (!dsk.disable_meta_fsync)
    {
        return false;
    }
    // Intent writes are only for replication
    if (op->opcode != BS_OP_WRITE_STABLE)
    {
        return false;
    }
    // Operation size should be less than or equal to atomic write size
    if (!op->len || op->len > dsk.atomic_write_size)
    {
        return false;
    }
    // Intent-writes are disabled if "absolutely correct during compaction" checksum validation algorithm is enabled
    // We could also do RMW here when perfect_csum_update is enabled, but it's unclear if we need it
    if (perfect_csum_update && dsk.csum_block_size > dsk.bitmap_granularity &&
        ((op->offset % dsk.csum_block_size) || (op->len % dsk.csum_block_size)))
    {
        return false;
    }
    bool ok = true;
    heap->iterate_with_stable(obj, obj->lsn, [&](heap_entry_t *wr, bool stable)
    {
        // Intent writes are not allowed over buffered writes
        auto t = wr->type();
        if (t == BS_HEAP_SMALL_WRITE)
        {
            ok = false;
            return false;
        }
        // Intent writes are not allowed over unstable writes
        if (!stable)
        {
            ok = false;
            return false;
        }
        // Intent writes are not allowed over unfinished intent writes
        if ((t == BS_HEAP_INTENT_WRITE || t == BS_HEAP_BIG_INTENT) && wr->lsn > heap->get_fsynced_lsn())
        {
            ok = false;
            return false;
        }
        // Intent writes are allowed over BIG_WRITEs even with fsyncs because BIG_WRITE is always counted as fsynced
        if (t == BS_HEAP_BIG_WRITE || t == BS_HEAP_BIG_INTENT)
        {
            return false;
        }
        return true;
    });
    return ok;
}

int blockstore_impl_t::dequeue_write(blockstore_op_t *op)
{
    if (PRIV(op)->op_state)
    {
        return continue_write(op);
    }
    if (write_iodepth >= max_write_iodepth)
    {
        return 0;
    }
    PRIV(op)->modified_block = UINT32_MAX;
    PRIV(op)->write_type = 0;
    heap_entry_t *obj = heap->read_entry(op->oid);
    if (op->opcode == BS_OP_DELETE)
    {
        // Delete
        if (!obj || obj->type() == BS_HEAP_DELETE)
        {
            // Already deleted
            op->retval = 0;
            FINISH_OP(op);
            return 2;
        }
        PRIV(op)->write_type = BS_HEAP_DELETE;
        BS_SUBMIT_CHECK_SQES(1);
        int res = heap->add_delete(obj, &PRIV(op)->modified_block);
        if (res == ENOSPC)
            goto enospc;
        assert(res == 0);
        prepare_meta_block_write(PRIV(op)->modified_block);
        PRIV(op)->pending_ops++;
        PRIV(op)->op_state = 5;
        write_iodepth++;
    }
    // FIXME: Allow to do initial writes as buffered, not redirected
    // FIXME: Allow to do direct writes over holes
    else if (!obj || obj->type() == BS_HEAP_DELETE || op->offset == 0 && op->len == dsk.data_block_size)
    {
        // Big (redirect) write
        PRIV(op)->write_type = dsk.disable_data_fsync || op->opcode != BS_OP_WRITE_STABLE ? BS_HEAP_BIG_WRITE : _REDIRECT_INTENT;
        BS_SUBMIT_CHECK_SQES(1);
        PRIV(op)->location = heap->find_free_data();
        if (PRIV(op)->location == UINT64_MAX)
        {
enospc:
            if (!heap->get_to_compact_count())
            {
                // no space
                op->retval = -ENOSPC;
                FINISH_OP(op);
                return 2;
            }
            PRIV(op)->wait_for = WAIT_COMPACTION;
            PRIV(op)->wait_detail = heap->get_compacted_count();
            flusher->request_trim();
            return 0;
        }
        uint64_t loc = PRIV(op)->location;
#ifdef BLOCKSTORE_DEBUG
        printf(
            "Allocate offset %ju for %jx:%jx v%ju\n",
            loc, op->oid.inode, op->oid.stripe, op->version
        );
#endif
        heap->use_data(op->oid.inode, PRIV(op)->location);
        io_uring_sqe *sqe = get_sqe();
        ring_data_t *data = ((ring_data_t*)sqe->user_data);
        data->iov = (struct iovec){ op->buf, op->len };
        data->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
        io_uring_prep_writev(sqe, dsk.data_fd, &data->iov, 1, dsk.data_offset + loc + op->offset);
        PRIV(op)->pending_ops++;
        write_iodepth++;
        if (PRIV(op)->write_type == BS_HEAP_BIG_WRITE)
        {
            PRIV(op)->op_state = 1;
            inflight_big++;
        }
        else
            PRIV(op)->op_state = 3;
    }
    else if (intent_write_allowed(op, obj))
    {
        // Direct intent-write
        BS_SUBMIT_CHECK_SQES(1);
        int res = 0;
        if (dsk.csum_block_size <= dsk.bitmap_granularity &&
            (obj->entry_type == (BS_HEAP_BIG_INTENT|BS_HEAP_STABLE) ||
            obj->entry_type == (BS_HEAP_BIG_WRITE|BS_HEAP_STABLE)))
        {
            // Even more simplified BIG_INTENT writes
            // FIXME: Support RMW mode for csum_block_size > bitmap_granularity
            PRIV(op)->write_type = BS_HEAP_BIG_INTENT;
            PRIV(op)->location = obj->big_location(heap);
            res = heap->add_big_intent(op->oid, &obj, op->version, op->offset, op->len, op->bitmap,
                (uint8_t*)op->buf, NULL, &PRIV(op)->modified_block);
            if (res == ENOSPC)
                goto enospc;
            assert(res == 0);
            PRIV(op)->lsn = obj->lsn;
        }
        else
        {
            PRIV(op)->write_type = BS_HEAP_INTENT_WRITE;
            auto wr = obj;
            while (wr && (wr->type() == BS_HEAP_INTENT_WRITE || wr->type() == BS_HEAP_COMMIT || wr->type() == BS_HEAP_ROLLBACK))
            {
                wr = heap->prev(wr);
            }
            assert(wr && (wr->type() == BS_HEAP_BIG_WRITE || wr->type() == BS_HEAP_BIG_INTENT));
            PRIV(op)->location = wr->big_location(heap);
            res = heap->add_small_write(op->oid, &obj, (BS_HEAP_INTENT_WRITE | (op->opcode == BS_OP_WRITE_STABLE ? BS_HEAP_STABLE : 0)),
                op->version, op->offset, op->len, 0, op->bitmap, (uint8_t*)op->buf, &PRIV(op)->modified_block);
            if (res == ENOSPC)
                goto enospc;
            assert(res == 0);
            PRIV(op)->lsn = obj->lsn;
        }
        prepare_meta_block_write(PRIV(op)->modified_block);
        PRIV(op)->pending_ops++;
        PRIV(op)->op_state = 9;
        write_iodepth++;
    }
    else
    {
        // Small (buffered) overwrite
        // First check if there is free buffer space
        PRIV(op)->write_type = BS_HEAP_SMALL_WRITE;
        uint64_t loc = !op->len ? 0 : heap->find_free_buffer_area(op->len);
        if (loc == UINT64_MAX)
        {
            PRIV(op)->wait_for = WAIT_COMPACTION;
            PRIV(op)->wait_detail = heap->get_compacted_count();
            flusher->request_trim();
            return 0;
        }
        // There is sufficient space. Check SQE(s)
        BS_SUBMIT_CHECK_SQES(1 + (op->len > 0 ? 1 : 0));
        int res = heap->add_small_write(op->oid, &obj, (BS_HEAP_SMALL_WRITE | (op->opcode == BS_OP_WRITE_STABLE ? BS_HEAP_STABLE : 0)),
            op->version, op->offset, op->len, loc, op->bitmap, (uint8_t*)op->buf, &PRIV(op)->modified_block);
        if (res == ENOSPC)
            goto enospc;
        assert(res == 0);
        PRIV(op)->lsn = obj->lsn;
        if (op->len)
            heap->use_buffer_area(op->oid.inode, loc, op->len);
        prepare_meta_block_write(PRIV(op)->modified_block);
        PRIV(op)->pending_ops++;
        if (op->len > 0)
        {
            // Prepare buffered data write
            if (dsk.inmemory_journal)
            {
                memcpy((uint8_t*)buffer_area + loc, op->buf, op->len);
            }
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
        assert(PRIV(op)->pending_ops);
        PRIV(op)->op_state = 5;
        write_iodepth++;
    }
    return 1;
}

int blockstore_impl_t::continue_write(blockstore_op_t *op)
{
    int op_state = PRIV(op)->op_state;
again:
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
        assert(op_state < 10);
        if (PRIV(op)->modified_block != UINT32_MAX &&
            !meta_block_is_pending(PRIV(op)->modified_block))
        {
            PRIV(op)->pending_ops--;
            PRIV(op)->modified_block = UINT32_MAX;
        }
        if (PRIV(op)->pending_ops > 0)
            return 1;
        op_state++;
        goto again;
    }
resume_2:
    // We must fsync all big writes to avoid complex write workflows
    // It's OK for all HDDs and for server SSDs, but slightly worse for desktop SSDs
    inflight_big--;
    if (!dsk.disable_data_fsync)
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
        auto obj = heap->read_entry(op->oid);
        int res = 0;
        if (PRIV(op)->write_type == _REDIRECT_INTENT)
        {
            res = heap->add_redirect_intent(op->oid, &obj, op->version, op->offset, op->len,
                PRIV(op)->location, op->bitmap, (uint8_t*)op->buf, &PRIV(op)->modified_block);
        }
        else
        {
            res = heap->add_big_write(op->oid, obj, op->opcode == BS_OP_WRITE_STABLE,
                op->version, op->offset, op->len, PRIV(op)->location, op->bitmap, (uint8_t*)op->buf, &PRIV(op)->modified_block);
        }
        if (res == ENOSPC)
        {
            if (!heap->get_to_compact_count())
            {
                // no space
                heap->free_data(op->oid.inode, PRIV(op)->location);
                write_iodepth--;
                op->retval = -ENOSPC;
                FINISH_OP(op);
                return 2;
            }
            PRIV(op)->wait_for = WAIT_COMPACTION;
            PRIV(op)->wait_detail = heap->get_compacted_count();
            flusher->request_trim();
            return 0;
        }
        assert(res == 0);
        prepare_meta_block_write(PRIV(op)->modified_block);
        PRIV(op)->pending_ops++;
        PRIV(op)->op_state = 5;
        return 1;
    }
resume_6:
    // Apply throttling to not fill the journal too quickly for the SSD+HDD case
    if (PRIV(op)->write_type == BS_HEAP_SMALL_WRITE && throttle_small_writes)
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
                PRIV(op)->op_state = 8;
                ringloop->wakeup();
            });
            return 1;
        }
    }
resume_8:
    // Acknowledge write
#ifdef BLOCKSTORE_DEBUG
    printf("Ack write %jx:%jx v%ju\n", op->oid.inode, op->oid.stripe, op->version);
#endif
    op->retval = op->len;
    if (PRIV(op)->write_type == BS_HEAP_BIG_WRITE)
    {
        unsynced_data_write_count++;
    }
    else if (PRIV(op)->write_type == BS_HEAP_SMALL_WRITE)
    {
        unsynced_buffer_write_count++;
        heap->complete_lsn_write(PRIV(op)->lsn);
    }
    else if (PRIV(op)->write_type == BS_HEAP_BIG_INTENT ||
        PRIV(op)->write_type == BS_HEAP_INTENT_WRITE)
    {
        unsynced_data_write_count++;
        intent_write_counter++;
        heap->complete_lsn_write(PRIV(op)->lsn);
    }
    else if (PRIV(op)->write_type == _REDIRECT_INTENT)
    {
        unsynced_data_write_count++;
        intent_write_counter++;
    }
    write_iodepth--;
    FINISH_OP(op);
    return 2;
resume_10:
    // Direct intent-write
    // LSN is not marked as completed so big_write won't be freed
    BS_SUBMIT_GET_SQE(sqe, data);
    data->iov = (struct iovec){ op->buf, op->len };
    data->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
    io_uring_prep_writev(sqe, dsk.data_fd, &data->iov, 1, dsk.data_offset + PRIV(op)->location + op->offset);
    if (dsk.use_atomic_flag)
        sqe->rw_flags = RWF_ATOMIC;
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
        ringloop->wakeup();
    }
}
