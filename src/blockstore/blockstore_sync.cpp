// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_impl.h"
#include "blockstore_internal.h"

int blockstore_impl_t::continue_sync(blockstore_op_t *op)
{
    if (!PRIV(op)->op_state)
    {
        op->retval = 0;
    }
    int res = do_sync(op, 0);
    if (res == 2)
    {
        FINISH_OP(op);
    }
    return res;
}

bool blockstore_impl_t::submit_fsyncs(int & wait_count)
{
    int n = ((unsynced_small_write_count > 0 || unsynced_data_write_count > 0 || unsynced_meta_write_count > 0) && !dsk.disable_meta_fsync) +
        (unsynced_small_write_count > 0 && !dsk.disable_journal_fsync && dsk.journal_fd != dsk.meta_fd) +
        (unsynced_data_write_count > 0 && !dsk.disable_data_fsync && dsk.data_fd != dsk.meta_fd && dsk.data_fd != dsk.journal_fd);
    if (ringloop->space_left() < n)
    {
        return false;
    }
    if (!n)
    {
        return true;
    }
    auto cb = [this, & wait_count](ring_data_t *data)
    {
        if (data->res != 0)
            disk_error_abort("sync meta", data->res, 0);
        wait_count--;
        assert(wait_count >= 0);
        if (!wait_count)
            ringloop->wakeup();
    };
    if ((unsynced_small_write_count > 0 || unsynced_data_write_count > 0 || unsynced_meta_write_count > 0) && !dsk.disable_meta_fsync)
    {
        // fsync meta
        io_uring_sqe *sqe = get_sqe();
        assert(sqe);
        ring_data_t *data = ((ring_data_t*)sqe->user_data);
        io_uring_prep_fsync(sqe, dsk.meta_fd, IORING_FSYNC_DATASYNC);
        data->iov = { 0 };
        data->callback = cb;
        wait_count++;
    }
    if (unsynced_small_write_count > 0 && !dsk.disable_journal_fsync && dsk.meta_fd != dsk.journal_fd)
    {
        // fsync buffer
        io_uring_sqe *sqe = get_sqe();
        assert(sqe);
        ring_data_t *data = ((ring_data_t*)sqe->user_data);
        io_uring_prep_fsync(sqe, dsk.journal_fd, IORING_FSYNC_DATASYNC);
        data->iov = { 0 };
        data->callback = cb;
        wait_count++;
    }
    if (unsynced_data_write_count > 0 && !dsk.disable_data_fsync && dsk.data_fd != dsk.meta_fd && dsk.data_fd != dsk.journal_fd)
    {
        // fsync data
        io_uring_sqe *sqe = get_sqe();
        assert(sqe);
        ring_data_t *data = ((ring_data_t*)sqe->user_data);
        io_uring_prep_fsync(sqe, dsk.data_fd, IORING_FSYNC_DATASYNC);
        data->iov = { 0 };
        data->callback = cb;
        wait_count++;
    }
    unsynced_data_write_count = 0;
    unsynced_small_write_count = 0;
    unsynced_meta_write_count = 0;
    return true;
}

int blockstore_impl_t::do_sync(blockstore_op_t *op, int base_state)
{
    int op_state = PRIV(op)->op_state - base_state;
    if (op_state == 1) goto resume_1;
    if (op_state == 2) goto resume_2;
    assert(!op_state);
    if (flusher->get_syncing_buffer())
    {
        // Wait for flusher-initiated sync
        return 0;
    }
    if (dsk.disable_journal_fsync && dsk.disable_meta_fsync && dsk.disable_data_fsync ||
        !unsynced_data_write_count && !unsynced_small_write_count && !unsynced_meta_write_count)
    {
        // We can return immediately because sync only syncs previous writes
        unsynced_data_write_count = unsynced_small_write_count = unsynced_meta_write_count = 0;
        return 2;
    }
    PRIV(op)->modified_block = heap->get_completed_lsn();
    if (!submit_fsyncs(PRIV(op)->pending_ops))
    {
        PRIV(op)->wait_detail = 1;
        PRIV(op)->wait_for = WAIT_SQE;
        return 0;
    }
resume_1:
    if (PRIV(op)->pending_ops > 0)
    {
        PRIV(op)->op_state = base_state+1;
        return 1;
    }
resume_2:
    heap->mark_lsn_fsynced(PRIV(op)->modified_block);
    return 2;
}
