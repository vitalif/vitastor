// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_impl.h"
#include "blockstore_internal.h"

int blockstore_impl_t::continue_sync(blockstore_op_t *op)
{
    int op_state = PRIV(op)->op_state;
    if (op_state == 1) goto resume_1;
    if (op_state == 2) goto resume_2;
    assert(!op_state);
    if (immediate_commit == IMMEDIATE_ALL || !unsynced_big_write_count && !unsynced_small_write_count)
    {
        // We can return immediately because sync is only dequeued after all previous writes
        unsynced_big_write_count = unsynced_small_write_count = 0;
        op->retval = 0;
        FINISH_OP(op);
        return 2;
    }
    stop_sync_submitted = false;
    if (unsynced_small_write_count > 0 && !disable_journal_fsync)
    {
        // fsync buffer
        BS_SUBMIT_GET_SQE(sqe, data);
        io_uring_prep_fsync(sqe, dsk.journal_fd, IORING_FSYNC_DATASYNC);
        data->iov = { 0 };
        data->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
        PRIV(op)->pending_ops++;
    }
    if (!disable_meta_fsync && dsk.meta_fd != dsk.journal_fd)
    {
        // fsync meta
        BS_SUBMIT_GET_SQE(sqe, data);
        io_uring_prep_fsync(sqe, dsk.meta_fd, IORING_FSYNC_DATASYNC);
        data->iov = { 0 };
        data->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
        PRIV(op)->pending_ops++;
    }
    unsynced_big_write_count = 0;
    unsynced_small_write_count = 0;
resume_1:
    if (PRIV(op)->pending_ops > 0)
    {
        PRIV(op)->op_state = 1;
        return 1;
    }
resume_2:
    op->retval = 0;
    FINISH_OP(op);
    return 2;
}
