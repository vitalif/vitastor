// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_impl.h"
#include "blockstore_internal.h"

// Handles both stabilize (commit) and rollback
int blockstore_impl_t::dequeue_stable(blockstore_op_t *op)
{
    obj_ver_id *v = (obj_ver_id*)op->buf;
    auto priv = PRIV(op);
    if (priv->op_state == 1)      goto resume_1;
    else if (priv->op_state == 2) goto resume_2;
    else if (priv->op_state == 3) goto resume_3;
    else if (priv->op_state == 4) goto resume_4;
    assert(!priv->op_state);
    priv->stab_pos = 0;
    op->retval = 0;
    priv->to_compact.resize(op->len);
    while (priv->stab_pos < op->len)
    {
        io_uring_sqe *sqe = get_sqe();
        if (!sqe)
        {
            if (priv->pending_ops > 0)
                return 1;
            priv->wait_detail = 1;
            priv->wait_for = WAIT_SQE;
            return 0;
        }
        uint32_t modified_block = 0;
        uint64_t before_compact = 0;
        uint64_t to_compact = 0;
        int res = op->opcode == BS_OP_STABLE
            ? heap->post_stabilize(v[priv->stab_pos].oid, v[priv->stab_pos].version, &modified_block, &before_compact, &to_compact)
            : heap->post_rollback(v[priv->stab_pos].oid, v[priv->stab_pos].version, &modified_block);
        if (res != 0)
        {
            op->retval = -res;
            FINISH_OP(op);
            return 2;
        }
        if (modified_block)
        {
            if (to_compact)
            {
                priv->to_compact[priv->stab_pos] = true;
                committing_lsn[v[priv->stab_pos].oid] = before_compact;
            }
            prepare_meta_block_write(op, modified_block);
            priv->pending_ops++;
        }
        priv->stab_pos++;
    }
resume_1:
    if (priv->pending_ops > 0)
    {
        priv->op_state = 1;
        return 0;
    }
resume_2:
    if (!disable_meta_fsync)
    {
        BS_SUBMIT_GET_SQE(sqe, data);
        io_uring_prep_fsync(sqe, dsk.meta_fd, IORING_FSYNC_DATASYNC);
        data->iov = { 0 };
        data->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
        priv->pending_ops++;
    }
resume_3:
    if (priv->pending_ops > 0)
    {
        priv->op_state = 3;
        return 0;
    }
resume_4:
    for (int i = 0; i < op->len; i++)
    {
        if (priv->to_compact[i])
        {
            // Add to compact queue only when metadata writes are finished
            heap->add_to_compact_queue(v[i].oid);
            auto cm_it = committing_lsn.find(v[i].oid);
            assert(cm_it != committing_lsn.end());
            if (cm_it->second <= priv->to_compact[i])
                committing_lsn.erase(cm_it);
        }
    }
    // Done. Don't touch op->retval - if anything resulted in ENOENT, return it as is
    FINISH_OP(op);
    return 2;
}
