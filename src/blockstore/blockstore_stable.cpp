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
    // Modify in-memory state and assign contiguous LSNs
    priv->stab_pos = 0;
    priv->lsn = priv->to_lsn = 0;
    op->retval = 0;
    while (priv->stab_pos < op->len)
    {
        uint32_t modified_block = 0;
        uint64_t new_lsn = 0;
        uint64_t new_to_lsn = 0;
        int res = op->opcode == BS_OP_STABLE
            ? heap->post_stabilize(v[priv->stab_pos].oid, v[priv->stab_pos].version, &modified_block, &new_lsn, &new_to_lsn)
            : heap->post_rollback(v[priv->stab_pos].oid, v[priv->stab_pos].version, &new_lsn, &modified_block);
        if (res != 0)
        {
            assert(res == ENOENT || res == EBUSY);
            op->retval = -res;
        }
        if (new_lsn)
        {
            if (!priv->lsn)
                priv->lsn = new_lsn;
            priv->to_lsn = op->opcode == BS_OP_STABLE ? new_to_lsn : new_lsn;
        }
        priv->stab_pos++;
    }
    // Submit metadata writes
    priv->stab_pos = 0;
resume_1:
    priv->op_state = 1;
    while (priv->stab_pos < op->len)
    {
        uint32_t block_num = 0;
        heap_object_t *obj = heap->read_entry(v[priv->stab_pos].oid, &block_num);
        if (obj)
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
            prepare_meta_block_write(op, block_num, sqe);
        }
        priv->stab_pos++;
    }
    if (priv->pending_ops > 0)
    {
        priv->op_state = 1;
        return 1;
    }
    // Mark writes as completed to allow compaction
    for (uint64_t lsn = priv->lsn; lsn <= priv->to_lsn; lsn++)
    {
        heap->mark_lsn_completed(lsn);
    }
    // Fsync, just because our semantics imply that commit (stabilize) is immediately fsynced
    priv->op_state = 2;
resume_2:
resume_3:
resume_4:
    int res = do_sync(op, 2);
    if (res != 2)
    {
        return res;
    }
    // Done. Don't touch op->retval - if anything resulted in ENOENT, return it as is
    FINISH_OP(op);
    return 2;
}
