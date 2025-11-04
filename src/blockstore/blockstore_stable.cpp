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
    else if (priv->op_state == 5) goto resume_5;
    assert(!priv->op_state);
    op->retval = 0;
    priv->modified_block = priv->modified_block2 = UINT32_MAX;
    for (priv->stab_pos = 0; priv->stab_pos < op->len; priv->stab_pos++)
    {
        {
            auto obj = heap->read_entry(v[priv->stab_pos].oid);
            if (!obj)
            {
                op->retval = -ENOENT;
                FINISH_OP(op);
                return 2;
            }
            int res = op->opcode == BS_OP_STABLE
                ? heap->add_commit(obj, v[priv->stab_pos].version, &priv->modified_block2)
                : heap->add_rollback(obj, v[priv->stab_pos].version, &priv->modified_block2);
            if (res == EBUSY)
            {
                op->retval = -EBUSY;
                FINISH_OP(op);
                return 2;
            }
            if (res == ENOSPC)
            {
                if (!heap->get_to_compact_count())
                {
                    // no space
                    op->retval = -ENOSPC;
                    FINISH_OP(op);
                    return 2;
                }
                if (priv->modified_block2 != UINT32_MAX)
                {
                    priv->stab_pos--;
                    goto resume_1;
                }
                priv->wait_for = WAIT_COMPACTION;
                priv->wait_detail = heap->get_compacted_count();
                flusher->request_trim();
                return 0;
            }
            assert(res == 0);
        }
        if (priv->modified_block != UINT32_MAX && priv->modified_block2 != priv->modified_block)
        {
resume_1:
            BS_SUBMIT_CHECK_SQES(1);
            prepare_meta_block_write(priv->modified_block);
resume_2:
            if (meta_block_is_pending(priv->modified_block))
            {
                priv->op_state = 2;
                return 1;
            }
        }
        priv->modified_block = priv->modified_block2;
        if (priv->stab_pos == op->len-1 && priv->modified_block2 != UINT32_MAX)
        {
            priv->modified_block2 = UINT32_MAX;
            goto resume_1;
        }
    }
    // Fsync, just because our semantics imply that commit (stabilize) is immediately fsynced
    priv->op_state = 3;
resume_3:
resume_4:
resume_5:
    int res = do_sync(op, 3);
    if (res != 2)
    {
        return res;
    }
    // Done. Don't touch op->retval - if anything resulted in ENOENT, return it as is
    FINISH_OP(op);
    return 2;
}
