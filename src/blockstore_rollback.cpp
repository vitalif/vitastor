// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_impl.h"

int blockstore_impl_t::dequeue_rollback(blockstore_op_t *op)
{
    if (PRIV(op)->op_state)
    {
        return continue_rollback(op);
    }
    int r = split_stab_op(op, [this](obj_ver_id ov)
    {
        // Check that there are some versions greater than v->version (which may be zero),
        // check that they're unstable, synced, and not currently written to
        auto dirty_it = dirty_db.lower_bound((obj_ver_id){
            .oid = ov.oid,
            .version = UINT64_MAX,
        });
        if (dirty_it == dirty_db.begin())
        {
            // Already rolled back, skip this object version
            return STAB_SPLIT_DONE;
        }
        else
        {
            dirty_it--;
            if (dirty_it->first.oid != ov.oid || dirty_it->first.version < ov.version)
            {
                // Already rolled back, skip this object version
                return STAB_SPLIT_DONE;
            }
            while (dirty_it->first.oid == ov.oid && dirty_it->first.version > ov.version)
            {
                if (IS_IN_FLIGHT(dirty_it->second.state))
                {
                    // Object write is still in progress. Wait until the write request completes
                    return STAB_SPLIT_WAIT;
                }
                else if (!IS_SYNCED(dirty_it->second.state) ||
                    IS_STABLE(dirty_it->second.state))
                {
                    // Sync the object
                    return STAB_SPLIT_SYNC;
                }
                if (dirty_it == dirty_db.begin())
                {
                    break;
                }
                dirty_it--;
            }
            return STAB_SPLIT_TODO;
        }
    });
    if (r != 1)
    {
        return r;
    }
    // Check journal space
    blockstore_journal_check_t space_check(this);
    if (!space_check.check_available(op, op->len, sizeof(journal_entry_rollback), 0))
    {
        return 0;
    }
    // There is sufficient space. Check SQEs
    BS_SUBMIT_CHECK_SQES(space_check.sectors_to_write);
    // Prepare and submit journal entries
    int s = 0;
    auto v = (obj_ver_id*)op->buf;
    for (int i = 0; i < op->len; i++, v++)
    {
        if (!journal.entry_fits(sizeof(journal_entry_rollback)) &&
            journal.sector_info[journal.cur_sector].dirty)
        {
            prepare_journal_sector_write(journal.cur_sector, op);
            s++;
        }
        journal_entry_rollback *je = (journal_entry_rollback*)
            prefill_single_journal_entry(journal, JE_ROLLBACK, sizeof(journal_entry_rollback));
        je->oid = v->oid;
        je->version = v->version;
        je->crc32 = je_crc32((journal_entry*)je);
        journal.crc32_last = je->crc32;
    }
    prepare_journal_sector_write(journal.cur_sector, op);
    s++;
    assert(s == space_check.sectors_to_write);
    PRIV(op)->op_state = 1;
    return 1;
}

int blockstore_impl_t::continue_rollback(blockstore_op_t *op)
{
    if (PRIV(op)->op_state == 2)
        goto resume_2;
    else if (PRIV(op)->op_state == 4)
        goto resume_4;
    else
        return 1;
resume_2:
    if (!disable_journal_fsync)
    {
        BS_SUBMIT_GET_SQE(sqe, data);
        my_uring_prep_fsync(sqe, dsk.journal_fd, IORING_FSYNC_DATASYNC);
        data->iov = { 0 };
        data->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
        PRIV(op)->min_flushed_journal_sector = PRIV(op)->max_flushed_journal_sector = 0;
        PRIV(op)->pending_ops = 1;
        PRIV(op)->op_state = 3;
        return 1;
    }
resume_4:
    obj_ver_id* v;
    int i;
    for (i = 0, v = (obj_ver_id*)op->buf; i < op->len; i++, v++)
    {
        mark_rolled_back(*v);
    }
    // Acknowledge op
    op->retval = 0;
    FINISH_OP(op);
    return 2;
}

void blockstore_impl_t::mark_rolled_back(const obj_ver_id & ov)
{
    auto it = dirty_db.lower_bound((obj_ver_id){
        .oid = ov.oid,
        .version = UINT64_MAX,
    });
    if (it != dirty_db.begin())
    {
        uint64_t max_unstable = 0;
        auto rm_start = it;
        auto rm_end = it;
        it--;
        while (1)
        {
            if (it->first.oid != ov.oid)
                break;
            else if (it->first.version <= ov.version)
            {
                if (!IS_STABLE(it->second.state))
                    max_unstable = it->first.version;
                break;
            }
            else if (IS_IN_FLIGHT(it->second.state) || IS_STABLE(it->second.state))
                break;
            // Remove entry
            rm_start = it;
            if (it == dirty_db.begin())
                break;
            it--;
        }
        if (rm_start != rm_end)
        {
            erase_dirty(rm_start, rm_end, UINT64_MAX);
            auto unstab_it = unstable_writes.find(ov.oid);
            if (unstab_it != unstable_writes.end())
            {
                if (max_unstable == 0)
                    unstable_writes.erase(unstab_it);
                else
                    unstab_it->second = max_unstable;
            }
        }
    }
}

void blockstore_impl_t::erase_dirty(blockstore_dirty_db_t::iterator dirty_start, blockstore_dirty_db_t::iterator dirty_end, uint64_t clean_loc)
{
    if (dirty_end == dirty_start)
    {
        return;
    }
    auto dirty_it = dirty_end;
    dirty_it--;
    if (IS_DELETE(dirty_it->second.state))
    {
        object_id oid = dirty_it->first.oid;
#ifdef BLOCKSTORE_DEBUG
        printf("Unblock writes-after-delete %lx:%lx v%lx\n", oid.inode, oid.stripe, dirty_it->first.version);
#endif
        dirty_it = dirty_end;
        // Unblock operations blocked by delete flushing
        uint32_t next_state = BS_ST_IN_FLIGHT;
        while (dirty_it != dirty_db.end() && dirty_it->first.oid == oid)
        {
            if ((dirty_it->second.state & BS_ST_WORKFLOW_MASK) == BS_ST_WAIT_DEL)
            {
                dirty_it->second.state = (dirty_it->second.state & ~BS_ST_WORKFLOW_MASK) | next_state;
                if (IS_BIG_WRITE(dirty_it->second.state))
                {
                    next_state = BS_ST_WAIT_BIG;
                }
            }
            dirty_it++;
        }
        dirty_it = dirty_end;
        dirty_it--;
    }
    while (1)
    {
        if (IS_BIG_WRITE(dirty_it->second.state) && dirty_it->second.location != clean_loc &&
            dirty_it->second.location != UINT64_MAX)
        {
#ifdef BLOCKSTORE_DEBUG
            printf("Free block %lu from %lx:%lx v%lu\n", dirty_it->second.location >> dsk.block_order,
                dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version);
#endif
            data_alloc->set(dirty_it->second.location >> dsk.block_order, false);
        }
        auto used = --journal.used_sectors[dirty_it->second.journal_sector];
#ifdef BLOCKSTORE_DEBUG
        printf(
            "remove usage of journal offset %08lx by %lx:%lx v%lu (%d refs)\n", dirty_it->second.journal_sector,
            dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version, used
        );
#endif
        if (used == 0)
        {
            journal.used_sectors.erase(dirty_it->second.journal_sector);
            flusher->mark_trim_possible();
        }
        if (dsk.clean_entry_bitmap_size > sizeof(void*))
        {
            free(dirty_it->second.bitmap);
            dirty_it->second.bitmap = NULL;
        }
        if (dirty_it == dirty_start)
        {
            break;
        }
        dirty_it--;
    }
    dirty_db.erase(dirty_start, dirty_end);
}
