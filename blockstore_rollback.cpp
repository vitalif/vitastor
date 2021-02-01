// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

#include "blockstore_impl.h"

int blockstore_impl_t::dequeue_rollback(blockstore_op_t *op)
{
    if (PRIV(op)->op_state)
    {
        return continue_rollback(op);
    }
    obj_ver_id *v, *nv;
    int i, todo = op->len;
    for (i = 0, v = (obj_ver_id*)op->buf, nv = (obj_ver_id*)op->buf; i < op->len; i++, v++, nv++)
    {
        if (nv != v)
        {
            *nv = *v;
        }
        // Check that there are some versions greater than v->version (which may be zero),
        // check that they're unstable, synced, and not currently written to
        auto dirty_it = dirty_db.lower_bound((obj_ver_id){
            .oid = v->oid,
            .version = UINT64_MAX,
        });
        if (dirty_it == dirty_db.begin())
        {
skip_ov:
            // Already rolled back, skip this object version
            todo--;
            nv--;
            continue;
        }
        else
        {
            dirty_it--;
            if (dirty_it->first.oid != v->oid || dirty_it->first.version < v->version)
            {
                goto skip_ov;
            }
            while (dirty_it->first.oid == v->oid && dirty_it->first.version > v->version)
            {
                if (IS_IN_FLIGHT(dirty_it->second.state))
                {
                    // Object write is still in progress. Wait until the write request completes
                    return 0;
                }
                else if (!IS_SYNCED(dirty_it->second.state) ||
                    IS_STABLE(dirty_it->second.state))
                {
                    op->retval = -EBUSY;
                    FINISH_OP(op);
                    return 1;
                }
                if (dirty_it == dirty_db.begin())
                {
                    break;
                }
                dirty_it--;
            }
        }
    }
    op->len = todo;
    if (!todo)
    {
        // Already rolled back
        op->retval = 0;
        FINISH_OP(op);
        return 1;
    }
    // Check journal space
    blockstore_journal_check_t space_check(this);
    if (!space_check.check_available(op, todo, sizeof(journal_entry_rollback), 0))
    {
        return 0;
    }
    // There is sufficient space. Get SQEs
    struct io_uring_sqe *sqe[space_check.sectors_required];
    for (i = 0; i < space_check.sectors_required; i++)
    {
        BS_SUBMIT_GET_SQE_DECL(sqe[i]);
    }
    // Prepare and submit journal entries
    auto cb = [this, op](ring_data_t *data) { handle_rollback_event(data, op); };
    int s = 0, cur_sector = -1;
    for (i = 0, v = (obj_ver_id*)op->buf; i < op->len; i++, v++)
    {
        if (!journal.entry_fits(sizeof(journal_entry_rollback)) &&
            journal.sector_info[journal.cur_sector].dirty)
        {
            if (cur_sector == -1)
                PRIV(op)->min_flushed_journal_sector = 1 + journal.cur_sector;
            prepare_journal_sector_write(journal, journal.cur_sector, sqe[s++], cb);
            cur_sector = journal.cur_sector;
        }
        journal_entry_rollback *je = (journal_entry_rollback*)
            prefill_single_journal_entry(journal, JE_ROLLBACK, sizeof(journal_entry_rollback));
        je->oid = v->oid;
        je->version = v->version;
        je->crc32 = je_crc32((journal_entry*)je);
        journal.crc32_last = je->crc32;
    }
    prepare_journal_sector_write(journal, journal.cur_sector, sqe[s++], cb);
    assert(s == space_check.sectors_required);
    if (cur_sector == -1)
        PRIV(op)->min_flushed_journal_sector = 1 + journal.cur_sector;
    PRIV(op)->max_flushed_journal_sector = 1 + journal.cur_sector;
    PRIV(op)->pending_ops = s;
    PRIV(op)->op_state = 1;
    return 1;
}

int blockstore_impl_t::continue_rollback(blockstore_op_t *op)
{
    if (PRIV(op)->op_state == 2)
        goto resume_2;
    else if (PRIV(op)->op_state == 3)
        goto resume_3;
    else if (PRIV(op)->op_state == 5)
        goto resume_5;
    else
        return 1;
resume_2:
    // Release used journal sectors
    release_journal_sectors(op);
resume_3:
    if (!disable_journal_fsync)
    {
        io_uring_sqe *sqe = get_sqe();
        if (!sqe)
        {
            return 0;
        }
        ring_data_t *data = ((ring_data_t*)sqe->user_data);
        my_uring_prep_fsync(sqe, journal.fd, IORING_FSYNC_DATASYNC);
        data->iov = { 0 };
        data->callback = [this, op](ring_data_t *data) { handle_rollback_event(data, op); };
        PRIV(op)->min_flushed_journal_sector = PRIV(op)->max_flushed_journal_sector = 0;
        PRIV(op)->pending_ops = 1;
        PRIV(op)->op_state = 4;
        return 1;
    }
resume_5:
    obj_ver_id* v;
    int i;
    for (i = 0, v = (obj_ver_id*)op->buf; i < op->len; i++, v++)
    {
        mark_rolled_back(*v);
    }
    flusher->mark_trim_possible();
    // Acknowledge op
    op->retval = 0;
    FINISH_OP(op);
    return 1;
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
        while (it->first.oid == ov.oid &&
            it->first.version > ov.version &&
            !IS_IN_FLIGHT(it->second.state) &&
            !IS_STABLE(it->second.state))
        {
            if (it->first.oid != ov.oid)
                break;
            else if (it->first.version <= ov.version)
            {
                if (!IS_STABLE(it->second.state))
                    max_unstable = it->first.version;
                break;
            }
            else if (IS_STABLE(it->second.state))
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
        }
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

void blockstore_impl_t::handle_rollback_event(ring_data_t *data, blockstore_op_t *op)
{
    live = true;
    if (data->res != data->iov.iov_len)
    {
        throw std::runtime_error(
            "write operation failed ("+std::to_string(data->res)+" != "+std::to_string(data->iov.iov_len)+
            "). in-memory state is corrupted. AAAAAAAaaaaaaaaa!!!111"
        );
    }
    PRIV(op)->pending_ops--;
    if (PRIV(op)->pending_ops == 0)
    {
        PRIV(op)->op_state++;
        if (!continue_rollback(op))
        {
            submit_queue.push_front(op);
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
        if (IS_BIG_WRITE(dirty_it->second.state) && dirty_it->second.location != clean_loc)
        {
#ifdef BLOCKSTORE_DEBUG
            printf("Free block %lu\n", dirty_it->second.location >> block_order);
#endif
            data_alloc->set(dirty_it->second.location >> block_order, false);
        }
        int used = --journal.used_sectors[dirty_it->second.journal_sector];
#ifdef BLOCKSTORE_DEBUG
        printf(
            "remove usage of journal offset %08lx by %lx:%lx v%lu (%d refs)\n", dirty_it->second.journal_sector,
            dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version, used
        );
#endif
        if (used == 0)
        {
            journal.used_sectors.erase(dirty_it->second.journal_sector);
        }
        if (dirty_it == dirty_start)
        {
            break;
        }
        dirty_it--;
    }
    dirty_db.erase(dirty_start, dirty_end);
}
