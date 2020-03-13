#include "blockstore_impl.h"

int blockstore_impl_t::dequeue_rollback(blockstore_op_t *op)
{
    if (PRIV(op)->op_state)
    {
        return continue_rollback(op);
    }
    obj_ver_id* v;
    int i, todo = op->len;
    for (i = 0, v = (obj_ver_id*)op->buf; i < op->len; i++, v++)
    {
        // Check that there are some versions greater than v->version (which may be zero),
        // check that they're unstable, synced, and not currently written to
        auto dirty_it = dirty_db.lower_bound((obj_ver_id){
            .oid = v->oid,
            .version = UINT64_MAX,
        });
        if (dirty_it == dirty_db.begin())
        {
            if (v->version == 0)
            {
                // Already rolled back
            }
        bad_op:
            op->retval = -EINVAL;
            FINISH_OP(op);
            return 1;
        }
        else
        {
            dirty_it--;
            if (dirty_it->first.oid != v->oid || dirty_it->first.version < v->version)
            {
                goto bad_op;
            }
            while (dirty_it->first.oid == v->oid && dirty_it->first.version > v->version)
            {
                if (!IS_SYNCED(dirty_it->second.state) ||
                    IS_STABLE(dirty_it->second.state))
                {
                    goto bad_op;
                }
                if (dirty_it == dirty_db.begin())
                {
                    break;
                }
                dirty_it--;
            }
        }
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
    if ((journal_block_size - journal.in_sector_pos) < sizeof(journal_entry_rollback) &&
        journal.sector_info[journal.cur_sector].dirty)
    {
        if (cur_sector == -1)
            PRIV(op)->min_flushed_journal_sector = 1 + journal.cur_sector;
        cur_sector = journal.cur_sector;
        prepare_journal_sector_write(journal, cur_sector, sqe[s++], cb);
    }
    for (i = 0, v = (obj_ver_id*)op->buf; i < op->len; i++, v++)
    {
        // FIXME This is here only for the purpose of tracking unstable_writes. Remove if not required
        // FIXME ...aaaand this is similar to blockstore_init.cpp - maybe dedup it?
        auto dirty_it = dirty_db.lower_bound((obj_ver_id){
            .oid = v->oid,
            .version = UINT64_MAX,
        });
        uint64_t max_unstable = 0;
        while (dirty_it != dirty_db.begin())
        {
            dirty_it--;
            if (dirty_it->first.oid != v->oid)
                break;
            else if (dirty_it->first.version <= v->version)
            {
                if (!IS_STABLE(dirty_it->second.state))
                    max_unstable = dirty_it->first.version;
                break;
            }
        }
        auto unstab_it = unstable_writes.find(v->oid);
        if (unstab_it != unstable_writes.end())
        {
            if (max_unstable == 0)
                unstable_writes.erase(unstab_it);
            else
                unstab_it->second = max_unstable;
        }
        journal_entry_rollback *je = (journal_entry_rollback*)
            prefill_single_journal_entry(journal, JE_ROLLBACK, sizeof(journal_entry_rollback));
        journal.sector_info[journal.cur_sector].dirty = false;
        je->oid = v->oid;
        je->version = v->version;
        je->crc32 = je_crc32((journal_entry*)je);
        journal.crc32_last = je->crc32;
        if (cur_sector != journal.cur_sector)
        {
            if (cur_sector == -1)
                PRIV(op)->min_flushed_journal_sector = 1 + journal.cur_sector;
            cur_sector = journal.cur_sector;
            prepare_journal_sector_write(journal, cur_sector, sqe[s++], cb);
        }
    }
    PRIV(op)->max_flushed_journal_sector = 1 + journal.cur_sector;
    PRIV(op)->pending_ops = s;
    PRIV(op)->op_state = 1;
    inflight_writes++;
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
        data->callback = [this, op](ring_data_t *data) { handle_stable_event(data, op); };
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
        // Erase dirty_db entries
        auto rm_end = dirty_db.lower_bound((obj_ver_id){
            .oid = v->oid,
            .version = UINT64_MAX,
        });
        rm_end--;
        auto rm_start = rm_end;
        while (1)
        {
            if (rm_end->first.oid != v->oid)
                break;
            else if (rm_end->first.version <= v->version)
                break;
            rm_start = rm_end;
            if (rm_end == dirty_db.begin())
                break;
            rm_end--;
        }
        if (rm_end != rm_start)
            erase_dirty(rm_start, rm_end, UINT64_MAX);
    }
    journal.trim();
    inflight_writes--;
    // Acknowledge op
    op->retval = 0;
    FINISH_OP(op);
    return 1;
}

void blockstore_impl_t::handle_rollback_event(ring_data_t *data, blockstore_op_t *op)
{
    live = true;
    if (data->res != data->iov.iov_len)
    {
        inflight_writes--;
        throw std::runtime_error(
            "write operation failed ("+std::to_string(data->res)+" != "+std::to_string(data->iov.iov_len)+
            "). in-memory state is corrupted. AAAAAAAaaaaaaaaa!!!111"
        );
    }
    PRIV(op)->pending_ops--;
    if (PRIV(op)->pending_ops == 0)
    {
        PRIV(op)->op_state++;
        if (!continue_stable(op))
        {
            submit_queue.push_front(op);
        }
    }
}

void blockstore_impl_t::erase_dirty(blockstore_dirty_db_t::iterator dirty_start, blockstore_dirty_db_t::iterator dirty_end, uint64_t clean_loc)
{
    auto dirty_it = dirty_end;
    while (dirty_it != dirty_start)
    {
        dirty_it--;
        if (IS_BIG_WRITE(dirty_it->second.state) && dirty_it->second.location != clean_loc)
        {
#ifdef BLOCKSTORE_DEBUG
            printf("Free block %lu\n", dirty_it->second.location >> block_order);
#endif
            data_alloc->set(dirty_it->second.location >> block_order, false);
        }
#ifdef BLOCKSTORE_DEBUG
        printf("remove usage of journal offset %lu by %lu:%lu v%lu\n", dirty_it->second.journal_sector,
            dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version);
#endif
        int used = --journal.used_sectors[dirty_it->second.journal_sector];
        if (used == 0)
        {
            journal.used_sectors.erase(dirty_it->second.journal_sector);
        }
    }
    dirty_db.erase(dirty_start, dirty_end);
}
