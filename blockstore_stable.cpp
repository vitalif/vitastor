#include "blockstore_impl.h"

// Stabilize small write:
// 1) Copy data from the journal to the data device
// 2) Increase version on the metadata device and sync it
// 3) Advance clean_db entry's version, clear previous journal entries
//
// This makes 1 4K small write+sync look like:
// 512b+4K (journal) + sync + 512b (journal) + sync + 4K (data) [+ sync?] + 512b (metadata) + sync.
// WA = 2.375. It's not the best, SSD FTL-like redirect-write could probably be lower
// even with defragmentation. But it's fixed and it's still better than in Ceph. :)
// except for HDD-only clusters, because each write results in 3 seeks.

// Stabilize big write:
// 1) Copy metadata from the journal to the metadata device
// 2) Move dirty_db entry to clean_db and clear previous journal entries
//
// This makes 1 128K big write+sync look like:
// 128K (data) + sync + 512b (journal) + sync + 512b (journal) + sync + 512b (metadata) + sync.
// WA = 1.012. Very good :)

// Stabilize delete:
// 1) Remove metadata entry and sync it
// 2) Remove dirty_db entry and clear previous journal entries
// We have 2 problems here:
// - In the cluster environment, we must store the "tombstones" of deleted objects until
//   all replicas (not just quorum) agrees about their deletion. That is, "stabilize" is
//   not possible for deletes in degraded placement groups
// - With simple "fixed" metadata tables we can't just clear the metadata entry of the latest
//   object version. We must clear all previous entries, too.
// FIXME Fix both problems - probably, by switching from "fixed" metadata tables to "dynamic"

// AND We must do it in batches, for the sake of reduced fsync call count
// AND We must know what we stabilize. Basic workflow is like:
// 1) primary OSD receives sync request
// 2) it submits syncs to blockstore and peers
// 3) after everyone acks sync it acks sync to the client
// 4) after a while it takes his synced object list and sends stabilize requests
//    to peers and to its own blockstore, thus freeing the old version

int blockstore_impl_t::dequeue_stable(blockstore_op_t *op)
{
    obj_ver_id* v;
    int i, todo = 0;
    for (i = 0, v = (obj_ver_id*)op->buf; i < op->len; i++, v++)
    {
        auto dirty_it = dirty_db.find(*v);
        if (dirty_it == dirty_db.end())
        {
            auto clean_it = clean_db.find(v->oid);
            if (clean_it == clean_db.end() || clean_it->second.version < v->version)
            {
                // No such object version
                op->retval = -EINVAL;
                FINISH_OP(op);
                return 1;
            }
            else
            {
                // Already stable
            }
        }
        else if (IS_UNSYNCED(dirty_it->second.state))
        {
            // Object not synced yet. Caller must sync it first
            op->retval = EAGAIN;
            FINISH_OP(op);
            return 1;
        }
        else if (!IS_STABLE(dirty_it->second.state))
        {
            todo++;
        }
    }
    if (!todo)
    {
        // Already stable
        op->retval = 0;
        FINISH_OP(op);
        return 1;
    }
    // Check journal space
    blockstore_journal_check_t space_check(this);
    if (!space_check.check_available(op, todo, sizeof(journal_entry_stable), 0))
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
    auto cb = [this, op](ring_data_t *data) { handle_stable_event(data, op); };
    int s = 0, cur_sector = -1;
    if ((journal_block_size - journal.in_sector_pos) < sizeof(journal_entry_stable) &&
        journal.sector_info[journal.cur_sector].dirty)
    {
        if (cur_sector == -1)
            PRIV(op)->min_flushed_journal_sector = 1 + journal.cur_sector;
        cur_sector = journal.cur_sector;
        prepare_journal_sector_write(journal, cur_sector, sqe[s++], cb);
    }
    for (i = 0, v = (obj_ver_id*)op->buf; i < op->len; i++, v++)
    {
        auto unstab_it = unstable_writes.find(v->oid);
        if (unstab_it != unstable_writes.end() &&
            unstab_it->second <= v->version)
        {
            unstable_writes.erase(unstab_it);
        }
        journal_entry_stable *je = (journal_entry_stable*)
            prefill_single_journal_entry(journal, JE_STABLE, sizeof(journal_entry_stable));
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
    return 1;
}

void blockstore_impl_t::handle_stable_event(ring_data_t *data, blockstore_op_t *op)
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
        // FIXME Oops. We must sync the device!
        // Release used journal sectors
        release_journal_sectors(op);
        // Mark dirty_db entries as stable, acknowledge op completion
        obj_ver_id* v;
        int i;
        for (i = 0, v = (obj_ver_id*)op->buf; i < op->len; i++, v++)
        {
            // Mark all dirty_db entries up to op->version as stable
            auto dirty_it = dirty_db.find(*v);
            if (dirty_it != dirty_db.end())
            {
                while (1)
                {
                    if (dirty_it->second.state == ST_J_SYNCED)
                    {
                        dirty_it->second.state = ST_J_STABLE;
                    }
                    else if (dirty_it->second.state == ST_D_META_SYNCED)
                    {
                        dirty_it->second.state = ST_D_STABLE;
                    }
                    else if (dirty_it->second.state == ST_DEL_SYNCED)
                    {
                        dirty_it->second.state = ST_DEL_STABLE;
                    }
                    else if (IS_STABLE(dirty_it->second.state))
                    {
                        break;
                    }
                    if (dirty_it == dirty_db.begin())
                    {
                        break;
                    }
                    dirty_it--;
                    if (dirty_it->first.oid != v->oid)
                    {
                        break;
                    }
                }
#ifdef BLOCKSTORE_DEBUG
                printf("enqueue_flush %lu:%lu v%lu\n", v->oid.inode, v->oid.stripe, v->version);
#endif
                flusher->enqueue_flush(*v);
            }
        }
        // Acknowledge op
        op->retval = 0;
        FINISH_OP(op);
    }
}
