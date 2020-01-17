#include "blockstore_impl.h"

#define SYNC_HAS_SMALL 1
#define SYNC_HAS_BIG 2
#define SYNC_DATA_SYNC_SENT 3
#define SYNC_DATA_SYNC_DONE 4
#define SYNC_JOURNAL_SYNC_SENT 5
#define SYNC_DONE 6

int blockstore_impl_t::dequeue_sync(blockstore_op_t *op)
{
    if (PRIV(op)->sync_state == 0)
    {
        stop_sync_submitted = false;
        PRIV(op)->sync_big_writes.swap(unsynced_big_writes);
        PRIV(op)->sync_small_writes.swap(unsynced_small_writes);
        if (PRIV(op)->sync_big_writes.size() > 0)
            PRIV(op)->sync_state = SYNC_HAS_BIG;
        else if (PRIV(op)->sync_small_writes.size() > 0)
            PRIV(op)->sync_state = SYNC_HAS_SMALL;
        else
            PRIV(op)->sync_state = SYNC_DONE;
        unsynced_big_writes.clear();
        unsynced_small_writes.clear();
    }
    int r = continue_sync(op);
    if (r)
    {
        PRIV(op)->prev_sync_count = in_progress_syncs.size();
        PRIV(op)->in_progress_ptr = in_progress_syncs.insert(in_progress_syncs.end(), op);
        ack_sync(op);
    }
    return r;
}

int blockstore_impl_t::continue_sync(blockstore_op_t *op)
{
    auto cb = [this, op](ring_data_t *data) { handle_sync_event(data, op); };
    if (PRIV(op)->sync_state == SYNC_HAS_SMALL)
    {
        // No big writes, just fsync the journal
        int n_sqes = disable_journal_fsync ? 0 : 1;
        if (journal.sector_info[journal.cur_sector].dirty)
        {
            n_sqes++;
        }
        if (n_sqes > 0)
        {
            io_uring_sqe* sqes[n_sqes];
            for (int i = 0; i < n_sqes; i++)
            {
                BS_SUBMIT_GET_SQE_DECL(sqes[i]);
            }
            int s = 0;
            if (journal.sector_info[journal.cur_sector].dirty)
            {
                prepare_journal_sector_write(journal, journal.cur_sector, sqes[s++], cb);
                PRIV(op)->min_used_journal_sector = PRIV(op)->max_used_journal_sector = 1 + journal.cur_sector;
            }
            else
            {
                PRIV(op)->min_used_journal_sector = PRIV(op)->max_used_journal_sector = 0;
            }
            if (!disable_journal_fsync)
            {
                ring_data_t *data = ((ring_data_t*)sqes[s]->user_data);
                my_uring_prep_fsync(sqes[s++], journal.fd, IORING_FSYNC_DATASYNC);
                data->iov = { 0 };
                data->callback = cb;
            }
            PRIV(op)->pending_ops = s;
            PRIV(op)->sync_state = SYNC_JOURNAL_SYNC_SENT;
        }
        else
        {
            PRIV(op)->sync_state = SYNC_DONE;
        }
    }
    else if (PRIV(op)->sync_state == SYNC_HAS_BIG)
    {
        // 1st step: fsync data
        if (!disable_data_fsync)
        {
            BS_SUBMIT_GET_SQE(sqe, data);
            my_uring_prep_fsync(sqe, data_fd, IORING_FSYNC_DATASYNC);
            data->iov = { 0 };
            data->callback = cb;
            PRIV(op)->min_used_journal_sector = PRIV(op)->max_used_journal_sector = 0;
            PRIV(op)->pending_ops = 1;
            PRIV(op)->sync_state = SYNC_DATA_SYNC_SENT;
        }
        else
        {
            PRIV(op)->sync_state = SYNC_DATA_SYNC_DONE;
        }
    }
    if (PRIV(op)->sync_state == SYNC_DATA_SYNC_DONE)
    {
        // 2nd step: Data device is synced, prepare & write journal entries
        // Check space in the journal and journal memory buffers
        blockstore_journal_check_t space_check(this);
        if (!space_check.check_available(op, PRIV(op)->sync_big_writes.size(), sizeof(journal_entry_big_write), 0))
        {
            return 0;
        }
        // Get SQEs. Don't bother about merging, submit each journal sector as a separate request
        struct io_uring_sqe *sqe[space_check.sectors_required + (disable_journal_fsync ? 0 : 1)];
        for (int i = 0; i < space_check.sectors_required + (disable_journal_fsync ? 0 : 1); i++)
        {
            BS_SUBMIT_GET_SQE_DECL(sqe[i]);
        }
        // Prepare and submit journal entries
        auto it = PRIV(op)->sync_big_writes.begin();
        int s = 0, cur_sector = -1;
        if ((journal_block_size - journal.in_sector_pos) < sizeof(journal_entry_big_write) &&
            journal.sector_info[journal.cur_sector].dirty)
        {
            if (cur_sector == -1)
                PRIV(op)->min_used_journal_sector = 1 + journal.cur_sector;
            cur_sector = journal.cur_sector;
            prepare_journal_sector_write(journal, cur_sector, sqe[s++], cb);
        }
        while (it != PRIV(op)->sync_big_writes.end())
        {
            journal_entry_big_write *je = (journal_entry_big_write*)
                prefill_single_journal_entry(journal, JE_BIG_WRITE, sizeof(journal_entry_big_write));
            dirty_db[*it].journal_sector = journal.sector_info[journal.cur_sector].offset;
            journal.sector_info[journal.cur_sector].dirty = false;
            journal.used_sectors[journal.sector_info[journal.cur_sector].offset]++;
#ifdef BLOCKSTORE_DEBUG
            printf("journal offset %lu is used by %lu:%lu v%lu\n", dirty_db[*it].journal_sector, it->oid.inode, it->oid.stripe, it->version);
#endif
            je->oid = it->oid;
            je->version = it->version;
            je->offset = dirty_db[*it].offset;
            je->len = dirty_db[*it].len;
            je->location = dirty_db[*it].location;
            je->crc32 = je_crc32((journal_entry*)je);
            journal.crc32_last = je->crc32;
            it++;
            if (cur_sector != journal.cur_sector)
            {
                if (cur_sector == -1)
                    PRIV(op)->min_used_journal_sector = 1 + journal.cur_sector;
                cur_sector = journal.cur_sector;
                prepare_journal_sector_write(journal, cur_sector, sqe[s++], cb);
            }
        }
        PRIV(op)->max_used_journal_sector = 1 + journal.cur_sector;
        // ... And a journal fsync
        if (!disable_journal_fsync)
        {
            my_uring_prep_fsync(sqe[s], journal.fd, IORING_FSYNC_DATASYNC);
            struct ring_data_t *data = ((ring_data_t*)sqe[s]->user_data);
            data->iov = { 0 };
            data->callback = cb;
            PRIV(op)->pending_ops = 1 + s;
        }
        else
        {
            PRIV(op)->pending_ops = s;
        }
        PRIV(op)->sync_state = SYNC_JOURNAL_SYNC_SENT;
        ringloop->submit();
    }
    return 1;
}

void blockstore_impl_t::handle_sync_event(ring_data_t *data, blockstore_op_t *op)
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
        // Release used journal sectors
        release_journal_sectors(op);
        // Handle states
        if (PRIV(op)->sync_state == SYNC_DATA_SYNC_SENT)
        {
            PRIV(op)->sync_state = SYNC_DATA_SYNC_DONE;
        }
        else if (PRIV(op)->sync_state == SYNC_JOURNAL_SYNC_SENT)
        {
            PRIV(op)->sync_state = SYNC_DONE;
            ack_sync(op);
        }
        else
        {
            throw std::runtime_error("BUG: unexpected sync op state");
        }
    }
}

int blockstore_impl_t::ack_sync(blockstore_op_t *op)
{
    if (PRIV(op)->sync_state == SYNC_DONE && PRIV(op)->prev_sync_count == 0)
    {
        // Remove dependency of subsequent syncs
        auto it = PRIV(op)->in_progress_ptr;
        int done_syncs = 1;
        ++it;
        // Acknowledge sync
        ack_one_sync(op);
        while (it != in_progress_syncs.end())
        {
            auto & next_sync = *it++;
            PRIV(next_sync)->prev_sync_count -= done_syncs;
            if (PRIV(next_sync)->prev_sync_count == 0 && PRIV(next_sync)->sync_state == SYNC_DONE)
            {
                done_syncs++;
                // Acknowledge next_sync
                ack_one_sync(next_sync);
            }
        }
        return 1;
    }
    return 0;
}

void blockstore_impl_t::ack_one_sync(blockstore_op_t *op)
{
    // Handle states
    for (auto it = PRIV(op)->sync_big_writes.begin(); it != PRIV(op)->sync_big_writes.end(); it++)
    {
#ifdef BLOCKSTORE_DEBUG
        printf("Ack sync big %lu:%lu v%lu\n", it->oid.inode, it->oid.stripe, it->version);
#endif
        auto & unstab = unstable_writes[it->oid];
        unstab = unstab < it->version ? it->version : unstab;
        dirty_db[*it].state = ST_D_META_SYNCED;
    }
    for (auto it = PRIV(op)->sync_small_writes.begin(); it != PRIV(op)->sync_small_writes.end(); it++)
    {
#ifdef BLOCKSTORE_DEBUG
        printf("Ack sync small %lu:%lu v%lu\n", it->oid.inode, it->oid.stripe, it->version);
#endif
        auto & unstab = unstable_writes[it->oid];
        unstab = unstab < it->version ? it->version : unstab;
        dirty_db[*it].state = dirty_db[*it].state == ST_DEL_WRITTEN ? ST_DEL_SYNCED : ST_J_SYNCED;
    }
    in_progress_syncs.erase(PRIV(op)->in_progress_ptr);
    op->retval = 0;
    FINISH_OP(op);
}
