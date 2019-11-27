#include "blockstore.h"

#define SYNC_HAS_SMALL 1
#define SYNC_HAS_BIG 2
#define SYNC_DATA_SYNC_SENT 3
#define SYNC_DATA_SYNC_DONE 4
#define SYNC_JOURNAL_SYNC_SENT 5
#define SYNC_DONE 6

int blockstore::dequeue_sync(blockstore_operation *op)
{
    if (op->sync_state == 0)
    {
        stop_sync_submitted = false;
        op->sync_big_writes.swap(unsynced_big_writes);
        op->sync_small_writes.swap(unsynced_small_writes);
        if (op->sync_big_writes.size() > 0)
            op->sync_state = SYNC_HAS_BIG;
        else if (op->sync_small_writes.size() > 0)
            op->sync_state = SYNC_HAS_SMALL;
        else
            op->sync_state = SYNC_DONE;
        unsynced_big_writes.clear();
        unsynced_small_writes.clear();
    }
    int r = continue_sync(op);
    if (r)
    {
        op->prev_sync_count = in_progress_syncs.size();
        op->in_progress_ptr = in_progress_syncs.insert(in_progress_syncs.end(), op);
        ack_sync(op);
    }
    return r;
}

int blockstore::continue_sync(blockstore_operation *op)
{
    auto cb = [this, op](ring_data_t *data) { handle_sync_event(data, op); };
    if (op->sync_state == SYNC_HAS_SMALL)
    {
        // No big writes, just fsync the journal
        // FIXME: Add no-fsync mode
        BS_SUBMIT_GET_SQE(sqe, data);
        my_uring_prep_fsync(sqe, journal.fd, IORING_FSYNC_DATASYNC);
        data->iov = { 0 };
        data->callback = cb;
        op->min_used_journal_sector = op->max_used_journal_sector = 0;
        op->pending_ops = 1;
        op->sync_state = SYNC_JOURNAL_SYNC_SENT;
    }
    else if (op->sync_state == SYNC_HAS_BIG)
    {
        // 1st step: fsync data
        // FIXME: Add no-fsync mode
        BS_SUBMIT_GET_SQE(sqe, data);
        my_uring_prep_fsync(sqe, data_fd, IORING_FSYNC_DATASYNC);
        data->iov = { 0 };
        data->callback = cb;
        op->min_used_journal_sector = op->max_used_journal_sector = 0;
        op->pending_ops = 1;
        op->sync_state = SYNC_DATA_SYNC_SENT;
    }
    else if (op->sync_state == SYNC_DATA_SYNC_DONE)
    {
        // 2nd step: Data device is synced, prepare & write journal entries
        // Check space in the journal and journal memory buffers
        blockstore_journal_check_t space_check(this);
        if (!space_check.check_available(op, op->sync_big_writes.size(), sizeof(journal_entry_big_write), 0))
        {
            return 0;
        }
        // Get SQEs. Don't bother about merging, submit each journal sector as a separate request
        struct io_uring_sqe *sqe[space_check.sectors_required+1];
        for (int i = 0; i < space_check.sectors_required+1; i++)
        {
            BS_SUBMIT_GET_SQE_DECL(sqe[i]);
        }
        // Prepare and submit journal entries
        auto it = op->sync_big_writes.begin();
        int s = 0, cur_sector = -1;
        while (it != op->sync_big_writes.end())
        {
            journal_entry_big_write *je = (journal_entry_big_write*)
                prefill_single_journal_entry(journal, JE_BIG_WRITE, sizeof(journal_entry_big_write));
            dirty_db[*it].journal_sector = journal.sector_info[journal.cur_sector].offset;
            journal.used_sectors[journal.sector_info[journal.cur_sector].offset]++;
            je->oid = it->oid;
            je->version = it->version;
            je->location = dirty_db[*it].location;
            je->crc32 = je_crc32((journal_entry*)je);
            journal.crc32_last = je->crc32;
            it++;
            if (cur_sector != journal.cur_sector)
            {
                if (cur_sector == -1)
                    op->min_used_journal_sector = 1 + journal.cur_sector;
                cur_sector = journal.cur_sector;
                prepare_journal_sector_write(journal, sqe[s++], cb);
            }
        }
        op->max_used_journal_sector = 1 + journal.cur_sector;
        // ... And a journal fsync
        my_uring_prep_fsync(sqe[s], journal.fd, IORING_FSYNC_DATASYNC);
        struct ring_data_t *data = ((ring_data_t*)sqe[s]->user_data);
        data->iov = { 0 };
        data->callback = cb;
        op->pending_ops = 1 + s;
        op->sync_state = SYNC_JOURNAL_SYNC_SENT;
        ringloop->submit();
    }
    return 1;
}

void blockstore::handle_sync_event(ring_data_t *data, blockstore_operation *op)
{
    if (data->res != data->iov.iov_len)
    {
        throw std::runtime_error(
            "write operation failed ("+std::to_string(data->res)+" != "+std::to_string(data->iov.iov_len)+
            "). in-memory state is corrupted. AAAAAAAaaaaaaaaa!!!111"
        );
    }
    op->pending_ops--;
    if (op->pending_ops == 0)
    {
        // Release used journal sectors
        if (op->min_used_journal_sector > 0)
        {
            uint64_t s = op->min_used_journal_sector;
            while (1)
            {
                journal.sector_info[s-1].usage_count--;
                if (s == op->max_used_journal_sector)
                    break;
                s = 1 + s % journal.sector_count;
            }
            op->min_used_journal_sector = op->max_used_journal_sector = 0;
        }
        // Handle states
        if (op->sync_state == SYNC_DATA_SYNC_SENT)
        {
            op->sync_state = SYNC_DATA_SYNC_DONE;
            // FIXME: This is not needed, in fact
            for (auto it = op->sync_big_writes.begin(); it != op->sync_big_writes.end(); it++)
            {
                dirty_db[*it].state = ST_D_SYNCED;
            }
        }
        else if (op->sync_state == SYNC_JOURNAL_SYNC_SENT)
        {
            op->sync_state = SYNC_DONE;
            ack_sync(op);
        }
        else
        {
            throw std::runtime_error("BUG: unexpected sync op state");
        }
    }
}

int blockstore::ack_sync(blockstore_operation *op)
{
    if (op->sync_state == SYNC_DONE && op->prev_sync_count == 0)
    {
        // Remove dependency of subsequent syncs
        auto it = op->in_progress_ptr;
        int done_syncs = 1;
        ++it;
        // Acknowledge sync
        ack_one_sync(op);
        while (it != in_progress_syncs.end())
        {
            auto & next_sync = *it++;
            next_sync->prev_sync_count -= done_syncs;
            if (next_sync->prev_sync_count == 0 && next_sync->sync_state == SYNC_DONE)
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

void blockstore::ack_one_sync(blockstore_operation *op)
{
    // Handle states
    for (auto it = op->sync_big_writes.begin(); it != op->sync_big_writes.end(); it++)
    {
        auto & unstab = unstable_writes[it->oid];
        unstab = unstab < it->version ? it->version : unstab;
        dirty_db[*it].state = ST_D_META_SYNCED;
    }
    for (auto it = op->sync_small_writes.begin(); it != op->sync_small_writes.end(); it++)
    {
        auto & unstab = unstable_writes[it->oid];
        unstab = unstab < it->version ? it->version : unstab;
        dirty_db[*it].state = ST_J_SYNCED;
    }
    in_progress_syncs.erase(op->in_progress_ptr);
    op->retval = 0;
    op->callback(op);
}
