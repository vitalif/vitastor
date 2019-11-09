#include "blockstore.h"

#define SYNC_NO_BIG 1
#define SYNC_HAS_BIG 2
#define SYNC_DATA_SYNC_SENT 3
#define SYNC_JOURNAL_SYNC_SENT 4
#define SYNC_DONE 5

int blockstore::dequeue_sync(blockstore_operation *op)
{
    if (op->sync_state == 0)
    {
        op->big_write_count = 0;
        op->sync_state = SYNC_NO_BIG;
        op->sync_writes.swap(unsynced_writes);
        unsynced_writes.clear();
        if (op->sync_writes.size() == 0)
        {
            op->sync_state = SYNC_DONE;
        }
        auto it = op->sync_writes.begin();
        while (it != op->sync_writes.end())
        {
            uint32_t state = dirty_db[*it].state;
            if (IS_BIG_WRITE(state))
            {
                op->big_write_count++;
                op->sync_state = SYNC_HAS_BIG;
            }
            it++;
        }
    }
    if (op->sync_state == SYNC_NO_BIG)
    {
        // No big writes, just fsync the journal
        BS_SUBMIT_GET_SQE(sqe, data);
        io_uring_prep_fsync(sqe, journal.fd, 0);
        data->op = op;
        op->pending_ops = 1;
        op->sync_state = SYNC_JOURNAL_SYNC_SENT;
    }
    else if (op->sync_state == SYNC_HAS_BIG)
    {
        // 1st step: fsync data
        BS_SUBMIT_GET_SQE(sqe, data);
        io_uring_prep_fsync(sqe, data_fd, 0);
        data->op = op;
        op->pending_ops = 1;
        op->sync_state = SYNC_DATA_SYNC_SENT;
    }
    else if (op->sync_state == SYNC_DATA_SYNC_SENT)
    {
        // 2nd step: Data device is synced, prepare & write journal entries
        // Check space in the journal and journal memory buffers
        int required = op->big_write_count, sectors_required = 1;
        uint64_t next_pos = journal.next_free, next_sector = journal.cur_sector;
        while (1)
        {
            int fits = (512 - journal.in_sector_pos) / sizeof(journal_entry_big_write);
            required -= fits;
            if (required <= 0)
                break;
            next_pos = (next_pos+512) < journal.len ? next_pos+512 : 512;
            sectors_required++;
            next_sector = ((next_sector + 1) % journal.sector_count);
            if (journal.sector_info[next_sector].usage_count > 0)
            {
                // No memory buffer available. Wait for it.
                op->wait_for = WAIT_JOURNAL_BUFFER;
                return 0;
            }
        }
        if (next_pos >= journal.used_start)
        {
            // No space in the journal. Wait for it.
            op->wait_for = WAIT_JOURNAL;
            op->wait_detail = next_pos;
            return 0;
        }
        // Get SQEs. Don't bother about merging, submit each journal sector as a separate request
        struct io_uring_sqe *sqe[sectors_required+1];
        for (int i = 0; i < sectors_required+1; i++)
        {
            BS_SUBMIT_GET_SQE_DECL(sqe[i]);
        }
        // Prepare and submit journal entries
        op->min_used_journal_sector = 1 + journal.cur_sector;
        sectors_required = 0;
        required = op->big_write_count;
        // FIXME: advance it
        auto it = op->sync_writes.begin();
        while (1)
        {
            int fits = (512 - journal.in_sector_pos) / sizeof(journal_entry_big_write);
            while (fits > 0 && required > 0)
            {
                journal_entry_big_write *je = (journal_entry_big_write*)(
                    journal.sector_buf + 512*journal.cur_sector + journal.in_sector_pos
                );
                *je = {
                    .crc32 = 0,
                    .magic = JOURNAL_MAGIC,
                    .type = JE_BIG_WRITE,
                    .size = sizeof(journal_entry_big_write),
                    .crc32_prev = journal.crc32_last,
                    .oid = it->oid,
                    .version = it->version,
                    .block = dirty_db[*it].location,
                };
                je->crc32 = je_crc32((journal_entry*)je);
                journal.crc32_last = je->crc32;
                journal.in_sector_pos += sizeof(journal_entry_big_write);
                required--;
            }
            if (required <= 0)
                break;
            journal.sector_info[journal.cur_sector].usage_count++;
            struct ring_data_t *data = ((ring_data_t*)sqe[sectors_required]->user_data);
            data->iov = (struct iovec){ journal.sector_buf + 512*journal.cur_sector, 512 };
            data->op = op;
            io_uring_prep_writev(
                sqe[sectors_required], journal.fd, &data->iov, 1, journal.offset + journal.sector_info[journal.cur_sector].offset
            );
            journal.cur_sector = ((journal.cur_sector + 1) % journal.sector_count);
            journal.sector_info[journal.cur_sector].offset = journal.next_free;
            journal.in_sector_pos = 0;
            journal.next_free = (journal.next_free + 512) < journal.len ? journal.next_free + 512 : 512;
            memset(journal.sector_buf + 512*journal.cur_sector, 0, 512);
            sectors_required++;
        }
        // ... And a journal fsync
        io_uring_prep_fsync(sqe[sectors_required], journal.fd, 0);
        struct ring_data_t *data = ((ring_data_t*)sqe[sectors_required]->user_data);
        data->op = op;
        op->pending_ops = 1 + sectors_required;
        op->max_used_journal_sector = 1 + journal.cur_sector;
        op->sync_state = SYNC_JOURNAL_SYNC_SENT;
    }
    // FIXME: resubmit op from in_progress
    op->prev_sync_count = in_progress_syncs.size();
    op->in_progress_ptr = in_progress_syncs.insert(in_progress_syncs.end(), op);
    return 1;
}

void blockstore::handle_sync_event(ring_data_t *data, blockstore_operation *op)
{
    if (data->res < 0)
    {
        // sync error
        // FIXME: our state becomes corrupted after a write error. maybe do something better than just die
        throw new std::runtime_error("write operation failed. in-memory state is corrupted. AAAAAAAaaaaaaaaa!!!111");
    }
    op->pending_ops--;
    if (op->min_used_journal_sector > 0)
    {
        for (uint64_t s = op->min_used_journal_sector; s != op->max_used_journal_sector; s = (s + 1) % journal.sector_count)
        {
            journal.sector_info[s-1].usage_count--;
        }
        op->min_used_journal_sector = op->max_used_journal_sector = 0;
    }
    if (op->pending_ops == 0)
    {
        // Acknowledge sync
        auto it = op->in_progress_ptr;
        int done_syncs = 1;
        ++it;
        while (it != in_progress_syncs.end())
        {
            auto & next_sync = *it;
            next_sync->prev_sync_count -= done_syncs;
            if (next_sync->prev_sync_count == 0/* && next_sync->DONE*/)
            {
                done_syncs++;
                auto next_it = it;
                it++;
                in_progress_syncs.erase(next_it);
                next_sync->retval = 0;
                next_sync->callback(next_sync);
                in_progress_ops.erase(next_sync);
            }
            else
                it++;
        }
        in_progress_syncs.erase(op->in_progress_ptr);
        op->retval = 0;
        op->callback(op);
        in_progress_ops.erase(op);
    }
}
