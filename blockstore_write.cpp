#include "blockstore.h"

// First step of the write algorithm: dequeue operation and submit initial write(s)
int blockstore::dequeue_write(blockstore_operation *op)
{
    auto dirty_it = dirty_db.find((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    });
    if (op->len == block_size)
    {
        // Big (redirect) write
        uint64_t loc = allocator_find_free(data_alloc);
        if (loc == (uint64_t)-1)
        {
            // no space
            op->retval = -ENOSPC;
            op->callback(op);
            return 1;
        }
        BS_SUBMIT_GET_SQE(sqe, data);
        dirty_it->second.location = loc << block_order;
        dirty_it->second.state = ST_D_SUBMITTED;
        allocator_set(data_alloc, loc, true);
        data->iov = (struct iovec){ op->buf, op->len };
        data->op = op;
        io_uring_prep_writev(
            sqe, data_fd, &data->iov, 1, data_offset + (loc << block_order)
        );
        op->pending_ops = 1;
        op->min_used_journal_sector = op->max_used_journal_sector = 0;
    }
    else
    {
        // Small (journaled) write
        // First check if the journal has sufficient space
        // FIXME Always two SQEs for now. Although it's possible to send 1 sometimes
        uint64_t next_pos = journal.next_free;
        if (512 - journal.in_sector_pos < sizeof(struct journal_entry_small_write))
        {
            //if (journal.len - next_pos < op->len)
            //    two_sqes = true;
            next_pos = (next_pos+512) < journal.len ? next_pos+512 : 512;
            // Also check if we have an unused memory buffer for the journal sector
            if (journal.sector_info[((journal.cur_sector + 1) % journal.sector_count)].usage_count > 0)
            {
                // No memory buffer available. Wait for it.
                op->wait_for = WAIT_JOURNAL_BUFFER;
                return 0;
            }
        }
        //else if (journal.sector_info[journal.cur_sector].offset + 512 != journal.next_free ||
        //    journal.len - next_pos < op->len)
        //    two_sqes = true;
        next_pos = (journal.len - next_pos < op->len ? 512 : next_pos) + op->len;
        if (next_pos >= journal.used_start)
        {
            // No space in the journal. Wait for it.
            op->wait_for = WAIT_JOURNAL;
            op->wait_detail = next_pos;
            return 0;
        }
        // There is sufficient space. Get SQE(s)
        unsigned prev_sqe_pos = ringloop->ring->sq.sqe_tail;
        BS_SUBMIT_GET_SQE(sqe1, data1);
        BS_SUBMIT_GET_SQE(sqe2, data2);
        // Got SQEs. Prepare journal sector write
        if (512 - journal.in_sector_pos < sizeof(struct journal_entry_small_write))
        {
            // Move to the next journal sector
            // Also select next sector buffer in memory
            journal.cur_sector = ((journal.cur_sector + 1) % journal.sector_count);
            journal.sector_info[journal.cur_sector].offset = journal.next_free;
            journal.in_sector_pos = 0;
            journal.next_free = (journal.next_free+512) < journal.len ? journal.next_free + 512 : 512;
            memset(journal.sector_buf + 512*journal.cur_sector, 0, 512);
        }
        journal_entry_small_write *je = (struct journal_entry_small_write*)(
            journal.sector_buf + 512*journal.cur_sector + journal.in_sector_pos
        );
        *je = {
            .crc32 = 0,
            .magic = JOURNAL_MAGIC,
            .type = JE_SMALL_WRITE,
            .size = sizeof(struct journal_entry_small_write),
            .crc32_prev = journal.crc32_last,
            .oid = op->oid,
            .version = op->version,
            .offset = op->offset,
            .len = op->len,
        };
        je->crc32 = je_crc32((journal_entry*)je);
        journal.crc32_last = je->crc32;
        data1->iov = (struct iovec){ journal.sector_buf + 512*journal.cur_sector, 512 };
        data1->op = op;
        io_uring_prep_writev(
            sqe1, journal.fd, &data1->iov, 1, journal.offset + journal.sector_info[journal.cur_sector].offset
        );
        // Prepare journal data write
        journal.next_free = (journal.next_free + op->len) < journal.len ? journal.next_free + op->len : 512;
        data2->iov = (struct iovec){ op->buf, op->len };
        data2->op = op;
        io_uring_prep_writev(
            sqe2, journal.fd, &data2->iov, 1, journal.offset + journal.next_free
        );
        dirty_it->second.location = journal.next_free;
        dirty_it->second.state = ST_J_SUBMITTED;
        // Move journal.next_free and save last write for current sector
        journal.next_free += op->len;
        journal.sector_info[journal.cur_sector].usage_count++;
        op->pending_ops = 2;
        op->min_used_journal_sector = op->max_used_journal_sector = 1 + journal.cur_sector;
    }
    return 1;
}

int blockstore::dequeue_sync(blockstore_operation *op)
{
    op->big_write_count = 0;
    op->big_write_state = 0x10000;
    op->sync_writes.swap(unsynced_writes);
    unsynced_writes.clear();
    auto it = op->sync_writes.begin();
    while (it != op->sync_writes.end())
    {
        uint32_t state = dirty_db[*it].state;
        if (IS_BIG_WRITE(state))
        {
            op->big_write_count++;
            op->big_write_state = op->big_write_state < state ? op->big_write_state : state;
        }
        it++;
    }
    if (op->big_write_count == 0 || op->big_write_state == ST_D_META_WRITTEN)
    {
        // Just fsync the journal
        BS_SUBMIT_GET_SQE(sqe, data);
        io_uring_prep_fsync(sqe, journal.fd, 0);
        data->op = op;
        op->pending_ops = 1;
    }
    else if (op->big_write_state == ST_D_WRITTEN)
    {
        // 1st step: fsync data
        BS_SUBMIT_GET_SQE(sqe, data);
        io_uring_prep_fsync(sqe, data_fd, 0);
        data->op = op;
        op->pending_ops = 1;
    }
    else if (op->big_write_state == ST_D_SYNCED)
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
        struct io_uring_sqe *sqe[sectors_required];
        for (int i = 0; i < sectors_required; i++)
        {
            BS_SUBMIT_GET_SQE_DECL(sqe[i]);
        }
        // Prepare and submit journal entries
        op->min_used_journal_sector = 1 + journal.cur_sector;
        sectors_required = 0;
        required = op->big_write_count;
        it = op->sync_writes.begin();
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
        op->pending_ops = sectors_required;
        op->max_used_journal_sector = 1 + journal.cur_sector;
    }
    return 1;
}
