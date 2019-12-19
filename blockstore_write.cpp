#include "blockstore_impl.h"

void blockstore_impl_t::enqueue_write(blockstore_op_t *op)
{
    // Check or assign version number
    bool found = false, deleted = false, is_del = (op->opcode & BS_OP_TYPE_MASK) == BS_OP_DELETE;
    if (dirty_db.size() > 0)
    {
        auto dirty_it = dirty_db.upper_bound((obj_ver_id){
            .oid = op->oid,
            .version = UINT64_MAX,
        });
        dirty_it--; // segfaults when dirty_db is empty
        if (dirty_it != dirty_db.end() && dirty_it->first.oid == op->oid)
        {
            found = true;
            op->version = dirty_it->first.version + 1;
            deleted = IS_DELETE(dirty_it->second.state);
        }
    }
    if (!found)
    {
        auto clean_it = clean_db.find(op->oid);
        if (clean_it != clean_db.end())
        {
            op->version = clean_it->second.version + 1;
        }
        else
        {
            deleted = true;
            op->version = 1;
        }
    }
    if (deleted && is_del)
    {
        // Already deleted
        op->retval = 0;
        FINISH_OP(op);
        return;
    }
    // Immediately add the operation into dirty_db, so subsequent reads could see it
#ifdef BLOCKSTORE_DEBUG
    printf("%s %lu:%lu v%lu\n", is_del ? "Delete" : "Write", op->oid.inode, op->oid.stripe, op->version);
#endif
    dirty_db.emplace((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    }, (dirty_entry){
        .state = (uint32_t)(
            is_del
                ? ST_DEL_IN_FLIGHT
                : (op->len == block_size || deleted ? ST_D_IN_FLIGHT : ST_J_IN_FLIGHT)
        ),
        .flags = 0,
        .location = 0,
        .offset = is_del ? 0 : op->offset,
        .len = is_del ? 0 : op->len,
        .journal_sector = 0,
    });
}

// First step of the write algorithm: dequeue operation and submit initial write(s)
int blockstore_impl_t::dequeue_write(blockstore_op_t *op)
{
    auto dirty_it = dirty_db.find((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    });
    if (dirty_it->second.state == ST_D_IN_FLIGHT)
    {
        // Big (redirect) write
        uint64_t loc = data_alloc->find_free();
        if (loc == UINT64_MAX)
        {
            // no space
            if (flusher->is_active())
            {
                // hope that some space will be available after flush
                PRIV(op)->wait_for = WAIT_FREE;
                return 0;
            }
            op->retval = -ENOSPC;
            FINISH_OP(op);
            return 1;
        }
        BS_SUBMIT_GET_SQE(sqe, data);
        dirty_it->second.location = loc << block_order;
        dirty_it->second.state = ST_D_SUBMITTED;
#ifdef BLOCKSTORE_DEBUG
        printf("Allocate block %lu\n", loc);
#endif
        data_alloc->set(loc, true);
        int vcnt = 0;
        if (op->version == 1 && op->len != block_size)
        {
            // Zero fill newly allocated object. First write is always a big write
            // FIXME: Add "no-zero-fill" mode which will just leave random garbage (insecure, but may be useful)
            if (op->offset > 0)
                PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ zero_object, op->offset };
            PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ op->buf, op->len };
            if (op->offset+op->len < block_size)
                PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ zero_object, block_size - (op->offset + op->len) };
            data->iov.iov_len = block_size;
        }
        else
        {
            vcnt = 1;
            PRIV(op)->iov_zerofill[0] = (struct iovec){ op->buf, op->len };
            data->iov.iov_len = op->len; // to check it in the callback
        }
        data->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
        my_uring_prep_writev(
            sqe, data_fd, PRIV(op)->iov_zerofill, vcnt, data_offset + (loc << block_order)
        );
        PRIV(op)->pending_ops = 1;
        PRIV(op)->min_used_journal_sector = PRIV(op)->max_used_journal_sector = 0;
        // Remember big write as unsynced
        unsynced_big_writes.push_back((obj_ver_id){
            .oid = op->oid,
            .version = op->version,
        });
    }
    else
    {
        // Small (journaled) write
        // First check if the journal has sufficient space
        // FIXME Always two SQEs for now. Although it's possible to send 1 sometimes
        //two_sqes = (512 - journal.in_sector_pos < sizeof(struct journal_entry_small_write)
        //    ? (journal.len - next_pos < op->len)
        //    : (journal.sector_info[journal.cur_sector].offset + 512 != journal.next_free ||
        //    journal.len - next_pos < op->len);
        blockstore_journal_check_t space_check(this);
        if (!space_check.check_available(op, 1, sizeof(journal_entry_small_write), op->len))
        {
            return 0;
        }
        // There is sufficient space. Get SQE(s)
        BS_SUBMIT_GET_ONLY_SQE(sqe1);
        BS_SUBMIT_GET_SQE(sqe2, data2);
        // Got SQEs. Prepare journal sector write
        journal_entry_small_write *je = (journal_entry_small_write*)
            prefill_single_journal_entry(journal, JE_SMALL_WRITE, sizeof(struct journal_entry_small_write));
        dirty_it->second.journal_sector = journal.sector_info[journal.cur_sector].offset;
        journal.used_sectors[journal.sector_info[journal.cur_sector].offset]++;
#ifdef BLOCKSTORE_DEBUG
        printf("journal offset %lu is used by %lu:%lu v%lu\n", dirty_it->second.journal_sector, dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version);
#endif
        // Figure out where data will be
        journal.next_free = (journal.next_free + op->len) <= journal.len ? journal.next_free : 512;
        je->oid = op->oid;
        je->version = op->version;
        je->offset = op->offset;
        je->len = op->len;
        je->data_offset = journal.next_free;
        je->crc32_data = crc32c(0, op->buf, op->len);
        je->crc32 = je_crc32((journal_entry*)je);
        journal.crc32_last = je->crc32;
        auto cb = [this, op](ring_data_t *data) { handle_write_event(data, op); };
        prepare_journal_sector_write(journal, sqe1, cb);
        PRIV(op)->min_used_journal_sector = PRIV(op)->max_used_journal_sector = 1 + journal.cur_sector;
        // Prepare journal data write
        if (journal.inmemory)
        {
            // Copy data
            memcpy(journal.buffer + journal.next_free, op->buf, op->len);
        }
        data2->iov = (struct iovec){ op->buf, op->len };
        data2->callback = cb;
        my_uring_prep_writev(
            sqe2, journal.fd, &data2->iov, 1, journal.offset + journal.next_free
        );
        dirty_it->second.location = journal.next_free;
        dirty_it->second.state = ST_J_SUBMITTED;
        journal.next_free += op->len;
        if (journal.next_free >= journal.len)
            journal.next_free = 512;
        PRIV(op)->pending_ops = 2;
        // Remember small write as unsynced
        unsynced_small_writes.push_back((obj_ver_id){
            .oid = op->oid,
            .version = op->version,
        });
    }
    return 1;
}

void blockstore_impl_t::handle_write_event(ring_data_t *data, blockstore_op_t *op)
{
    if (data->res != data->iov.iov_len)
    {
        // FIXME: our state becomes corrupted after a write error. maybe do something better than just die
        throw std::runtime_error(
            "write operation failed ("+std::to_string(data->res)+" != "+std::to_string(data->iov.iov_len)+
            "). in-memory state is corrupted. AAAAAAAaaaaaaaaa!!!111"
        );
    }
    PRIV(op)->pending_ops--;
    if (PRIV(op)->pending_ops == 0)
    {
        // Release used journal sectors
        if (PRIV(op)->min_used_journal_sector > 0)
        {
            uint64_t s = PRIV(op)->min_used_journal_sector;
            while (1)
            {
                journal.sector_info[s-1].usage_count--;
                if (s == PRIV(op)->max_used_journal_sector)
                    break;
                s = 1 + s % journal.sector_count;
            }
            PRIV(op)->min_used_journal_sector = PRIV(op)->max_used_journal_sector = 0;
        }
        // Switch object state
        auto & dirty_entry = dirty_db[(obj_ver_id){
            .oid = op->oid,
            .version = op->version,
        }];
#ifdef BLOCKSTORE_DEBUG
        printf("Ack write %lu:%lu v%lu = %d\n", op->oid.inode, op->oid.stripe, op->version, dirty_entry.state);
#endif
        if (dirty_entry.state == ST_J_SUBMITTED)
        {
            dirty_entry.state = ST_J_WRITTEN;
        }
        else if (dirty_entry.state == ST_D_SUBMITTED)
        {
            dirty_entry.state = ST_D_WRITTEN;
        }
        else if (dirty_entry.state == ST_DEL_SUBMITTED)
        {
            dirty_entry.state = ST_DEL_WRITTEN;
        }
        // Acknowledge write without sync
        op->retval = op->len;
        FINISH_OP(op);
    }
}

int blockstore_impl_t::dequeue_del(blockstore_op_t *op)
{
    auto dirty_it = dirty_db.find((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    });
    blockstore_journal_check_t space_check(this);
    if (!space_check.check_available(op, 1, sizeof(journal_entry_del), 0))
    {
        return 0;
    }
    BS_SUBMIT_GET_ONLY_SQE(sqe);
    // Prepare journal sector write
    journal_entry_del *je = (journal_entry_del*)
        prefill_single_journal_entry(journal, JE_DELETE, sizeof(struct journal_entry_del));
    dirty_it->second.journal_sector = journal.sector_info[journal.cur_sector].offset;
    journal.used_sectors[journal.sector_info[journal.cur_sector].offset]++;
#ifdef BLOCKSTORE_DEBUG
    printf("journal offset %lu is used by %lu:%lu v%lu\n", dirty_it->second.journal_sector, dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version);
#endif
    je->oid = op->oid;
    je->version = op->version;
    je->crc32 = je_crc32((journal_entry*)je);
    journal.crc32_last = je->crc32;
    auto cb = [this, op](ring_data_t *data) { handle_write_event(data, op); };
    prepare_journal_sector_write(journal, sqe, cb);
    PRIV(op)->min_used_journal_sector = PRIV(op)->max_used_journal_sector = 1 + journal.cur_sector;
    PRIV(op)->pending_ops = 1;
    dirty_it->second.state = ST_DEL_SUBMITTED;
    // Remember small write as unsynced
    unsynced_small_writes.push_back((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    });
    return 1;
}
