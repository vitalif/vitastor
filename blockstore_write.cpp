#include "blockstore_impl.h"

bool blockstore_impl_t::enqueue_write(blockstore_op_t *op)
{
    // Check or assign version number
    bool found = false, deleted = false, is_del = (op->opcode == BS_OP_DELETE);
    bool is_inflight_big = false;
    uint64_t version = 1;
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
            version = dirty_it->first.version + 1;
            deleted = IS_DELETE(dirty_it->second.state);
            is_inflight_big = (dirty_it->second.state & BS_ST_TYPE_MASK) == BS_ST_BIG_WRITE
                ? !IS_SYNCED(dirty_it->second.state)
                : ((dirty_it->second.state & BS_ST_WORKFLOW_MASK) == BS_ST_WAIT_BIG);
        }
    }
    if (!found)
    {
        auto clean_it = clean_db.find(op->oid);
        if (clean_it != clean_db.end())
        {
            version = clean_it->second.version + 1;
        }
        else
        {
            deleted = true;
        }
    }
    if (op->version == 0)
    {
        op->version = version;
    }
    else if (op->version < version)
    {
        // Invalid version requested
        op->retval = -EEXIST;
        return false;
    }
    if (deleted && is_del)
    {
        // Already deleted
        op->retval = 0;
        return false;
    }
    if (is_inflight_big && !is_del && !deleted && op->len < block_size &&
        immediate_commit != IMMEDIATE_ALL)
    {
        // Issue an additional sync so that the previous big write can reach the journal
        blockstore_op_t *sync_op = new blockstore_op_t;
        sync_op->opcode = BS_OP_SYNC;
        sync_op->callback = [this, op](blockstore_op_t *sync_op)
        {
            delete sync_op;
        };
        enqueue_op(sync_op);
    }
#ifdef BLOCKSTORE_DEBUG
    if (is_del)
        printf("Delete %lx:%lx v%lu\n", op->oid.inode, op->oid.stripe, op->version);
    else
        printf("Write %lx:%lx v%lu offset=%u len=%u\n", op->oid.inode, op->oid.stripe, op->version, op->offset, op->len);
#endif
    // No strict need to add it into dirty_db here, it's just left
    // from the previous implementation where reads waited for writes
    dirty_db.emplace((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    }, (dirty_entry){
        .state = (uint32_t)(
            is_del
                ? (BS_ST_DELETE | BS_ST_IN_FLIGHT)
                : (op->opcode == BS_OP_WRITE_STABLE ? BS_ST_INSTANT : 0) | (op->len == block_size || deleted
                    ? (BS_ST_BIG_WRITE | BS_ST_IN_FLIGHT)
                    : (is_inflight_big ? (BS_ST_SMALL_WRITE | BS_ST_WAIT_BIG) : (BS_ST_SMALL_WRITE | BS_ST_IN_FLIGHT)))
        ),
        .flags = 0,
        .location = 0,
        .offset = is_del ? 0 : op->offset,
        .len = is_del ? 0 : op->len,
        .journal_sector = 0,
    });
    return true;
}

// First step of the write algorithm: dequeue operation and submit initial write(s)
int blockstore_impl_t::dequeue_write(blockstore_op_t *op)
{
    if (PRIV(op)->op_state)
    {
        return continue_write(op);
    }
    auto dirty_it = dirty_db.find((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    });
    assert(dirty_it != dirty_db.end());
    if ((dirty_it->second.state & BS_ST_WORKFLOW_MASK) == BS_ST_WAIT_BIG)
    {
        // Don't dequeue
        return 0;
    }
    else if ((dirty_it->second.state & BS_ST_TYPE_MASK) == BS_ST_BIG_WRITE)
    {
        blockstore_journal_check_t space_check(this);
        if (!space_check.check_available(op, unsynced_big_writes.size() + 1, sizeof(journal_entry_big_write), JOURNAL_STABILIZE_RESERVATION))
        {
            return 0;
        }
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
        dirty_it->second.state = (dirty_it->second.state & ~BS_ST_WORKFLOW_MASK) | BS_ST_SUBMITTED;
#ifdef BLOCKSTORE_DEBUG
        printf("Allocate block %lu\n", loc);
#endif
        data_alloc->set(loc, true);
        uint64_t stripe_offset = (op->offset % bitmap_granularity);
        uint64_t stripe_end = (op->offset + op->len) % bitmap_granularity;
        // Zero fill up to bitmap_granularity
        int vcnt = 0;
        if (stripe_offset)
        {
            PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ zero_object, stripe_offset };
        }
        PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ op->buf, op->len };
        if (stripe_end)
        {
            stripe_end = bitmap_granularity - stripe_end;
            PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ zero_object, stripe_end };
        }
        data->iov.iov_len = op->len + stripe_offset + stripe_end; // to check it in the callback
        data->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
        my_uring_prep_writev(
            sqe, data_fd, PRIV(op)->iov_zerofill, vcnt, data_offset + (loc << block_order) + op->offset - stripe_offset
        );
        PRIV(op)->pending_ops = 1;
        PRIV(op)->min_flushed_journal_sector = PRIV(op)->max_flushed_journal_sector = 0;
        if (immediate_commit != IMMEDIATE_ALL)
        {
            // Remember big write as unsynced
            unsynced_big_writes.push_back((obj_ver_id){
                .oid = op->oid,
                .version = op->version,
            });
            PRIV(op)->op_state = 3;
        }
        else
        {
            PRIV(op)->op_state = 1;
        }
    }
    else /* if ((dirty_it->second.state & BS_ST_TYPE_MASK) == BS_ST_SMALL_WRITE) */
    {
        // Small (journaled) write
        // First check if the journal has sufficient space
        blockstore_journal_check_t space_check(this);
        if (unsynced_big_writes.size() && !space_check.check_available(op, unsynced_big_writes.size(), sizeof(journal_entry_big_write), 0)
            || !space_check.check_available(op, 1, sizeof(journal_entry_small_write), op->len + JOURNAL_STABILIZE_RESERVATION))
        {
            return 0;
        }
        // There is sufficient space. Get SQE(s)
        struct io_uring_sqe *sqe1 = NULL;
        if (immediate_commit != IMMEDIATE_NONE ||
            (journal_block_size - journal.in_sector_pos) < sizeof(journal_entry_small_write) &&
            journal.sector_info[journal.cur_sector].dirty)
        {
            // Write current journal sector only if it's dirty and full, or in the immediate_commit mode
            BS_SUBMIT_GET_SQE_DECL(sqe1);
        }
        struct io_uring_sqe *sqe2 = NULL;
        if (op->len > 0)
        {
            BS_SUBMIT_GET_SQE_DECL(sqe2);
        }
        // Got SQEs. Prepare previous journal sector write if required
        auto cb = [this, op](ring_data_t *data) { handle_write_event(data, op); };
        if (immediate_commit == IMMEDIATE_NONE)
        {
            if (sqe1)
            {
                prepare_journal_sector_write(journal, journal.cur_sector, sqe1, cb);
                PRIV(op)->min_flushed_journal_sector = PRIV(op)->max_flushed_journal_sector = 1 + journal.cur_sector;
                PRIV(op)->pending_ops++;
            }
            else
            {
                PRIV(op)->min_flushed_journal_sector = PRIV(op)->max_flushed_journal_sector = 0;
            }
        }
        // Then pre-fill journal entry
        journal_entry_small_write *je = (journal_entry_small_write*)prefill_single_journal_entry(
            journal, op->opcode == BS_OP_WRITE_STABLE ? JE_SMALL_WRITE_INSTANT : JE_SMALL_WRITE,
            sizeof(journal_entry_small_write)
        );
        dirty_it->second.journal_sector = journal.sector_info[journal.cur_sector].offset;
        journal.used_sectors[journal.sector_info[journal.cur_sector].offset]++;
#ifdef BLOCKSTORE_DEBUG
        printf(
            "journal offset %08lx is used by %lx:%lx v%lu (%lu refs)\n",
            dirty_it->second.journal_sector, dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version,
            journal.used_sectors[journal.sector_info[journal.cur_sector].offset]
        );
#endif
        // Figure out where data will be
        journal.next_free = (journal.next_free + op->len) <= journal.len ? journal.next_free : journal_block_size;
        je->oid = op->oid;
        je->version = op->version;
        je->offset = op->offset;
        je->len = op->len;
        je->data_offset = journal.next_free;
        je->crc32_data = crc32c(0, op->buf, op->len);
        je->crc32 = je_crc32((journal_entry*)je);
        journal.crc32_last = je->crc32;
        if (immediate_commit != IMMEDIATE_NONE)
        {
            prepare_journal_sector_write(journal, journal.cur_sector, sqe1, cb);
            PRIV(op)->min_flushed_journal_sector = PRIV(op)->max_flushed_journal_sector = 1 + journal.cur_sector;
            PRIV(op)->pending_ops++;
        }
        if (op->len > 0)
        {
            // Prepare journal data write
            if (journal.inmemory)
            {
                // Copy data
                memcpy(journal.buffer + journal.next_free, op->buf, op->len);
            }
            ring_data_t *data2 = ((ring_data_t*)sqe2->user_data);
            data2->iov = (struct iovec){ op->buf, op->len };
            data2->callback = cb;
            my_uring_prep_writev(
                sqe2, journal.fd, &data2->iov, 1, journal.offset + journal.next_free
            );
            PRIV(op)->pending_ops++;
        }
        else
        {
            // Zero-length overwrite. Allowed to bump object version in EC placement groups without actually writing data
        }
        dirty_it->second.location = journal.next_free;
        dirty_it->second.state = (dirty_it->second.state & ~BS_ST_WORKFLOW_MASK) | BS_ST_SUBMITTED;
        journal.next_free += op->len;
        if (journal.next_free >= journal.len)
        {
            journal.next_free = journal_block_size;
        }
        if (immediate_commit == IMMEDIATE_NONE)
        {
            // Remember small write as unsynced
            unsynced_small_writes.push_back((obj_ver_id){
                .oid = op->oid,
                .version = op->version,
            });
        }
        if (!PRIV(op)->pending_ops)
        {
            PRIV(op)->op_state = 4;
            continue_write(op);
        }
        else
        {
            PRIV(op)->op_state = 3;
        }
    }
    inflight_writes++;
    return 1;
}

int blockstore_impl_t::continue_write(blockstore_op_t *op)
{
    io_uring_sqe *sqe = NULL;
    journal_entry_big_write *je;
    auto dirty_it = dirty_db.find((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    });
    assert(dirty_it != dirty_db.end());
    if (PRIV(op)->op_state == 2)
        goto resume_2;
    else if (PRIV(op)->op_state == 4)
        goto resume_4;
    else
        return 1;
resume_2:
    // Only for the immediate_commit mode: prepare and submit big_write journal entry
    sqe = get_sqe();
    if (!sqe)
    {
        return 0;
    }
    je = (journal_entry_big_write*)prefill_single_journal_entry(
        journal, op->opcode == BS_OP_WRITE_STABLE ? JE_BIG_WRITE_INSTANT : JE_BIG_WRITE,
        sizeof(journal_entry_big_write)
    );
    dirty_it->second.journal_sector = journal.sector_info[journal.cur_sector].offset;
    journal.sector_info[journal.cur_sector].dirty = false;
    journal.used_sectors[journal.sector_info[journal.cur_sector].offset]++;
#ifdef BLOCKSTORE_DEBUG
    printf(
        "journal offset %08lx is used by %lx:%lx v%lu (%lu refs)\n",
        journal.sector_info[journal.cur_sector].offset, op->oid.inode, op->oid.stripe, op->version,
        journal.used_sectors[journal.sector_info[journal.cur_sector].offset]
    );
#endif
    je->oid = op->oid;
    je->version = op->version;
    je->offset = op->offset;
    je->len = op->len;
    je->location = dirty_it->second.location;
    je->crc32 = je_crc32((journal_entry*)je);
    journal.crc32_last = je->crc32;
    prepare_journal_sector_write(journal, journal.cur_sector, sqe,
        [this, op](ring_data_t *data) { handle_write_event(data, op); });
    PRIV(op)->min_flushed_journal_sector = PRIV(op)->max_flushed_journal_sector = 1 + journal.cur_sector;
    PRIV(op)->pending_ops = 1;
    PRIV(op)->op_state = 3;
    return 1;
resume_4:
    // Switch object state
#ifdef BLOCKSTORE_DEBUG
    printf("Ack write %lx:%lx v%lu = %d\n", op->oid.inode, op->oid.stripe, op->version, dirty_it->second.state);
#endif
    bool imm = (dirty_it->second.state & BS_ST_TYPE_MASK) == BS_ST_BIG_WRITE
        ? (immediate_commit == IMMEDIATE_ALL)
        : (immediate_commit != IMMEDIATE_NONE);
    if (imm)
    {
        auto & unstab = unstable_writes[op->oid];
        unstab = unstab < op->version ? op->version : unstab;
    }
    dirty_it->second.state = (dirty_it->second.state & ~BS_ST_WORKFLOW_MASK)
        | (imm ? BS_ST_SYNCED : BS_ST_WRITTEN);
    if (imm && ((dirty_it->second.state & BS_ST_TYPE_MASK) == BS_ST_DELETE || (dirty_it->second.state & BS_ST_INSTANT)))
    {
        // Deletions are treated as immediately stable
        mark_stable(dirty_it->first);
    }
    if (immediate_commit == IMMEDIATE_ALL)
    {
        dirty_it++;
        while (dirty_it != dirty_db.end() && dirty_it->first.oid == op->oid)
        {
            if ((dirty_it->second.state & BS_ST_WORKFLOW_MASK) == BS_ST_WAIT_BIG)
            {
                dirty_it->second.state = (dirty_it->second.state & ~BS_ST_WORKFLOW_MASK) | BS_ST_IN_FLIGHT;
            }
            dirty_it++;
        }
    }
    inflight_writes--;
    // Acknowledge write
    op->retval = op->len;
    FINISH_OP(op);
    return 1;
}

void blockstore_impl_t::handle_write_event(ring_data_t *data, blockstore_op_t *op)
{
    live = true;
    if (data->res != data->iov.iov_len)
    {
        inflight_writes--;
        // FIXME: our state becomes corrupted after a write error. maybe do something better than just die
        throw std::runtime_error(
            "write operation failed ("+std::to_string(data->res)+" != "+std::to_string(data->iov.iov_len)+
            "). in-memory state is corrupted. AAAAAAAaaaaaaaaa!!!111"
        );
    }
    PRIV(op)->pending_ops--;
    if (PRIV(op)->pending_ops == 0)
    {
        release_journal_sectors(op);
        PRIV(op)->op_state++;
        if (!continue_write(op))
        {
            submit_queue.push_front(op);
        }
    }
}

void blockstore_impl_t::release_journal_sectors(blockstore_op_t *op)
{
    // Release flushed journal sectors
    if (PRIV(op)->min_flushed_journal_sector > 0 &&
        PRIV(op)->max_flushed_journal_sector > 0)
    {
        uint64_t s = PRIV(op)->min_flushed_journal_sector;
        while (1)
        {
            journal.sector_info[s-1].usage_count--;
            if (s != (1+journal.cur_sector) && journal.sector_info[s-1].usage_count == 0)
            {
                // We know for sure that we won't write into this sector anymore
                uint64_t new_ds = journal.sector_info[s-1].offset + journal.block_size;
                if (new_ds >= journal.len)
                {
                    new_ds = journal.block_size;
                }
                if ((journal.dirty_start + (journal.dirty_start >= journal.used_start ? 0 : journal.len)) <
                    (new_ds + (new_ds >= journal.used_start ? 0 : journal.len)))
                {
                    journal.dirty_start = new_ds;
                }
            }
            if (s == PRIV(op)->max_flushed_journal_sector)
                break;
            s = 1 + s % journal.sector_count;
        }
        PRIV(op)->min_flushed_journal_sector = PRIV(op)->max_flushed_journal_sector = 0;
    }
}

int blockstore_impl_t::dequeue_del(blockstore_op_t *op)
{
    auto dirty_it = dirty_db.find((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    });
    assert(dirty_it != dirty_db.end());
    blockstore_journal_check_t space_check(this);
    if (!space_check.check_available(op, 1, sizeof(journal_entry_del), 0))
    {
        return 0;
    }
    io_uring_sqe *sqe = NULL;
    if (immediate_commit != IMMEDIATE_NONE ||
        (journal_block_size - journal.in_sector_pos) < sizeof(journal_entry_del) &&
        journal.sector_info[journal.cur_sector].dirty)
    {
        // Write current journal sector only if it's dirty and full, or in the immediate_commit mode
        BS_SUBMIT_GET_SQE_DECL(sqe);
    }
    auto cb = [this, op](ring_data_t *data) { handle_write_event(data, op); };
    // Prepare journal sector write
    if (immediate_commit == IMMEDIATE_NONE)
    {
        if (sqe)
        {
            prepare_journal_sector_write(journal, journal.cur_sector, sqe, cb);
            PRIV(op)->min_flushed_journal_sector = PRIV(op)->max_flushed_journal_sector = 1 + journal.cur_sector;
            PRIV(op)->pending_ops++;
        }
        else
        {
            PRIV(op)->min_flushed_journal_sector = PRIV(op)->max_flushed_journal_sector = 0;
        }
    }
    // Pre-fill journal entry
    journal_entry_del *je = (journal_entry_del*)prefill_single_journal_entry(
        journal, JE_DELETE, sizeof(struct journal_entry_del)
    );
    dirty_it->second.journal_sector = journal.sector_info[journal.cur_sector].offset;
    journal.used_sectors[journal.sector_info[journal.cur_sector].offset]++;
#ifdef BLOCKSTORE_DEBUG
    printf(
        "journal offset %08lx is used by %lx:%lx v%lu (%lu refs)\n",
        dirty_it->second.journal_sector, dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version,
        journal.used_sectors[journal.sector_info[journal.cur_sector].offset]
    );
#endif
    je->oid = op->oid;
    je->version = op->version;
    je->crc32 = je_crc32((journal_entry*)je);
    journal.crc32_last = je->crc32;
    dirty_it->second.state = BS_ST_DELETE | BS_ST_SUBMITTED;
    if (immediate_commit != IMMEDIATE_NONE)
    {
        prepare_journal_sector_write(journal, journal.cur_sector, sqe, cb);
        PRIV(op)->min_flushed_journal_sector = PRIV(op)->max_flushed_journal_sector = 1 + journal.cur_sector;
        PRIV(op)->pending_ops++;
        // Remember small write as unsynced
        unsynced_small_writes.push_back((obj_ver_id){
            .oid = op->oid,
            .version = op->version,
        });
    }
    if (!PRIV(op)->pending_ops)
    {
        PRIV(op)->op_state = 4;
        continue_write(op);
    }
    else
    {
        PRIV(op)->op_state = 3;
    }
    return 1;
}
