// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_impl.h"

bool blockstore_impl_t::enqueue_write(blockstore_op_t *op)
{
    // Check or assign version number
    bool found = false, deleted = false, is_del = (op->opcode == BS_OP_DELETE);
    bool wait_big = false, wait_del = false;
    void *bmp = NULL;
    uint64_t version = 1;
    if (!is_del && clean_entry_bitmap_size > sizeof(void*))
    {
        bmp = calloc_or_die(1, clean_entry_bitmap_size);
    }
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
            wait_del = ((dirty_it->second.state & BS_ST_WORKFLOW_MASK) == BS_ST_WAIT_DEL);
            wait_big = (dirty_it->second.state & BS_ST_TYPE_MASK) == BS_ST_BIG_WRITE
                ? !IS_SYNCED(dirty_it->second.state)
                : ((dirty_it->second.state & BS_ST_WORKFLOW_MASK) == BS_ST_WAIT_BIG);
            if (!is_del && !deleted)
            {
                if (clean_entry_bitmap_size > sizeof(void*))
                    memcpy(bmp, dirty_it->second.bitmap, clean_entry_bitmap_size);
                else
                    bmp = dirty_it->second.bitmap;
            }
        }
    }
    if (!found)
    {
        auto clean_it = clean_db.find(op->oid);
        if (clean_it != clean_db.end())
        {
            version = clean_it->second.version + 1;
            if (!is_del)
            {
                void *bmp_ptr = get_clean_entry_bitmap(clean_it->second.location, clean_entry_bitmap_size);
                memcpy((clean_entry_bitmap_size > sizeof(void*) ? bmp : &bmp), bmp_ptr, clean_entry_bitmap_size);
            }
        }
        else
        {
            deleted = true;
        }
    }
    if (deleted && is_del)
    {
        // Already deleted
        op->retval = 0;
        return false;
    }
    PRIV(op)->real_version = 0;
    if (op->version == 0)
    {
        op->version = version;
    }
    else if (op->version < version)
    {
        // Implicit operations must be added like that: DEL [FLUSH] BIG [SYNC] SMALL SMALL
        if (deleted || wait_del)
        {
            // It's allowed to write versions with low numbers over deletes
            // However, we have to flush those deletes first as we use version number for ordering
#ifdef BLOCKSTORE_DEBUG
            printf("Write %lx:%lx v%lu over delete (real v%lu) offset=%u len=%u\n", op->oid.inode, op->oid.stripe, version, op->version, op->offset, op->len);
#endif
            wait_del = true;
            PRIV(op)->real_version = op->version;
            op->version = version;
            flusher->unshift_flush((obj_ver_id){
                .oid = op->oid,
                .version = version-1,
            }, true);
        }
        else
        {
            // Invalid version requested
            op->retval = -EEXIST;
            if (!is_del && clean_entry_bitmap_size > sizeof(void*))
            {
                free(bmp);
            }
            return false;
        }
    }
    if (wait_big && !is_del && !deleted && op->len < block_size &&
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
    else if (!wait_del)
        printf("Write %lx:%lx v%lu offset=%u len=%u\n", op->oid.inode, op->oid.stripe, op->version, op->offset, op->len);
#endif
    // FIXME No strict need to add it into dirty_db here, it's just left
    // from the previous implementation where reads waited for writes
    uint32_t state;
    if (is_del)
        state = BS_ST_DELETE | BS_ST_IN_FLIGHT;
    else
    {
        state = (op->len == block_size || deleted ? BS_ST_BIG_WRITE : BS_ST_SMALL_WRITE);
        if (state == BS_ST_SMALL_WRITE && throttle_small_writes)
            clock_gettime(CLOCK_REALTIME, &PRIV(op)->tv_begin);
        if (wait_del)
            state |= BS_ST_WAIT_DEL;
        else if (state == BS_ST_SMALL_WRITE && wait_big)
            state |= BS_ST_WAIT_BIG;
        else
            state |= BS_ST_IN_FLIGHT;
        if (op->opcode == BS_OP_WRITE_STABLE)
            state |= BS_ST_INSTANT;
        if (op->bitmap)
        {
            // Only allow to overwrite part of the object bitmap respective to the write's offset/len
            uint8_t *bmp_ptr = (uint8_t*)(clean_entry_bitmap_size > sizeof(void*) ? bmp : &bmp);
            uint32_t bit = op->offset/bitmap_granularity;
            uint32_t bits_left = op->len/bitmap_granularity;
            while (!(bit % 8) && bits_left > 8)
            {
                // Copy bytes
                bmp_ptr[bit/8] = ((uint8_t*)op->bitmap)[bit/8];
                bit += 8;
                bits_left -= 8;
            }
            while (bits_left > 0)
            {
                // Copy bits
                bmp_ptr[bit/8] = (bmp_ptr[bit/8] & ~(1 << (bit%8)))
                    | (((uint8_t*)op->bitmap)[bit/8] & (1 << bit%8));
                bit++;
                bits_left--;
            }
        }
    }
    dirty_db.emplace((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    }, (dirty_entry){
        .state = state,
        .flags = 0,
        .location = 0,
        .offset = is_del ? 0 : op->offset,
        .len = is_del ? 0 : op->len,
        .journal_sector = 0,
        .bitmap = bmp,
    });
    return true;
}

void blockstore_impl_t::cancel_all_writes(blockstore_op_t *op, blockstore_dirty_db_t::iterator dirty_it, int retval)
{
    while (dirty_it != dirty_db.end() && dirty_it->first.oid == op->oid)
    {
        if (clean_entry_bitmap_size > sizeof(void*))
            free(dirty_it->second.bitmap);
        dirty_db.erase(dirty_it++);
    }
    bool found = false;
    for (auto other_op: submit_queue)
    {
        if (!found && other_op == op)
            found = true;
        else if (found && other_op->oid == op->oid &&
            (other_op->opcode == BS_OP_WRITE || other_op->opcode == BS_OP_WRITE_STABLE))
        {
            // Mark operations to cancel them
            PRIV(other_op)->real_version = UINT64_MAX;
            other_op->retval = retval;
        }
    }
    op->retval = retval;
    FINISH_OP(op);
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
    if ((dirty_it->second.state & BS_ST_WORKFLOW_MASK) < BS_ST_IN_FLIGHT)
    {
        // Don't dequeue
        return 0;
    }
    if (PRIV(op)->real_version != 0)
    {
        if (PRIV(op)->real_version == UINT64_MAX)
        {
            // This is the flag value used to cancel operations
            FINISH_OP(op);
            return 2;
        }
        // Restore original low version number for unblocked operations
#ifdef BLOCKSTORE_DEBUG
        printf("Restoring %lx:%lx version: v%lu -> v%lu\n", op->oid.inode, op->oid.stripe, op->version, PRIV(op)->real_version);
#endif
        auto prev_it = dirty_it;
        prev_it--;
        if (prev_it->first.oid == op->oid && prev_it->first.version >= PRIV(op)->real_version)
        {
            // Original version is still invalid
            // All subsequent writes to the same object must be canceled too
            cancel_all_writes(op, dirty_it, -EEXIST);
            return 2;
        }
        op->version = PRIV(op)->real_version;
        PRIV(op)->real_version = 0;
        dirty_entry e = dirty_it->second;
        dirty_db.erase(dirty_it);
        dirty_it = dirty_db.emplace((obj_ver_id){
            .oid = op->oid,
            .version = op->version,
        }, e).first;
    }
    if (write_iodepth >= max_write_iodepth)
    {
        return 0;
    }
    if ((dirty_it->second.state & BS_ST_TYPE_MASK) == BS_ST_BIG_WRITE)
    {
        blockstore_journal_check_t space_check(this);
        if (!space_check.check_available(op, unsynced_big_write_count + 1,
            sizeof(journal_entry_big_write) + clean_entry_bitmap_size, JOURNAL_STABILIZE_RESERVATION))
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
            cancel_all_writes(op, dirty_it, -ENOSPC);
            return 2;
        }
        write_iodepth++;
        BS_SUBMIT_GET_SQE(sqe, data);
        dirty_it->second.location = loc << block_order;
        dirty_it->second.state = (dirty_it->second.state & ~BS_ST_WORKFLOW_MASK) | BS_ST_SUBMITTED;
#ifdef BLOCKSTORE_DEBUG
        printf(
            "Allocate block %lu for %lx:%lx v%lu\n",
            loc, op->oid.inode, op->oid.stripe, op->version
        );
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
            // Increase the counter, but don't save into unsynced_writes yet (can't sync until the write is finished)
            unsynced_big_write_count++;
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
        if (unsynced_big_write_count &&
            !space_check.check_available(op, unsynced_big_write_count,
                sizeof(journal_entry_big_write) + clean_entry_bitmap_size, 0)
            || !space_check.check_available(op, 1,
                sizeof(journal_entry_small_write) + clean_entry_bitmap_size, op->len + JOURNAL_STABILIZE_RESERVATION))
        {
            return 0;
        }
        write_iodepth++;
        // There is sufficient space. Get SQE(s)
        struct io_uring_sqe *sqe1 = NULL;
        if (immediate_commit != IMMEDIATE_NONE ||
            !journal.entry_fits(sizeof(journal_entry_small_write) + clean_entry_bitmap_size))
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
            sizeof(journal_entry_small_write) + clean_entry_bitmap_size
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
        memcpy((void*)(je+1), (clean_entry_bitmap_size > sizeof(void*) ? dirty_it->second.bitmap : &dirty_it->second.bitmap), clean_entry_bitmap_size);
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
        if (!PRIV(op)->pending_ops)
        {
            PRIV(op)->op_state = 4;
            return continue_write(op);
        }
        else
        {
            PRIV(op)->op_state = 3;
        }
    }
    return 1;
}

int blockstore_impl_t::continue_write(blockstore_op_t *op)
{
    int op_state = PRIV(op)->op_state;
    if (op_state == 2)
        goto resume_2;
    else if (op_state == 4)
        goto resume_4;
    else if (op_state == 6)
        goto resume_6;
    else
    {
        // In progress
        return 1;
    }
resume_2:
    // Only for the immediate_commit mode: prepare and submit big_write journal entry
    {
        auto dirty_it = dirty_db.find((obj_ver_id){
            .oid = op->oid,
            .version = op->version,
        });
        assert(dirty_it != dirty_db.end());
        io_uring_sqe *sqe = NULL;
        BS_SUBMIT_GET_SQE_DECL(sqe);
        journal_entry_big_write *je = (journal_entry_big_write*)prefill_single_journal_entry(
            journal, op->opcode == BS_OP_WRITE_STABLE ? JE_BIG_WRITE_INSTANT : JE_BIG_WRITE,
            sizeof(journal_entry_big_write) + clean_entry_bitmap_size
        );
        dirty_it->second.journal_sector = journal.sector_info[journal.cur_sector].offset;
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
        memcpy((void*)(je+1), (clean_entry_bitmap_size > sizeof(void*) ? dirty_it->second.bitmap : &dirty_it->second.bitmap), clean_entry_bitmap_size);
        je->crc32 = je_crc32((journal_entry*)je);
        journal.crc32_last = je->crc32;
        prepare_journal_sector_write(journal, journal.cur_sector, sqe,
            [this, op](ring_data_t *data) { handle_write_event(data, op); });
        PRIV(op)->min_flushed_journal_sector = PRIV(op)->max_flushed_journal_sector = 1 + journal.cur_sector;
        PRIV(op)->pending_ops = 1;
        PRIV(op)->op_state = 3;
        return 1;
    }
resume_4:
    // Switch object state
#ifdef BLOCKSTORE_DEBUG
    printf("Ack write %lx:%lx v%lu = state 0x%x\n", op->oid.inode, op->oid.stripe, op->version, dirty_it->second.state);
#endif
    {
        auto dirty_it = dirty_db.find((obj_ver_id){
            .oid = op->oid,
            .version = op->version,
        });
        assert(dirty_it != dirty_db.end());
        bool is_big = (dirty_it->second.state & BS_ST_TYPE_MASK) == BS_ST_BIG_WRITE;
        bool imm = is_big ? (immediate_commit == IMMEDIATE_ALL) : (immediate_commit != IMMEDIATE_NONE);
        if (imm)
        {
            auto & unstab = unstable_writes[op->oid];
            unstab = unstab < op->version ? op->version : unstab;
        }
        dirty_it->second.state = (dirty_it->second.state & ~BS_ST_WORKFLOW_MASK)
            | (imm ? BS_ST_SYNCED : BS_ST_WRITTEN);
        if (imm && ((dirty_it->second.state & BS_ST_TYPE_MASK) == BS_ST_DELETE || (dirty_it->second.state & BS_ST_INSTANT)))
        {
            // Deletions and 'instant' operations are treated as immediately stable
            mark_stable(dirty_it->first);
        }
        if (!imm)
        {
            if (is_big)
            {
                // Remember big write as unsynced
                unsynced_big_writes.push_back((obj_ver_id){
                    .oid = op->oid,
                    .version = op->version,
                });
            }
            else
            {
                // Remember small write as unsynced
                unsynced_small_writes.push_back((obj_ver_id){
                    .oid = op->oid,
                    .version = op->version,
                });
            }
        }
        if (imm && (dirty_it->second.state & BS_ST_TYPE_MASK) == BS_ST_BIG_WRITE)
        {
            // Unblock small writes
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
        // Apply throttling to not fill the journal too fast for the SSD+HDD case
        if (!is_big && throttle_small_writes)
        {
            // Apply throttling
            timespec tv_end;
            clock_gettime(CLOCK_REALTIME, &tv_end);
            uint64_t exec_us =
                (tv_end.tv_sec - PRIV(op)->tv_begin.tv_sec)*1000000 +
                (tv_end.tv_nsec - PRIV(op)->tv_begin.tv_nsec)/1000;
            // Compare with target execution time
            // 100% free -> target time = 0
            // 0% free -> target time = iodepth/parallelism * (iops + size/bw) / write per second
            uint64_t used_start = journal.get_trim_pos();
            uint64_t journal_free_space = journal.next_free < used_start
                ? (used_start - journal.next_free)
                : (journal.len - journal.next_free + used_start - journal.block_size);
            uint64_t ref_us =
                (write_iodepth <= throttle_target_parallelism ? 100 : 100*write_iodepth/throttle_target_parallelism)
                * (1000000/throttle_target_iops + op->len*1000000/throttle_target_mbs/1024/1024)
                / 100;
            ref_us -= ref_us * journal_free_space / journal.len;
            if (ref_us > exec_us + throttle_threshold_us)
            {
                // Pause reply
                tfd->set_timer_us(ref_us-exec_us, false, [this, op](int timer_id)
                {
                    PRIV(op)->op_state++;
                    ringloop->wakeup();
                });
                PRIV(op)->op_state = 5;
                return 1;
            }
        }
    }
resume_6:
    // Acknowledge write
    op->retval = op->len;
    write_iodepth--;
    FINISH_OP(op);
    return 2;
}

void blockstore_impl_t::handle_write_event(ring_data_t *data, blockstore_op_t *op)
{
    live = true;
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
        release_journal_sectors(op);
        PRIV(op)->op_state++;
        ringloop->wakeup();
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
            journal.sector_info[s-1].flush_count--;
            if (s != (1+journal.cur_sector) && journal.sector_info[s-1].flush_count == 0)
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
    if (PRIV(op)->op_state)
    {
        return continue_write(op);
    }
    auto dirty_it = dirty_db.find((obj_ver_id){
        .oid = op->oid,
        .version = op->version,
    });
    assert(dirty_it != dirty_db.end());
    blockstore_journal_check_t space_check(this);
    if (!space_check.check_available(op, 1, sizeof(journal_entry_del), JOURNAL_STABILIZE_RESERVATION))
    {
        return 0;
    }
    write_iodepth++;
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
    }
    if (!PRIV(op)->pending_ops)
    {
        PRIV(op)->op_state = 4;
        return continue_write(op);
    }
    else
    {
        PRIV(op)->op_state = 3;
    }
    return 1;
}
