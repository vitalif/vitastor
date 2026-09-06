// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "impl.h"
#include "internal.h"

namespace v1 {

// A deletion supersedes everything written before it and is stable as soon as it's written,
// so versions preceding it that aren't in the journal yet are free to forget. And they have to
// be forgotten: a big write is only journaled when it gets synced, so leaving one behind would
// put its journal entry after the deletion and recovery would replay them in the wrong
// order, resurrecting the object. Returns false if a preceding write is still running -
// its completion refers to the entry, so we have to let it finish first.
//
// Entries which are already synced must NOT be forgotten. Their journal entries are on the disk
// and can't be taken back, while the deletion itself only reaches the journal on the next sync.
// Forgetting them drops their journal references, and the journal may then be trimmed past the
// object's big write while its small writes still sit in later sectors kept alive by other
// objects - recovery would then replay those small writes with no base under them.
// An entry which isn't in the unsynced lists any more has been taken over by a sync which is
// still in flight: continue_sync() swaps those lists over to itself, but the entries only get
// the SYNCED state later, in ack_sync()
static bool is_still_unsynced(const std::vector<obj_ver_id> & unsynced, obj_ver_id ov)
{
    for (auto & u: unsynced)
        if (u == ov)
            return true;
    return false;
}

bool blockstore_impl_t::forget_unstable_before_delete(blockstore_op_t *op, blockstore_dirty_db_t::iterator dirty_it)
{
    if (immediate_commit == IMMEDIATE_ALL)
    {
        // Everything is journaled as soon as it's written here, so nothing can end up
        // behind the deletion in the journal and there is nothing to forget
        return true;
    }
    auto first_it = dirty_it;
    while (first_it != dirty_db.begin())
    {
        auto prev_it = first_it;
        prev_it--;
        if (prev_it->first.oid != op->oid || IS_SYNCED(prev_it->second.state))
            break;
        if (IS_IN_FLIGHT(prev_it->second.state))
            return false;
        if (!is_still_unsynced(IS_BIG_WRITE(prev_it->second.state) ? unsynced_big_writes : unsynced_small_writes, prev_it->first))
        {
            // A sync in flight already owns it, so we can't erase it here: that would leave
            // the sync with a dangling dirty_db entry in ack_sync() and would decrement
            // unstable_unsynced a second time.
            // A small write is journaled as soon as it's dequeued, so it is already in front
            // of our deletion. A big write is not: its journal entry is only written when the
            // sync reaches its journaling phase, which may well be after our deletion is
            // journaled. Journal replay would then take it for a new write over the deletion
            // and resurrect the object, so wait for that sync to finish instead
            if (IS_BIG_WRITE(prev_it->second.state))
                return false;
            break;
        }
        first_it = prev_it;
    }
    if (first_it == dirty_it)
        return true;
    for (auto it = first_it; it != dirty_it; it++)
    {
        // Drop it from the unsynced lists so that a later sync doesn't journal it
        bool is_big = IS_BIG_WRITE(it->second.state);
        auto & unsynced = is_big ? unsynced_big_writes : unsynced_small_writes;
        for (size_t i = 0; i < unsynced.size(); i++)
        {
            if (unsynced[i].oid == it->first.oid && unsynced[i].version == it->first.version)
            {
                unsynced.erase(unsynced.begin()+i, unsynced.begin()+i+1);
                if (is_big && immediate_commit != IMMEDIATE_ALL)
                    unsynced_big_write_count--;
                break;
            }
        }
        if (!(it->second.state & BS_ST_INSTANT) && !IS_SYNCED(it->second.state))
        {
            unstable_unsynced--;
            assert(unstable_unsynced >= 0);
        }
    }
    auto & clean_db = clean_db_shard(op->oid);
    auto clean_it = clean_db.find(op->oid);
    uint64_t clean_loc = clean_it != clean_db.end() ? clean_it->second.location : UINT64_MAX;
    erase_dirty(first_it, dirty_it, clean_loc);
    wakeup_wait_journal();
    return true;
}

bool blockstore_impl_t::enqueue_write(blockstore_op_t *op)
{
    // Check or assign version number
    bool found = false, deleted = false, unsynced = false, is_del = (op->opcode == BS_OP_DELETE);
    bool wait_big = false, wait_del = false;
    void *dyn = NULL;
    uint8_t *dyn_ptr = NULL;
    if (is_del)
    {
        op->len = 0;
    }
    else
    {
        if (alloc_dyn_data)
        {
            // FIXME: Working with `dyn_data` has to be refactored somehow but I first have to decide how :)
            // +sizeof(int) = refcount
            size_t dyn_size = dsk.dirty_dyn_size(op->offset, op->len);
            dyn = calloc_or_die(1, dyn_size+sizeof(int));
            *((int*)dyn) = 1;
        }
        dyn_ptr = (alloc_dyn_data ? (uint8_t*)dyn+sizeof(int) : (uint8_t*)&dyn);
    }
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
            if (op->opcode == BS_OP_WRITE_STABLE && !IS_INSTANT(dirty_it->second.state))
            {
                op->retval = -EINVAL;
                if (!is_del && alloc_dyn_data)
                {
                    free(dyn);
                }
                return false;
            }
            found = true;
            version = dirty_it->first.version + 1;
            deleted = IS_DELETE(dirty_it->second.state);
            unsynced = !IS_SYNCED(dirty_it->second.state);
            wait_del = ((dirty_it->second.state & BS_ST_WORKFLOW_MASK) == BS_ST_WAIT_DEL);
            wait_big = (dirty_it->second.state & BS_ST_TYPE_MASK) == BS_ST_BIG_WRITE
                ? !IS_SYNCED(dirty_it->second.state)
                : ((dirty_it->second.state & BS_ST_WORKFLOW_MASK) == BS_ST_WAIT_BIG);
            if (!is_del && !deleted)
            {
                void *dyn_from = alloc_dyn_data
                    ? (uint8_t*)dirty_it->second.dyn_data + sizeof(int) : (uint8_t*)&dirty_it->second.dyn_data;
                memcpy(dyn_ptr, dyn_from, dsk.clean_entry_bitmap_size);
            }
        }
    }
    if (!found)
    {
        auto & clean_db = clean_db_shard(op->oid);
        auto clean_it = clean_db.find(op->oid);
        if (clean_it != clean_db.end())
        {
            version = clean_it->second.version + 1;
            if (!is_del)
            {
                void *bmp_ptr = get_clean_entry_bitmap(clean_it->second.location, dsk.clean_entry_bitmap_size);
                memcpy(dyn_ptr, bmp_ptr, dsk.clean_entry_bitmap_size);
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
            printf("Write %jx:%jx v%ju over delete (real v%ju) offset=%u len=%u\n", op->oid.inode, op->oid.stripe, version, op->version, op->offset, op->len);
#endif
            wait_del = true;
            PRIV(op)->real_version = op->version;
            op->version = version;
            if (unsynced)
            {
                // Issue an additional sync so the delete reaches the journal
                blockstore_op_t *sync_op = new blockstore_op_t;
                sync_op->opcode = BS_OP_SYNC;
                sync_op->oid = op->oid;
                sync_op->version = op->version;
                sync_op->callback = [this](blockstore_op_t *sync_op)
                {
                    unshift_stable_flush((obj_ver_id){
                        .oid = sync_op->oid,
                        .version = sync_op->version-1,
                    });
                    delete sync_op;
                };
                enqueue_own_op(sync_op);
            }
            else
            {
                unshift_stable_flush((obj_ver_id){
                    .oid = op->oid,
                    .version = version-1,
                });
            }
        }
        else
        {
            // Invalid version requested
#ifdef BLOCKSTORE_DEBUG
            printf("Write %jx:%jx v%ju requested, but we already have v%ju\n", op->oid.inode, op->oid.stripe, op->version, version);
#endif
            op->retval = -EEXIST;
            if (!is_del && alloc_dyn_data)
            {
                free(dyn);
            }
            return false;
        }
    }
    bool imm = (op->len < dsk.data_block_size ? (immediate_commit != IMMEDIATE_NONE) : (immediate_commit == IMMEDIATE_ALL));
    if (wait_big && !is_del && !deleted && op->len < dsk.data_block_size && !imm ||
        !imm && autosync_writes && unsynced_queued_ops >= autosync_writes)
    {
        // Issue an additional sync so that the previous big write can reach the journal
        blockstore_op_t *sync_op = new blockstore_op_t;
        sync_op->opcode = BS_OP_SYNC;
        sync_op->callback = [](blockstore_op_t *sync_op)
        {
            delete sync_op;
        };
        enqueue_own_op(sync_op);
    }
    else if (!imm)
        unsynced_queued_ops++;
#ifdef BLOCKSTORE_DEBUG
    if (is_del)
        printf("Delete %jx:%jx v%ju\n", op->oid.inode, op->oid.stripe, op->version);
    else if (!wait_del)
        printf("Write %jx:%jx v%ju offset=%u len=%u\n", op->oid.inode, op->oid.stripe, op->version, op->offset, op->len);
#endif
    // No strict need to add it into dirty_db here except maybe for listings to return
    // correct data when there are inflight operations in the queue
    uint32_t state;
    if (is_del)
        state = BS_ST_DELETE | BS_ST_IN_FLIGHT;
    else
    {
        state = (op->len == dsk.data_block_size || deleted ? BS_ST_BIG_WRITE : BS_ST_SMALL_WRITE);
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
            memcpy(dyn_ptr, op->bitmap, dsk.clean_entry_bitmap_size);
    }
    if (wait_del)
    {
        // Entering WAIT_DEL -> request_trim, exiting -> release_trim
        flusher->request_trim();
    }
    // Calculate checksums
    // FIXME: Allow to receive checksums from outside?
    if (!is_del && dsk.data_csum_type && op->len > 0)
    {
        uint32_t *data_csums = (uint32_t*)(dyn_ptr + dsk.clean_entry_bitmap_size);
        uint32_t start = op->offset / dsk.csum_block_size;
        uint32_t end = (op->offset+op->len-1) / dsk.csum_block_size;
        auto fn = state & BS_ST_BIG_WRITE ? crc32c_pad : crc32c_nopad;
        if (start == end)
            data_csums[0] = fn(0, op->buf, op->len, op->offset - start*dsk.csum_block_size, (end+1)*dsk.csum_block_size - (op->offset+op->len));
        else
        {
            // First block
            data_csums[0] = fn(0, op->buf, dsk.csum_block_size*(start+1)-op->offset, op->offset - start*dsk.csum_block_size, 0);
            // Intermediate blocks
            for (uint32_t i = start+1; i < end; i++)
                data_csums[i-start] = crc32c(0, (uint8_t*)op->buf + dsk.csum_block_size*i-op->offset, dsk.csum_block_size);
            // Last block
            data_csums[end-start] = fn(
                0, (uint8_t*)op->buf + end*dsk.csum_block_size - op->offset,
                op->offset+op->len - end*dsk.csum_block_size,
                0, (end+1)*dsk.csum_block_size - (op->offset+op->len)
            );
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
        .dyn_data = dyn,
    });
    return true;
}

// Ask the flusher to flush <ov> right now, but only if it's already stable.
// A sync we submit ourselves may complete without syncing anything, because a sync submitted
// before it has already taken those writes over and is still in flight. In that case the entry
// becomes stable - and is queued for flushing by mark_stable() - when that sync completes,
// and forcing a flush of a still unstable entry would abort the flusher
void blockstore_impl_t::unshift_stable_flush(obj_ver_id ov)
{
    auto dirty_it = dirty_db.find(ov);
    if (dirty_it != dirty_db.end() && IS_STABLE(dirty_it->second.state))
    {
        flusher->unshift_flush(ov, true);
    }
}

void blockstore_impl_t::cancel_all_writes(blockstore_op_t *op, blockstore_dirty_db_t::iterator dirty_it, int retval)
{
    while (dirty_it != dirty_db.end() && dirty_it->first.oid == op->oid)
    {
        free_dirty_dyn_data(dirty_it->second);
        dirty_db.erase(dirty_it++);
    }
    bool found = false;
    for (auto other_op: submit_queue)
    {
        if (!other_op)
        {
            // freed operations during submitting are zeroed
        }
        else if (other_op == op)
        {
            // <op> may be present in queue multiple times due to moving operations in submit_queue
            found = true;
        }
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
        printf("Restoring %jx:%jx version: v%ju -> v%ju\n", op->oid.inode, op->oid.stripe, op->version, PRIV(op)->real_version);
#endif
        auto prev_it = dirty_it;
        if (prev_it != dirty_db.begin())
        {
            prev_it--;
            if (prev_it->first.oid == op->oid && prev_it->first.version >= PRIV(op)->real_version)
            {
                // Original version is still invalid
                // All subsequent writes to the same object must be canceled too
                printf("Tried to write %jx:%jx v%ju after delete (old version v%ju), but already have v%ju\n",
                    op->oid.inode, op->oid.stripe, PRIV(op)->real_version, op->version, prev_it->first.version);
                cancel_all_writes(op, dirty_it, -EEXIST);
                return 2;
            }
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
            sizeof(journal_entry_big_write) + dsk.clean_dyn_size,
            (unstable_writes.size()+unstable_unsynced+((dirty_it->second.state & BS_ST_INSTANT) ? 0 : 1))*journal.block_size))
        {
            if (space_check.give_up)
            {
                cancel_all_writes(op, dirty_it, -EAGAIN);
                return 2;
            }
            return 0;
        }
        // Big (redirect) write
        uint64_t loc = data_alloc->find_free();
        if (loc == UINT64_MAX)
        {
            // no space
            if (big_to_flush > 0)
            {
                // hope that some space will be available after flush
                flusher->request_trim();
                PRIV(op)->wait_for = WAIT_FREE;
                return 0;
            }
            cancel_all_writes(op, dirty_it, -ENOSPC);
            return 2;
        }
        if (inmemory_meta)
        {
            // Check once more that metadata entry is zeroed (the reverse means a bug or corruption)
            uint64_t sector = (loc / (dsk.meta_block_size / dsk.clean_entry_size)) * dsk.meta_block_size;
            uint64_t pos = (loc % (dsk.meta_block_size / dsk.clean_entry_size));
            clean_disk_entry *entry = (clean_disk_entry*)((uint8_t*)metadata_buffer + sector + pos*dsk.clean_entry_size);
            if (entry->oid.inode || entry->oid.stripe || entry->version)
            {
                printf(
                    "Fatal error (metadata corruption or bug): tried to write object %jx:%jx v%ju"
                    " over a non-zero metadata entry %ju with %jx:%jx v%ju\n", op->oid.inode,
                    op->oid.stripe, op->version, loc, entry->oid.inode, entry->oid.stripe, entry->version
                );
                exit(1);
            }
        }
        BS_SUBMIT_GET_SQE(sqe, data);
        write_iodepth++;
        dirty_it->second.location = loc * dsk.data_block_size;
        dirty_it->second.state = (dirty_it->second.state & ~BS_ST_WORKFLOW_MASK) | BS_ST_SUBMITTED;
#ifdef BLOCKSTORE_DEBUG
        printf(
            "Allocate block %ju for %jx:%jx v%ju\n",
            loc, op->oid.inode, op->oid.stripe, op->version
        );
#endif
        data_alloc->set(loc, true);
        uint64_t stripe_offset = (op->offset % dsk.bitmap_granularity);
        uint64_t stripe_end = (op->offset + op->len) % dsk.bitmap_granularity;
        // Zero fill up to dsk.bitmap_granularity
        int vcnt = 0;
        if (stripe_offset)
        {
            PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ zero_object, (size_t)stripe_offset };
        }
        PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ op->buf, op->len };
        if (stripe_end)
        {
            stripe_end = dsk.bitmap_granularity - stripe_end;
            PRIV(op)->iov_zerofill[vcnt++] = (struct iovec){ zero_object, (size_t)stripe_end };
        }
        data->iov.iov_len = op->len + stripe_offset + stripe_end; // to check it in the callback
        data->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
        const uint64_t write_offset = (loc * dsk.data_block_size) + op->offset - stripe_offset;
        assert(write_offset+op->len+stripe_offset+stripe_end <= dsk.block_count*dsk.data_block_size);
        io_uring_prep_writev(sqe, dsk.data_fd, PRIV(op)->iov_zerofill, vcnt, dsk.data_offset + write_offset);
        PRIV(op)->pending_ops = 1;
        if (!(dirty_it->second.state & BS_ST_INSTANT))
        {
            unstable_unsynced++;
        }
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
        uint64_t dyn_size = dsk.dirty_dyn_size(op->offset, op->len);
        blockstore_journal_check_t space_check(this);
        if (unsynced_big_write_count &&
            !space_check.check_available(op, unsynced_big_write_count,
                sizeof(journal_entry_big_write) + dsk.clean_dyn_size, 0)
            || !space_check.check_available(op, 1,
                sizeof(journal_entry_small_write) + dyn_size,
                op->len + (unstable_writes.size()+unstable_unsynced+((dirty_it->second.state & BS_ST_INSTANT) ? 0 : 1))*journal.block_size))
        {
            if (space_check.give_up)
            {
                cancel_all_writes(op, dirty_it, -EAGAIN);
                return 2;
            }
            return 0;
        }
        // There is sufficient space. Check SQE(s)
        BS_SUBMIT_CHECK_SQES(
            // Write current journal sector only if it's dirty and full, or in the immediate_commit mode
            (immediate_commit != IMMEDIATE_NONE ||
                !journal.entry_fits(sizeof(journal_entry_small_write) + dyn_size) ? 1 : 0) +
            (op->len > 0 ? 1 : 0)
        );
        write_iodepth++;
        // Got SQEs. Prepare previous journal sector write if required
        if (immediate_commit == IMMEDIATE_NONE &&
            !journal.entry_fits(sizeof(journal_entry_small_write) + dyn_size) &&
            journal.sector_info[journal.cur_sector].dirty)
        {
            prepare_journal_sector_write(journal.cur_sector, op);
        }
        // Then pre-fill journal entry
        journal_entry_small_write *je = (journal_entry_small_write*)prefill_single_journal_entry(
            journal, op->opcode == BS_OP_WRITE_STABLE ? JE_SMALL_WRITE_INSTANT : JE_SMALL_WRITE,
            sizeof(journal_entry_small_write) + dyn_size
        );
        auto jsec = dirty_it->second.journal_sector = journal.sector_info[journal.cur_sector].offset;
        if (!(journal.next_free >= journal.used_start
            ? (jsec >= journal.used_start && jsec < journal.next_free)
            : (jsec >= journal.used_start || jsec < journal.next_free)))
        {
            printf(
                "BUG: journal offset %08jx is used by %jx:%jx v%ju (%ju refs) BUT used_start=%jx next_free=%jx\n",
                dirty_it->second.journal_sector, dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version,
                journal.used_sectors[journal.sector_info[journal.cur_sector].offset],
                journal.used_start, journal.next_free
            );
            abort();
        }
        journal.used_sectors[journal.sector_info[journal.cur_sector].offset]++;
#ifdef BLOCKSTORE_DEBUG
        printf(
            "journal offset %08jx is used by %jx:%jx v%ju (%ju refs)\n",
            dirty_it->second.journal_sector, dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version,
            journal.used_sectors[journal.sector_info[journal.cur_sector].offset]
        );
#endif
        // Figure out where data will be
        auto next_next_free = (journal.next_free + op->len) <= journal.len ? journal.next_free : dsk.journal_block_size;
        if (op->len > 0)
        {
            auto journal_used_it = journal.used_sectors.lower_bound(next_next_free);
            if (journal_used_it != journal.used_sectors.end() &&
                journal_used_it->first < next_next_free + op->len)
            {
                printf(
                    "BUG: Attempt to overwrite used offset (%jx, %ju refs) of the journal with the object %jx:%jx v%ju: data at %jx, len %x!"
                    " Journal used_start=%08jx (%ju refs), next_free=%08jx, dirty_start=%08jx\n",
                    journal_used_it->first, journal_used_it->second, op->oid.inode, op->oid.stripe, op->version, next_next_free, op->len,
                    journal.used_start, journal.used_sectors[journal.used_start], journal.next_free, journal.dirty_start
                );
                exit(1);
            }
        }
        // double check that next_free doesn't cross used_start from the left
        assert(journal.next_free >= journal.used_start && next_next_free >= journal.next_free || next_next_free < journal.used_start);
        journal.next_free = next_next_free;
        je->oid = op->oid;
        je->version = op->version;
        je->offset = op->offset;
        je->len = op->len;
        je->data_offset = journal.next_free;
        je->crc32_data = dsk.csum_block_size ? 0 : crc32c(0, op->buf, op->len);
        memcpy((void*)(je+1), (alloc_dyn_data
            ? (uint8_t*)dirty_it->second.dyn_data+sizeof(int) : (uint8_t*)&dirty_it->second.dyn_data), dyn_size);
        je->crc32 = je_crc32((journal_entry*)je);
        journal.crc32_last = je->crc32;
        // With immediate_commit=none we normally leave the sector to be written out by a later
        // sync. But if it is already queued for writing in this same batch, our entry goes to
        // the disk with that write anyway - so join it instead of leaving the sector dirty,
        // otherwise the next operation to write the sector out would queue a second write of
        // it while the first one is still in flight. It costs no extra I/O: the write is
        // already prepared, so prepare_journal_sector_write() only subscribes us to it
        if (immediate_commit != IMMEDIATE_NONE || journal.sector_info[journal.cur_sector].submit_id)
        {
            prepare_journal_sector_write(journal.cur_sector, op);
        }
        if (op->len > 0)
        {
            // Prepare journal data write
            if (journal.inmemory)
            {
                // Copy data
                memcpy((uint8_t*)journal.buffer + journal.next_free, op->buf, op->len);
            }
            BS_SUBMIT_GET_SQE(sqe2, data2);
            data2->iov = (struct iovec){ op->buf, op->len };
            ++journal.submit_id;
            assert(journal.submit_id != 0); // check overflow
            // Make subsequent journal writes wait for our data write
            journal.flushing_ops.emplace(journal.submit_id, (pending_journaling_t){
                .pending = 1,
                .sector = -1,
                .op = op,
            });
            data2->callback = [this, flush_id = journal.submit_id](ring_data_t *data) { handle_journal_write(data, flush_id); };
            assert(journal.next_free > 0);
            assert(journal.next_free+op->len <= dsk.journal_len);
            io_uring_prep_writev(sqe2, dsk.journal_fd, &data2->iov, 1, journal.offset + journal.next_free);
            PRIV(op)->pending_ops++;
        }
        else
        {
            // Zero-length overwrite. Allowed to bump object version in EC placement groups without actually writing data
        }
        dirty_it->second.location = journal.next_free;
        dirty_it->second.state = (dirty_it->second.state & ~BS_ST_WORKFLOW_MASK) | BS_ST_SUBMITTED;
        next_next_free = journal.next_free + op->len;
        if (next_next_free >= journal.len)
            next_next_free = dsk.journal_block_size;
        // double check that next_free doesn't cross used_start from the left
        assert(journal.next_free >= journal.used_start && next_next_free >= journal.next_free || next_next_free < journal.used_start);
        journal.next_free = next_next_free;
        if (!(dirty_it->second.state & BS_ST_INSTANT))
        {
            unstable_unsynced++;
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
        uint64_t dyn_size = dsk.dirty_dyn_size(op->offset, op->len);
        blockstore_journal_check_t space_check(this);
        if (!space_check.check_available(op, 1, sizeof(journal_entry_big_write) + dyn_size,
            (unstable_writes.size()+unstable_unsynced+((dirty_it->second.state & BS_ST_INSTANT) ? 0 : 1))*journal.block_size))
        {
            if (space_check.give_up)
            {
                auto uo_it = used_clean_objects.find(dirty_it->second.location);
                if (uo_it != used_clean_objects.end())
                    uo_it->second.was_freed = true;
                else
                    data_alloc->set(dirty_it->second.location / dsk.data_block_size, false);
                if (!(dirty_it->second.state & BS_ST_INSTANT))
                {
                    unstable_unsynced--;
                    assert(unstable_unsynced >= 0);
                    wakeup_wait_journal();
                }
                if (immediate_commit != IMMEDIATE_ALL)
                    unsynced_big_write_count--;
                write_iodepth--;
                cancel_all_writes(op, dirty_it, -EAGAIN);
                return 2;
            }
            return 0;
        }
        BS_SUBMIT_CHECK_SQES(1);
        journal_entry_big_write *je = (journal_entry_big_write*)prefill_single_journal_entry(
            journal, op->opcode == BS_OP_WRITE_STABLE ? JE_BIG_WRITE_INSTANT : JE_BIG_WRITE,
            sizeof(journal_entry_big_write) + dyn_size
        );
        auto jsec = dirty_it->second.journal_sector = journal.sector_info[journal.cur_sector].offset;
        if (!(journal.next_free >= journal.used_start
            ? (jsec >= journal.used_start && jsec < journal.next_free)
            : (jsec >= journal.used_start || jsec < journal.next_free)))
        {
            printf(
                "BUG: journal offset %08jx is used by %jx:%jx v%ju (%ju refs) BUT used_start=%jx next_free=%jx\n",
                dirty_it->second.journal_sector, dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version,
                journal.used_sectors[journal.sector_info[journal.cur_sector].offset],
                journal.used_start, journal.next_free
            );
            abort();
        }
        journal.used_sectors[journal.sector_info[journal.cur_sector].offset]++;
#ifdef BLOCKSTORE_DEBUG
        printf(
            "journal offset %08jx is used by %jx:%jx v%ju (%ju refs)\n",
            journal.sector_info[journal.cur_sector].offset, op->oid.inode, op->oid.stripe, op->version,
            journal.used_sectors[journal.sector_info[journal.cur_sector].offset]
        );
#endif
        je->oid = op->oid;
        je->version = op->version;
        je->offset = op->offset;
        je->len = op->len;
        je->location = dirty_it->second.location;
        memcpy((void*)(je+1), (alloc_dyn_data
            ? (uint8_t*)dirty_it->second.dyn_data+sizeof(int) : (uint8_t*)&dirty_it->second.dyn_data), dyn_size);
        je->crc32 = je_crc32((journal_entry*)je);
        journal.crc32_last = je->crc32;
        prepare_journal_sector_write(journal.cur_sector, op);
        PRIV(op)->op_state = 3;
        return 1;
    }
resume_4:
    // Switch object state
    {
        auto dirty_it = dirty_db.find((obj_ver_id){
            .oid = op->oid,
            .version = op->version,
        });
        assert(dirty_it != dirty_db.end());
#ifdef BLOCKSTORE_DEBUG
        printf("Ack write %jx:%jx v%ju = state 0x%x\n", op->oid.inode, op->oid.stripe, op->version, dirty_it->second.state);
#endif
        bool is_big = (dirty_it->second.state & BS_ST_TYPE_MASK) == BS_ST_BIG_WRITE;
        bool imm = is_big ? (immediate_commit == IMMEDIATE_ALL) : (immediate_commit != IMMEDIATE_NONE);
        bool is_instant = IS_INSTANT(dirty_it->second.state);
        if (imm)
        {
            auto & unstab = unstable_writes[op->oid];
            unstab = unstab < op->version ? op->version : unstab;
            if (!is_instant)
            {
                unstable_unsynced--;
                assert(unstable_unsynced >= 0);
                wakeup_wait_journal();
            }
        }
        dirty_it->second.state = (dirty_it->second.state & ~BS_ST_WORKFLOW_MASK)
            | (imm ? BS_ST_SYNCED : BS_ST_WRITTEN);
        if (imm && is_instant)
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
                PRIV(op)->op_state = 5;
                // Remember that the timer can in theory be called right here
                tfd->set_timer_us(ref_us-exec_us, false, [this, op](int timer_id)
                {
                    PRIV(op)->op_state++;
                    ringloop->wakeup();
                });
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
        disk_error_abort("data write", data->res, data->iov.iov_len);
    }
    PRIV(op)->pending_ops--;
    assert(PRIV(op)->pending_ops >= 0);
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
            if (!journal.sector_info[s-1].dirty && journal.sector_info[s-1].flush_count == 0)
            {
                if (s == (1+journal.cur_sector))
                {
                    // Forcibly move to the next sector and move dirty position
                    journal.in_sector_pos = journal.block_size;
                }
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
    if (!forget_unstable_before_delete(op, dirty_it))
    {
        // A preceding write is still running, retry when it finishes
        return 0;
    }
    blockstore_journal_check_t space_check(this);
    if (!space_check.check_available(op, 1, sizeof(journal_entry_del), (unstable_writes.size()+unstable_unsynced)*journal.block_size))
    {
        if (space_check.give_up)
        {
            cancel_all_writes(op, dirty_it, -EAGAIN);
            return 2;
        }
        return 0;
    }
    // Write current journal sector only if it's dirty and full, or in the immediate_commit mode.
    BS_SUBMIT_CHECK_SQES(
        (immediate_commit != IMMEDIATE_NONE ||
            !journal.entry_fits(sizeof(journal_entry_del)) ? 1 : 0)
    );
    if (write_iodepth >= max_write_iodepth)
    {
        return 0;
    }
    write_iodepth++;
    // Prepare journal sector write
    if (immediate_commit == IMMEDIATE_NONE &&
        !journal.entry_fits(sizeof(journal_entry_del)) &&
        journal.sector_info[journal.cur_sector].dirty)
    {
        prepare_journal_sector_write(journal.cur_sector, op);
    }
    // Pre-fill journal entry
    journal_entry_del *je = (journal_entry_del*)prefill_single_journal_entry(
        journal, JE_DELETE, sizeof(struct journal_entry_del)
    );
    dirty_it->second.journal_sector = journal.sector_info[journal.cur_sector].offset;
    journal.used_sectors[journal.sector_info[journal.cur_sector].offset]++;
#ifdef BLOCKSTORE_DEBUG
    printf(
        "journal offset %08jx is used by %jx:%jx v%ju (%ju refs)\n",
        dirty_it->second.journal_sector, dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version,
        journal.used_sectors[journal.sector_info[journal.cur_sector].offset]
    );
#endif
    je->oid = op->oid;
    je->version = op->version;
    je->crc32 = je_crc32((journal_entry*)je);
    journal.crc32_last = je->crc32;
    dirty_it->second.state = BS_ST_DELETE | BS_ST_SUBMITTED;
    // Join a sector write which is already queued in this same batch, see dequeue_write()
    if (immediate_commit != IMMEDIATE_NONE || journal.sector_info[journal.cur_sector].submit_id)
    {
        prepare_journal_sector_write(journal.cur_sector, op);
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

} // namespace v1
