// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

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

struct ver_vector_t
{
    obj_ver_id *items = NULL;
    uint64_t alloc = 0, size = 0;
};

static void init_versions(ver_vector_t & vec, obj_ver_id *start, obj_ver_id *end, uint64_t len)
{
    if (!vec.items)
    {
        vec.alloc = len;
        vec.items = (obj_ver_id*)malloc_or_die(sizeof(obj_ver_id) * vec.alloc);
        for (auto sv = start; sv < end; sv++)
        {
            vec.items[vec.size++] = *sv;
        }
    }
}

static void append_version(ver_vector_t & vec, obj_ver_id ov)
{
    if (vec.size >= vec.alloc)
    {
        vec.alloc = !vec.alloc ? 4 : vec.alloc*2;
        vec.items = (obj_ver_id*)realloc_or_die(vec.items, sizeof(obj_ver_id) * vec.alloc);
    }
    vec.items[vec.size++] = ov;
}

static bool check_unsynced(std::vector<obj_ver_id> & check, obj_ver_id ov, std::vector<obj_ver_id> & to, int *count)
{
    bool found = false;
    int j = 0, k = 0;
    while (j < check.size())
    {
        if (check[j] == ov)
            found = true;
        if (check[j].oid == ov.oid && check[j].version <= ov.version)
        {
            to.push_back(check[j++]);
            if (count)
                (*count)--;
        }
        else
            check[k++] = check[j++];
    }
    check.resize(k);
    return found;
}

blockstore_op_t* blockstore_impl_t::selective_sync(blockstore_op_t *op)
{
    unsynced_big_write_count -= unsynced_big_writes.size();
    unsynced_big_writes.swap(PRIV(op)->sync_big_writes);
    unsynced_big_write_count += unsynced_big_writes.size();
    unsynced_small_writes.swap(PRIV(op)->sync_small_writes);
    // Create a sync operation, insert into the end of the queue
    // And move ourselves into the end too!
    // Rather hacky but that's what we need...
    blockstore_op_t *sync_op = new blockstore_op_t;
    sync_op->opcode = BS_OP_SYNC;
    sync_op->buf = NULL;
    sync_op->callback = [](blockstore_op_t *sync_op)
    {
        delete sync_op;
    };
    init_op(sync_op);
    int sync_res = continue_sync(sync_op);
    if (sync_res != 2)
    {
        // Put SYNC into the queue if it's not finished yet
        submit_queue.push_back(sync_op);
    }
    // Restore unsynced_writes
    unsynced_small_writes.swap(PRIV(op)->sync_small_writes);
    unsynced_big_write_count -= unsynced_big_writes.size();
    unsynced_big_writes.swap(PRIV(op)->sync_big_writes);
    unsynced_big_write_count += unsynced_big_writes.size();
    if (sync_res == 2)
    {
        // Sync is immediately completed
        return NULL;
    }
    return sync_op;
}

// Returns: 2 = stop processing and dequeue, 0 = stop processing and do not dequeue, 1 = proceed with op itself
int blockstore_impl_t::split_stab_op(blockstore_op_t *op, std::function<int(obj_ver_id v)> decider)
{
    bool add_sync = false;
    ver_vector_t good_vers, bad_vers;
    obj_ver_id* v;
    int i, todo = 0;
    for (i = 0, v = (obj_ver_id*)op->buf; i < op->len; i++, v++)
    {
        int action = decider(*v);
        if (action < 0)
        {
            // Rollback changes
            for (auto & ov: PRIV(op)->sync_big_writes)
            {
                unsynced_big_writes.push_back(ov);
                unsynced_big_write_count++;
            }
            for (auto & ov: PRIV(op)->sync_small_writes)
            {
                unsynced_small_writes.push_back(ov);
            }
            free(good_vers.items);
            good_vers.items = NULL;
            free(bad_vers.items);
            bad_vers.items = NULL;
            // Error
            op->retval = action;
            FINISH_OP(op);
            return 2;
        }
        else if (action == STAB_SPLIT_DONE)
        {
            // Already done
            init_versions(good_vers, (obj_ver_id*)op->buf, v, op->len);
        }
        else if (action == STAB_SPLIT_WAIT)
        {
            // Already in progress, we just have to wait until it finishes
            init_versions(good_vers, (obj_ver_id*)op->buf, v, op->len);
            append_version(bad_vers, *v);
        }
        else if (action == STAB_SPLIT_SYNC)
        {
            // Needs a SYNC, we have to send a SYNC if not already in progress
            //
            // If the object is not present in unsynced_(big|small)_writes then
            // it's currently being synced. If it's present then we can initiate
            // its sync ourselves.
            init_versions(good_vers, (obj_ver_id*)op->buf, v, op->len);
            append_version(bad_vers, *v);
            if (!add_sync)
            {
                PRIV(op)->sync_big_writes.clear();
                PRIV(op)->sync_small_writes.clear();
                add_sync = true;
            }
            check_unsynced(unsynced_small_writes, *v, PRIV(op)->sync_small_writes, NULL);
            check_unsynced(unsynced_big_writes, *v, PRIV(op)->sync_big_writes, &unsynced_big_write_count);
        }
        else /* if (action == STAB_SPLIT_TODO) */
        {
            if (good_vers.items)
            {
                // If we're selecting versions then append it
                // Main idea is that 99% of the time all versions passed to BS_OP_STABLE are synced
                // And we don't want to select/allocate anything in that optimistic case
                append_version(good_vers, *v);
            }
            todo++;
        }
    }
    // In a pessimistic scenario, an operation may be split into 3:
    // - Stabilize synced entries
    // - Sync unsynced entries
    // - Continue for unsynced entries after sync
    add_sync = add_sync && (PRIV(op)->sync_big_writes.size() || PRIV(op)->sync_small_writes.size());
    if (!todo && !bad_vers.size)
    {
        // Already stable
        op->retval = 0;
        FINISH_OP(op);
        return 2;
    }
    op->retval = 0;
    if (!todo && !add_sync)
    {
        // Only wait for inflight writes or current in-progress syncs
        return 0;
    }
    blockstore_op_t *sync_op = NULL, *split_stab_op = NULL;
    if (add_sync)
    {
        // Initiate a selective sync for PRIV(op)->sync_(big|small)_writes
        sync_op = selective_sync(op);
    }
    if (bad_vers.size)
    {
        // Split part of the request into a separate operation
        split_stab_op = new blockstore_op_t;
        split_stab_op->opcode = op->opcode;
        split_stab_op->buf = bad_vers.items;
        split_stab_op->len = bad_vers.size;
        init_op(split_stab_op);
        submit_queue.push_back(split_stab_op);
    }
    if (sync_op || split_stab_op || good_vers.items)
    {
        void *orig_buf = op->buf;
        if (good_vers.items)
        {
            op->buf = good_vers.items;
            op->len = good_vers.size;
        }
        // Make a wrapped callback
        int *split_op_counter = (int*)malloc_or_die(sizeof(int));
        *split_op_counter = (sync_op ? 1 : 0) + (split_stab_op ? 1 : 0) + (todo ? 1 : 0);
        auto cb = [op, good_items = good_vers.items,
            bad_items = bad_vers.items, split_op_counter,
            orig_buf, real_cb = op->callback](blockstore_op_t *split_op)
        {
            if (split_op->retval != 0)
                op->retval = split_op->retval;
            (*split_op_counter)--;
            assert((*split_op_counter) >= 0);
            if (op != split_op)
                delete split_op;
            if (!*split_op_counter)
            {
                free(good_items);
                free(bad_items);
                free(split_op_counter);
                op->buf = orig_buf;
                real_cb(op);
            }
        };
        if (sync_op)
        {
            sync_op->callback = cb;
        }
        if (split_stab_op)
        {
            split_stab_op->callback = cb;
        }
        op->callback = cb;
    }
    if (!todo)
    {
        // All work is postponed
        op->callback = NULL;
        return 2;
    }
    return 1;
}

int blockstore_impl_t::dequeue_stable(blockstore_op_t *op)
{
    if (PRIV(op)->op_state)
    {
        return continue_stable(op);
    }
    int r = split_stab_op(op, [this](obj_ver_id ov)
    {
        auto dirty_it = dirty_db.find(ov);
        if (dirty_it == dirty_db.end())
        {
            auto & clean_db = clean_db_shard(ov.oid);
            auto clean_it = clean_db.find(ov.oid);
            if (clean_it == clean_db.end() || clean_it->second.version < ov.version)
            {
                // No such object version
                printf("Error: %jx:%jx v%ju not found while stabilizing\n", ov.oid.inode, ov.oid.stripe, ov.version);
                return -ENOENT;
            }
            else
            {
                // Already stable
                return STAB_SPLIT_DONE;
            }
        }
        else if (IS_STABLE(dirty_it->second.state))
        {
            // Already stable
            return STAB_SPLIT_DONE;
        }
        while (true)
        {
            if (IS_IN_FLIGHT(dirty_it->second.state))
            {
                // Object write is still in progress. Wait until the write request completes
                return STAB_SPLIT_WAIT;
            }
            else if (!IS_SYNCED(dirty_it->second.state))
            {
                // Object not synced yet - sync it
                // In previous versions we returned EBUSY here and required
                // the caller (OSD) to issue a global sync first. But a global sync
                // waits for all writes in the queue including inflight writes. And
                // inflight writes may themselves be blocked by unstable writes being
                // still present in the journal and not flushed away from it.
                // So we must sync specific objects here.
                //
                // Even more, we have to process "stabilize" request in parts. That is,
                // we must stabilize all objects which are already synced. Otherwise
                // they may block objects which are NOT synced yet.
                return STAB_SPLIT_SYNC;
            }
            else if (IS_STABLE(dirty_it->second.state))
            {
                break;
            }
            // Check previous versions too
            if (dirty_it == dirty_db.begin())
            {
                break;
            }
            dirty_it--;
            if (dirty_it->first.oid != ov.oid)
            {
                break;
            }
        }
        return STAB_SPLIT_TODO;
    });
    if (r != 1)
    {
        return r;
    }
    // Check journal space
    blockstore_journal_check_t space_check(this);
    if (!space_check.check_available(op, op->len, sizeof(journal_entry_stable), 0))
    {
        return 0;
    }
    // There is sufficient space. Check SQEs
    BS_SUBMIT_CHECK_SQES(space_check.sectors_to_write);
    // Prepare and submit journal entries
    int s = 0;
    auto v = (obj_ver_id*)op->buf;
    for (int i = 0; i < op->len; i++, v++)
    {
        if (!journal.entry_fits(sizeof(journal_entry_stable)) &&
            journal.sector_info[journal.cur_sector].dirty)
        {
            prepare_journal_sector_write(journal.cur_sector, op);
            s++;
        }
        journal_entry_stable *je = (journal_entry_stable*)
            prefill_single_journal_entry(journal, JE_STABLE, sizeof(journal_entry_stable));
        je->oid = v->oid;
        je->version = v->version;
        je->crc32 = je_crc32((journal_entry*)je);
        journal.crc32_last = je->crc32;
    }
    prepare_journal_sector_write(journal.cur_sector, op);
    s++;
    assert(s == space_check.sectors_to_write);
    PRIV(op)->op_state = 1;
    return 1;
}

int blockstore_impl_t::continue_stable(blockstore_op_t *op)
{
    if (PRIV(op)->op_state == 2)
        goto resume_2;
    else if (PRIV(op)->op_state == 4)
        goto resume_4;
    else
        return 1;
resume_2:
    if (!disable_journal_fsync)
    {
        BS_SUBMIT_GET_SQE(sqe, data);
        io_uring_prep_fsync(sqe, dsk.journal_fd, IORING_FSYNC_DATASYNC);
        data->iov = { 0 };
        data->callback = [this, op](ring_data_t *data) { handle_write_event(data, op); };
        PRIV(op)->min_flushed_journal_sector = PRIV(op)->max_flushed_journal_sector = 0;
        PRIV(op)->pending_ops = 1;
        PRIV(op)->op_state = 3;
        return 1;
    }
resume_4:
    // Mark dirty_db entries as stable, acknowledge op completion
    obj_ver_id* v;
    int i;
    for (i = 0, v = (obj_ver_id*)op->buf; i < op->len; i++, v++)
    {
        // Mark all dirty_db entries up to op->version as stable
#ifdef BLOCKSTORE_DEBUG
        printf("Stabilize %jx:%jx v%ju\n", v->oid.inode, v->oid.stripe, v->version);
#endif
        mark_stable(*v);
    }
    // Acknowledge op
    op->retval = 0;
    FINISH_OP(op);
    return 2;
}

void blockstore_impl_t::mark_stable(obj_ver_id v, bool forget_dirty)
{
    auto dirty_it = dirty_db.find(v);
    if (dirty_it != dirty_db.end())
    {
        if (IS_INSTANT(dirty_it->second.state))
        {
            // 'Instant' (non-EC) operations may complete and try to become stable out of order. Prevent it.
            auto back_it = dirty_it;
            while (back_it != dirty_db.begin())
            {
                back_it--;
                if (back_it->first.oid != v.oid)
                {
                    break;
                }
                if (!IS_STABLE(back_it->second.state))
                {
                    // There are preceding unstable versions, can't flush <v>
                    return;
                }
            }
            while (true)
            {
                dirty_it++;
                if (dirty_it == dirty_db.end() || dirty_it->first.oid != v.oid ||
                    !IS_SYNCED(dirty_it->second.state))
                {
                    dirty_it--;
                    break;
                }
                v.version = dirty_it->first.version;
            }
        }
        while (1)
        {
            bool was_stable = IS_STABLE(dirty_it->second.state);
            if ((dirty_it->second.state & BS_ST_WORKFLOW_MASK) == BS_ST_SYNCED)
            {
                dirty_it->second.state = (dirty_it->second.state & ~BS_ST_WORKFLOW_MASK) | BS_ST_STABLE;
                // Allocations and deletions are counted when they're stabilized
                if (IS_BIG_WRITE(dirty_it->second.state))
                {
                    int exists = -1;
                    if (dirty_it != dirty_db.begin())
                    {
                        auto prev_it = dirty_it;
                        prev_it--;
                        if (prev_it->first.oid == v.oid)
                        {
                            exists = IS_DELETE(prev_it->second.state) ? 0 : 1;
                        }
                    }
                    if (exists == -1)
                    {
                        auto & clean_db = clean_db_shard(v.oid);
                        auto clean_it = clean_db.find(v.oid);
                        exists = clean_it != clean_db.end() ? 1 : 0;
                    }
                    if (!exists)
                    {
                        uint64_t space_id = dirty_it->first.oid.inode;
                        if (no_inode_stats[dirty_it->first.oid.inode >> (64-POOL_ID_BITS)])
                            space_id = space_id & ~(((uint64_t)1 << (64-POOL_ID_BITS)) - 1);
                        inode_space_stats[space_id] += dsk.data_block_size;
                        used_blocks++;
                    }
                    big_to_flush++;
                }
                else if (IS_DELETE(dirty_it->second.state))
                {
                    uint64_t space_id = dirty_it->first.oid.inode;
                    if (no_inode_stats[dirty_it->first.oid.inode >> (64-POOL_ID_BITS)])
                        space_id = space_id & ~(((uint64_t)1 << (64-POOL_ID_BITS)) - 1);
                    auto & sp = inode_space_stats[space_id];
                    if (sp > dsk.data_block_size)
                        sp -= dsk.data_block_size;
                    else
                        inode_space_stats.erase(space_id);
                    used_blocks--;
                    big_to_flush++;
                }
            }
            else if (IS_IN_FLIGHT(dirty_it->second.state))
            {
                // mark_stable should never be called for in-flight or submitted writes
                printf(
                    "BUG: Attempt to mark_stable object %jx:%jx v%ju state of which is %x\n",
                    dirty_it->first.oid.inode, dirty_it->first.oid.stripe, dirty_it->first.version,
                    dirty_it->second.state
                );
                exit(1);
            }
            if (forget_dirty && (IS_BIG_WRITE(dirty_it->second.state) ||
                IS_DELETE(dirty_it->second.state)))
            {
                // Big write overrides all previous dirty entries
                auto erase_end = dirty_it;
                while (dirty_it != dirty_db.begin())
                {
                    dirty_it--;
                    if (dirty_it->first.oid != v.oid)
                    {
                        dirty_it++;
                        break;
                    }
                }
                auto & clean_db = clean_db_shard(v.oid);
                auto clean_it = clean_db.find(v.oid);
                uint64_t clean_loc = clean_it != clean_db.end()
                    ? clean_it->second.location : UINT64_MAX;
                erase_dirty(dirty_it, erase_end, clean_loc);
                break;
            }
            if (was_stable || dirty_it == dirty_db.begin())
            {
                break;
            }
            dirty_it--;
            if (dirty_it->first.oid != v.oid)
            {
                break;
            }
        }
        flusher->enqueue_flush(v);
    }
    auto unstab_it = unstable_writes.find(v.oid);
    if (unstab_it != unstable_writes.end() &&
        unstab_it->second <= v.version)
    {
        unstable_writes.erase(unstab_it);
    }
}
