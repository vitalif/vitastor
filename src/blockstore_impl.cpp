// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "blockstore_impl.h"

blockstore_impl_t::blockstore_impl_t(blockstore_config_t & config, ring_loop_t *ringloop, timerfd_manager_t *tfd)
{
    assert(sizeof(blockstore_op_private_t) <= BS_OP_PRIVATE_DATA_SIZE);
    this->tfd = tfd;
    this->ringloop = ringloop;
    ring_consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(&ring_consumer);
    initialized = 0;
    parse_config(config);
    zero_object = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, dsk.data_block_size);
    try
    {
        dsk.open_data();
        dsk.open_meta();
        dsk.open_journal();
        calc_lengths();
        data_alloc = new allocator(dsk.block_count);
    }
    catch (std::exception & e)
    {
        dsk.close_all();
        throw;
    }
    flusher = new journal_flusher_t(this);
}

blockstore_impl_t::~blockstore_impl_t()
{
    delete data_alloc;
    delete flusher;
    free(zero_object);
    ringloop->unregister_consumer(&ring_consumer);
    dsk.close_all();
    if (metadata_buffer)
        free(metadata_buffer);
    if (clean_bitmap)
        free(clean_bitmap);
}

bool blockstore_impl_t::is_started()
{
    return initialized == 10;
}

bool blockstore_impl_t::is_stalled()
{
    return queue_stall;
}

// main event loop - produce requests
void blockstore_impl_t::loop()
{
    // FIXME: initialized == 10 is ugly
    if (initialized != 10)
    {
        // read metadata, then journal
        if (initialized == 0)
        {
            metadata_init_reader = new blockstore_init_meta(this);
            initialized = 1;
        }
        if (initialized == 1)
        {
            int res = metadata_init_reader->loop();
            if (!res)
            {
                delete metadata_init_reader;
                metadata_init_reader = NULL;
                journal_init_reader = new blockstore_init_journal(this);
                initialized = 2;
            }
        }
        if (initialized == 2)
        {
            int res = journal_init_reader->loop();
            if (!res)
            {
                delete journal_init_reader;
                journal_init_reader = NULL;
                if (journal.flush_journal)
                    initialized = 3;
                else
                    initialized = 10;
                ringloop->wakeup();
            }
        }
        if (initialized == 3)
        {
            if (readonly)
            {
                printf("Can't flush the journal in readonly mode\n");
                exit(1);
            }
            flusher->loop();
            ringloop->submit();
        }
    }
    else
    {
        // try to submit ops
        unsigned initial_ring_space = ringloop->space_left();
        // has_writes == 0 - no writes before the current queue item
        // has_writes == 1 - some writes in progress
        // has_writes == 2 - tried to submit some writes, but failed
        int has_writes = 0, op_idx = 0, new_idx = 0;
        for (; op_idx < submit_queue.size(); op_idx++, new_idx++)
        {
            auto op = submit_queue[op_idx];
            submit_queue[new_idx] = op;
            // FIXME: This needs some simplification
            // Writes should not block reads if the ring is not full and reads don't depend on them
            // In all other cases we should stop submission
            if (PRIV(op)->wait_for)
            {
                check_wait(op);
                if (PRIV(op)->wait_for == WAIT_SQE)
                {
                    break;
                }
                else if (PRIV(op)->wait_for)
                {
                    if (op->opcode == BS_OP_WRITE || op->opcode == BS_OP_WRITE_STABLE || op->opcode == BS_OP_DELETE)
                    {
                        has_writes = 2;
                    }
                    continue;
                }
            }
            unsigned prev_sqe_pos = ringloop->save();
            // 0 = can't submit
            // 1 = in progress
            // 2 = can be removed from queue
            int wr_st = 0;
            if (op->opcode == BS_OP_READ)
            {
                wr_st = dequeue_read(op);
            }
            else if (op->opcode == BS_OP_WRITE || op->opcode == BS_OP_WRITE_STABLE)
            {
                if (has_writes == 2)
                {
                    // Some writes already could not be submitted
                    continue;
                }
                wr_st = dequeue_write(op);
                has_writes = wr_st > 0 ? 1 : 2;
            }
            else if (op->opcode == BS_OP_DELETE)
            {
                if (has_writes == 2)
                {
                    // Some writes already could not be submitted
                    continue;
                }
                wr_st = dequeue_del(op);
                has_writes = wr_st > 0 ? 1 : 2;
            }
            else if (op->opcode == BS_OP_SYNC)
            {
                // wait for all small writes to be submitted
                // wait for all big writes to complete, submit data device fsync
                // wait for the data device fsync to complete, then submit journal writes for big writes
                // then submit an fsync operation
                if (has_writes)
                {
                    // Can't submit SYNC before previous writes
                    continue;
                }
                wr_st = continue_sync(op, false);
                if (wr_st != 2)
                {
                    has_writes = wr_st > 0 ? 1 : 2;
                }
            }
            else if (op->opcode == BS_OP_STABLE)
            {
                wr_st = dequeue_stable(op);
            }
            else if (op->opcode == BS_OP_ROLLBACK)
            {
                wr_st = dequeue_rollback(op);
            }
            else if (op->opcode == BS_OP_LIST)
            {
                // LIST doesn't have to be blocked by previous modifications
                process_list(op);
                wr_st = 2;
            }
            if (wr_st == 2)
            {
                submit_queue[op_idx] = NULL;
                new_idx--;
            }
            if (wr_st == 0)
            {
                ringloop->restore(prev_sqe_pos);
                if (PRIV(op)->wait_for == WAIT_SQE)
                {
                    // ring is full, stop submission
                    break;
                }
            }
        }
        if (op_idx != new_idx)
        {
            while (op_idx < submit_queue.size())
            {
                submit_queue[new_idx++] = submit_queue[op_idx++];
            }
            submit_queue.resize(new_idx);
        }
        if (!readonly)
        {
            flusher->loop();
        }
        int ret = ringloop->submit();
        if (ret < 0)
        {
            throw std::runtime_error(std::string("io_uring_submit: ") + strerror(-ret));
        }
        for (auto s: journal.submitting_sectors)
        {
            // Mark journal sector writes as submitted
            journal.sector_info[s].submit_id = 0;
        }
        journal.submitting_sectors.clear();
        if ((initial_ring_space - ringloop->space_left()) > 0)
        {
            live = true;
        }
        queue_stall = !live && !ringloop->has_work();
        live = false;
    }
}

bool blockstore_impl_t::is_safe_to_stop()
{
    // It's safe to stop blockstore when there are no in-flight operations,
    // no in-progress syncs and flusher isn't doing anything
    if (submit_queue.size() > 0 || !readonly && flusher->is_active())
    {
        return false;
    }
    if (unsynced_big_writes.size() > 0 || unsynced_small_writes.size() > 0)
    {
        if (!readonly && !stop_sync_submitted)
        {
            // We should sync the blockstore before unmounting
            blockstore_op_t *op = new blockstore_op_t;
            op->opcode = BS_OP_SYNC;
            op->buf = NULL;
            op->callback = [](blockstore_op_t *op)
            {
                delete op;
            };
            enqueue_op(op);
            stop_sync_submitted = true;
        }
        return false;
    }
    return true;
}

void blockstore_impl_t::check_wait(blockstore_op_t *op)
{
    if (PRIV(op)->wait_for == WAIT_SQE)
    {
        if (ringloop->sqes_left() < PRIV(op)->wait_detail)
        {
            // stop submission if there's still no free space
#ifdef BLOCKSTORE_DEBUG
            printf("Still waiting for %lu SQE(s)\n", PRIV(op)->wait_detail);
#endif
            return;
        }
        PRIV(op)->wait_for = 0;
    }
    else if (PRIV(op)->wait_for == WAIT_JOURNAL)
    {
        if (journal.used_start == PRIV(op)->wait_detail)
        {
            // do not submit
#ifdef BLOCKSTORE_DEBUG
            printf("Still waiting to flush journal offset %08lx\n", PRIV(op)->wait_detail);
#endif
            return;
        }
        flusher->release_trim();
        PRIV(op)->wait_for = 0;
    }
    else if (PRIV(op)->wait_for == WAIT_JOURNAL_BUFFER)
    {
        int next = ((journal.cur_sector + 1) % journal.sector_count);
        if (journal.sector_info[next].flush_count > 0 ||
            journal.sector_info[next].dirty)
        {
            // do not submit
#ifdef BLOCKSTORE_DEBUG
            printf("Still waiting for a journal buffer\n");
#endif
            return;
        }
        PRIV(op)->wait_for = 0;
    }
    else
    {
        throw std::runtime_error("BUG: op->wait_for value is unexpected");
    }
}

void blockstore_impl_t::enqueue_op(blockstore_op_t *op)
{
    if (op->opcode < BS_OP_MIN || op->opcode > BS_OP_MAX ||
        ((op->opcode == BS_OP_READ || op->opcode == BS_OP_WRITE || op->opcode == BS_OP_WRITE_STABLE) && (
            op->offset >= dsk.data_block_size ||
            op->len > dsk.data_block_size-op->offset ||
            (op->len % dsk.disk_alignment)
        )) ||
        readonly && op->opcode != BS_OP_READ && op->opcode != BS_OP_LIST)
    {
        // Basic verification not passed
        op->retval = -EINVAL;
        std::function<void (blockstore_op_t*)>(op->callback)(op);
        return;
    }
    if (op->opcode == BS_OP_SYNC_STAB_ALL)
    {
        std::function<void(blockstore_op_t*)> *old_callback = new std::function<void(blockstore_op_t*)>(op->callback);
        op->opcode = BS_OP_SYNC;
        op->callback = [this, old_callback](blockstore_op_t *op)
        {
            if (op->retval >= 0 && unstable_writes.size() > 0)
            {
                op->opcode = BS_OP_STABLE;
                op->len = unstable_writes.size();
                obj_ver_id *vers = new obj_ver_id[op->len];
                op->buf = vers;
                int i = 0;
                for (auto it = unstable_writes.begin(); it != unstable_writes.end(); it++, i++)
                {
                    vers[i] = {
                        .oid = it->first,
                        .version = it->second,
                    };
                }
                unstable_writes.clear();
                op->callback = [old_callback](blockstore_op_t *op)
                {
                    obj_ver_id *vers = (obj_ver_id*)op->buf;
                    delete[] vers;
                    op->buf = NULL;
                    (*old_callback)(op);
                    delete old_callback;
                };
                this->enqueue_op(op);
            }
            else
            {
                (*old_callback)(op);
                delete old_callback;
            }
        };
    }
    if ((op->opcode == BS_OP_WRITE || op->opcode == BS_OP_WRITE_STABLE || op->opcode == BS_OP_DELETE) && !enqueue_write(op))
    {
        std::function<void (blockstore_op_t*)>(op->callback)(op);
        return;
    }
    // Call constructor without allocating memory. We'll call destructor before returning op back
    new ((void*)op->private_data) blockstore_op_private_t;
    PRIV(op)->wait_for = 0;
    PRIV(op)->op_state = 0;
    PRIV(op)->pending_ops = 0;
    submit_queue.push_back(op);
    ringloop->wakeup();
}

static bool replace_stable(object_id oid, uint64_t version, int search_start, int search_end, obj_ver_id* list)
{
    while (search_start < search_end)
    {
        int pos = search_start+(search_end-search_start)/2;
        if (oid < list[pos].oid)
        {
            search_end = pos;
        }
        else if (list[pos].oid < oid)
        {
            search_start = pos+1;
        }
        else
        {
            list[pos].version = version;
            return true;
        }
    }
    return false;
}

blockstore_clean_db_t& blockstore_impl_t::clean_db_shard(object_id oid)
{
    uint64_t pg_num = 0;
    uint64_t pool_id = (oid.inode >> (64-POOL_ID_BITS));
    auto sh_it = clean_db_settings.find(pool_id);
    if (sh_it != clean_db_settings.end())
    {
        // like map_to_pg()
        pg_num = (oid.stripe / sh_it->second.pg_stripe_size) % sh_it->second.pg_count + 1;
    }
    return clean_db_shards[(pool_id << (64-POOL_ID_BITS)) | pg_num];
}

void blockstore_impl_t::reshard_clean_db(pool_id_t pool, uint32_t pg_count, uint32_t pg_stripe_size)
{
    uint64_t pool_id = (uint64_t)pool;
    std::map<pool_pg_id_t, blockstore_clean_db_t> new_shards;
    auto sh_it = clean_db_shards.lower_bound((pool_id << (64-POOL_ID_BITS)));
    while (sh_it != clean_db_shards.end() &&
        (sh_it->first >> (64-POOL_ID_BITS)) == pool_id)
    {
        for (auto & pair: sh_it->second)
        {
            // like map_to_pg()
            uint64_t pg_num = (pair.first.stripe / pg_stripe_size) % pg_count + 1;
            uint64_t shard_id = (pool_id << (64-POOL_ID_BITS)) | pg_num;
            new_shards[shard_id][pair.first] = pair.second;
        }
        clean_db_shards.erase(sh_it++);
    }
    for (sh_it = new_shards.begin(); sh_it != new_shards.end(); sh_it++)
    {
        auto & to = clean_db_shards[sh_it->first];
        to.swap(sh_it->second);
    }
    clean_db_settings[pool_id] = (pool_shard_settings_t){
        .pg_count = pg_count,
        .pg_stripe_size = pg_stripe_size,
    };
}

void blockstore_impl_t::process_list(blockstore_op_t *op)
{
    uint32_t list_pg = op->offset+1;
    uint32_t pg_count = op->len;
    uint64_t pg_stripe_size = op->oid.stripe;
    uint64_t min_inode = op->oid.inode;
    uint64_t max_inode = op->version;
    // Check PG
    if (pg_count != 0 && (pg_stripe_size < MIN_DATA_BLOCK_SIZE || list_pg > pg_count))
    {
        op->retval = -EINVAL;
        FINISH_OP(op);
        return;
    }
    // Check if the DB needs resharding
    // (we don't know about PGs from the beginning, we only create "shards" here)
    uint64_t first_shard = 0, last_shard = UINT64_MAX;
    if (min_inode != 0 &&
        // Check if min_inode == max_inode == pool_id<<N, i.e. this is a pool listing
        (min_inode >> (64-POOL_ID_BITS)) == (max_inode >> (64-POOL_ID_BITS)))
    {
        pool_id_t pool_id = (min_inode >> (64-POOL_ID_BITS));
        if (pg_count > 1)
        {
            // Per-pg listing
            auto sh_it = clean_db_settings.find(pool_id);
            if (sh_it == clean_db_settings.end() ||
                sh_it->second.pg_count != pg_count ||
                sh_it->second.pg_stripe_size != pg_stripe_size)
            {
                reshard_clean_db(pool_id, pg_count, pg_stripe_size);
            }
            first_shard = last_shard = ((uint64_t)pool_id << (64-POOL_ID_BITS)) | list_pg;
        }
        else
        {
            // Per-pool listing
            first_shard = ((uint64_t)pool_id << (64-POOL_ID_BITS));
            last_shard = ((uint64_t)(pool_id+1) << (64-POOL_ID_BITS)) - 1;
        }
    }
    // Copy clean_db entries
    int stable_count = 0, stable_alloc = 0;
    if (min_inode != max_inode)
    {
        for (auto shard_it = clean_db_shards.lower_bound(first_shard);
            shard_it != clean_db_shards.end() && shard_it->first <= last_shard;
            shard_it++)
        {
            auto & clean_db = shard_it->second;
            stable_alloc += clean_db.size();
        }
    }
    else
    {
        stable_alloc = 32768;
    }
    obj_ver_id *stable = (obj_ver_id*)malloc(sizeof(obj_ver_id) * stable_alloc);
    if (!stable)
    {
        op->retval = -ENOMEM;
        FINISH_OP(op);
        return;
    }
    for (auto shard_it = clean_db_shards.lower_bound(first_shard);
        shard_it != clean_db_shards.end() && shard_it->first <= last_shard;
        shard_it++)
    {
        auto & clean_db = shard_it->second;
        auto clean_it = clean_db.begin(), clean_end = clean_db.end();
        if ((min_inode != 0 || max_inode != 0) && min_inode <= max_inode)
        {
            clean_it = clean_db.lower_bound({
                .inode = min_inode,
                .stripe = 0,
            });
            clean_end = clean_db.upper_bound({
                .inode = max_inode,
                .stripe = UINT64_MAX,
            });
        }
        for (; clean_it != clean_end; clean_it++)
        {
            if (stable_count >= stable_alloc)
            {
                stable_alloc *= 2;
                stable = (obj_ver_id*)realloc(stable, sizeof(obj_ver_id) * stable_alloc);
                if (!stable)
                {
                    op->retval = -ENOMEM;
                    FINISH_OP(op);
                    return;
                }
            }
            stable[stable_count++] = {
                .oid = clean_it->first,
                .version = clean_it->second.version,
            };
        }
    }
    if (first_shard != last_shard)
    {
        // If that's not a per-PG listing, sort clean entries
        std::sort(stable, stable+stable_count);
    }
    int clean_stable_count = stable_count;
    // Copy dirty_db entries (sorted, too)
    int unstable_count = 0, unstable_alloc = 0;
    obj_ver_id *unstable = NULL;
    {
        auto dirty_it = dirty_db.begin(), dirty_end = dirty_db.end();
        if ((min_inode != 0 || max_inode != 0) && min_inode <= max_inode)
        {
            dirty_it = dirty_db.lower_bound({
                .oid = {
                    .inode = min_inode,
                    .stripe = 0,
                },
                .version = 0,
            });
            dirty_end = dirty_db.upper_bound({
                .oid = {
                    .inode = max_inode,
                    .stripe = UINT64_MAX,
                },
                .version = UINT64_MAX,
            });
        }
        for (; dirty_it != dirty_end; dirty_it++)
        {
            if (!pg_count || ((dirty_it->first.oid.stripe / pg_stripe_size) % pg_count + 1) == list_pg) // like map_to_pg()
            {
                if (IS_DELETE(dirty_it->second.state))
                {
                    // Deletions are always stable, so try to zero out two possible entries
                    if (!replace_stable(dirty_it->first.oid, 0, 0, clean_stable_count, stable))
                    {
                        replace_stable(dirty_it->first.oid, 0, clean_stable_count, stable_count, stable);
                    }
                }
                else if (IS_STABLE(dirty_it->second.state))
                {
                    // First try to replace a clean stable version in the first part of the list
                    if (!replace_stable(dirty_it->first.oid, dirty_it->first.version, 0, clean_stable_count, stable))
                    {
                        // Then try to replace the last dirty stable version in the second part of the list
                        if (stable_count > 0 && stable[stable_count-1].oid == dirty_it->first.oid)
                        {
                            stable[stable_count-1].version = dirty_it->first.version;
                        }
                        else
                        {
                            if (stable_count >= stable_alloc)
                            {
                                stable_alloc += 32768;
                                stable = (obj_ver_id*)realloc(stable, sizeof(obj_ver_id) * stable_alloc);
                                if (!stable)
                                {
                                    if (unstable)
                                        free(unstable);
                                    op->retval = -ENOMEM;
                                    FINISH_OP(op);
                                    return;
                                }
                            }
                            stable[stable_count++] = dirty_it->first;
                        }
                    }
                }
                else
                {
                    if (unstable_count >= unstable_alloc)
                    {
                        unstable_alloc += 32768;
                        unstable = (obj_ver_id*)realloc(unstable, sizeof(obj_ver_id) * unstable_alloc);
                        if (!unstable)
                        {
                            if (stable)
                                free(stable);
                            op->retval = -ENOMEM;
                            FINISH_OP(op);
                            return;
                        }
                    }
                    unstable[unstable_count++] = dirty_it->first;
                }
            }
        }
    }
    // Remove zeroed out stable entries
    int j = 0;
    for (int i = 0; i < stable_count; i++)
    {
        if (stable[i].version != 0)
        {
            stable[j++] = stable[i];
        }
    }
    stable_count = j;
    if (stable_count+unstable_count > stable_alloc)
    {
        stable_alloc = stable_count+unstable_count;
        stable = (obj_ver_id*)realloc(stable, sizeof(obj_ver_id) * stable_alloc);
        if (!stable)
        {
            if (unstable)
                free(unstable);
            op->retval = -ENOMEM;
            FINISH_OP(op);
            return;
        }
    }
    // Copy unstable entries
    for (int i = 0; i < unstable_count; i++)
    {
        stable[j++] = unstable[i];
    }
    free(unstable);
    op->version = stable_count;
    op->retval = stable_count+unstable_count;
    op->buf = stable;
    FINISH_OP(op);
}

void blockstore_impl_t::dump_diagnostics()
{
    journal.dump_diagnostics();
    flusher->dump_diagnostics();
}

void blockstore_impl_t::disk_error_abort(const char *op, int retval, int expected)
{
    if (retval == -EAGAIN)
    {
        fprintf(stderr, "EAGAIN error received from a disk %s during flush."
            " It must never happen with io_uring and indicates a kernel bug."
            " Please upgrade your kernel. Aborting.\n", op);
        exit(1);
    }
    fprintf(stderr, "Disk %s failed: result is %d, expected %d. Can't continue, sorry :-(\n", op, retval, expected);
    exit(1);
}
