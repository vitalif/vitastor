// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "impl.h"
#include "internal.h"

namespace v1 {

blockstore_impl_t::blockstore_impl_t(blockstore_config_t & config, ring_loop_i *ringloop, timerfd_manager_t *tfd)
{
    assert(sizeof(blockstore_op_private_t) <= BS_OP_PRIVATE_DATA_SIZE);
    this->tfd = tfd;
    this->ringloop = ringloop;
    ring_consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(&ring_consumer);
    initialized = 0;
    parse_config(config, true);
    try
    {
        dsk.open_data();
        dsk.open_meta();
        dsk.open_journal();
        calc_lengths();
        alloc_dyn_data = dsk.clean_dyn_size > sizeof(void*) || dsk.csum_block_size > 0;
        zero_object = (uint8_t*)memalign_or_die(MEM_ALIGNMENT, dsk.data_block_size);
        data_alloc = new allocator_t(dsk.block_count);
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
    if (zero_object)
        free(zero_object);
    ringloop->unregister_consumer(&ring_consumer);
    dsk.close_all();
    if (metadata_buffer)
        free(metadata_buffer);
    if (clean_bitmaps)
        free(clean_bitmaps);
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
                initialized = 3;
                ringloop->wakeup();
            }
        }
        if (initialized == 3)
        {
            if (!readonly && dsk.discard_on_start)
                dsk.trim_data([this](uint64_t block_num){ return data_alloc->get(block_num); });
            if (journal.flush_journal)
                initialized = 4;
            else
                initialized = 10;
        }
        if (initialized == 4)
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
                // sync only completed writes?
                // wait for the data device fsync to complete, then submit journal writes for big writes
                // then submit an fsync operation
                wr_st = continue_sync(op);
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
                else if (PRIV(op)->wait_for == WAIT_JOURNAL)
                {
                    PRIV(op)->wait_detail2 = (unstable_writes.size()+unstable_unsynced);
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
            if (journal.sector_info[s].submit_id)
                journal.sector_info[s].written = true;
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
        if (ringloop->space_left() < PRIV(op)->wait_detail)
        {
            // stop submission if there's still no free space
#ifdef BLOCKSTORE_DEBUG
            printf("Still waiting for %ju SQE(s)\n", PRIV(op)->wait_detail);
#endif
            return;
        }
        PRIV(op)->wait_for = 0;
    }
    else if (PRIV(op)->wait_for == WAIT_JOURNAL)
    {
        if (journal.used_start == PRIV(op)->wait_detail &&
            (unstable_writes.size()+unstable_unsynced) == PRIV(op)->wait_detail2)
        {
            // do not submit
#ifdef BLOCKSTORE_DEBUG
            printf("Still waiting to flush journal offset %08jx\n", PRIV(op)->wait_detail);
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
    else if (PRIV(op)->wait_for == WAIT_FREE)
    {
        if (!data_alloc->get_free_count() && big_to_flush > 0)
        {
#ifdef BLOCKSTORE_DEBUG
            printf("Still waiting for free space on the data device\n");
#endif
            return;
        }
        flusher->release_trim();
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
        ringloop->set_immediate([op]() { std::function<void (blockstore_op_t*)>(op->callback)(op); });
        return;
    }
    if ((op->opcode == BS_OP_WRITE || op->opcode == BS_OP_WRITE_STABLE || op->opcode == BS_OP_DELETE) && !enqueue_write(op))
    {
        ringloop->set_immediate([op]() { std::function<void (blockstore_op_t*)>(op->callback)(op); });
        return;
    }
    if (op->opcode == BS_OP_SYNC)
    {
        unsynced_queued_ops = 0;
    }
    init_op(op);
    submit_queue.push_back(op);
    ringloop->wakeup();
}

void blockstore_impl_t::init_op(blockstore_op_t *op)
{
    // Call constructor without allocating memory. We'll call destructor before returning op back
    new ((void*)op->private_data) blockstore_op_private_t;
    PRIV(op)->min_flushed_journal_sector = PRIV(op)->max_flushed_journal_sector = 0;
    PRIV(op)->wait_for = 0;
    PRIV(op)->op_state = 0;
    PRIV(op)->pending_ops = 0;
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

struct bs_reshard_state_t
{
    int state = 0;
    uint64_t pool_id = 0;
    uint32_t pg_count = 0;
    uint32_t pg_stripe_size = 0;
    uint64_t chunk_size = 0;
    std::map<pool_pg_id_t, blockstore_clean_db_t> old_shards;
    std::map<pool_pg_id_t, blockstore_clean_db_t> new_shards;
    std::map<pool_pg_id_t, blockstore_clean_db_t>::iterator sh_it;
    blockstore_clean_db_t::iterator obj_it;
};

void* blockstore_impl_t::reshard_start(pool_id_t pool, uint32_t pg_count, uint32_t pg_stripe_size, uint64_t chunk_limit)
{
    auto & settings = clean_db_settings[pool];
    if (settings.pg_count == pg_count && settings.pg_stripe_size == pg_stripe_size)
    {
        return NULL;
    }
    bs_reshard_state_t *st = new bs_reshard_state_t;
    st->state = 0;
    st->pool_id = pool;
    st->pg_count = pg_count;
    st->pg_stripe_size = pg_stripe_size;
    auto sh_it = clean_db_shards.lower_bound((st->pool_id << (64-POOL_ID_BITS)));
    while (sh_it != clean_db_shards.end() &&
        (sh_it->first >> (64-POOL_ID_BITS)) == st->pool_id)
    {
        st->old_shards[sh_it->first] = std::move(sh_it->second);
        clean_db_shards.erase(sh_it++);
    }
    bool finished = reshard_continue(st, chunk_limit);
    return finished ? NULL : st;
}

bool blockstore_impl_t::reshard_continue(void *reshard_state, uint64_t chunk_limit)
{
    bs_reshard_state_t *st = (bs_reshard_state_t*)reshard_state;
    uint64_t chunk_size = 0;
    if (st->state == 1)
        goto resume_1;
    for (st->sh_it = st->old_shards.begin(); st->sh_it != st->old_shards.end(); )
    {
        for (st->obj_it = st->sh_it->second.begin(); st->obj_it != st->sh_it->second.end(); st->obj_it++)
        {
            if (chunk_limit > 0 && chunk_size >= chunk_limit)
            {
                st->state = 1;
                return false;
            }
resume_1:
            // like map_to_pg()
            uint64_t pg_num = (st->obj_it->first.stripe / st->pg_stripe_size) % st->pg_count + 1;
            uint64_t shard_id = (st->pool_id << (64-POOL_ID_BITS)) | pg_num;
            st->new_shards[shard_id][st->obj_it->first] = st->obj_it->second;
            chunk_size++;
        }
        st->old_shards.erase(st->sh_it++);
    }
    for (auto sh_it = st->new_shards.begin(); sh_it != st->new_shards.end(); sh_it++)
    {
        auto & to = clean_db_shards[sh_it->first];
        to.swap(sh_it->second);
    }
    clean_db_settings[st->pool_id] = (pool_shard_settings_t){
        .pg_count = st->pg_count,
        .pg_stripe_size = st->pg_stripe_size,
    };
    delete st;
    return true;
}

void blockstore_impl_t::reshard_abort(void *reshard_state)
{
    bs_reshard_state_t *st = (bs_reshard_state_t*)reshard_state;
    for (auto sh_it = st->old_shards.begin(); sh_it != st->old_shards.end(); sh_it++)
    {
        auto & to = clean_db_shards[sh_it->first];
        to.swap(sh_it->second);
    }
    delete st;
}

void blockstore_impl_t::process_list(blockstore_op_t *op)
{
    uint32_t list_pg = op->pg_number+1;
    uint32_t pg_count = op->pg_count;
    uint64_t pg_stripe_size = op->pg_alignment;
    uint64_t min_inode = op->min_oid.inode;
    uint64_t max_inode = op->max_oid.inode;
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
                // Sharding mismatch
                op->retval = -EAGAIN;
                FINISH_OP(op);
                return;
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
    if (op->list_stable_limit > 0)
    {
        stable_alloc = op->list_stable_limit;
        if (stable_alloc > 1024*1024)
            stable_alloc = 1024*1024;
    }
    if (stable_alloc < 32768)
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
    auto max_oid = op->max_oid;
    bool limited = false;
    pool_pg_id_t last_shard_id = 0;
    for (auto shard_it = clean_db_shards.lower_bound(first_shard);
        shard_it != clean_db_shards.end() && shard_it->first <= last_shard;
        shard_it++)
    {
        auto & clean_db = shard_it->second;
        auto clean_it = clean_db.begin(), clean_end = clean_db.end();
        if (op->min_oid.inode != 0 || op->min_oid.stripe != 0)
        {
            clean_it = clean_db.lower_bound(op->min_oid);
        }
        if ((max_oid.inode != 0 || max_oid.stripe != 0) && !(max_oid < op->min_oid))
        {
            clean_end = clean_db.upper_bound(max_oid);
        }
        for (; clean_it != clean_end; clean_it++)
        {
            if (stable_count >= stable_alloc)
            {
                stable_alloc *= 2;
                obj_ver_id* nst = (obj_ver_id*)realloc(stable, sizeof(obj_ver_id) * stable_alloc);
                if (!nst)
                {
                    op->retval = -ENOMEM;
                    FINISH_OP(op);
                    return;
                }
                stable = nst;
            }
            stable[stable_count++] = {
                .oid = clean_it->first,
                .version = clean_it->second.version,
            };
            if (op->list_stable_limit > 0 && stable_count >= op->list_stable_limit)
            {
                if (!limited)
                {
                    limited = true;
                    max_oid = stable[stable_count-1].oid;
                }
                break;
            }
        }
        if (op->list_stable_limit > 0)
        {
            // To maintain the order, we have to include objects in the same range from other shards
            if (last_shard_id != 0 && last_shard_id != shard_it->first)
                std::sort(stable, stable+stable_count);
            if (stable_count > op->list_stable_limit)
                stable_count = op->list_stable_limit;
        }
        last_shard_id = shard_it->first;
    }
    if (op->list_stable_limit == 0 && first_shard != last_shard)
    {
        // If that's not a per-PG listing, sort clean entries (already sorted if list_stable_limit != 0)
        std::sort(stable, stable+stable_count);
    }
    int clean_stable_count = stable_count;
    // Copy dirty_db entries (sorted, too)
    int unstable_count = 0, unstable_alloc = 0;
    obj_ver_id *unstable = NULL;
    {
        auto dirty_it = dirty_db.begin(), dirty_end = dirty_db.end();
        if (op->min_oid.inode != 0 || op->min_oid.stripe != 0)
        {
            dirty_it = dirty_db.lower_bound({
                .oid = op->min_oid,
                .version = 0,
            });
        }
        if ((max_oid.inode != 0 || max_oid.stripe != 0) && !(max_oid < op->min_oid))
        {
            dirty_end = dirty_db.upper_bound({
                .oid = max_oid,
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
                else if (IS_STABLE(dirty_it->second.state) || (dirty_it->second.state & BS_ST_INSTANT))
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
                                obj_ver_id *nst = (obj_ver_id*)realloc(stable, sizeof(obj_ver_id) * stable_alloc);
                                if (!nst)
                                {
                                    if (unstable)
                                        free(unstable);
                                    op->retval = -ENOMEM;
                                    FINISH_OP(op);
                                    return;
                                }
                                stable = nst;
                            }
                            stable[stable_count++] = dirty_it->first;
                        }
                    }
                    if (op->list_stable_limit > 0 && stable_count >= op->list_stable_limit)
                    {
                        // Stop here
                        break;
                    }
                }
                else
                {
                    if (unstable_count >= unstable_alloc)
                    {
                        unstable_alloc += 32768;
                        obj_ver_id *nst = (obj_ver_id*)realloc(unstable, sizeof(obj_ver_id) * unstable_alloc);
                        if (!nst)
                        {
                            if (stable)
                                free(stable);
                            op->retval = -ENOMEM;
                            FINISH_OP(op);
                            return;
                        }
                        unstable = nst;
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
        obj_ver_id *nst = (obj_ver_id*)realloc(stable, sizeof(obj_ver_id) * stable_alloc);
        if (!nst)
        {
            if (unstable)
                free(unstable);
            op->retval = -ENOMEM;
            FINISH_OP(op);
            return;
        }
        stable = nst;
    }
    // Copy unstable entries
    for (int i = 0; i < unstable_count; i++)
    {
        stable[j++] = unstable[i];
    }
    free(unstable);
    op->version = stable_count;
    op->retval = stable_count+unstable_count;
    op->buf = (uint8_t*)stable;
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

const std::map<uint64_t, uint64_t> & blockstore_impl_t::get_inode_space_stats()
{
    return inode_space_stats;
}

void blockstore_impl_t::set_no_inode_stats(const std::vector<uint64_t> & pool_ids)
{
    for (auto & np: no_inode_stats)
    {
        np.second = 2;
    }
    for (auto pool_id: pool_ids)
    {
        if (!no_inode_stats[pool_id])
            recalc_inode_space_stats(pool_id, false);
        no_inode_stats[pool_id] = 1;
    }
    for (auto np_it = no_inode_stats.begin(); np_it != no_inode_stats.end(); )
    {
        if (np_it->second == 2)
        {
            recalc_inode_space_stats(np_it->first, true);
            no_inode_stats.erase(np_it++);
        }
        else
            np_it++;
    }
}

void blockstore_impl_t::recalc_inode_space_stats(uint64_t pool_id, bool per_inode)
{
    auto sp_begin = inode_space_stats.lower_bound((pool_id << (64-POOL_ID_BITS)));
    auto sp_end = inode_space_stats.lower_bound(((pool_id+1) << (64-POOL_ID_BITS)));
    inode_space_stats.erase(sp_begin, sp_end);
    auto sh_it = clean_db_shards.lower_bound((pool_id << (64-POOL_ID_BITS)));
    while (sh_it != clean_db_shards.end() &&
        (sh_it->first >> (64-POOL_ID_BITS)) == pool_id)
    {
        for (auto & pair: sh_it->second)
        {
            uint64_t space_id = per_inode ? pair.first.inode : (pool_id << (64-POOL_ID_BITS));
            inode_space_stats[space_id] += dsk.data_block_size;
        }
        sh_it++;
    }
    object_id last_oid = {};
    bool last_exists = false;
    auto dirty_it = dirty_db.lower_bound((obj_ver_id){ .oid = { .inode = (pool_id << (64-POOL_ID_BITS)) } });
    while (dirty_it != dirty_db.end() && (dirty_it->first.oid.inode >> (64-POOL_ID_BITS)) == pool_id)
    {
        if (IS_STABLE(dirty_it->second.state) && (IS_BIG_WRITE(dirty_it->second.state) || IS_DELETE(dirty_it->second.state)))
        {
            bool exists = false;
            if (last_oid == dirty_it->first.oid)
            {
                exists = last_exists;
            }
            else
            {
                auto & clean_db = clean_db_shard(dirty_it->first.oid);
                auto clean_it = clean_db.find(dirty_it->first.oid);
                exists = clean_it != clean_db.end();
            }
            uint64_t space_id = per_inode ? dirty_it->first.oid.inode : (pool_id << (64-POOL_ID_BITS));
            if (IS_BIG_WRITE(dirty_it->second.state))
            {
                if (!exists)
                    inode_space_stats[space_id] += dsk.data_block_size;
                last_exists = true;
            }
            else
            {
                if (exists)
                {
                    auto & sp = inode_space_stats[space_id];
                    if (sp > dsk.data_block_size)
                        sp -= dsk.data_block_size;
                    else
                        inode_space_stats.erase(space_id);
                }
                last_exists = false;
            }
            last_oid = dirty_it->first.oid;
        }
        dirty_it++;
    }
}

std::string blockstore_impl_t::get_op_diag(blockstore_op_t *op)
{
    char buf[256];
    auto priv = PRIV(op);
    if (priv->wait_for)
        snprintf(buf, sizeof(buf), "state=%d wait=%d (detail=%ju)", priv->op_state, priv->wait_for, priv->wait_detail);
    else
        snprintf(buf, sizeof(buf), "state=%d", priv->op_state);
    return std::string(buf);
}

} // namespace v1
