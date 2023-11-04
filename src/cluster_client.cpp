// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <stdexcept>
#include <assert.h>
#include "cluster_client_impl.h"
#include "http_client.h" // json_is_true

cluster_client_t::cluster_client_t(ring_loop_t *ringloop, timerfd_manager_t *tfd, json11::Json & config)
{
    wb = new writeback_cache_t();

    cli_config = config.object_items();
    file_config = osd_messenger_t::read_config(config);
    config = osd_messenger_t::merge_configs(cli_config, file_config, etcd_global_config, {});

    this->ringloop = ringloop;
    this->tfd = tfd;

    msgr.osd_num = 0;
    msgr.tfd = tfd;
    msgr.ringloop = ringloop;
    msgr.repeer_pgs = [this](osd_num_t peer_osd)
    {
        if (msgr.osd_peer_fds.find(peer_osd) != msgr.osd_peer_fds.end())
        {
            // peer_osd just connected
            continue_ops();
            continue_lists();
            continue_raw_ops(peer_osd);
        }
        else
        {
            // peer_osd just dropped connection
            // determine WHICH dirty_buffers are now obsolete and repeat them
            if (wb->repeat_ops_for(this, peer_osd) > 0)
            {
                continue_ops();
            }
        }
    };
    msgr.exec_op = [this](osd_op_t *op)
    {
        // Garbage in
        fprintf(stderr, "Incoming garbage from peer %d\n", op->peer_fd);
        msgr.stop_client(op->peer_fd);
        delete op;
    };
    msgr.parse_config(config);

    st_cli.tfd = tfd;
    st_cli.on_load_config_hook = [this](json11::Json::object & cfg) { on_load_config_hook(cfg); };
    st_cli.on_change_osd_state_hook = [this](uint64_t peer_osd) { on_change_osd_state_hook(peer_osd); };
    st_cli.on_change_hook = [this](std::map<std::string, etcd_kv_t> & changes) { on_change_hook(changes); };
    st_cli.on_load_pgs_hook = [this](bool success) { on_load_pgs_hook(success); };
    st_cli.on_reload_hook = [this]() { st_cli.load_global_config(); };

    st_cli.parse_config(config);
    st_cli.load_global_config();

    scrap_buffer_size = SCRAP_BUFFER_SIZE;
    scrap_buffer = malloc_or_die(scrap_buffer_size);
}

cluster_client_t::~cluster_client_t()
{
    msgr.repeer_pgs = [](osd_num_t){};
    if (ringloop)
    {
        ringloop->unregister_consumer(&consumer);
    }
    free(scrap_buffer);
    delete wb;
    wb = NULL;
}

cluster_op_t::~cluster_op_t()
{
    if (buf)
    {
        free(buf);
        buf = NULL;
    }
    if (bitmap_buf)
    {
        free(bitmap_buf);
        part_bitmaps = NULL;
        bitmap_buf = NULL;
    }
}

void cluster_client_t::continue_raw_ops(osd_num_t peer_osd)
{
    auto it = raw_ops.find(peer_osd);
    while (it != raw_ops.end() && it->first == peer_osd)
    {
        auto op = it->second;
        op->op_type = OSD_OP_OUT;
        op->peer_fd = msgr.osd_peer_fds.at(peer_osd);
        msgr.outbox_push(op);
        raw_ops.erase(it++);
    }
}

void cluster_client_t::init_msgr()
{
    if (msgr_initialized)
        return;
    msgr.init();
    msgr_initialized = true;
    if (ringloop)
    {
        consumer.loop = [this]()
        {
            msgr.read_requests();
            msgr.send_replies();
            this->ringloop->submit();
        };
        ringloop->register_consumer(&consumer);
    }
}

void cluster_client_t::unshift_op(cluster_op_t *op)
{
    op->next = op_queue_head;
    if (op_queue_head)
    {
        op_queue_head->prev = op;
        op_queue_head = op;
    }
    else
        op_queue_tail = op_queue_head = op;
    inc_wait(op->opcode, op->flags, op->next, 1);
}

void cluster_client_t::calc_wait(cluster_op_t *op)
{
    op->prev_wait = 0;
    if (op->opcode == OSD_OP_WRITE)
    {
        for (auto prev = op->prev; prev; prev = prev->prev)
        {
            if (prev->opcode == OSD_OP_SYNC ||
                prev->opcode == OSD_OP_WRITE && !(op->flags & OP_FLUSH_BUFFER) && (prev->flags & OP_FLUSH_BUFFER))
            {
                op->prev_wait++;
            }
        }
        if (!op->prev_wait)
            continue_rw(op);
    }
    else if (op->opcode == OSD_OP_SYNC)
    {
        for (auto prev = op->prev; prev; prev = prev->prev)
        {
            if (prev->opcode == OSD_OP_SYNC || prev->opcode == OSD_OP_WRITE && (!(prev->flags & OP_IMMEDIATE_COMMIT) || enable_writeback))
            {
                op->prev_wait++;
            }
        }
        if (!op->prev_wait)
            continue_sync(op);
    }
    else /* if (op->opcode == OSD_OP_READ || op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP) */
    {
        continue_rw(op);
    }
}

void cluster_client_t::inc_wait(uint64_t opcode, uint64_t flags, cluster_op_t *next, int inc)
{
    if (opcode != OSD_OP_WRITE && opcode != OSD_OP_SYNC)
    {
        return;
    }
    cluster_op_t *bh_ops_local[32], **bh_ops = bh_ops_local;
    int bh_op_count = 0, bh_op_max = 32;
    while (next)
    {
        auto n2 = next->next;
        if (opcode == OSD_OP_WRITE
            ? (next->opcode == OSD_OP_SYNC && (!(flags & OP_IMMEDIATE_COMMIT) || enable_writeback) ||
                next->opcode == OSD_OP_WRITE && (flags & OP_FLUSH_BUFFER) && !(next->flags & OP_FLUSH_BUFFER))
            : (next->opcode == OSD_OP_SYNC || next->opcode == OSD_OP_WRITE))
        {
            next->prev_wait += inc;
            assert(next->prev_wait >= 0);
            if (!next->prev_wait)
            {
                // Kind of std::vector with local "small vector optimisation"
                if (bh_op_count >= bh_op_max)
                {
                    bh_op_max *= 2;
                    cluster_op_t **n = (cluster_op_t**)malloc_or_die(sizeof(cluster_op_t*) * bh_op_max);
                    memcpy(n, bh_ops, sizeof(cluster_op_t*) * bh_op_count);
                    if (bh_ops != bh_ops_local)
                    {
                        free(bh_ops);
                    }
                    bh_ops = n;
                }
                bh_ops[bh_op_count++] = next;
            }
        }
        next = n2;
    }
    for (int i = 0; i < bh_op_count; i++)
    {
        cluster_op_t *next = bh_ops[i];
        if (next->opcode == OSD_OP_SYNC)
            continue_sync(next);
        else
            continue_rw(next);
    }
    if (bh_ops != bh_ops_local)
    {
        free(bh_ops);
    }
}

void cluster_client_t::erase_op(cluster_op_t *op)
{
    uint64_t opcode = op->opcode, flags = op->flags;
    cluster_op_t *next = op->next;
    if (op->prev)
        op->prev->next = op->next;
    if (op->next)
        op->next->prev = op->prev;
    if (op_queue_head == op)
        op_queue_head = op->next;
    if (op_queue_tail == op)
        op_queue_tail = op->prev;
    op->next = op->prev = NULL;
    if (flags & OP_FLUSH_BUFFER)
    {
        // Completed flushes change writeback buffer states,
        // so the callback should be run before inc_wait()
        // which may continue following SYNCs, but these SYNCs
        // should know about the changed buffer state
        // This is ugly but this is the way we do it
        std::function<void(cluster_op_t*)>(op->callback)(op);
    }
    if (!(flags & OP_IMMEDIATE_COMMIT) || enable_writeback)
    {
        inc_wait(opcode, flags, next, -1);
    }
    if (!(flags & OP_FLUSH_BUFFER))
    {
        // Call callback at the end to avoid inconsistencies in prev_wait
        // if the callback adds more operations itself
        std::function<void(cluster_op_t*)>(op->callback)(op);
    }
    if (flags & OP_FLUSH_BUFFER)
    {
        int i = 0;
        while (i < wb->writeback_overflow.size() && wb->writebacks_active < client_max_writeback_iodepth)
        {
            execute_internal(wb->writeback_overflow[i]);
            i++;
        }
        if (i > 0)
        {
            wb->writeback_overflow.erase(wb->writeback_overflow.begin(), wb->writeback_overflow.begin()+i);
        }
    }
}

void cluster_client_t::continue_ops(bool up_retry)
{
    if (!pgs_loaded)
    {
        // We're offline
        return;
    }
    if (continuing_ops)
    {
        // Attempt to reenter the function
        return;
    }
restart:
    continuing_ops = 1;
    for (auto op = op_queue_head; op; )
    {
        cluster_op_t *next_op = op->next;
        if (!op->up_wait || up_retry)
        {
            op->up_wait = false;
            if (!op->prev_wait)
            {
                if (op->opcode == OSD_OP_SYNC)
                    continue_sync(op);
                else
                    continue_rw(op);
            }
        }
        op = next_op;
        if (continuing_ops == 2)
        {
            goto restart;
        }
    }
    continuing_ops = 0;
}

void cluster_client_t::on_load_config_hook(json11::Json::object & etcd_global_config)
{
    this->etcd_global_config = etcd_global_config;
    config = osd_messenger_t::merge_configs(cli_config, file_config, etcd_global_config, {});
    // client_max_dirty_bytes/client_dirty_limit
    if (config.find("client_max_dirty_bytes") != config.end())
    {
        client_max_dirty_bytes = config["client_max_dirty_bytes"].uint64_value();
    }
    else if (config.find("client_dirty_limit") != config.end())
    {
        // Old name
        client_max_dirty_bytes = config["client_dirty_limit"].uint64_value();
    }
    else
        client_max_dirty_bytes = 0;
    if (!client_max_dirty_bytes)
    {
        client_max_dirty_bytes = DEFAULT_CLIENT_MAX_DIRTY_BYTES;
    }
    // client_max_dirty_ops
    client_max_dirty_ops = config["client_max_dirty_ops"].uint64_value();
    if (!client_max_dirty_ops)
    {
        client_max_dirty_ops = DEFAULT_CLIENT_MAX_DIRTY_OPS;
    }
    // client_enable_writeback
    enable_writeback = json_is_true(config["client_enable_writeback"]) &&
        json_is_true(config["client_writeback_allowed"]);
    // client_max_buffered_bytes
    client_max_buffered_bytes = config["client_max_buffered_bytes"].uint64_value();
    if (!client_max_buffered_bytes)
    {
        client_max_buffered_bytes = DEFAULT_CLIENT_MAX_BUFFERED_BYTES;
    }
    // client_max_buffered_ops
    client_max_buffered_ops = config["client_max_buffered_ops"].uint64_value();
    if (!client_max_buffered_ops)
    {
        client_max_buffered_ops = DEFAULT_CLIENT_MAX_BUFFERED_OPS;
    }
    // client_max_writeback_iodepth
    client_max_writeback_iodepth = config["client_max_writeback_iodepth"].uint64_value();
    if (!client_max_writeback_iodepth)
    {
        client_max_writeback_iodepth = DEFAULT_CLIENT_MAX_WRITEBACK_IODEPTH;
    }
    // up_wait_retry_interval
    up_wait_retry_interval = config["up_wait_retry_interval"].uint64_value();
    if (!up_wait_retry_interval)
    {
        up_wait_retry_interval = 500;
    }
    else if (up_wait_retry_interval < 50)
    {
        up_wait_retry_interval = 50;
    }
    msgr.parse_config(config);
    st_cli.parse_config(config);
    st_cli.load_pgs();
}

void cluster_client_t::on_load_pgs_hook(bool success)
{
    for (auto pool_item: st_cli.pool_config)
    {
        pg_counts[pool_item.first] = pool_item.second.real_pg_count;
    }
    pgs_loaded = true;
    for (auto fn: on_ready_hooks)
    {
        fn();
    }
    on_ready_hooks.clear();
    for (auto op: offline_ops)
    {
        execute(op);
    }
    offline_ops.clear();
    continue_ops();
}

void cluster_client_t::on_change_hook(std::map<std::string, etcd_kv_t> & changes)
{
    for (auto pool_item: st_cli.pool_config)
    {
        if (pg_counts[pool_item.first] != pool_item.second.real_pg_count)
        {
            // At this point, all pool operations should have been suspended
            // And now they have to be resliced!
            for (auto op = op_queue_head; op; op = op->next)
            {
                if ((op->opcode == OSD_OP_WRITE || op->opcode == OSD_OP_READ ||
                    op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP) &&
                    INODE_POOL(op->cur_inode) == pool_item.first)
                {
                    op->needs_reslice = true;
                }
            }
            pg_counts[pool_item.first] = pool_item.second.real_pg_count;
        }
    }
    continue_ops();
}

bool cluster_client_t::get_immediate_commit(uint64_t inode)
{
    if (enable_writeback)
        return false;
    pool_id_t pool_id = INODE_POOL(inode);
    if (!pool_id)
        return true;
    auto pool_it = st_cli.pool_config.find(pool_id);
    if (pool_it == st_cli.pool_config.end())
        return true;
    return pool_it->second.immediate_commit == IMMEDIATE_ALL;
}

void cluster_client_t::on_change_osd_state_hook(uint64_t peer_osd)
{
    if (msgr.wanted_peers.find(peer_osd) != msgr.wanted_peers.end())
    {
        msgr.connect_peer(peer_osd, st_cli.peer_states[peer_osd]);
    }
}

bool cluster_client_t::is_ready()
{
    return pgs_loaded;
}

void cluster_client_t::on_ready(std::function<void(void)> fn)
{
    if (pgs_loaded)
    {
        fn();
    }
    else
    {
        on_ready_hooks.push_back(fn);
    }
}

bool cluster_client_t::flush()
{
    if (!ringloop)
    {
        if (wb->writeback_queue.size())
        {
            wb->start_writebacks(this, 0);
            cluster_op_t *sync = new cluster_op_t;
            sync->opcode = OSD_OP_SYNC;
            sync->callback = [](cluster_op_t *sync)
            {
                delete sync;
            };
            execute(sync);
        }
        return op_queue_head == NULL;
    }
    bool sync_done = false;
    cluster_op_t *sync = new cluster_op_t;
    sync->opcode = OSD_OP_SYNC;
    sync->callback = [&sync_done](cluster_op_t *sync)
    {
        delete sync;
        sync_done = true;
    };
    execute(sync);
    while (!sync_done)
    {
        ringloop->loop();
        if (!sync_done)
            ringloop->wait();
    }
    return true;
}

/**
 * How writes are synced when immediate_commit is false
 *
 * "Continue" WRITE:
 * 1) if the operation is not sliced yet - slice it
 * 2) if the operation doesn't require reslice - try to connect & send all remaining parts
 * 3) if any of them fail due to disconnected peers or PGs not up, repeat after reconnecting or small timeout
 * 4) if any of them fail due to other errors, fail the operation and forget it from the current "unsynced batch"
 * 5) if PG count changes before all parts are done, wait for all in-progress parts to finish,
 *    throw all results away, reslice and resubmit op
 * 6) when all parts are done, try to "continue" the current SYNC
 * 7) if the operation succeeds, but then some OSDs drop their connections, repeat
 *    parts from the current "unsynced batch" previously sent to those OSDs in any order
 *
 * "Continue" current SYNC:
 * 1) take all unsynced operations from the current batch
 * 2) check if all affected OSDs are still alive
 * 3) if yes, send all SYNCs. otherwise, leave current SYNC as is.
 * 4) if any of them fail due to disconnected peers, repeat SYNC after repeating all writes
 * 5) if any of them fail due to other errors, fail the SYNC operation
 *
 * If writeback caching is turned on and writeback limit is not exhausted:
 * data is just copied and the write is confirmed to the client.
 */
void cluster_client_t::execute(cluster_op_t *op)
{
    if (op->opcode != OSD_OP_SYNC && op->opcode != OSD_OP_READ &&
        op->opcode != OSD_OP_READ_BITMAP && op->opcode != OSD_OP_READ_CHAIN_BITMAP && op->opcode != OSD_OP_WRITE)
    {
        op->retval = -EINVAL;
        std::function<void(cluster_op_t*)>(op->callback)(op);
        return;
    }
    if (!pgs_loaded)
    {
        offline_ops.push_back(op);
        return;
    }
    op->flags = op->flags & OSD_OP_IGNORE_READONLY; // the only allowed flag
    execute_internal(op);
}

void cluster_client_t::execute_internal(cluster_op_t *op)
{
    op->cur_inode = op->inode;
    op->retval = 0;
    // check alignment, readonly flag and so on
    if (!check_rw(op))
    {
        return;
    }
    if (op->opcode == OSD_OP_WRITE && enable_writeback && !(op->flags & OP_FLUSH_BUFFER) &&
        !op->version /* FIXME no CAS writeback */)
    {
        if (wb->writebacks_active >= client_max_writeback_iodepth)
        {
            // Writeback queue is full, postpone the operation
            wb->writeback_overflow.push_back(op);
            return;
        }
        // Just copy and acknowledge the operation
        wb->copy_write(op, CACHE_DIRTY);
        while (wb->writeback_bytes + op->len > client_max_buffered_bytes || wb->writeback_queue_size > client_max_buffered_ops)
        {
            // Initiate some writeback (asynchronously)
            wb->start_writebacks(this, 1);
        }
        op->retval = op->len;
        std::function<void(cluster_op_t*)>(op->callback)(op);
        return;
    }
    if (op->opcode == OSD_OP_WRITE && !(op->flags & OP_IMMEDIATE_COMMIT))
    {
        if (!(op->flags & OP_FLUSH_BUFFER))
        {
            wb->copy_write(op, CACHE_WRITTEN);
        }
        if (dirty_bytes >= client_max_dirty_bytes || dirty_ops >= client_max_dirty_ops)
        {
            // Push an extra SYNC operation to flush previous writes
            cluster_op_t *sync_op = new cluster_op_t;
            sync_op->opcode = OSD_OP_SYNC;
            sync_op->flags = OP_FLUSH_BUFFER;
            sync_op->callback = [](cluster_op_t* sync_op)
            {
                delete sync_op;
            };
            execute_internal(sync_op);
        }
        dirty_bytes += op->len;
        dirty_ops++;
    }
    else if (op->opcode == OSD_OP_SYNC)
    {
        // Flush the whole write-back queue first
        if (!(op->flags & OP_FLUSH_BUFFER) && wb->writeback_overflow.size() > 0)
        {
            // Writeback queue is full, postpone the operation
            wb->writeback_overflow.push_back(op);
            return;
        }
        if (wb->writeback_queue.size())
        {
            wb->start_writebacks(this, 0);
        }
        dirty_bytes = 0;
        dirty_ops = 0;
    }
    op->prev = op_queue_tail;
    if (op_queue_tail)
    {
        op_queue_tail->next = op;
        op_queue_tail = op;
    }
    else
        op_queue_tail = op_queue_head = op;
    if (!(op->flags & OP_IMMEDIATE_COMMIT) || enable_writeback)
        calc_wait(op);
    else
    {
        if (op->opcode == OSD_OP_SYNC)
            continue_sync(op);
        else
            continue_rw(op);
    }
}

bool cluster_client_t::check_rw(cluster_op_t *op)
{
    if (op->opcode == OSD_OP_SYNC)
    {
        return true;
    }
    pool_id_t pool_id = INODE_POOL(op->cur_inode);
    if (!pool_id)
    {
        op->retval = -EINVAL;
        std::function<void(cluster_op_t*)>(op->callback)(op);
        return false;
    }
    auto pool_it = st_cli.pool_config.find(pool_id);
    if (pool_it == st_cli.pool_config.end() || pool_it->second.real_pg_count == 0)
    {
        // Pools are loaded, but this one is unknown
        op->retval = -EINVAL;
        std::function<void(cluster_op_t*)>(op->callback)(op);
        return false;
    }
    // Check alignment
    if (!op->len && (op->opcode == OSD_OP_READ || op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP || op->opcode == OSD_OP_WRITE) ||
        op->offset % pool_it->second.bitmap_granularity || op->len % pool_it->second.bitmap_granularity)
    {
        op->retval = -EINVAL;
        std::function<void(cluster_op_t*)>(op->callback)(op);
        return false;
    }
    if (pool_it->second.immediate_commit == IMMEDIATE_ALL)
    {
        op->flags |= OP_IMMEDIATE_COMMIT;
    }
    if ((op->opcode == OSD_OP_WRITE || op->opcode == OSD_OP_DELETE) && !(op->flags & OSD_OP_IGNORE_READONLY))
    {
        auto ino_it = st_cli.inode_config.find(op->inode);
        if (ino_it != st_cli.inode_config.end() && ino_it->second.readonly)
        {
            op->retval = -EROFS;
            std::function<void(cluster_op_t*)>(op->callback)(op);
            return false;
        }
    }
    return true;
}

void cluster_client_t::execute_raw(osd_num_t osd_num, osd_op_t *op)
{
    auto fd_it = msgr.osd_peer_fds.find(osd_num);
    if (fd_it != msgr.osd_peer_fds.end())
    {
        op->op_type = OSD_OP_OUT;
        op->peer_fd = fd_it->second;
        msgr.outbox_push(op);
    }
    else
    {
        if (msgr.wanted_peers.find(osd_num) == msgr.wanted_peers.end())
            msgr.connect_peer(osd_num, st_cli.peer_states[osd_num]);
        raw_ops.emplace(osd_num, op);
    }
}

int cluster_client_t::continue_rw(cluster_op_t *op)
{
    if (op->state == 0)
        goto resume_0;
    else if (op->state == 1)
        goto resume_1;
    else if (op->state == 2)
        goto resume_2;
resume_0:
    // Slice the operation into parts
    slice_rw(op);
    op->needs_reslice = false;
    if ((op->opcode == OSD_OP_WRITE || op->opcode == OSD_OP_DELETE) && op->version && op->parts.size() > 1)
    {
        // Atomic writes to multiple stripes are unsupported
        op->retval = -EINVAL;
        erase_op(op);
        return 1;
    }
resume_1:
    // Send unsent parts, if they're not subject to change
    op->state = 2;
    if (op->needs_reslice)
    {
        for (int i = 0; i < op->parts.size(); i++)
        {
            if (!(op->parts[i].flags & PART_SENT) && op->retval)
            {
                op->retval = -EPIPE;
            }
        }
        goto resume_2;
    }
    for (int i = 0; i < op->parts.size(); i++)
    {
        if (!(op->parts[i].flags & PART_SENT))
        {
            if (!try_send(op, i))
            {
                // We'll need to retry again
                if (op->parts[i].flags & PART_RETRY)
                {
                    op->up_wait = true;
                    if (!retry_timeout_id)
                    {
                        retry_timeout_id = tfd->set_timer(up_wait_retry_interval, false, [this](int)
                        {
                            retry_timeout_id = 0;
                            continue_ops(true);
                        });
                    }
                }
                op->state = 1;
            }
        }
    }
    if (op->state == 1)
    {
        return 0;
    }
resume_2:
    if (op->inflight_count > 0)
    {
        op->state = 2;
        return 0;
    }
    if (op->done_count >= op->parts.size())
    {
        // Finished successfully
        // Even if the PG count has changed in meanwhile we treat it as success
        // because if some operations were invalid for the new PG count we'd get errors
        if (op->opcode == OSD_OP_READ || op->opcode == OSD_OP_READ_CHAIN_BITMAP)
        {
            // Check parent inode
            auto ino_it = st_cli.inode_config.find(op->cur_inode);
            while (ino_it != st_cli.inode_config.end() && ino_it->second.parent_id &&
                INODE_POOL(ino_it->second.parent_id) == INODE_POOL(op->cur_inode) &&
                // Check for loops
                ino_it->second.parent_id != op->inode)
            {
                // Skip parents from the same pool
                ino_it = st_cli.inode_config.find(ino_it->second.parent_id);
            }
            if (ino_it != st_cli.inode_config.end() &&
                ino_it->second.parent_id &&
                ino_it->second.parent_id != op->inode)
            {
                // Continue reading from the parent inode
                op->cur_inode = ino_it->second.parent_id;
                op->parts.clear();
                op->done_count = 0;
                goto resume_0;
            }
        }
        op->retval = op->len;
        if (op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP)
        {
            auto & pool_cfg = st_cli.pool_config.at(INODE_POOL(op->inode));
            op->retval = op->len / pool_cfg.bitmap_granularity;
        }
        erase_op(op);
        return 1;
    }
    else if (op->retval != 0 && !(op->flags & OP_FLUSH_BUFFER) &&
        op->retval != -EPIPE && op->retval != -EIO && op->retval != -ENOSPC)
    {
        // Fatal error (neither -EPIPE, -EIO nor -ENOSPC)
        // FIXME: Add a parameter to allow to not wait for EIOs (incomplete or corrupted objects) to heal
        erase_op(op);
        return 1;
    }
    else
    {
        // Non-fatal error - clear the error and retry
        op->retval = 0;
        if (op->needs_reslice)
        {
            op->parts.clear();
            op->done_count = 0;
            goto resume_0;
        }
        else
        {
            for (int i = 0; i < op->parts.size(); i++)
            {
                if (!(op->parts[i].flags & PART_DONE))
                {
                    op->parts[i].flags = PART_RETRY;
                }
            }
            goto resume_1;
        }
    }
    return 0;
}

static void add_iov(int size, bool skip, cluster_op_t *op, int &iov_idx, size_t &iov_pos, osd_op_buf_list_t &iov, void *scrap, int scrap_len)
{
    int left = size;
    while (left > 0 && iov_idx < op->iov.count)
    {
        int cur_left = op->iov.buf[iov_idx].iov_len - iov_pos;
        if (cur_left < left)
        {
            if (!skip)
            {
                iov.push_back((uint8_t*)op->iov.buf[iov_idx].iov_base + iov_pos, cur_left);
            }
            left -= cur_left;
            iov_pos = 0;
            iov_idx++;
        }
        else
        {
            if (!skip)
            {
                iov.push_back((uint8_t*)op->iov.buf[iov_idx].iov_base + iov_pos, left);
            }
            iov_pos += left;
            left = 0;
        }
    }
    assert(left == 0);
    if (skip && scrap_len > 0)
    {
        // All skipped ranges are read into the same useless buffer
        left = size;
        while (left > 0)
        {
            int cur_left = scrap_len < left ? scrap_len : left;
            iov.push_back(scrap, cur_left);
            left -= cur_left;
        }
    }
}

void cluster_client_t::slice_rw(cluster_op_t *op)
{
    // Slice the request into individual object stripe requests
    // Primary OSDs still operate individual stripes, but their size is multiplied by PG minsize in case of EC
    auto & pool_cfg = st_cli.pool_config.at(INODE_POOL(op->cur_inode));
    uint32_t pg_data_size = (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks);
    uint64_t pg_block_size = pool_cfg.data_block_size * pg_data_size;
    uint64_t first_stripe = (op->offset / pg_block_size) * pg_block_size;
    uint64_t last_stripe = op->len > 0 ? ((op->offset + op->len - 1) / pg_block_size) * pg_block_size : first_stripe;
    op->retval = 0;
    op->parts.resize((last_stripe - first_stripe) / pg_block_size + 1);
    if (op->opcode == OSD_OP_READ || op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP)
    {
        // Allocate memory for the bitmap
        unsigned object_bitmap_size = ((op->len / pool_cfg.bitmap_granularity + 7) / 8);
        object_bitmap_size = (object_bitmap_size < 8 ? 8 : object_bitmap_size);
        unsigned bitmap_mem = object_bitmap_size + (pool_cfg.data_block_size / pool_cfg.bitmap_granularity / 8 * pg_data_size) * op->parts.size();
        if (!op->bitmap_buf || op->bitmap_buf_size < bitmap_mem)
        {
            op->bitmap_buf = realloc_or_die(op->bitmap_buf, bitmap_mem);
            op->part_bitmaps = (uint8_t*)op->bitmap_buf + object_bitmap_size;
            op->bitmap_buf_size = bitmap_mem;
        }
        memset(op->bitmap_buf, 0, bitmap_mem);
    }
    int iov_idx = 0;
    size_t iov_pos = 0;
    int i = 0;
    // We also have to return reads from CACHE_REPEATING buffers - they are not
    // guaranteed to be present on target OSDs at the moment of repeating
    // And we're also free to return data from other cached buffers just
    // because it's faster
    bool dirty_copied = wb->read_from_cache(op, pool_cfg.bitmap_granularity);
    for (uint64_t stripe = first_stripe; stripe <= last_stripe; stripe += pg_block_size)
    {
        pg_num_t pg_num = (stripe/pool_cfg.pg_stripe_size) % pool_cfg.real_pg_count + 1; // like map_to_pg()
        uint64_t begin = (op->offset < stripe ? stripe : op->offset);
        uint64_t end = (op->offset + op->len) > (stripe + pg_block_size)
            ? (stripe + pg_block_size) : (op->offset + op->len);
        op->parts[i].iov.reset();
        op->parts[i].flags = 0;
        if (op->cur_inode != op->inode || op->opcode == OSD_OP_READ && dirty_copied)
        {
            // Read remaining parts from upper layers
            uint64_t prev = begin, cur = begin;
            bool skip_prev = true;
            while (cur < end)
            {
                unsigned bmp_loc = (cur - op->offset)/pool_cfg.bitmap_granularity;
                bool skip = (((*((uint8_t*)op->bitmap_buf + bmp_loc/8)) >> (bmp_loc%8)) & 0x1);
                if (skip_prev != skip)
                {
                    if (cur > prev)
                    {
                        if (prev == begin && skip_prev)
                        {
                            begin = cur;
                            // Just advance iov_idx & iov_pos
                            add_iov(cur-prev, true, op, iov_idx, iov_pos, op->parts[i].iov, NULL, 0);
                        }
                        else
                            add_iov(cur-prev, skip_prev, op, iov_idx, iov_pos, op->parts[i].iov, scrap_buffer, scrap_buffer_size);
                    }
                    skip_prev = skip;
                    prev = cur;
                }
                cur += pool_cfg.bitmap_granularity;
            }
            assert(cur > prev);
            if (skip_prev)
            {
                // Just advance iov_idx & iov_pos
                add_iov(end-prev, true, op, iov_idx, iov_pos, op->parts[i].iov, NULL, 0);
                end = prev;
            }
            else
                add_iov(cur-prev, skip_prev, op, iov_idx, iov_pos, op->parts[i].iov, scrap_buffer, scrap_buffer_size);
            if (end == begin)
            {
                op->done_count++;
                op->parts[i].flags = PART_DONE;
            }
        }
        else if (op->opcode != OSD_OP_READ_BITMAP && op->opcode != OSD_OP_READ_CHAIN_BITMAP && op->opcode != OSD_OP_DELETE)
        {
            add_iov(end-begin, false, op, iov_idx, iov_pos, op->parts[i].iov, NULL, 0);
        }
        op->parts[i].parent = op;
        op->parts[i].offset = begin;
        op->parts[i].len = op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP ||
            op->opcode == OSD_OP_DELETE ? 0 : (uint32_t)(end - begin);
        op->parts[i].pg_num = pg_num;
        op->parts[i].osd_num = 0;
        i++;
    }
}

bool cluster_client_t::affects_osd(uint64_t inode, uint64_t offset, uint64_t len, osd_num_t osd)
{
    auto & pool_cfg = st_cli.pool_config.at(INODE_POOL(inode));
    uint32_t pg_data_size = (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks);
    uint64_t pg_block_size = pool_cfg.data_block_size * pg_data_size;
    uint64_t first_stripe = (offset / pg_block_size) * pg_block_size;
    uint64_t last_stripe = len > 0 ? ((offset + len - 1) / pg_block_size) * pg_block_size : first_stripe;
    for (uint64_t stripe = first_stripe; stripe <= last_stripe; stripe += pg_block_size)
    {
        pg_num_t pg_num = (stripe/pool_cfg.pg_stripe_size) % pool_cfg.real_pg_count + 1; // like map_to_pg()
        auto pg_it = pool_cfg.pg_config.find(pg_num);
        if (pg_it != pool_cfg.pg_config.end() && pg_it->second.cur_primary == osd)
        {
            return true;
        }
    }
    return false;
}

bool cluster_client_t::try_send(cluster_op_t *op, int i)
{
    if (!msgr_initialized)
    {
        init_msgr();
    }
    auto part = &op->parts[i];
    auto & pool_cfg = st_cli.pool_config.at(INODE_POOL(op->cur_inode));
    auto pg_it = pool_cfg.pg_config.find(part->pg_num);
    if (pg_it != pool_cfg.pg_config.end() &&
        !pg_it->second.pause && pg_it->second.cur_primary)
    {
        osd_num_t primary_osd = pg_it->second.cur_primary;
        auto peer_it = msgr.osd_peer_fds.find(primary_osd);
        if (peer_it != msgr.osd_peer_fds.end())
        {
            int peer_fd = peer_it->second;
            part->osd_num = primary_osd;
            part->flags |= PART_SENT;
            op->inflight_count++;
            uint64_t pg_bitmap_size = (pool_cfg.data_block_size / pool_cfg.bitmap_granularity / 8) * (
                pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks
            );
            uint64_t meta_rev = 0;
            if (op->opcode != OSD_OP_READ_BITMAP && op->opcode != OSD_OP_READ_CHAIN_BITMAP && op->opcode != OSD_OP_DELETE)
            {
                auto ino_it = st_cli.inode_config.find(op->inode);
                if (ino_it != st_cli.inode_config.end())
                    meta_rev = ino_it->second.mod_revision;
            }
            part->op = (osd_op_t){
                .op_type = OSD_OP_OUT,
                .peer_fd = peer_fd,
                .req = { .rw = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = next_op_id(),
                        .opcode = op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP ? OSD_OP_READ : op->opcode,
                    },
                    .inode = op->cur_inode,
                    .offset = part->offset,
                    .len = part->len,
                    .meta_revision = meta_rev,
                    .version = op->opcode == OSD_OP_WRITE || op->opcode == OSD_OP_DELETE ? op->version : 0,
                } },
                .bitmap = (op->opcode == OSD_OP_READ || op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP
                    ? (uint8_t*)op->part_bitmaps + pg_bitmap_size*i : NULL),
                .bitmap_len = (unsigned)(op->opcode == OSD_OP_READ || op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP
                    ? pg_bitmap_size : 0),
                .callback = [this, part](osd_op_t *op_part)
                {
                    handle_op_part(part);
                },
            };
            part->op.iov = part->iov;
            msgr.outbox_push(&part->op);
            return true;
        }
        else if (msgr.wanted_peers.find(primary_osd) == msgr.wanted_peers.end())
        {
            msgr.connect_peer(primary_osd, st_cli.peer_states[primary_osd]);
        }
    }
    return false;
}

int cluster_client_t::continue_sync(cluster_op_t *op)
{
    if (op->state == 1)
        goto resume_1;
    if (!dirty_osds.size())
    {
        // Sync is not required in the immediate_commit mode or if there are no dirty_osds
        op->retval = 0;
        erase_op(op);
        return 1;
    }
    // Check that all OSD connections are still alive
    for (auto do_it = dirty_osds.begin(); do_it != dirty_osds.end(); )
    {
        osd_num_t sync_osd = *do_it;
        auto peer_it = msgr.osd_peer_fds.find(sync_osd);
        if (peer_it == msgr.osd_peer_fds.end())
            dirty_osds.erase(do_it++);
        else
            do_it++;
    }
    // Post sync to affected OSDs
    wb->fsync_start();
    op->parts.resize(dirty_osds.size());
    op->retval = 0;
    {
        int i = 0;
        for (auto sync_osd: dirty_osds)
        {
            op->parts[i] = {
                .parent = op,
                .osd_num = sync_osd,
                .flags = 0,
            };
            send_sync(op, &op->parts[i]);
            i++;
        }
    }
    dirty_osds.clear();
resume_1:
    if (op->inflight_count > 0)
    {
        op->state = 1;
        return 0;
    }
    if (op->retval != 0)
    {
        wb->fsync_error();
        if (op->retval == -EPIPE || op->retval == -EIO || op->retval == -ENOSPC)
        {
            // Retry later
            op->parts.clear();
            op->retval = 0;
            op->inflight_count = 0;
            op->done_count = 0;
            op->state = 0;
            return 0;
        }
    }
    else
    {
        wb->fsync_ok();
    }
    erase_op(op);
    return 1;
}

void cluster_client_t::send_sync(cluster_op_t *op, cluster_op_part_t *part)
{
    auto peer_it = msgr.osd_peer_fds.find(part->osd_num);
    assert(peer_it != msgr.osd_peer_fds.end());
    part->flags |= PART_SENT;
    op->inflight_count++;
    part->op = (osd_op_t){
        .op_type = OSD_OP_OUT,
        .peer_fd = peer_it->second,
        .req = {
            .hdr = {
                .magic = SECONDARY_OSD_OP_MAGIC,
                .id = next_op_id(),
                .opcode = OSD_OP_SYNC,
            },
        },
        .callback = [this, part](osd_op_t *op_part)
        {
            handle_op_part(part);
        },
    };
    msgr.outbox_push(&part->op);
}

static inline void mem_or(void *res, const void *r2, unsigned int len)
{
    unsigned int i;
    for (i = 0; i < len; ++i)
    {
        // Hope the compiler vectorizes this
        ((uint8_t*)res)[i] = ((uint8_t*)res)[i] | ((uint8_t*)r2)[i];
    }
}

void cluster_client_t::handle_op_part(cluster_op_part_t *part)
{
    cluster_op_t *op = part->parent;
    op->inflight_count--;
    int expected = part->op.req.hdr.opcode == OSD_OP_SYNC ? 0 : part->op.req.rw.len;
    if (part->op.reply.hdr.retval != expected)
    {
        // Operation failed, retry
        part->flags |= PART_ERROR;
        if (!op->retval || op->retval == -EPIPE || part->op.reply.hdr.retval == -EIO)
        {
            // Error priority: EIO > ENOSPC > EPIPE
            op->retval = part->op.reply.hdr.retval;
        }
        int stop_fd = -1;
        if (op->retval != -EINTR && op->retval != -EIO && op->retval != -ENOSPC)
        {
            stop_fd = part->op.peer_fd;
            fprintf(
                stderr, "%s operation failed on OSD %lu: retval=%ld (expected %d), dropping connection\n",
                osd_op_names[part->op.req.hdr.opcode], part->osd_num, part->op.reply.hdr.retval, expected
            );
        }
        else
        {
            fprintf(
                stderr, "%s operation failed on OSD %lu: retval=%ld (expected %d)\n",
                osd_op_names[part->op.req.hdr.opcode], part->osd_num, part->op.reply.hdr.retval, expected
            );
        }
        // All next things like timer, continue_sync/rw and stop_client may affect the operation again
        // So do all these things after modifying operation state, otherwise we may hit reenterability bugs
        // FIXME postpone such things to set_immediate here to avoid bugs
        // Mark op->up_wait = true to retry operation after a short pause (not immediately)
        op->up_wait = true;
        if (!retry_timeout_id)
        {
            retry_timeout_id = tfd->set_timer(up_wait_retry_interval, false, [this](int)
            {
                retry_timeout_id = 0;
                continue_ops(true);
            });
        }
        if (op->inflight_count == 0)
        {
            if (op->opcode == OSD_OP_SYNC)
                continue_sync(op);
            else
                continue_rw(op);
        }
        if (stop_fd >= 0)
        {
            msgr.stop_client(stop_fd);
        }
    }
    else
    {
        // OK
        if ((op->opcode == OSD_OP_WRITE || op->opcode == OSD_OP_DELETE) && !(op->flags & OP_IMMEDIATE_COMMIT))
            dirty_osds.insert(part->osd_num);
        part->flags |= PART_DONE;
        op->done_count++;
        if (op->opcode == OSD_OP_READ || op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP)
        {
            copy_part_bitmap(op, part);
            op->version = op->parts.size() == 1 ? part->op.reply.rw.version : 0;
        }
        else if (op->opcode == OSD_OP_WRITE)
        {
            op->version = op->parts.size() == 1 ? part->op.reply.rw.version : 0;
        }
        if (op->inflight_count == 0)
        {
            if (op->opcode == OSD_OP_SYNC)
                continue_sync(op);
            else
                continue_rw(op);
        }
    }
}

void cluster_client_t::copy_part_bitmap(cluster_op_t *op, cluster_op_part_t *part)
{
    // Copy (OR) bitmap
    auto & pool_cfg = st_cli.pool_config.at(INODE_POOL(op->cur_inode));
    uint32_t pg_block_size = pool_cfg.data_block_size * (
        pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks
    );
    uint32_t object_offset = (part->op.req.rw.offset - op->offset) / pool_cfg.bitmap_granularity;
    uint32_t part_offset = (part->op.req.rw.offset % pg_block_size) / pool_cfg.bitmap_granularity;
    uint32_t op_len = op->len / pool_cfg.bitmap_granularity;
    uint32_t part_len = pg_block_size/pool_cfg.bitmap_granularity - part_offset;
    if (part_len > op_len-object_offset)
    {
        part_len = op_len-object_offset;
    }
    if (!(object_offset & 0x7) && !(part_offset & 0x7) && (part_len >= 8))
    {
        // Copy bytes
        mem_or((uint8_t*)op->bitmap_buf + object_offset/8, (uint8_t*)part->op.bitmap + part_offset/8, part_len/8);
        object_offset += (part_len & ~0x7);
        part_offset += (part_len & ~0x7);
        part_len = (part_len & 0x7);
    }
    while (part_len > 0)
    {
        // Copy bits
        (*((uint8_t*)op->bitmap_buf + (object_offset >> 3))) |= (
            (((*((uint8_t*)part->op.bitmap + (part_offset >> 3))) >> (part_offset & 0x7)) & 0x1) << (object_offset & 0x7)
        );
        part_offset++;
        object_offset++;
        part_len--;
    }
}

uint64_t cluster_client_t::next_op_id()
{
    return msgr.next_subop_id++;
}
