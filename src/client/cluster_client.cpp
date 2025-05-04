// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <stdexcept>
#include <assert.h>
#include "pg_states.h"
#include "cluster_client_impl.h"
#include "json_util.h"

#define TRY_SEND_OFFLINE 0
#define TRY_SEND_CONNECTING 1
#define TRY_SEND_OK 2

cluster_client_t::cluster_client_t(ring_loop_t *ringloop, timerfd_manager_t *tfd, json11::Json config)
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
            if (wb->repeat_ops_for(this, peer_osd, 0, 0) > 0)
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
    st_cli.on_change_pool_config_hook = [this]() { on_change_pool_config_hook(); };
    st_cli.on_change_pg_state_hook = [this](pool_id_t pool_id, pg_num_t pg_num, osd_num_t prev_primary) { on_change_pg_state_hook(pool_id, pg_num, prev_primary); };
    st_cli.on_change_node_placement_hook = [this]() { on_change_node_placement_hook(); };
    st_cli.on_load_pgs_hook = [this](bool success) { on_load_pgs_hook(success); };
    st_cli.on_reload_hook = [this]() { st_cli.load_global_config(); };

    st_cli.parse_config(config);
    st_cli.infinite_start = false;
    if (!config["client_infinite_start"].is_null())
    {
        st_cli.infinite_start = config["client_infinite_start"].bool_value();
    }
    st_cli.load_global_config();

    scrap_buffer_size = SCRAP_BUFFER_SIZE;
    scrap_buffer = malloc_or_die(scrap_buffer_size);
}

cluster_client_t::~cluster_client_t()
{
    if (retry_timeout_id >= 0)
    {
        tfd->clear_timer(retry_timeout_id);
        retry_timeout_duration = 0;
        retry_timeout_id = -1;
    }
    if (list_retry_timeout_id >= 0)
    {
        tfd->clear_timer(list_retry_timeout_id);
        list_retry_timeout_id = -1;
        list_retry_time = {};
    }
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
    if (bitmap_buf)
    {
        free(bitmap_buf);
        part_bitmaps = NULL;
        bitmap_buf = NULL;
    }
}

bool cluster_op_t::support_left_on_dead()
{
    if (!parts.size())
    {
        return false;
    }
    for (auto & part: parts)
    {
        if (!(part.flags & PART_DONE) ||
            part.op.reply.hdr.opcode != OSD_OP_DELETE ||
            part.op.reply.hdr.retval != 0 ||
            !(part.op.reply.del.flags & OSD_DEL_SUPPORT_LEFT_ON_DEAD))
        {
            return false;
        }
    }
    return true;
}

std::vector<osd_num_t> cluster_op_t::get_left_on_dead()
{
    std::set<osd_num_t> osds;
    for (auto & part: parts)
    {
        if ((part.flags & PART_DONE) ||
            part.op.reply.hdr.opcode == OSD_OP_DELETE &&
            part.op.reply.hdr.retval == 0 &&
            (part.op.reply.del.flags & OSD_DEL_LEFT_ON_DEAD) != 0)
        {
            int del_count = (OSD_PACKET_SIZE-sizeof(part.op.reply.del)) / sizeof(uint32_t);
            if (del_count > part.op.reply.del.left_on_dead_count)
                del_count = part.op.reply.del.left_on_dead_count;
            uint32_t *left_on_dead = (uint32_t*)((&part.op.reply.del) + 1);
            for (int i = 0; i < del_count; i++)
                osds.insert(left_on_dead[i]);
        }
    }
    return std::vector<osd_num_t>(osds.begin(), osds.end());
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
    if (op->opcode == OSD_OP_WRITE || op->opcode == OSD_OP_DELETE)
    {
        for (auto prev = op->prev; prev; prev = prev->prev)
        {
            if (prev->opcode == OSD_OP_SYNC ||
                (prev->opcode == OSD_OP_WRITE || prev->opcode == OSD_OP_DELETE) && !(op->flags & OP_FLUSH_BUFFER) && (prev->flags & OP_FLUSH_BUFFER))
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
            if (prev->opcode == OSD_OP_SYNC || (prev->opcode == OSD_OP_WRITE || prev->opcode == OSD_OP_DELETE) &&
                (!(prev->flags & OP_IMMEDIATE_COMMIT) || enable_writeback))
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
    if (opcode != OSD_OP_WRITE && opcode != OSD_OP_DELETE && opcode != OSD_OP_SYNC)
    {
        return;
    }
    cluster_op_t *bh_ops_local[32], **bh_ops = bh_ops_local;
    int bh_op_count = 0, bh_op_max = 32;
    while (next)
    {
        auto n2 = next->next;
        if ((opcode == OSD_OP_WRITE || opcode == OSD_OP_DELETE)
            ? (next->opcode == OSD_OP_SYNC && (!(flags & OP_IMMEDIATE_COMMIT) || enable_writeback) ||
                (next->opcode == OSD_OP_WRITE || next->opcode == OSD_OP_DELETE) && (flags & OP_FLUSH_BUFFER) && !(next->flags & OP_FLUSH_BUFFER))
            : (next->opcode == OSD_OP_SYNC || next->opcode == OSD_OP_WRITE || next->opcode == OSD_OP_DELETE))
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
        auto cb = std::move(op->callback);
        cb(op);
    }
    if (!(flags & OP_IMMEDIATE_COMMIT) || enable_writeback)
    {
        inc_wait(opcode, flags, next, -1);
    }
    if (!(flags & OP_FLUSH_BUFFER))
    {
        // Call callback at the end to avoid inconsistencies in prev_wait
        // if the callback adds more operations itself
        auto cb = std::move(op->callback);
        cb(op);
    }
    if (flags & OP_FLUSH_BUFFER)
    {
        auto overflow = std::move(wb->writeback_overflow);
        int i = 0;
        while (i < overflow.size() && wb->writebacks_active < client_max_writeback_iodepth)
        {
            execute_internal(overflow[i]);
            i++;
        }
        if (i > 0)
            overflow.erase(overflow.begin(), overflow.begin()+i);
        assert(!wb->writeback_overflow.size());
        wb->writeback_overflow.swap(overflow);
    }
}

void cluster_client_t::continue_ops(int time_passed)
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
    int reset_duration = 0;
restart:
    continuing_ops = 1;
    for (auto op = op_queue_head; op; )
    {
        cluster_op_t *next_op = op->next;
        if (op->retry_after && time_passed)
        {
            op->retry_after = op->retry_after > time_passed ? op->retry_after-time_passed : 0;
            if (op->retry_after && (!reset_duration || op->retry_after < reset_duration))
            {
                reset_duration = op->retry_after;
            }
        }
        if (!op->retry_after && !op->prev_wait)
        {
            if (op->opcode == OSD_OP_SYNC)
                continue_sync(op);
            else
                continue_rw(op);
        }
        op = next_op;
        if (continuing_ops == 2)
        {
            goto restart;
        }
    }
    continuing_ops = 0;
    reset_retry_timer(reset_duration);
}

void cluster_client_t::reset_retry_timer(int new_duration)
{
    if (retry_timeout_duration && retry_timeout_duration <= new_duration || !new_duration)
    {
        return;
    }
    if (retry_timeout_id >= 0)
    {
        tfd->clear_timer(retry_timeout_id);
    }
    retry_timeout_duration = new_duration;
    retry_timeout_id = tfd->set_timer(retry_timeout_duration, false, [this](int)
    {
        int time_passed = retry_timeout_duration;
        retry_timeout_id = -1;
        retry_timeout_duration = 0;
        continue_ops(time_passed);
    });
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
    // client_retry_interval
    client_retry_interval = config["client_retry_interval"].uint64_value();
    if (!client_retry_interval)
    {
        client_retry_interval = 50;
    }
    else if (client_retry_interval < 10)
    {
        client_retry_interval = 10;
    }
    // client_eio_retry_interval
    client_eio_retry_interval = 1000;
    if (!config["client_eio_retry_interval"].is_null())
    {
        client_eio_retry_interval = config["client_eio_retry_interval"].uint64_value();
        if (client_eio_retry_interval && client_eio_retry_interval < 10)
        {
            client_eio_retry_interval = 10;
        }
    }
    // client_retry_enospc
    client_retry_enospc = config["client_retry_enospc"].is_null() ? true : config["client_retry_enospc"].bool_value();
    // client_wait_up_timeout
    if (!config["client_wait_up_timeout"].is_null())
        client_wait_up_timeout = config["client_wait_up_timeout"].uint64_value();
    else
    {
        auto etcd_report_interval = config["etcd_report_interval"].uint64_value();
        if (!etcd_report_interval)
            etcd_report_interval = 5;
        client_wait_up_timeout = 1+etcd_report_interval+(st_cli.max_etcd_attempts*(2*st_cli.etcd_quick_timeout)+999)/1000;
    }
    // log_level
    log_level = config["log_level"].uint64_value();
    // hostname
    conf_hostname = config["hostname"].string_value();
    auto new_hostname = conf_hostname != "" ? conf_hostname : gethostname_str();
    if (new_hostname != client_hostname)
    {
        self_tree_metrics.clear();
        client_hostname = new_hostname;
    }
    msgr.parse_config(config);
    st_cli.parse_config(config);
    st_cli.load_pgs();
}

osd_num_t cluster_client_t::select_random_osd(const std::vector<osd_num_t> & osds)
{
    osd_num_t alive_set[osds.size()];
    int alive_count = 0;
    for (auto & osd_num: osds)
    {
        if (!st_cli.peer_states[osd_num].is_null())
            alive_set[alive_count++] = osd_num;
    }
    if (!alive_count)
        return 0;
    return alive_set[lrand48() % alive_count];
}

osd_num_t cluster_client_t::select_nearest_osd(const std::vector<osd_num_t> & osds)
{
    if (!self_tree_metrics.size())
    {
        std::string cur_id = client_hostname;
        int metric = 0;
        while (self_tree_metrics.find(cur_id) == self_tree_metrics.end())
        {
            self_tree_metrics[cur_id] = metric++;
            json11::Json cur_placement = st_cli.node_placement[cur_id];
            cur_id = cur_placement["parent"].string_value();
        }
        if (cur_id != "")
        {
            self_tree_metrics[""] = metric++;
        }
    }
    osd_num_t best_osd = 0;
    int best_metric = -1;
    for (auto & osd_num: osds)
    {
        int metric = -1;
        auto met_it = osd_tree_metrics.find(osd_num);
        if (met_it != osd_tree_metrics.end())
        {
            metric = met_it->second;
        }
        else
        {
            auto & peer_state = st_cli.peer_states[osd_num];
            if (!peer_state.is_null())
            {
                metric = self_tree_metrics[""];
                bool first = true;
                std::string cur_id = std::to_string(osd_num);
                std::set<std::string> seen;
                while (seen.find(cur_id) == seen.end())
                {
                    seen.insert(cur_id);
                    json11::Json cur_placement = st_cli.node_placement[cur_id];
                    std::string cur_parent = cur_placement["parent"].string_value();
                    cur_id = (!first || cur_parent != "" ? cur_parent : peer_state["host"].string_value());
                    first = false;
                    auto self_it = self_tree_metrics.find(cur_id);
                    if (self_it != self_tree_metrics.end())
                    {
                        metric = self_it->second;
                        break;
                    }
                }
            }
            osd_tree_metrics[osd_num] = metric;
        }
        if (metric >= 0 && (best_metric < 0 || metric < best_metric))
        {
            best_metric = metric;
            best_osd = osd_num;
        }
    }
    return best_osd;
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

void cluster_client_t::on_change_pool_config_hook()
{
    for (auto pool_item: st_cli.pool_config)
    {
        if (pg_counts[pool_item.first] != pool_item.second.real_pg_count)
        {
            // At this point, all pool operations should have been suspended
            // And now they have to be resliced!
            for (auto op = op_queue_head; op; op = op->next)
            {
                if ((op->opcode == OSD_OP_WRITE || op->opcode == OSD_OP_DELETE || op->opcode == OSD_OP_READ ||
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

void cluster_client_t::on_change_pg_state_hook(pool_id_t pool_id, pg_num_t pg_num, osd_num_t prev_primary)
{
    auto & pg_cfg = st_cli.pool_config[pool_id].pg_config[pg_num];
    if (pg_cfg.cur_primary != prev_primary)
    {
        // Repeat this PG operations because an OSD which stopped being primary may not fsync operations
        wb->repeat_ops_for(this, 0, pool_id, pg_num);
    }
    // Always continue to resume operations hung because of lack of the primary OSD
    continue_ops();
    continue_lists();
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
    osd_tree_metrics.erase(peer_osd);
    if (msgr.wanted_peers.find(peer_osd) != msgr.wanted_peers.end())
    {
        msgr.connect_peer(peer_osd, st_cli.peer_states[peer_osd]);
        continue_lists();
    }
}

void cluster_client_t::on_change_node_placement_hook()
{
    osd_tree_metrics.clear();
    self_tree_metrics.clear();
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
        op->opcode != OSD_OP_READ_BITMAP && op->opcode != OSD_OP_READ_CHAIN_BITMAP &&
        op->opcode != OSD_OP_WRITE && op->opcode != OSD_OP_DELETE)
    {
        op->retval = -EINVAL;
        auto cb = std::move(op->callback);
        cb(op);
        return;
    }
    if (!pgs_loaded)
    {
        offline_ops.push_back(op);
        return;
    }
    op->flags = op->flags & (OSD_OP_IGNORE_READONLY | OSD_OP_WAIT_UP_TIMEOUT); // allowed client flags
    execute_internal(op);
}

void cluster_client_t::execute_internal(cluster_op_t *op)
{
    op->cur_inode = op->inode;
    op->retval = 0;
    op->state = 0;
    op->retry_after = 0;
    op->inflight_count = 0;
    op->done_count = 0;
    op->part_bitmaps = NULL;
    op->bitmap_buf_size = 0;
    op->prev_wait = 0;
    assert(!op->prev && !op->next);
    // check alignment, readonly flag and so on
    if (!check_rw(op))
    {
        return;
    }
    if ((op->opcode == OSD_OP_WRITE || op->opcode == OSD_OP_DELETE) && enable_writeback && !(op->flags & OP_FLUSH_BUFFER) &&
        !op->version /* no CAS writeback */)
    {
        if (wb->writebacks_active >= client_max_writeback_iodepth)
        {
            // Writeback queue is full, postpone the operation
            wb->writeback_overflow.push_back(op);
            return;
        }
        // Just copy and acknowledge the operation
        wb->copy_write(op, CACHE_DIRTY);
        while (wb->writeback_bytes > client_max_buffered_bytes || wb->writeback_queue_size > client_max_buffered_ops)
        {
            // Initiate some writeback (asynchronously)
            wb->start_writebacks(this, 1);
        }
        op->retval = op->len;
        auto cb = std::move(op->callback);
        cb(op);
        return;
    }
    if ((op->opcode == OSD_OP_WRITE || op->opcode == OSD_OP_DELETE) && !(op->flags & OP_IMMEDIATE_COMMIT))
    {
        if (!(op->flags & OP_FLUSH_BUFFER) && !op->version /* no CAS write-repeat */)
        {
            uint64_t flush_id = ++wb->last_flush_id;
            wb->copy_write(op, CACHE_REPEATING, flush_id);
            op->flush_id = flush_id;
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
        if (op->opcode != OSD_OP_DELETE)
        {
            dirty_bytes += op->len;
        }
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
        auto cb = std::move(op->callback);
        cb(op);
        return false;
    }
    auto pool_it = st_cli.pool_config.find(pool_id);
    if (pool_it == st_cli.pool_config.end() || pool_it->second.real_pg_count == 0)
    {
        // Pools are loaded, but this one is unknown
        op->retval = -EINVAL;
        auto cb = std::move(op->callback);
        cb(op);
        return false;
    }
    // Check alignment
    if (!op->len && (op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP || op->opcode == OSD_OP_WRITE) ||
        op->offset % pool_it->second.bitmap_granularity || op->len % pool_it->second.bitmap_granularity)
    {
        op->retval = -EINVAL;
        auto cb = std::move(op->callback);
        cb(op);
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
            auto cb = std::move(op->callback);
            cb(op);
            return false;
        }
    }
    op->deoptimise_snapshot = false;
    if (enable_writeback && (op->opcode == OSD_OP_READ || op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP))
    {
        auto ino_it = st_cli.inode_config.find(op->inode);
        if (ino_it != st_cli.inode_config.end())
        {
            int chain_size = 0;
            while (ino_it != st_cli.inode_config.end() && ino_it->second.parent_id)
            {
                // Check for loops - FIXME check it in etcd_state_client
                if (ino_it->second.parent_id == op->inode ||
                    chain_size > st_cli.inode_config.size())
                {
                    op->retval = -EINVAL;
                    auto cb = std::move(op->callback);
                    cb(op);
                    return false;
                }
                if (INODE_POOL(ino_it->second.parent_id) == INODE_POOL(ino_it->first) &&
                    wb->has_inode(ino_it->second.parent_id))
                {
                    // Deoptimise reads - we have dirty data for one of the parent layer(s).
                    op->deoptimise_snapshot = true;
                    break;
                }
                chain_size++;
                ino_it = st_cli.inode_config.find(ino_it->second.parent_id);
            }
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
    // Protect from try_send completing the operation immediately
    op->inflight_count++;
    for (int i = 0; i < op->parts.size(); i++)
    {
        if (!(op->parts[i].flags & PART_SENT))
        {
            int is_ok = try_send(op, i);
            if (is_ok != TRY_SEND_OK)
            {
                // We'll need to retry again
                if (op->flags & OSD_OP_WAIT_UP_TIMEOUT)
                {
                    if (is_ok != TRY_SEND_OFFLINE)
                    {
                        // Reset "wait_up" timer
                        op->wait_up_until = {};
                    }
                    else if (!op->wait_up_until.tv_sec && !client_wait_up_timeout)
                    {
                        // Don't wait for the PG to come up at all and fail
                        op->parts[i].flags |= PART_ERROR;
                        if (!op->retval)
                            op->retval = -ETIMEDOUT;
                        break;
                    }
                    else if (!op->wait_up_until.tv_sec)
                    {
                        // Set "wait_up" timer
                        clock_gettime(CLOCK_REALTIME, &op->wait_up_until);
                        op->wait_up_until.tv_sec += client_wait_up_timeout;
                    }
                    else
                    {
                        // Check if the timeout expired
                        timespec tv;
                        clock_gettime(CLOCK_REALTIME, &tv);
                        if (tv.tv_sec > op->wait_up_until.tv_sec ||
                            tv.tv_sec == op->wait_up_until.tv_sec &&
                            tv.tv_nsec > op->wait_up_until.tv_nsec)
                        {
                            // Fail
                            op->parts[i].flags |= PART_ERROR;
                            if (!op->retval)
                                op->retval = -ETIMEDOUT;
                            break;
                        }
                    }
                }
                if (op->parts[i].flags & PART_RETRY)
                {
                    op->retry_after = client_retry_interval;
                    reset_retry_timer(client_retry_interval);
                }
                op->state = 1;
            }
        }
    }
    op->inflight_count--;
    if (op->state == 1)
    {
        // Some suboperations have to be resent
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
            // Skip parents from the same pool
            int skipped = 0;
            while (!op->deoptimise_snapshot &&
                ino_it != st_cli.inode_config.end() && ino_it->second.parent_id &&
                INODE_POOL(ino_it->second.parent_id) == INODE_POOL(op->cur_inode))
            {
                // Check for loops - FIXME check it in etcd_state_client
                if (ino_it->second.parent_id == op->inode ||
                    skipped > st_cli.inode_config.size())
                {
                    op->retval = -EINVAL;
                    erase_op(op);
                    return 1;
                }
                skipped++;
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
        if (op->flush_id)
        {
            wb->mark_flush_written(op->inode, op->offset, op->len, op->flush_id);
        }
        erase_op(op);
        return 1;
    }
    else if (op->retval != 0 && !(op->flags & OP_FLUSH_BUFFER) &&
        op->retval != -EPIPE && (op->retval != -EIO || !client_eio_retry_interval) && (op->retval != -ENOSPC || !client_retry_enospc))
    {
        // Fatal error (neither -EPIPE, -EIO nor -ENOSPC)
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
        if (op->opcode != OSD_OP_READ_CHAIN_BITMAP && op->cur_inode != op->inode || op->opcode == OSD_OP_READ && dirty_copied)
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
                op->parts[i].flags = PART_SENT|PART_DONE;
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

bool cluster_client_t::affects_pg(uint64_t inode, uint64_t offset, uint64_t len, pool_id_t pool_id, pg_num_t pg_num)
{
    if (INODE_POOL(inode) != pool_id)
    {
        return false;
    }
    auto & pool_cfg = st_cli.pool_config.at(INODE_POOL(inode));
    uint32_t pg_data_size = (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks);
    uint64_t pg_block_size = pool_cfg.data_block_size * pg_data_size;
    uint64_t first_stripe = (offset / pg_block_size) * pg_block_size;
    uint64_t last_stripe = len > 0 ? ((offset + len - 1) / pg_block_size) * pg_block_size : first_stripe;
    if ((last_stripe/pool_cfg.pg_stripe_size) - (first_stripe/pool_cfg.pg_stripe_size) + 1 >= pool_cfg.real_pg_count)
    {
        // All PGs are affected
        return true;
    }
    pg_num_t first_pg_num = (first_stripe/pool_cfg.pg_stripe_size) % pool_cfg.real_pg_count + 1; // like map_to_pg()
    pg_num_t last_pg_num = (last_stripe/pool_cfg.pg_stripe_size) % pool_cfg.real_pg_count + 1; // like map_to_pg()
    return (first_pg_num <= last_pg_num
        ? (pg_num >= first_pg_num && pg_num <= last_pg_num)
        : (pg_num >= first_pg_num || pg_num <= last_pg_num));
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

int cluster_client_t::try_send(cluster_op_t *op, int i)
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
        if (pool_cfg.local_reads != POOL_LOCAL_READ_PRIMARY &&
            pool_cfg.scheme == POOL_SCHEME_REPLICATED &&
            (op->opcode == OSD_OP_READ || op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP) &&
            (pg_it->second.cur_state == PG_ACTIVE || pg_it->second.cur_state == (PG_ACTIVE|PG_LEFT_ON_DEAD)))
        {
            osd_num_t nearest_osd = pool_cfg.local_reads == POOL_LOCAL_READ_NEAREST
                ? select_nearest_osd(pg_it->second.target_set)
                : select_random_osd(pg_it->second.target_set);
            if (nearest_osd)
                primary_osd = nearest_osd;
        }
        part->osd_num = primary_osd;
        auto peer_it = msgr.osd_peer_fds.find(primary_osd);
        if (peer_it != msgr.osd_peer_fds.end())
        {
            int peer_fd = peer_it->second;
            part->flags |= PART_SENT;
            op->inflight_count++;
            uint64_t pg_bitmap_size = (pool_cfg.data_block_size / pool_cfg.bitmap_granularity / 8) * (
                pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks
            );
            uint64_t meta_rev = 0;
            if (op->opcode != OSD_OP_READ_BITMAP && op->opcode != OSD_OP_DELETE && !op->deoptimise_snapshot)
            {
                auto ino_it = st_cli.inode_config.find(op->cur_inode);
                if (ino_it != st_cli.inode_config.end())
                    meta_rev = ino_it->second.mod_revision;
            }
            part->op = (osd_op_t){
                .op_type = OSD_OP_OUT,
                .peer_fd = peer_fd,
                .req = { .rw = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
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
            return TRY_SEND_OK;
        }
        else if (msgr.wanted_peers.find(primary_osd) == msgr.wanted_peers.end())
        {
            msgr.connect_peer(primary_osd, st_cli.peer_states[primary_osd]);
            return TRY_SEND_CONNECTING;
        }
    }
    return TRY_SEND_OFFLINE;
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
    auto peer_fd = msgr.osd_peer_fds.at(part->osd_num);
    part->flags |= PART_SENT;
    op->inflight_count++;
    part->op = (osd_op_t){
        .op_type = OSD_OP_OUT,
        .peer_fd = peer_fd,
        .req = {
            .hdr = {
                .magic = SECONDARY_OSD_OP_MAGIC,
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
    int expected = part->op.req.hdr.opcode == OSD_OP_SYNC ? 0 : part->op.req.rw.len;
    if (part->op.reply.hdr.retval != expected)
    {
        // Operation failed, retry
        part->flags |= PART_ERROR;
        if (!op->retval || op->retval == -EPIPE ||
            part->op.reply.hdr.retval == -ENOSPC && op->retval == -ETIMEDOUT ||
            part->op.reply.hdr.retval == -EIO)
        {
            // Error priority: EIO > ENOSPC > ETIMEDOUT > EPIPE
            op->retval = part->op.reply.hdr.retval;
        }
        int stop_fd = -1;
        if (op->retval != -EINTR && op->retval != -EIO && op->retval != -ENOSPC)
        {
            stop_fd = part->op.peer_fd;
            if (op->retval != -EPIPE || log_level > 0)
            {
                fprintf(
                    stderr, "%s operation failed on OSD %ju: retval=%jd (expected %d), dropping connection\n",
                    osd_op_names[part->op.req.hdr.opcode], part->osd_num, part->op.reply.hdr.retval, expected
                );
            }
        }
        else if (log_level > 0)
        {
            fprintf(
                stderr, "%s operation failed on OSD %ju: retval=%jd (expected %d)\n",
                osd_op_names[part->op.req.hdr.opcode], part->osd_num, part->op.reply.hdr.retval, expected
            );
        }
        // All next things like timer, continue_sync/rw and stop_client may affect the operation again
        // So do all these things after modifying operation state, otherwise we may hit reenterability bugs
        // FIXME postpone such things to set_immediate here to avoid bugs
        // Set op->retry_after to retry operation after a short pause (not immediately)
        if (!op->retry_after && (op->retval == -EPIPE ||
            op->retval == -EIO && client_eio_retry_interval ||
            op->retval == -ENOSPC && client_retry_enospc))
        {
            op->retry_after = op->retval != -EPIPE ? client_eio_retry_interval : client_retry_interval;
        }
        reset_retry_timer(op->retry_after);
        if (stop_fd >= 0)
        {
            msgr.stop_client(stop_fd);
        }
        op->inflight_count--;
        if (op->inflight_count == 0 && !op->retry_after)
        {
            if (op->opcode == OSD_OP_SYNC)
                continue_sync(op);
            else
                continue_rw(op);
        }
    }
    else
    {
        // OK
        op->inflight_count--;
        if ((op->opcode == OSD_OP_WRITE || op->opcode == OSD_OP_DELETE) && !(op->flags & OP_IMMEDIATE_COMMIT))
            dirty_osds.insert(part->osd_num);
        part->flags |= PART_DONE;
        op->done_count++;
        if (op->opcode == OSD_OP_READ || op->opcode == OSD_OP_READ_BITMAP || op->opcode == OSD_OP_READ_CHAIN_BITMAP)
        {
            copy_part_bitmap(op, part);
            if (op->inode == op->cur_inode)
            {
                // Read only returns the version of the uppermost layer
                op->version = op->parts.size() == 1 ? part->op.reply.rw.version : 0;
            }
        }
        else if (op->opcode == OSD_OP_WRITE || op->opcode == OSD_OP_DELETE)
        {
            op->version = op->parts.size() == 1 ? part->op.reply.rw.version : 0;
        }
        if (op->inflight_count == 0 && !op->retry_after)
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
