#include "cluster_client.h"

cluster_client_t::cluster_client_t(ring_loop_t *ringloop, timerfd_manager_t *tfd, json11::Json & config)
{
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
        }
        else if (unsynced_writes.size())
        {
            // peer_osd just dropped connection
            for (auto op: syncing_writes)
            {
                for (auto & part: op->parts)
                {
                    if (part.osd_num == peer_osd && part.done)
                    {
                        // repeat this operation
                        part.osd_num = 0;
                        part.done = false;
                        assert(!part.sent);
                        op->done_count--;
                    }
                }
            }
            for (auto op: unsynced_writes)
            {
                for (auto & part: op->parts)
                {
                    if (part.osd_num == peer_osd && part.done)
                    {
                        // repeat this operation
                        part.osd_num = 0;
                        part.done = false;
                        assert(!part.sent);
                        op->done_count--;
                    }
                }
                if (op->done_count < op->parts.size())
                {
                    cur_ops.insert(op);
                }
            }
            continue_ops();
        }
    };
    msgr.exec_op = [this](osd_op_t *op)
    {
        // Garbage in
        printf("Incoming garbage from peer %d\n", op->peer_fd);
        msgr.stop_client(op->peer_fd);
        delete op;
    };
    msgr.use_sync_send_recv = config["use_sync_send_recv"].bool_value() ||
        config["use_sync_send_recv"].uint64_value();

    st_cli.tfd = tfd;
    st_cli.on_load_config_hook = [this](json11::Json::object & cfg) { on_load_config_hook(cfg); };
    st_cli.on_change_osd_state_hook = [this](uint64_t peer_osd) { on_change_osd_state_hook(peer_osd); };
    st_cli.on_change_hook = [this](json11::Json::object & changes) { on_change_hook(changes); };
    st_cli.on_load_pgs_hook = [this](bool success) { on_load_pgs_hook(success); };

    log_level = config["log_level"].int64_value();
    st_cli.parse_config(config);
    st_cli.load_global_config();

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

cluster_client_t::~cluster_client_t()
{
    if (ringloop)
    {
        ringloop->unregister_consumer(&consumer);
    }
}

void cluster_client_t::stop()
{
    while (msgr.clients.size() > 0)
    {
        msgr.stop_client(msgr.clients.begin()->first);
    }
}

void cluster_client_t::continue_ops()
{
    if (retry_timeout_id)
    {
        tfd->clear_timer(retry_timeout_id);
        retry_timeout_id = 0;
    }
    for (auto op_it = cur_ops.begin(); op_it != cur_ops.end(); )
    {
        continue_rw(*op_it++);
    }
}

static uint32_t is_power_of_two(uint64_t value)
{
    uint32_t l = 0;
    while (value > 1)
    {
        if (value & 1)
        {
            return 64;
        }
        value = value >> 1;
        l++;
    }
    return l;
}

void cluster_client_t::on_load_config_hook(json11::Json::object & config)
{
    bs_block_size = config["block_size"].uint64_value();
    bs_disk_alignment = config["disk_alignment"].uint64_value();
    bs_bitmap_granularity = config["bitmap_granularity"].uint64_value();
    if (!bs_block_size)
    {
        bs_block_size = DEFAULT_BLOCK_SIZE;
    }
    if (!bs_disk_alignment)
    {
        bs_disk_alignment = DEFAULT_DISK_ALIGNMENT;
    }
    if (!bs_bitmap_granularity)
    {
        bs_bitmap_granularity = DEFAULT_BITMAP_GRANULARITY;
    }
    uint32_t block_order;
    if ((block_order = is_power_of_two(bs_block_size)) >= 64 || bs_block_size < MIN_BLOCK_SIZE || bs_block_size >= MAX_BLOCK_SIZE)
    {
        throw std::runtime_error("Bad block size");
    }
    // FIXME: pg_stripe_size may be a per-pool config
    if (config.find("pg_stripe_size") != config.end())
    {
        pg_stripe_size = config["pg_stripe_size"].uint64_value();
    }
    if (!pg_stripe_size)
    {
        pg_stripe_size = DEFAULT_PG_STRIPE_SIZE;
    }
    if (config["immediate_commit"] == "all")
    {
        // Cluster-wide immediate_commit mode
        immediate_commit = true;
    }
    else if (config.find("client_dirty_limit") != config.end())
    {
        client_dirty_limit = config["client_dirty_limit"].uint64_value();
    }
    if (!client_dirty_limit)
    {
        client_dirty_limit = DEFAULT_CLIENT_DIRTY_LIMIT;
    }
    msgr.peer_connect_interval = config["peer_connect_interval"].uint64_value();
    if (!msgr.peer_connect_interval)
    {
        msgr.peer_connect_interval = DEFAULT_PEER_CONNECT_INTERVAL;
    }
    msgr.peer_connect_timeout = config["peer_connect_timeout"].uint64_value();
    if (!msgr.peer_connect_timeout)
    {
        msgr.peer_connect_timeout = DEFAULT_PEER_CONNECT_TIMEOUT;
    }
    st_cli.load_pgs();
}

void cluster_client_t::on_load_pgs_hook(bool success)
{
    for (auto pool_item: st_cli.pool_config)
    {
        pg_counts[pool_item.first] = pool_item.second.real_pg_count;
    }
    for (auto op: offline_ops)
    {
        execute(op);
    }
    offline_ops.clear();
}

void cluster_client_t::on_change_hook(json11::Json::object & changes)
{
    for (auto pool_item: st_cli.pool_config)
    {
        if (pg_counts[pool_item.first] != pool_item.second.real_pg_count)
        {
            // At this point, all pool operations should have been suspended
            // And now they have to be resliced!
            for (auto op: cur_ops)
            {
                if (INODE_POOL(op->inode) == pool_item.first)
                {
                    op->needs_reslice = true;
                }
            }
            for (auto op: unsynced_writes)
            {
                if (INODE_POOL(op->inode) == pool_item.first)
                {
                    op->needs_reslice = true;
                }
            }
            for (auto op: syncing_writes)
            {
                if (INODE_POOL(op->inode) == pool_item.first)
                {
                    op->needs_reslice = true;
                }
            }
            pg_counts[pool_item.first] = pool_item.second.real_pg_count;
        }
    }
    continue_ops();
}

void cluster_client_t::on_change_osd_state_hook(uint64_t peer_osd)
{
    if (msgr.wanted_peers.find(peer_osd) != msgr.wanted_peers.end())
    {
        msgr.connect_peer(peer_osd, st_cli.peer_states[peer_osd]);
    }
}

/**
 * How writes are synced when immediate_commit is false
 *
 * 1) accept up to <client_dirty_limit> write operations for execution,
 *    queue all subsequent writes into <next_writes>
 * 2) accept exactly one SYNC, queue all subsequent SYNCs into <next_writes>, too
 * 3) "continue" all accepted writes
 *
 * "Continue" WRITE:
 * 1) if the operation is not a copy yet - copy it (required for replay)
 * 2) if the operation is not sliced yet - slice it
 * 3) if the operation doesn't require reslice - try to connect & send all remaining parts
 * 4) if any of them fail due to disconnected peers or PGs not up, repeat after reconnecting or small timeout
 * 5) if any of them fail due to other errors, fail the operation and forget it from the current "unsynced batch"
 * 6) if PG count changes before all parts are done, wait for all in-progress parts to finish,
 *    throw all results away, reslice and resubmit op
 * 7) when all parts are done, try to "continue" the current SYNC
 * 8) if the operation succeeds, but then some OSDs drop their connections, repeat
 *    parts from the current "unsynced batch" previously sent to those OSDs in any order
 *
 * "Continue" current SYNC:
 * 1) take all unsynced operations from the current batch
 * 2) check if all affected OSDs are still alive
 * 3) if yes, send all SYNCs. otherwise, leave current SYNC as is.
 * 4) if any of them fail due to disconnected peers, repeat SYNC after repeating all writes
 * 5) if any of them fail due to other errors, fail the SYNC operation
 */

void cluster_client_t::execute(cluster_op_t *op)
{
    if (!bs_disk_alignment)
    {
        // We're offline
        offline_ops.push_back(op);
        return;
    }
    op->retval = 0;
    if (op->opcode != OSD_OP_SYNC && op->opcode != OSD_OP_READ && op->opcode != OSD_OP_WRITE ||
        (op->opcode == OSD_OP_READ || op->opcode == OSD_OP_WRITE) && (!op->inode || !op->len ||
        op->offset % bs_disk_alignment || op->len % bs_disk_alignment))
    {
        op->retval = -EINVAL;
        std::function<void(cluster_op_t*)>(op->callback)(op);
        return;
    }
    if (op->opcode == OSD_OP_SYNC)
    {
        execute_sync(op);
        return;
    }
    if (op->opcode == OSD_OP_WRITE && !immediate_commit)
    {
        if (next_writes.size() > 0)
        {
            assert(cur_sync);
            next_writes.push_back(op);
            return;
        }
        if (queued_bytes >= client_dirty_limit)
        {
            // Push an extra SYNC operation to flush previous writes
            next_writes.push_back(op);
            cluster_op_t *sync_op = new cluster_op_t;
            sync_op->is_internal = true;
            sync_op->opcode = OSD_OP_SYNC;
            sync_op->callback = [](cluster_op_t* sync_op) {};
            execute_sync(sync_op);
            return;
        }
        queued_bytes += op->len;
    }
    cur_ops.insert(op);
    continue_rw(op);
}

void cluster_client_t::continue_rw(cluster_op_t *op)
{
    pool_id_t pool_id = INODE_POOL(op->inode);
    if (!pool_id)
    {
        op->retval = -EINVAL;
        std::function<void(cluster_op_t*)>(op->callback)(op);
        return;
    }
    if (st_cli.pool_config.find(pool_id) == st_cli.pool_config.end() ||
        st_cli.pool_config[pool_id].real_pg_count == 0)
    {
        // Postpone operations to unknown pools
        return;
    }
    if (op->opcode == OSD_OP_WRITE && !immediate_commit && !op->is_internal)
    {
        // Save operation for replay when PG goes out of sync
        // (primary OSD drops our connection in this case)
        cluster_op_t *op_copy = new cluster_op_t();
        op_copy->is_internal = true;
        op_copy->orig_op = op;
        op_copy->opcode = op->opcode;
        op_copy->inode = op->inode;
        op_copy->offset = op->offset;
        op_copy->len = op->len;
        op_copy->buf = malloc_or_die(op->len);
        op_copy->iov.push_back(op_copy->buf, op->len);
        op_copy->callback = [](cluster_op_t* op_copy)
        {
            if (op_copy->orig_op)
            {
                // Acknowledge write and forget the original pointer
                op_copy->orig_op->retval = op_copy->retval;
                std::function<void(cluster_op_t*)>(op_copy->orig_op->callback)(op_copy->orig_op);
                op_copy->orig_op = NULL;
            }
        };
        void *cur_buf = op_copy->buf;
        for (int i = 0; i < op->iov.count; i++)
        {
            memcpy(cur_buf, op->iov.buf[i].iov_base, op->iov.buf[i].iov_len);
            cur_buf += op->iov.buf[i].iov_len;
        }
        unsynced_writes.push_back(op_copy);
        cur_ops.erase(op);
        cur_ops.insert(op_copy);
        op = op_copy;
    }
    if (!op->parts.size())
    {
        // Slice the operation into parts
        slice_rw(op);
    }
    if (!op->needs_reslice)
    {
        // Send unsent parts, if they're not subject to change
        for (auto & op_part: op->parts)
        {
            if (!op_part.sent && !op_part.done)
            {
                try_send(op, &op_part);
            }
        }
    }
    if (!op->sent_count)
    {
        if (op->done_count >= op->parts.size())
        {
            // Finished successfully
            // Even if the PG count has changed in meanwhile we treat it as success
            // because if some operations were invalid for the new PG count we'd get errors
            cur_ops.erase(op);
            op->retval = op->len;
            std::function<void(cluster_op_t*)>(op->callback)(op);
            continue_sync();
            return;
        }
        else if (op->retval != 0 && op->retval != -EPIPE)
        {
            // Fatal error (not -EPIPE)
            cur_ops.erase(op);
            if (!immediate_commit && op->opcode == OSD_OP_WRITE)
            {
                for (int i = 0; i < unsynced_writes.size(); i++)
                {
                    if (unsynced_writes[i] == op)
                    {
                        unsynced_writes.erase(unsynced_writes.begin()+i, unsynced_writes.begin()+i+1);
                        break;
                    }
                }
            }
            bool del = op->is_internal;
            std::function<void(cluster_op_t*)>(op->callback)(op);
            if (del)
            {
                if (op->buf)
                    free(op->buf);
                delete op;
            }
            continue_sync();
            return;
        }
        else
        {
            // -EPIPE or no error - clear the error
            op->retval = 0;
            if (op->needs_reslice)
            {
                op->parts.clear();
                op->done_count = 0;
                op->needs_reslice = false;
                continue_rw(op);
            }
        }
    }
}

void cluster_client_t::slice_rw(cluster_op_t *op)
{
    // Slice the request into individual object stripe requests
    // Primary OSDs still operate individual stripes, but their size is multiplied by PG minsize in case of EC
    auto & pool_cfg = st_cli.pool_config[INODE_POOL(op->inode)];
    uint64_t pg_block_size = bs_block_size * (
        pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_minsize
    );
    uint64_t first_stripe = (op->offset / pg_block_size) * pg_block_size;
    uint64_t last_stripe = ((op->offset + op->len + pg_block_size - 1) / pg_block_size - 1) * pg_block_size;
    op->retval = 0;
    op->parts.resize((last_stripe - first_stripe) / pg_block_size + 1);
    int iov_idx = 0;
    size_t iov_pos = 0;
    int i = 0;
    for (uint64_t stripe = first_stripe; stripe <= last_stripe; stripe += pg_block_size)
    {
        pg_num_t pg_num = (op->inode + stripe/pg_stripe_size) % pool_cfg.real_pg_count + 1;
        uint64_t begin = (op->offset < stripe ? stripe : op->offset);
        uint64_t end = (op->offset + op->len) > (stripe + pg_block_size)
            ? (stripe + pg_block_size) : (op->offset + op->len);
        op->parts[i] = {
            .parent = op,
            .offset = begin,
            .len = (uint32_t)(end - begin),
            .pg_num = pg_num,
            .sent = false,
            .done = false,
        };
        int left = end-begin;
        while (left > 0 && iov_idx < op->iov.count)
        {
            if (op->iov.buf[iov_idx].iov_len - iov_pos < left)
            {
                op->parts[i].iov.push_back(op->iov.buf[iov_idx].iov_base + iov_pos, op->iov.buf[iov_idx].iov_len - iov_pos);
                left -= (op->iov.buf[iov_idx].iov_len - iov_pos);
                iov_pos = 0;
                iov_idx++;
            }
            else
            {
                op->parts[i].iov.push_back(op->iov.buf[iov_idx].iov_base + iov_pos, left);
                iov_pos += left;
                left = 0;
            }
        }
        assert(left == 0);
        i++;
    }
}

bool cluster_client_t::try_send(cluster_op_t *op, cluster_op_part_t *part)
{
    auto & pool_cfg = st_cli.pool_config[INODE_POOL(op->inode)];
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
            part->sent = true;
            op->sent_count++;
            part->op = {
                .op_type = OSD_OP_OUT,
                .peer_fd = peer_fd,
                .req = { .rw = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = op_id++,
                        .opcode = op->opcode,
                    },
                    .inode = op->inode,
                    .offset = part->offset,
                    .len = part->len,
                } },
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

void cluster_client_t::execute_sync(cluster_op_t *op)
{
    if (immediate_commit)
    {
        // Syncs are not required in the immediate_commit mode
        op->retval = 0;
        std::function<void(cluster_op_t*)>(op->callback)(op);
    }
    else if (cur_sync != NULL)
    {
        next_writes.push_back(op);
    }
    else
    {
        cur_sync = op;
        continue_sync();
    }
}

void cluster_client_t::continue_sync()
{
    if (!cur_sync || cur_sync->parts.size() > 0)
    {
        // Already submitted
        return;
    }
    cur_sync->retval = 0;
    std::set<osd_num_t> sync_osds;
    for (auto prev_op: unsynced_writes)
    {
        if (prev_op->done_count < prev_op->parts.size())
        {
            // Writes not finished yet
            return;
        }
        for (auto & part: prev_op->parts)
        {
            if (part.osd_num)
            {
                sync_osds.insert(part.osd_num);
            }
        }
    }
    if (!sync_osds.size())
    {
        // No dirty writes
        finish_sync();
        return;
    }
    // Check that all OSD connections are still alive
    for (auto sync_osd: sync_osds)
    {
        auto peer_it = msgr.osd_peer_fds.find(sync_osd);
        if (peer_it == msgr.osd_peer_fds.end())
        {
            // SYNC is pointless to send to a non connected OSD
            return;
        }
    }
    syncing_writes.swap(unsynced_writes);
    // Post sync to affected OSDs
    cur_sync->parts.resize(sync_osds.size());
    int i = 0;
    for (auto sync_osd: sync_osds)
    {
        cur_sync->parts[i] = {
            .parent = cur_sync,
            .osd_num = sync_osd,
            .sent = false,
            .done = false,
        };
        send_sync(cur_sync, &cur_sync->parts[i]);
        i++;
    }
}

void cluster_client_t::finish_sync()
{
    int retval = cur_sync->retval;
    if (retval != 0)
    {
        for (auto op: syncing_writes)
        {
            if (op->done_count < op->parts.size())
            {
                cur_ops.insert(op);
            }
        }
        unsynced_writes.insert(unsynced_writes.begin(), syncing_writes.begin(), syncing_writes.end());
        syncing_writes.clear();
    }
    if (retval == -EPIPE)
    {
        // Retry later
        cur_sync->parts.clear();
        cur_sync->retval = 0;
        cur_sync->sent_count = 0;
        cur_sync->done_count = 0;
        return;
    }
    std::function<void(cluster_op_t*)>(cur_sync->callback)(cur_sync);
    if (!retval)
    {
        for (auto op: syncing_writes)
        {
            assert(op->sent_count == 0);
            if (op->is_internal)
            {
                if (op->buf)
                    free(op->buf);
                delete op;
            }
        }
        syncing_writes.clear();
    }
    cur_sync = NULL;
    queued_bytes = 0;
    std::vector<cluster_op_t*> next_wr_copy;
    next_wr_copy.swap(next_writes);
    for (auto next_op: next_wr_copy)
    {
        execute(next_op);
    }
}

void cluster_client_t::send_sync(cluster_op_t *op, cluster_op_part_t *part)
{
    auto peer_it = msgr.osd_peer_fds.find(part->osd_num);
    assert(peer_it != msgr.osd_peer_fds.end());
    part->sent = true;
    op->sent_count++;
    part->op = {
        .op_type = OSD_OP_OUT,
        .peer_fd = peer_it->second,
        .req = {
            .hdr = {
                .magic = SECONDARY_OSD_OP_MAGIC,
                .id = op_id++,
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

void cluster_client_t::handle_op_part(cluster_op_part_t *part)
{
    cluster_op_t *op = part->parent;
    part->sent = false;
    op->sent_count--;
    int expected = part->op.req.hdr.opcode == OSD_OP_SYNC ? 0 : part->op.req.rw.len;
    if (part->op.reply.hdr.retval != expected)
    {
        // Operation failed, retry
        printf(
            "Operation failed on OSD %lu: retval=%ld (expected %d), dropping connection\n",
            part->osd_num, part->op.reply.hdr.retval, expected
        );
        msgr.stop_client(part->op.peer_fd);
        if (part->op.reply.hdr.retval && !retry_timeout_id)
        {
            retry_timeout_id = tfd->set_timer(up_wait_retry_interval, false, [this](int) { retry_timeout_id = 0; continue_ops(); });
        }
        if (!op->retval || op->retval == -EPIPE)
        {
            // Don't overwrite other errors with -EPIPE
            op->retval = part->op.reply.hdr.retval;
        }
    }
    else
    {
        // OK
        part->done = true;
        op->done_count++;
    }
    if (op->sent_count == 0)
    {
        if (op->opcode == OSD_OP_SYNC)
        {
            assert(op == cur_sync);
            finish_sync();
        }
        else
        {
            continue_rw(op);
        }
    }
}
