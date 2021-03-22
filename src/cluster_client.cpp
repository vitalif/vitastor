// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <stdexcept>
#include <assert.h>
#include "cluster_client.h"

#define SCRAP_BUFFER_SIZE 4*1024*1024
#define PART_SENT 1
#define PART_DONE 2
#define PART_ERROR 4
#define CACHE_DIRTY 1
#define CACHE_FLUSHING 2
#define CACHE_REPEATING 3
#define OP_FLUSH_BUFFER 2

cluster_client_t::cluster_client_t(ring_loop_t *ringloop, timerfd_manager_t *tfd, json11::Json & config)
{
    this->ringloop = ringloop;
    this->tfd = tfd;
    this->config = config;

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
        else if (dirty_buffers.size())
        {
            // peer_osd just dropped connection
            // determine WHICH dirty_buffers are now obsolete and repeat them
            for (auto & wr: dirty_buffers)
            {
                if (affects_osd(wr.first.inode, wr.first.stripe, wr.second.len, peer_osd) &&
                    wr.second.state != CACHE_REPEATING)
                {
                    // FIXME: Flush in larger parts
                    flush_buffer(wr.first, &wr.second);
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
    msgr.init();

    st_cli.tfd = tfd;
    st_cli.on_load_config_hook = [this](json11::Json::object & cfg) { on_load_config_hook(cfg); };
    st_cli.on_change_osd_state_hook = [this](uint64_t peer_osd) { on_change_osd_state_hook(peer_osd); };
    st_cli.on_change_hook = [this](std::map<std::string, etcd_kv_t> & changes) { on_change_hook(changes); };
    st_cli.on_load_pgs_hook = [this](bool success) { on_load_pgs_hook(success); };

    st_cli.parse_config(config);
    st_cli.load_global_config();

    scrap_buffer_size = SCRAP_BUFFER_SIZE;
    scrap_buffer = malloc_or_die(scrap_buffer_size);

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
    for (auto bp: dirty_buffers)
    {
        free(bp.second.buf);
    }
    dirty_buffers.clear();
    if (ringloop)
    {
        ringloop->unregister_consumer(&consumer);
    }
    free(scrap_buffer);
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
        continuing_ops = 2;
        return;
    }
restart:
    continuing_ops = 1;
    op_queue_pos = 0;
    bool has_flushes = false, has_writes = false;
    while (op_queue_pos < op_queue.size())
    {
        auto op = op_queue[op_queue_pos];
        bool rm = false, is_flush = op->flags & OP_FLUSH_BUFFER;
        auto opcode = op->opcode;
        if (!op->up_wait || up_retry)
        {
            op->up_wait = false;
            if (opcode == OSD_OP_READ || opcode == OSD_OP_WRITE)
            {
                if (is_flush || !has_flushes)
                {
                    // Regular writes can't proceed before buffer flushes
                    rm = continue_rw(op);
                }
            }
            else if (opcode == OSD_OP_SYNC)
            {
                if (!has_writes)
                {
                    // SYNC can't proceed before previous writes
                    rm = continue_sync(op);
                }
            }
        }
        if (opcode == OSD_OP_WRITE)
        {
            has_writes = has_writes || !rm;
            if (is_flush)
            {
                has_flushes = has_writes || !rm;
            }
        }
        else if (opcode == OSD_OP_SYNC)
        {
            // Postpone writes until previous SYNC completes
            // ...so dirty_writes can't contain anything newer than SYNC
            has_flushes = has_writes || !rm;
        }
        if (rm)
        {
            op_queue.erase(op_queue.begin()+op_queue_pos, op_queue.begin()+op_queue_pos+1);
        }
        else
        {
            op_queue_pos++;
        }
        if (continuing_ops == 2)
        {
            goto restart;
        }
    }
    continuing_ops = 0;
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
    bs_bitmap_granularity = config["bitmap_granularity"].uint64_value();
    if (!bs_block_size)
    {
        bs_block_size = DEFAULT_BLOCK_SIZE;
    }
    if (!bs_bitmap_granularity)
    {
        bs_bitmap_granularity = DEFAULT_BITMAP_GRANULARITY;
    }
    bs_bitmap_size = bs_block_size / bs_bitmap_granularity / 8;
    uint32_t block_order;
    if ((block_order = is_power_of_two(bs_block_size)) >= 64 || bs_block_size < MIN_BLOCK_SIZE || bs_block_size >= MAX_BLOCK_SIZE)
    {
        throw std::runtime_error("Bad block size");
    }
    if (config["immediate_commit"] == "all")
    {
        // Cluster-wide immediate_commit mode
        immediate_commit = true;
    }
    if (config.find("client_max_dirty_bytes") != config.end())
    {
        client_max_dirty_bytes = config["client_max_dirty_bytes"].uint64_value();
    }
    else if (config.find("client_dirty_limit") != config.end())
    {
        // Old name
        client_max_dirty_bytes = config["client_dirty_limit"].uint64_value();
    }
    if (config.find("client_max_dirty_ops") != config.end())
    {
        client_max_dirty_ops = config["client_max_dirty_ops"].uint64_value();
    }
    if (!client_max_dirty_bytes)
    {
        client_max_dirty_bytes = DEFAULT_CLIENT_MAX_DIRTY_BYTES;
    }
    if (!client_max_dirty_ops)
    {
        client_max_dirty_ops = DEFAULT_CLIENT_MAX_DIRTY_OPS;
    }
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
    msgr.parse_config(this->config);
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
            for (auto op: op_queue)
            {
                if ((op->opcode == OSD_OP_WRITE || op->opcode == OSD_OP_READ) &&
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
 */
void cluster_client_t::execute(cluster_op_t *op)
{
    if (op->opcode != OSD_OP_SYNC && op->opcode != OSD_OP_READ && op->opcode != OSD_OP_WRITE)
    {
        op->retval = -EINVAL;
        std::function<void(cluster_op_t*)>(op->callback)(op);
        return;
    }
    op->cur_inode = op->inode;
    op->retval = 0;
    if (op->opcode == OSD_OP_WRITE && !immediate_commit)
    {
        if (dirty_bytes >= client_max_dirty_bytes || dirty_ops >= client_max_dirty_ops)
        {
            // Push an extra SYNC operation to flush previous writes
            cluster_op_t *sync_op = new cluster_op_t;
            sync_op->opcode = OSD_OP_SYNC;
            sync_op->callback = [](cluster_op_t* sync_op)
            {
                delete sync_op;
            };
            op_queue.push_back(sync_op);
            dirty_bytes = 0;
            dirty_ops = 0;
        }
        dirty_bytes += op->len;
        dirty_ops++;
    }
    else if (op->opcode == OSD_OP_SYNC)
    {
        dirty_bytes = 0;
        dirty_ops = 0;
    }
    op_queue.push_back(op);
    continue_ops();
}

void cluster_client_t::copy_write(cluster_op_t *op, std::map<object_id, cluster_buffer_t> & dirty_buffers)
{
    // Save operation for replay when one of PGs goes out of sync
    // (primary OSD drops our connection in this case)
    auto dirty_it = dirty_buffers.lower_bound((object_id){
        .inode = op->inode,
        .stripe = op->offset,
    });
    while (dirty_it != dirty_buffers.begin())
    {
        dirty_it--;
        if (dirty_it->first.inode != op->inode ||
            (dirty_it->first.stripe + dirty_it->second.len) <= op->offset)
        {
            dirty_it++;
            break;
        }
    }
    uint64_t pos = op->offset, len = op->len, iov_idx = 0, iov_pos = 0;
    while (len > 0)
    {
        uint64_t new_len = 0;
        if (dirty_it == dirty_buffers.end())
        {
            new_len = len;
        }
        else if (dirty_it->first.inode != op->inode || dirty_it->first.stripe > pos)
        {
            new_len = dirty_it->first.stripe - pos;
            if (new_len > len)
            {
                new_len = len;
            }
        }
        if (new_len > 0)
        {
            dirty_it = dirty_buffers.emplace_hint(dirty_it, (object_id){
                .inode = op->inode,
                .stripe = pos,
            }, (cluster_buffer_t){
                .buf = malloc_or_die(new_len),
                .len = new_len,
            });
        }
        // FIXME: Split big buffers into smaller ones on overwrites. But this will require refcounting
        dirty_it->second.state = CACHE_DIRTY;
        uint64_t cur_len = (dirty_it->first.stripe + dirty_it->second.len - pos);
        if (cur_len > len)
        {
            cur_len = len;
        }
        while (cur_len > 0 && iov_idx < op->iov.count)
        {
            unsigned iov_len = (op->iov.buf[iov_idx].iov_len - iov_pos);
            if (iov_len <= cur_len)
            {
                memcpy(dirty_it->second.buf + pos - dirty_it->first.stripe,
                    op->iov.buf[iov_idx].iov_base + iov_pos, iov_len);
                pos += iov_len;
                len -= iov_len;
                cur_len -= iov_len;
                iov_pos = 0;
                iov_idx++;
            }
            else
            {
                memcpy(dirty_it->second.buf + pos - dirty_it->first.stripe,
                    op->iov.buf[iov_idx].iov_base + iov_pos, cur_len);
                pos += cur_len;
                len -= cur_len;
                iov_pos += cur_len;
                cur_len = 0;
            }
        }
        dirty_it++;
    }
}

void cluster_client_t::flush_buffer(const object_id & oid, cluster_buffer_t *wr)
{
    wr->state = CACHE_REPEATING;
    cluster_op_t *op = new cluster_op_t;
    op->flags = OP_FLUSH_BUFFER;
    op->opcode = OSD_OP_WRITE;
    op->cur_inode = op->inode = oid.inode;
    op->offset = oid.stripe;
    op->len = wr->len;
    op->iov.push_back(wr->buf, wr->len);
    op->callback = [wr](cluster_op_t* op)
    {
        if (wr->state == CACHE_REPEATING)
        {
            wr->state = CACHE_DIRTY;
        }
        delete op;
    };
    op_queue.insert(op_queue.begin(), op);
    if (continuing_ops)
    {
        continuing_ops = 2;
        op_queue_pos++;
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
    else if (op->state == 3)
        goto resume_3;
resume_0:
    if (!op->len || op->offset % bs_bitmap_granularity || op->len % bs_bitmap_granularity)
    {
        op->retval = -EINVAL;
        std::function<void(cluster_op_t*)>(op->callback)(op);
        return 1;
    }
    {
        pool_id_t pool_id = INODE_POOL(op->cur_inode);
        if (!pool_id)
        {
            op->retval = -EINVAL;
            std::function<void(cluster_op_t*)>(op->callback)(op);
            return 1;
        }
        if (st_cli.pool_config.find(pool_id) == st_cli.pool_config.end() ||
            st_cli.pool_config[pool_id].real_pg_count == 0)
        {
            // Postpone operations to unknown pools
            return 0;
        }
    }
    if (op->opcode == OSD_OP_WRITE)
    {
        auto ino_it = st_cli.inode_config.find(op->inode);
        if (ino_it != st_cli.inode_config.end() && ino_it->second.readonly)
        {
            op->retval = -EINVAL;
            std::function<void(cluster_op_t*)>(op->callback)(op);
            return 1;
        }
        if (!immediate_commit && !(op->flags & OP_FLUSH_BUFFER))
        {
            copy_write(op, dirty_buffers);
        }
    }
resume_1:
    // Slice the operation into parts
    slice_rw(op);
    op->needs_reslice = false;
resume_2:
    // Send unsent parts, if they're not subject to change
    op->state = 3;
    if (op->needs_reslice)
    {
        for (int i = 0; i < op->parts.size(); i++)
        {
            if (!(op->parts[i].flags & PART_SENT) && op->retval)
            {
                op->retval = -EPIPE;
            }
        }
        goto resume_3;
    }
    for (int i = 0; i < op->parts.size(); i++)
    {
        if (!(op->parts[i].flags & PART_SENT))
        {
            if (!try_send(op, i))
            {
                // We'll need to retry again
                op->up_wait = true;
                if (!retry_timeout_id)
                {
                    retry_timeout_id = tfd->set_timer(up_wait_retry_interval, false, [this](int)
                    {
                        retry_timeout_id = 0;
                        continue_ops(true);
                    });
                }
                op->state = 2;
            }
        }
    }
    if (op->state == 2)
    {
        return 0;
    }
resume_3:
    if (op->inflight_count > 0)
    {
        op->state = 3;
        return 0;
    }
    if (op->done_count >= op->parts.size())
    {
        // Finished successfully
        // Even if the PG count has changed in meanwhile we treat it as success
        // because if some operations were invalid for the new PG count we'd get errors
        bool is_read = op->opcode == OSD_OP_READ;
        if (is_read)
        {
            // Check parent inode
            auto ino_it = st_cli.inode_config.find(op->cur_inode);
            if (ino_it != st_cli.inode_config.end() &&
                ino_it->second.parent_id)
            {
                // Continue reading from the parent inode
                // FIXME: This obviously requires optimizations for long snapshot chains
                op->cur_inode = ino_it->second.parent_id;
                op->parts.clear();
                op->done_count = 0;
                goto resume_1;
            }
        }
        op->retval = op->len;
        std::function<void(cluster_op_t*)>(op->callback)(op);
        return 1;
    }
    else if (op->retval != 0 && op->retval != -EPIPE)
    {
        // Fatal error (not -EPIPE)
        std::function<void(cluster_op_t*)>(op->callback)(op);
        return 1;
    }
    else
    {
        // -EPIPE - clear the error and retry
        op->retval = 0;
        if (op->needs_reslice)
        {
            op->parts.clear();
            op->done_count = 0;
            goto resume_1;
        }
        else
        {
            for (int i = 0; i < op->parts.size(); i++)
            {
                op->parts[i].flags = 0;
            }
            goto resume_2;
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
                iov.push_back(op->iov.buf[iov_idx].iov_base + iov_pos, cur_left);
            }
            left -= cur_left;
            iov_pos = 0;
            iov_idx++;
        }
        else
        {
            if (!skip)
            {
                iov.push_back(op->iov.buf[iov_idx].iov_base + iov_pos, left);
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
    uint64_t pg_block_size = bs_block_size * pg_data_size;
    uint64_t first_stripe = (op->offset / pg_block_size) * pg_block_size;
    uint64_t last_stripe = ((op->offset + op->len + pg_block_size - 1) / pg_block_size - 1) * pg_block_size;
    op->retval = 0;
    op->parts.resize((last_stripe - first_stripe) / pg_block_size + 1);
    if (op->opcode == OSD_OP_READ)
    {
        // Allocate memory for the bitmap
        unsigned object_bitmap_size = ((op->len / bs_bitmap_granularity + 7) / 8);
        object_bitmap_size = (object_bitmap_size < 8 ? 8 : object_bitmap_size);
        unsigned bitmap_mem = object_bitmap_size + (bs_bitmap_size * pg_data_size) * op->parts.size();
        if (op->bitmap_buf_size < bitmap_mem)
        {
            op->bitmap_buf = realloc_or_die(op->bitmap_buf, bitmap_mem);
            if (!op->bitmap_buf_size)
            {
                // First allocation
                memset(op->bitmap_buf, 0, object_bitmap_size);
            }
            op->part_bitmaps = op->bitmap_buf + object_bitmap_size;
            op->bitmap_buf_size = bitmap_mem;
        }
    }
    int iov_idx = 0;
    size_t iov_pos = 0;
    int i = 0;
    for (uint64_t stripe = first_stripe; stripe <= last_stripe; stripe += pg_block_size)
    {
        pg_num_t pg_num = (stripe/pool_cfg.pg_stripe_size) % pool_cfg.real_pg_count + 1; // like map_to_pg()
        uint64_t begin = (op->offset < stripe ? stripe : op->offset);
        uint64_t end = (op->offset + op->len) > (stripe + pg_block_size)
            ? (stripe + pg_block_size) : (op->offset + op->len);
        op->parts[i].iov.reset();
        if (op->cur_inode != op->inode)
        {
            // Read remaining parts from upper layers
            uint64_t prev = begin, cur = begin;
            bool skip_prev = true;
            while (cur < end)
            {
                unsigned bmp_loc = (cur - op->offset)/bs_bitmap_granularity;
                bool skip = (((*(uint8_t*)(op->bitmap_buf + bmp_loc/8)) >> (bmp_loc%8)) & 0x1);
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
                cur += bs_bitmap_granularity;
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
                op->done_count++;
        }
        else
        {
            add_iov(end-begin, false, op, iov_idx, iov_pos, op->parts[i].iov, NULL, 0);
        }
        op->parts[i].parent = op;
        op->parts[i].offset = begin;
        op->parts[i].len = (uint32_t)(end - begin);
        op->parts[i].pg_num = pg_num;
        op->parts[i].osd_num = 0;
        op->parts[i].flags = 0;
        i++;
    }
}

bool cluster_client_t::affects_osd(uint64_t inode, uint64_t offset, uint64_t len, osd_num_t osd)
{
    auto & pool_cfg = st_cli.pool_config.at(INODE_POOL(inode));
    uint32_t pg_data_size = (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks);
    uint64_t pg_block_size = bs_block_size * pg_data_size;
    uint64_t first_stripe = (offset / pg_block_size) * pg_block_size;
    uint64_t last_stripe = ((offset + len + pg_block_size - 1) / pg_block_size - 1) * pg_block_size;
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
            uint64_t pg_bitmap_size = bs_bitmap_size * (
                pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks
            );
            part->op = (osd_op_t){
                .op_type = OSD_OP_OUT,
                .peer_fd = peer_fd,
                .req = { .rw = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = op_id++,
                        .opcode = op->opcode,
                    },
                    .inode = op->cur_inode,
                    .offset = part->offset,
                    .len = part->len,
                    .meta_revision = 0,
                } },
                .bitmap = op->opcode == OSD_OP_WRITE ? NULL : op->part_bitmaps + pg_bitmap_size*i,
                .bitmap_len = (unsigned)(op->opcode == OSD_OP_WRITE ? 0 : pg_bitmap_size),
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
    if (immediate_commit || !dirty_osds.size())
    {
        // Sync is not required in the immediate_commit mode or if there are no dirty_osds
        op->retval = 0;
        std::function<void(cluster_op_t*)>(op->callback)(op);
        return 1;
    }
    // Check that all OSD connections are still alive
    for (auto sync_osd: dirty_osds)
    {
        auto peer_it = msgr.osd_peer_fds.find(sync_osd);
        if (peer_it == msgr.osd_peer_fds.end())
        {
            return 0;
        }
    }
    // Post sync to affected OSDs
    for (auto & prev_op: dirty_buffers)
    {
        if (prev_op.second.state == CACHE_DIRTY)
        {
            prev_op.second.state = CACHE_FLUSHING;
        }
    }
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
        for (auto uw_it = dirty_buffers.begin(); uw_it != dirty_buffers.end(); uw_it++)
        {
            if (uw_it->second.state == CACHE_FLUSHING)
            {
                uw_it->second.state = CACHE_DIRTY;
            }
        }
        if (op->retval == -EPIPE)
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
        for (auto uw_it = dirty_buffers.begin(); uw_it != dirty_buffers.end(); )
        {
            if (uw_it->second.state == CACHE_FLUSHING)
            {
                free(uw_it->second.buf);
                dirty_buffers.erase(uw_it++);
            }
            else
                uw_it++;
        }
    }
    std::function<void(cluster_op_t*)>(op->callback)(op);
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
        printf(
            "%s operation failed on OSD %lu: retval=%ld (expected %d), dropping connection\n",
            osd_op_names[part->op.req.hdr.opcode], part->osd_num, part->op.reply.hdr.retval, expected
        );
        if (part->op.reply.hdr.retval == -EPIPE)
        {
            // Mark op->up_wait = true before stopping the client
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
        if (!op->retval || op->retval == -EPIPE)
        {
            // Don't overwrite other errors with -EPIPE
            op->retval = part->op.reply.hdr.retval;
        }
        msgr.stop_client(part->op.peer_fd);
        part->flags |= PART_ERROR;
    }
    else
    {
        // OK
        dirty_osds.insert(part->osd_num);
        part->flags |= PART_DONE;
        op->done_count++;
        if (op->opcode == OSD_OP_READ)
        {
            copy_part_bitmap(op, part);
        }
    }
    if (op->inflight_count == 0)
    {
        continue_ops();
    }
}

void cluster_client_t::copy_part_bitmap(cluster_op_t *op, cluster_op_part_t *part)
{
    // Copy (OR) bitmap
    auto & pool_cfg = st_cli.pool_config.at(INODE_POOL(op->cur_inode));
    uint32_t pg_block_size = bs_block_size * (
        pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks
    );
    uint32_t object_offset = (part->op.req.rw.offset - op->offset) / bs_bitmap_granularity;
    uint32_t part_offset = (part->op.req.rw.offset % pg_block_size) / bs_bitmap_granularity;
    uint32_t part_len = part->op.req.rw.len / bs_bitmap_granularity;
    if (!(object_offset & 0x7) && !(part_offset & 0x7) && (part_len >= 8))
    {
        // Copy bytes
        mem_or(op->bitmap_buf + object_offset/8, part->op.bitmap + part_offset/8, part_len/8);
        object_offset += (part_len & ~0x7);
        part_offset += (part_len & ~0x7);
        part_len = (part_len & 0x7);
    }
    while (part_len > 0)
    {
        // Copy bits
        (*(uint8_t*)(op->bitmap_buf + (object_offset >> 3))) |= (
            (((*(uint8_t*)(part->op.bitmap + (part_offset >> 3))) >> (part_offset & 0x7)) & 0x1) << (object_offset & 0x7)
        );
        part_offset++;
        object_offset++;
        part_len--;
    }
}
