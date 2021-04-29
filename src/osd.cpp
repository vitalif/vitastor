// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <sys/socket.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include "osd.h"
#include "http_client.h"

static blockstore_config_t json_to_bs(const json11::Json::object & config)
{
    blockstore_config_t bs;
    for (auto kv: config)
    {
        if (kv.second.is_string())
            bs[kv.first] = kv.second.string_value();
        else
            bs[kv.first] = kv.second.dump();
    }
    return bs;
}

osd_t::osd_t(const json11::Json & config, ring_loop_t *ringloop)
{
    zero_buffer_size = 1<<20;
    zero_buffer = malloc_or_die(zero_buffer_size);
    memset(zero_buffer, 0, zero_buffer_size);

    this->ringloop = ringloop;

    this->config = msgr.read_config(config).object_items();
    if (this->config.find("log_level") == this->config.end())
        this->config["log_level"] = 1;
    parse_config(this->config);

    epmgr = new epoll_manager_t(ringloop);
    // FIXME: Use timerfd_interval based directly on io_uring
    this->tfd = epmgr->tfd;

    // FIXME: Create Blockstore from on-disk superblock config and check it against the OSD cluster config
    auto bs_cfg = json_to_bs(this->config);
    this->bs = new blockstore_t(bs_cfg, ringloop, tfd);

    this->tfd->set_timer(print_stats_interval*1000, true, [this](int timer_id)
    {
        print_stats();
    });
    this->tfd->set_timer(slow_log_interval*1000, true, [this](int timer_id)
    {
        print_slow();
    });

    msgr.tfd = this->tfd;
    msgr.ringloop = this->ringloop;
    msgr.exec_op = [this](osd_op_t *op) { exec_op(op); };
    msgr.repeer_pgs = [this](osd_num_t peer_osd) { repeer_pgs(peer_osd); };
    msgr.init();

    init_cluster();

    consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(&consumer);
}

osd_t::~osd_t()
{
    ringloop->unregister_consumer(&consumer);
    delete epmgr;
    delete bs;
    close(listen_fd);
    free(zero_buffer);
}

void osd_t::parse_config(const json11::Json & config)
{
    st_cli.parse_config(config);
    msgr.parse_config(config);
    // OSD number
    osd_num = config["osd_num"].uint64_value();
    if (!osd_num)
        throw std::runtime_error("osd_num is required in the configuration");
    msgr.osd_num = osd_num;
    // Vital Blockstore parameters
    bs_block_size = config["block_size"].uint64_value();
    if (!bs_block_size)
        bs_block_size = DEFAULT_BLOCK_SIZE;
    bs_bitmap_granularity = config["bitmap_granularity"].uint64_value();
    if (!bs_bitmap_granularity)
        bs_bitmap_granularity = DEFAULT_BITMAP_GRANULARITY;
    clean_entry_bitmap_size = bs_block_size / bs_bitmap_granularity / 8;
    // Bind address
    bind_address = config["bind_address"].string_value();
    if (bind_address == "")
        bind_address = "0.0.0.0";
    bind_port = config["bind_port"].uint64_value();
    if (bind_port <= 0 || bind_port > 65535)
        bind_port = 0;
    // OSD configuration
    log_level = config["log_level"].uint64_value();
    etcd_report_interval = config["etcd_report_interval"].uint64_value();
    if (etcd_report_interval <= 0)
        etcd_report_interval = 30;
    readonly = config["readonly"] == "true" || config["readonly"] == "1" || config["readonly"] == "yes";
    run_primary = config["run_primary"] != "false" && config["run_primary"] != "0" && config["run_primary"] != "no";
    no_rebalance = config["no_rebalance"] == "true" || config["no_rebalance"] == "1" || config["no_rebalance"] == "yes";
    no_recovery = config["no_recovery"] == "true" || config["no_recovery"] == "1" || config["no_recovery"] == "yes";
    allow_test_ops = config["allow_test_ops"] == "true" || config["allow_test_ops"] == "1" || config["allow_test_ops"] == "yes";
    if (config["immediate_commit"] == "all")
        immediate_commit = IMMEDIATE_ALL;
    else if (config["immediate_commit"] == "small")
        immediate_commit = IMMEDIATE_SMALL;
    if (!config["autosync_interval"].is_null())
    {
        // Allow to set it to 0
        autosync_interval = config["autosync_interval"].uint64_value();
        if (autosync_interval > MAX_AUTOSYNC_INTERVAL)
            autosync_interval = DEFAULT_AUTOSYNC_INTERVAL;
    }
    if (!config["client_queue_depth"].is_null())
    {
        client_queue_depth = config["client_queue_depth"].uint64_value();
        if (client_queue_depth < 128)
            client_queue_depth = 128;
    }
    recovery_queue_depth = config["recovery_queue_depth"].uint64_value();
    if (recovery_queue_depth < 1 || recovery_queue_depth > MAX_RECOVERY_QUEUE)
        recovery_queue_depth = DEFAULT_RECOVERY_QUEUE;
    recovery_sync_batch = config["recovery_sync_batch"].uint64_value();
    if (recovery_sync_batch < 1 || recovery_sync_batch > MAX_RECOVERY_QUEUE)
        recovery_sync_batch = DEFAULT_RECOVERY_BATCH;
    print_stats_interval = config["print_stats_interval"].uint64_value();
    if (!print_stats_interval)
        print_stats_interval = 3;
    slow_log_interval = config["slow_log_interval"].uint64_value();
    if (!slow_log_interval)
        slow_log_interval = 10;
}

void osd_t::bind_socket()
{
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0)
    {
        throw std::runtime_error(std::string("socket: ") + strerror(errno));
    }
    int enable = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));

    sockaddr_in addr;
    int r;
    if ((r = inet_pton(AF_INET, bind_address.c_str(), &addr.sin_addr)) != 1)
    {
        close(listen_fd);
        throw std::runtime_error("bind address "+bind_address+(r == 0 ? " is not valid" : ": no ipv4 support"));
    }
    addr.sin_family = AF_INET;

    addr.sin_port = htons(bind_port);
    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0)
    {
        close(listen_fd);
        throw std::runtime_error(std::string("bind: ") + strerror(errno));
    }
    if (bind_port == 0)
    {
        socklen_t len = sizeof(addr);
        if (getsockname(listen_fd, (sockaddr *)&addr, &len) == -1)
        {
            close(listen_fd);
            throw std::runtime_error(std::string("getsockname: ") + strerror(errno));
        }
        listening_port = ntohs(addr.sin_port);
    }
    else
    {
        listening_port = bind_port;
    }

    if (listen(listen_fd, listen_backlog) < 0)
    {
        close(listen_fd);
        throw std::runtime_error(std::string("listen: ") + strerror(errno));
    }

    fcntl(listen_fd, F_SETFL, fcntl(listen_fd, F_GETFL, 0) | O_NONBLOCK);

    epmgr->set_fd_handler(listen_fd, false, [this](int fd, int events)
    {
        msgr.accept_connections(listen_fd);
    });
}

bool osd_t::shutdown()
{
    stopping = true;
    if (inflight_ops > 0)
    {
        return false;
    }
    return !bs || bs->is_safe_to_stop();
}

void osd_t::loop()
{
    handle_peers();
    msgr.read_requests();
    msgr.send_replies();
    ringloop->submit();
}

void osd_t::exec_op(osd_op_t *cur_op)
{
    clock_gettime(CLOCK_REALTIME, &cur_op->tv_begin);
    if (stopping)
    {
        // Throw operation away
        delete cur_op;
        return;
    }
    // Clear the reply buffer
    memset(cur_op->reply.buf, 0, OSD_PACKET_SIZE);
    inflight_ops++;
    if (cur_op->req.hdr.magic != SECONDARY_OSD_OP_MAGIC ||
        cur_op->req.hdr.opcode < OSD_OP_MIN || cur_op->req.hdr.opcode > OSD_OP_MAX ||
        ((cur_op->req.hdr.opcode == OSD_OP_SEC_READ ||
            cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
            cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE) &&
            (cur_op->req.sec_rw.len > OSD_RW_MAX ||
            cur_op->req.sec_rw.len % bs_bitmap_granularity ||
            cur_op->req.sec_rw.offset % bs_bitmap_granularity)) ||
        ((cur_op->req.hdr.opcode == OSD_OP_READ ||
            cur_op->req.hdr.opcode == OSD_OP_WRITE ||
            cur_op->req.hdr.opcode == OSD_OP_DELETE) &&
            (cur_op->req.rw.len > OSD_RW_MAX ||
            cur_op->req.rw.len % bs_bitmap_granularity ||
            cur_op->req.rw.offset % bs_bitmap_granularity)))
    {
        // Bad command
        finish_op(cur_op, -EINVAL);
        return;
    }
    if (cur_op->req.hdr.opcode == OSD_OP_PING)
    {
        // Pong
        finish_op(cur_op, 0);
        return;
    }
    if (readonly &&
        cur_op->req.hdr.opcode != OSD_OP_SEC_READ &&
        cur_op->req.hdr.opcode != OSD_OP_SEC_LIST &&
        cur_op->req.hdr.opcode != OSD_OP_READ &&
        cur_op->req.hdr.opcode != OSD_OP_SEC_READ_BMP &&
        cur_op->req.hdr.opcode != OSD_OP_SHOW_CONFIG)
    {
        // Readonly mode
        finish_op(cur_op, -EROFS);
        return;
    }
    if (cur_op->req.hdr.opcode == OSD_OP_TEST_SYNC_STAB_ALL)
    {
        exec_sync_stab_all(cur_op);
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SHOW_CONFIG)
    {
        exec_show_config(cur_op);
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_READ)
    {
        continue_primary_read(cur_op);
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_WRITE)
    {
        continue_primary_write(cur_op);
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SYNC)
    {
        continue_primary_sync(cur_op);
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_DELETE)
    {
        continue_primary_del(cur_op);
    }
    else
    {
        exec_secondary(cur_op);
    }
}

void osd_t::reset_stats()
{
    msgr.stats = { 0 };
    prev_stats = { 0 };
    memset(recovery_stat_count, 0, sizeof(recovery_stat_count));
    memset(recovery_stat_bytes, 0, sizeof(recovery_stat_bytes));
}

void osd_t::print_stats()
{
    for (int i = OSD_OP_MIN; i <= OSD_OP_MAX; i++)
    {
        if (msgr.stats.op_stat_count[i] != prev_stats.op_stat_count[i] && i != OSD_OP_PING)
        {
            uint64_t avg = (msgr.stats.op_stat_sum[i] - prev_stats.op_stat_sum[i])/(msgr.stats.op_stat_count[i] - prev_stats.op_stat_count[i]);
            uint64_t bw = (msgr.stats.op_stat_bytes[i] - prev_stats.op_stat_bytes[i]) / print_stats_interval;
            if (msgr.stats.op_stat_bytes[i] != 0)
            {
                printf(
                    "[OSD %lu] avg latency for op %d (%s): %lu us, B/W: %.2f %s\n", osd_num, i, osd_op_names[i], avg,
                    (bw > 1024*1024*1024 ? bw/1024.0/1024/1024 : (bw > 1024*1024 ? bw/1024.0/1024 : bw/1024.0)),
                    (bw > 1024*1024*1024 ? "GB/s" : (bw > 1024*1024 ? "MB/s" : "KB/s"))
                );
            }
            else
            {
                printf("[OSD %lu] avg latency for op %d (%s): %lu us\n", osd_num, i, osd_op_names[i], avg);
            }
            prev_stats.op_stat_count[i] = msgr.stats.op_stat_count[i];
            prev_stats.op_stat_sum[i] = msgr.stats.op_stat_sum[i];
            prev_stats.op_stat_bytes[i] = msgr.stats.op_stat_bytes[i];
        }
    }
    for (int i = OSD_OP_MIN; i <= OSD_OP_MAX; i++)
    {
        if (msgr.stats.subop_stat_count[i] != prev_stats.subop_stat_count[i])
        {
            uint64_t avg = (msgr.stats.subop_stat_sum[i] - prev_stats.subop_stat_sum[i])/(msgr.stats.subop_stat_count[i] - prev_stats.subop_stat_count[i]);
            printf("[OSD %lu] avg latency for subop %d (%s): %ld us\n", osd_num, i, osd_op_names[i], avg);
            prev_stats.subop_stat_count[i] = msgr.stats.subop_stat_count[i];
            prev_stats.subop_stat_sum[i] = msgr.stats.subop_stat_sum[i];
        }
    }
    for (int i = 0; i < 2; i++)
    {
        if (recovery_stat_count[0][i] != recovery_stat_count[1][i])
        {
            uint64_t bw = (recovery_stat_bytes[0][i] - recovery_stat_bytes[1][i]) / print_stats_interval;
            printf(
                "[OSD %lu] %s recovery: %.1f op/s, B/W: %.2f %s\n", osd_num, recovery_stat_names[i],
                (recovery_stat_count[0][i] - recovery_stat_count[1][i]) * 1.0 / print_stats_interval,
                (bw > 1024*1024*1024 ? bw/1024.0/1024/1024 : (bw > 1024*1024 ? bw/1024.0/1024 : bw/1024.0)),
                (bw > 1024*1024*1024 ? "GB/s" : (bw > 1024*1024 ? "MB/s" : "KB/s"))
            );
            recovery_stat_count[1][i] = recovery_stat_count[0][i];
            recovery_stat_bytes[1][i] = recovery_stat_bytes[0][i];
        }
    }
    if (incomplete_objects > 0)
    {
        printf("[OSD %lu] %lu object(s) incomplete\n", osd_num, incomplete_objects);
    }
    if (degraded_objects > 0)
    {
        printf("[OSD %lu] %lu object(s) degraded\n", osd_num, degraded_objects);
    }
    if (misplaced_objects > 0)
    {
        printf("[OSD %lu] %lu object(s) misplaced\n", osd_num, misplaced_objects);
    }
}

void osd_t::print_slow()
{
    char alloc[1024];
    timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    for (auto & kv: msgr.clients)
    {
        for (auto op: kv.second->received_ops)
        {
            if ((now.tv_sec - op->tv_begin.tv_sec) >= slow_log_interval)
            {
                int l = sizeof(alloc), n;
                char *buf = alloc;
#define bufprintf(s, ...) { n = snprintf(buf, l, s, __VA_ARGS__); n = n < 0 ? 0 : n; buf += n; l -= n; }
                bufprintf("[OSD %lu] Slow op", osd_num);
                if (kv.second->osd_num)
                {
                    bufprintf(" from peer OSD %lu (client %d)", kv.second->osd_num, kv.second->peer_fd);
                }
                else
                {
                    bufprintf(" from client %d", kv.second->peer_fd);
                }
                bufprintf(": %s id=%lu", osd_op_names[op->req.hdr.opcode], op->req.hdr.id);
                if (op->req.hdr.opcode == OSD_OP_SEC_READ || op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
                    op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE || op->req.hdr.opcode == OSD_OP_SEC_DELETE)
                {
                    bufprintf(" %lx:%lx v", op->req.sec_rw.oid.inode, op->req.sec_rw.oid.stripe);
                    if (op->req.sec_rw.version == UINT64_MAX)
                    {
                        bufprintf("%s", "max");
                    }
                    else
                    {
                        bufprintf("%lu", op->req.sec_rw.version);
                    }
                    if (op->req.hdr.opcode != OSD_OP_SEC_DELETE)
                    {
                        bufprintf(" offset=%x len=%x", op->req.sec_rw.offset, op->req.sec_rw.len);
                    }
                }
                else if (op->req.hdr.opcode == OSD_OP_SEC_STABILIZE || op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK)
                {
                    for (uint64_t i = 0; i < op->req.sec_stab.len; i += sizeof(obj_ver_id))
                    {
                        obj_ver_id *ov = (obj_ver_id*)(op->buf + i);
                        bufprintf(i == 0 ? " %lx:%lx v%lu" : ", %lx:%lx v%lu", ov->oid.inode, ov->oid.stripe, ov->version);
                    }
                }
                else if (op->req.hdr.opcode == OSD_OP_SEC_LIST)
                {
                    bufprintf(
                        " inode=%lx-%lx pg=%u/%u, stripe=%lu",
                        op->req.sec_list.min_inode, op->req.sec_list.max_inode,
                        op->req.sec_list.list_pg, op->req.sec_list.pg_count,
                        op->req.sec_list.pg_stripe_size
                    );
                }
                else if (op->req.hdr.opcode == OSD_OP_READ || op->req.hdr.opcode == OSD_OP_WRITE ||
                    op->req.hdr.opcode == OSD_OP_DELETE)
                {
                    bufprintf(" inode=%lx offset=%lx len=%x", op->req.rw.inode, op->req.rw.offset, op->req.rw.len);
                }
#undef bufprintf
                printf("%s\n", alloc);
            }
        }
    }
}
