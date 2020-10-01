// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

#include <sys/socket.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include "osd.h"

osd_t::osd_t(blockstore_config_t & config, blockstore_t *bs, ring_loop_t *ringloop)
{
    this->config = config;
    this->bs = bs;
    this->ringloop = ringloop;

    this->bs_block_size = bs->get_block_size();
    // FIXME: use bitmap granularity instead
    this->bs_disk_alignment = bs->get_disk_alignment();

    parse_config(config);

    epmgr = new epoll_manager_t(ringloop);
    this->tfd = epmgr->tfd;

    this->tfd->set_timer(print_stats_interval*1000, true, [this](int timer_id)
    {
        print_stats();
    });

    c_cli.tfd = this->tfd;
    c_cli.ringloop = this->ringloop;
    c_cli.exec_op = [this](osd_op_t *op) { exec_op(op); };
    c_cli.repeer_pgs = [this](osd_num_t peer_osd) { repeer_pgs(peer_osd); };

    init_cluster();

    consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(&consumer);
}

osd_t::~osd_t()
{
    ringloop->unregister_consumer(&consumer);
    delete epmgr;
    close(listen_fd);
}

void osd_t::parse_config(blockstore_config_t & config)
{
    // Initial startup configuration
    json11::Json json_config = json11::Json(config);
    st_cli.parse_config(json_config);
    etcd_report_interval = strtoull(config["etcd_report_interval"].c_str(), NULL, 10);
    if (etcd_report_interval <= 0)
        etcd_report_interval = 30;
    osd_num = strtoull(config["osd_num"].c_str(), NULL, 10);
    if (!osd_num)
        throw std::runtime_error("osd_num is required in the configuration");
    c_cli.osd_num = osd_num;
    run_primary = config["run_primary"] != "false" && config["run_primary"] != "0" && config["run_primary"] != "no";
    // Cluster configuration
    bind_address = config["bind_address"];
    if (bind_address == "")
        bind_address = "0.0.0.0";
    bind_port = stoull_full(config["bind_port"]);
    if (bind_port <= 0 || bind_port > 65535)
        bind_port = 0;
    if (config["immediate_commit"] == "all")
        immediate_commit = IMMEDIATE_ALL;
    else if (config["immediate_commit"] == "small")
        immediate_commit = IMMEDIATE_SMALL;
    if (config.find("autosync_interval") != config.end())
    {
        autosync_interval = strtoull(config["autosync_interval"].c_str(), NULL, 10);
        if (autosync_interval > MAX_AUTOSYNC_INTERVAL)
            autosync_interval = DEFAULT_AUTOSYNC_INTERVAL;
    }
    if (config.find("client_queue_depth") != config.end())
    {
        client_queue_depth = strtoull(config["client_queue_depth"].c_str(), NULL, 10);
        if (client_queue_depth < 128)
            client_queue_depth = 128;
    }
    recovery_queue_depth = strtoull(config["recovery_queue_depth"].c_str(), NULL, 10);
    if (recovery_queue_depth < 1 || recovery_queue_depth > MAX_RECOVERY_QUEUE)
        recovery_queue_depth = DEFAULT_RECOVERY_QUEUE;
    if (config["readonly"] == "true" || config["readonly"] == "1" || config["readonly"] == "yes")
        readonly = true;
    print_stats_interval = strtoull(config["print_stats_interval"].c_str(), NULL, 10);
    if (!print_stats_interval)
        print_stats_interval = 3;
    c_cli.peer_connect_interval = strtoull(config["peer_connect_interval"].c_str(), NULL, 10);
    if (!c_cli.peer_connect_interval)
        c_cli.peer_connect_interval = DEFAULT_PEER_CONNECT_INTERVAL;
    c_cli.peer_connect_timeout = strtoull(config["peer_connect_timeout"].c_str(), NULL, 10);
    if (!c_cli.peer_connect_timeout)
        c_cli.peer_connect_timeout = DEFAULT_PEER_CONNECT_TIMEOUT;
    log_level = strtoull(config["log_level"].c_str(), NULL, 10);
    c_cli.log_level = log_level;
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
        c_cli.accept_connections(listen_fd);
    });
}

bool osd_t::shutdown()
{
    stopping = true;
    if (inflight_ops > 0)
    {
        return false;
    }
    return bs->is_safe_to_stop();
}

void osd_t::loop()
{
    handle_peers();
    c_cli.read_requests();
    c_cli.send_replies();
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
    inflight_ops++;
    if (cur_op->req.hdr.magic != SECONDARY_OSD_OP_MAGIC ||
        cur_op->req.hdr.opcode < OSD_OP_MIN || cur_op->req.hdr.opcode > OSD_OP_MAX ||
        ((cur_op->req.hdr.opcode == OSD_OP_SEC_READ ||
            cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
            cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE) &&
            (cur_op->req.sec_rw.len > OSD_RW_MAX ||
            cur_op->req.sec_rw.len % bs_disk_alignment ||
            cur_op->req.sec_rw.offset % bs_disk_alignment)) ||
        ((cur_op->req.hdr.opcode == OSD_OP_READ ||
            cur_op->req.hdr.opcode == OSD_OP_WRITE ||
            cur_op->req.hdr.opcode == OSD_OP_DELETE) &&
            (cur_op->req.rw.len > OSD_RW_MAX ||
            cur_op->req.rw.len % bs_disk_alignment ||
            cur_op->req.rw.offset % bs_disk_alignment)))
    {
        // Bad command
        finish_op(cur_op, -EINVAL);
        return;
    }
    if (readonly &&
        cur_op->req.hdr.opcode != OSD_OP_SEC_READ &&
        cur_op->req.hdr.opcode != OSD_OP_SEC_LIST &&
        cur_op->req.hdr.opcode != OSD_OP_READ &&
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
    c_cli.stats = { 0 };
    prev_stats = { 0 };
    memset(recovery_stat_count, 0, sizeof(recovery_stat_count));
    memset(recovery_stat_bytes, 0, sizeof(recovery_stat_bytes));
}

void osd_t::print_stats()
{
    for (int i = 0; i <= OSD_OP_MAX; i++)
    {
        if (c_cli.stats.op_stat_count[i] != prev_stats.op_stat_count[i])
        {
            uint64_t avg = (c_cli.stats.op_stat_sum[i] - prev_stats.op_stat_sum[i])/(c_cli.stats.op_stat_count[i] - prev_stats.op_stat_count[i]);
            uint64_t bw = (c_cli.stats.op_stat_bytes[i] - prev_stats.op_stat_bytes[i]) / print_stats_interval;
            if (c_cli.stats.op_stat_bytes[i] != 0)
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
            prev_stats.op_stat_count[i] = c_cli.stats.op_stat_count[i];
            prev_stats.op_stat_sum[i] = c_cli.stats.op_stat_sum[i];
            prev_stats.op_stat_bytes[i] = c_cli.stats.op_stat_bytes[i];
        }
    }
    for (int i = 0; i <= OSD_OP_MAX; i++)
    {
        if (c_cli.stats.subop_stat_count[i] != prev_stats.subop_stat_count[i])
        {
            uint64_t avg = (c_cli.stats.subop_stat_sum[i] - prev_stats.subop_stat_sum[i])/(c_cli.stats.subop_stat_count[i] - prev_stats.subop_stat_count[i]);
            printf("[OSD %lu] avg latency for subop %d (%s): %ld us\n", osd_num, i, osd_op_names[i], avg);
            prev_stats.subop_stat_count[i] = c_cli.stats.subop_stat_count[i];
            prev_stats.subop_stat_sum[i] = c_cli.stats.subop_stat_sum[i];
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
