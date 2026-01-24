// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <sys/socket.h>
#include <sys/poll.h>
#include <sys/mman.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include "addr_util.h"
#include "osd_primary.h"
#include "osd.h"
#include "http_client.h"
#include "str_util.h"
#include "json_util.h"

osd_t::osd_t(const json11::Json & config, ring_loop_t *ringloop)
{
    zero_buffer_size = 1<<20;
    zero_buffer = malloc_or_die(zero_buffer_size);
    memset(zero_buffer, 0, zero_buffer_size);

    this->ringloop = ringloop;

    this->cli_config = config.object_items();
    this->file_config = msgr.read_config(this->cli_config);
    parse_config(true);

    epmgr = new epoll_manager_t(ringloop);
    // FIXME: Use timerfd_interval based directly on io_uring
    this->tfd = epmgr->tfd;

    if (json_is_true(this->config["osd_memlock"]))
    {
        // Lock all OSD memory if requested
        if (mlockall(MCL_CURRENT|MCL_FUTURE
#ifdef MCL_ONFAULT
            | MCL_ONFAULT
#endif
            ) != 0)
        {
            fprintf(stderr, "osd_memlock is set to true, but mlockall() failed: %s\n", strerror(errno));
            exit(-1);
        }
    }

    if (print_stats_timer_id == -1)
    {
        print_stats_timer_id = this->tfd->set_timer(print_stats_interval*1000, true, [this](int timer_id)
        {
            print_stats();
        });
    }
    if (slow_log_timer_id == -1)
    {
        slow_log_timer_id = this->tfd->set_timer(slow_log_interval*1000, true, [this](int timer_id)
        {
            print_slow();
        });
    }
    apply_recovery_tune_interval();

    msgr.tfd = this->tfd;
    msgr.ringloop = this->ringloop;
    msgr.exec_op = [this](osd_op_t *op) { exec_op(op); };
    msgr.repeer_pgs = [this](osd_num_t peer_osd) { repeer_pgs(peer_osd); };
    msgr.break_pg_locks = [this](osd_num_t peer_osd) { break_pg_locks(peer_osd); };
    msgr.check_config_hook = [this](osd_client_t *cl, json11::Json conf) { return check_peer_config(cl, conf); };
    msgr.init();

    init_cluster();

    consumer.loop = [this]() { loop(); };
    ringloop->register_consumer(&consumer);
}

osd_t::~osd_t()
{
    if (slow_log_timer_id >= 0)
    {
        tfd->clear_timer(slow_log_timer_id);
        slow_log_timer_id = -1;
    }
    if (rtune_timer_id >= 0)
    {
        tfd->clear_timer(rtune_timer_id);
        rtune_timer_id = -1;
    }
    if (print_stats_timer_id >= 0)
    {
        tfd->clear_timer(print_stats_timer_id);
        print_stats_timer_id = -1;
    }
    if (autosync_timer_id >= 0)
    {
        tfd->clear_timer(autosync_timer_id);
        autosync_timer_id = -1;
    }
    ringloop->unregister_consumer(&consumer);
    ringloop->unregister_consumer(&init_consumer);
    delete epmgr;
    if (bs)
        delete bs;
#ifdef WITH_RDMACM
    for (rdma_cm_id *listener: rdmacm_listeners)
        msgr.rdmacm_destroy_listener(listener);
    rdmacm_listeners.clear();
#endif
    for (auto listen_fd: listen_fds)
        close(listen_fd);
    listen_fds.clear();
    free(zero_buffer);
}

void osd_t::init_blockstore(std::function<void()> on_init)
{
    if (!json_is_true(this->config["disable_blockstore"]))
    {
        auto bs_cfg = json_to_string_map(this->config);
        this->bs = blockstore_i::create(bs_cfg, ringloop, tfd);
        // Pre-configure pool PG shards
        for (auto & pool_item: st_cli.pool_config)
        {
            auto st = bs->reshard_start(pool_item.first, pool_item.second.pg_count, pool_item.second.pg_stripe_size, 0);
            assert(!st);
        }
        // Autosync based on the number of unstable writes to prevent stalls due to insufficient journal space
        uint64_t max_autosync = bs->get_journal_size() / bs->get_block_size() / 2;
        if (autosync_writes > max_autosync)
            autosync_writes = max_autosync;
        if (on_init)
        {
            init_consumer.loop = [this, on_init]()
            {
                // Wait for blockstore initialisation before actually starting OSD logic
                // to prevent peering timeouts during restart with filled databases
                if (bs->is_started())
                {
                    ringloop->set_immediate([this, on_init] { init_consumer.loop = NULL; on_init(); });
                    ringloop->unregister_consumer(&init_consumer);
                }
            };
            ringloop->register_consumer(&init_consumer);
        }
    }
    else if (on_init)
    {
        on_init();
    }
}

void osd_t::parse_config(bool init)
{
    config = msgr.merge_configs(cli_config, file_config, etcd_global_config, etcd_osd_config);
    if (config.find("log_level") == this->config.end())
        config["log_level"] = 1;
    if (init)
    {
        // OSD number
        osd_num = config["osd_num"].uint64_value();
        if (!osd_num)
            throw std::runtime_error("osd_num is required in the configuration");
        msgr.osd_num = osd_num;
    }
    if (bs)
    {
        auto bs_cfg = json_to_string_map(config);
        bs->parse_config(bs_cfg);
    }
    st_cli.parse_config(config);
    msgr.parse_config(config);
    if (init)
    {
        // Vital Blockstore parameters
        bs_block_size = config["block_size"].uint64_value();
        if (!bs_block_size)
            bs_block_size = DEFAULT_BLOCK_SIZE;
        bs_bitmap_granularity = config["bitmap_granularity"].uint64_value();
        if (!bs_bitmap_granularity)
            bs_bitmap_granularity = DEFAULT_BITMAP_GRANULARITY;
        clean_entry_bitmap_size = bs_block_size / bs_bitmap_granularity / 8;
        // immediate_commit
        if (config["immediate_commit"] == "all")
            immediate_commit = IMMEDIATE_ALL;
        else if (config["immediate_commit"] == "small")
            immediate_commit = IMMEDIATE_SMALL;
        else
            immediate_commit = IMMEDIATE_NONE;
        // Bind address
        cfg_bind_addresses.clear();
        if (config.find("bind_address") != config.end())
        {
            if (config["bind_address"].is_string())
                cfg_bind_addresses.push_back(config["bind_address"].string_value());
            else if (config["bind_address"].is_array())
                for (auto & addr: config["bind_address"].array_items())
                    cfg_bind_addresses.push_back(addr.string_value());
        }
        bind_port = config["bind_port"].uint64_value();
        if (bind_port <= 0 || bind_port > 65535)
            bind_port = 0;
#ifdef WITH_RDMACM
        // Use RDMA CM? (required for iWARP and may be useful for IB)
        this->use_rdmacm = config["use_rdmacm"].bool_value() || config["use_rdmacm"].uint64_value() != 0;
        this->disable_tcp = this->use_rdmacm && (config["disable_tcp"].bool_value() || config["disable_tcp"].uint64_value() != 0);
#endif
        // OSD configuration
        etcd_report_interval = config["etcd_report_interval"].uint64_value();
        if (etcd_report_interval <= 0)
            etcd_report_interval = 5;
        etcd_stats_interval = config["etcd_stats_interval"].uint64_value();
        if (etcd_stats_interval <= 0)
            etcd_stats_interval = 30;
        readonly = json_is_true(config["readonly"]);
        run_primary = !json_is_false(config["run_primary"]);
    }
    log_level = config["log_level"].uint64_value();
    auto old_no_rebalance = no_rebalance;
    no_rebalance = json_is_true(config["no_rebalance"]);
    auto old_no_recovery = no_recovery;
    no_recovery = json_is_true(config["no_recovery"]);
    auto old_no_scrub = no_scrub;
    no_scrub = json_is_true(config["no_scrub"]);
    allow_net_split = json_is_true(config["allow_net_split"]);
    auto old_autosync_interval = autosync_interval;
    if (!config["autosync_interval"].is_null())
    {
        // Allow to set it to 0
        autosync_interval = config["autosync_interval"].uint64_value();
        if (autosync_interval > MAX_AUTOSYNC_INTERVAL)
            autosync_interval = DEFAULT_AUTOSYNC_INTERVAL;
    }
    if (config["autosync_writes"].is_number() ||
        config["autosync_writes"].string_value() != "")
    {
        // Allow to set it to 0
        autosync_writes = config["autosync_writes"].uint64_value();
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
    recovery_sleep_us = config["recovery_sleep_us"].uint64_value();
    recovery_tune_util_low = config["recovery_tune_util_low"].is_null()
        ? 0.1 : config["recovery_tune_util_low"].number_value();
    if (recovery_tune_util_low < 0.01)
        recovery_tune_util_low = 0.01;
    recovery_tune_util_high = config["recovery_tune_util_high"].is_null()
        ? 1.0 : config["recovery_tune_util_high"].number_value();
    if (recovery_tune_util_high < 0.01)
        recovery_tune_util_high = 0.01;
    recovery_tune_client_util_low = config["recovery_tune_client_util_low"].is_null()
        ? 0 : config["recovery_tune_client_util_low"].number_value();
    if (recovery_tune_client_util_low < 0.01)
        recovery_tune_client_util_low = 0.01;
    recovery_tune_client_util_high = config["recovery_tune_client_util_high"].is_null()
        ? 0.5 : config["recovery_tune_client_util_high"].number_value();
    if (recovery_tune_client_util_high < 0.01)
        recovery_tune_client_util_high = 0.01;
    auto old_recovery_tune_interval = recovery_tune_interval;
    recovery_tune_interval = config["recovery_tune_interval"].is_null()
        ? 1 : config["recovery_tune_interval"].uint64_value();
    recovery_tune_agg_interval = config["recovery_tune_agg_interval"].is_null()
        ? 10 : config["recovery_tune_agg_interval"].uint64_value();
    recovery_tune_sleep_min_us = config["recovery_tune_sleep_min_us"].is_null()
        ? 10 : config["recovery_tune_sleep_min_us"].uint64_value();
    recovery_tune_sleep_cutoff_us = config["recovery_tune_sleep_cutoff_us"].is_null()
        ? 10000000 : config["recovery_tune_sleep_cutoff_us"].uint64_value();
    recovery_pg_switch = config["recovery_pg_switch"].uint64_value();
    if (recovery_pg_switch < 1)
        recovery_pg_switch = DEFAULT_RECOVERY_PG_SWITCH;
    recovery_sync_batch = config["recovery_sync_batch"].uint64_value();
    if (recovery_sync_batch < 1 || recovery_sync_batch > MAX_RECOVERY_QUEUE)
        recovery_sync_batch = DEFAULT_RECOVERY_BATCH;
    auto old_print_stats_interval = print_stats_interval;
    print_stats_interval = config["print_stats_interval"].uint64_value();
    if (!print_stats_interval)
        print_stats_interval = 3;
    auto old_slow_log_interval = slow_log_interval;
    slow_log_interval = config["slow_log_interval"].uint64_value();
    if (!slow_log_interval)
        slow_log_interval = 10;
    inode_vanish_time = config["inode_vanish_time"].uint64_value();
    if (!inode_vanish_time)
        inode_vanish_time = 60;
    enable_pg_locks = config["enable_pg_locks"].is_null() || json_is_true(config["enable_pg_locks"]);
    bool old_pg_locks_localize_only = pg_locks_localize_only;
    pg_locks_localize_only = config["enable_pg_locks"].is_null();
    pg_lock_retry_interval_ms = config["pg_lock_retry_interval"].uint64_value();
    if (pg_lock_retry_interval_ms <= 1)
        pg_lock_retry_interval_ms = 100;
    auto old_auto_scrub = auto_scrub;
    auto_scrub = json_is_true(config["auto_scrub"]);
    global_scrub_interval = parse_time(config["scrub_interval"].string_value());
    if (!global_scrub_interval)
        global_scrub_interval = 30*86400;
    scrub_queue_depth = config["scrub_queue_depth"].uint64_value();
    if (scrub_queue_depth < 1 || scrub_queue_depth > MAX_RECOVERY_QUEUE)
        scrub_queue_depth = 1;
    scrub_find_best = !json_is_false(config["scrub_find_best"]);
    scrub_ec_max_bruteforce = config["scrub_ec_max_bruteforce"].uint64_value();
    if (scrub_ec_max_bruteforce < 1)
        scrub_ec_max_bruteforce = 100;
    scrub_sleep_ms = config["scrub_sleep"].uint64_value();
    scrub_list_limit = config["scrub_list_limit"].uint64_value();
    if (!scrub_list_limit)
        scrub_list_limit = 262144;
    pg_reshard_chunk_size = config["pg_reshard_chunk_size"].uint64_value();
    if (!pg_reshard_chunk_size)
        pg_reshard_chunk_size = 100000;
    pg_reshard_chunk_pause_ms = config["pg_reshard_chunk_pause_ms"].uint64_value();
    if (!pg_reshard_chunk_pause_ms)
        pg_reshard_chunk_pause_ms = 100;
    if (!old_auto_scrub && auto_scrub)
    {
        // Schedule scrubbing
        for (auto & pgp: pgs)
        {
            plan_scrub(pgp.second);
        }
    }
    if (old_no_scrub && !no_scrub)
    {
        // Wakeup scrubbing
        for (auto & pgp: pgs)
        {
            schedule_scrub(pgp.second);
        }
    }
    if ((old_no_rebalance && !no_rebalance || old_no_recovery && !no_recovery) &&
        !(peering_state & (OSD_RECOVERING | OSD_FLUSHING_PGS)))
    {
        peering_state = peering_state | OSD_RECOVERING;
    }
    if (old_autosync_interval != autosync_interval && autosync_timer_id >= 0)
    {
        this->tfd->clear_timer(autosync_timer_id);
        autosync_timer_id = this->tfd->set_timer(autosync_interval*1000, true, [this](int timer_id)
        {
            autosync();
        });
    }
    if (old_print_stats_interval != print_stats_interval && print_stats_timer_id >= 0)
    {
        tfd->clear_timer(print_stats_timer_id);
        print_stats_timer_id = this->tfd->set_timer(print_stats_interval*1000, true, [this](int timer_id)
        {
            print_stats();
        });
    }
    if (old_slow_log_interval != slow_log_interval && slow_log_timer_id >= 0)
    {
        tfd->clear_timer(slow_log_timer_id);
        slow_log_timer_id = this->tfd->set_timer(slow_log_interval*1000, true, [this](int timer_id)
        {
            print_slow();
        });
    }
    if (old_recovery_tune_interval != recovery_tune_interval)
    {
        apply_recovery_tune_interval();
    }
    if (old_pg_locks_localize_only != pg_locks_localize_only)
    {
        apply_pg_locks_localize_only();
    }
}

void osd_t::bind_socket()
{
    if (cfg_bind_addresses.size())
    {
        bind_addresses = cfg_bind_addresses;
    }
    else if (msgr.all_osd_network_masks.size())
    {
        bind_addresses = getifaddr_list(msgr.all_osd_network_masks);
        if (!bind_addresses.size())
        {
            auto nets = implode(", ", msgr.all_osd_networks);
            fprintf(stderr, "Addresses matching osd_network(s) %s not found\n", nets.c_str());
            force_stop(1);
        }
    }
    else
    {
        bind_addresses.push_back("0.0.0.0");
    }
    if (!disable_tcp)
    {
        for (auto & bind_address: bind_addresses)
        {
            int listen_fd = create_and_bind_socket(bind_address, listening_port ? listening_port : bind_port, listen_backlog, &listening_port);
            fcntl(listen_fd, F_SETFL, fcntl(listen_fd, F_GETFL, 0) | O_NONBLOCK);
            epmgr->set_fd_handler(listen_fd, false, [this](int fd, int events)
            {
                msgr.accept_connections(fd);
            });
            listen_fds.push_back(listen_fd);
        }
    }
#ifdef WITH_RDMACM
    if (use_rdmacm)
    {
        for (auto & bind_address: bind_addresses)
        {
            // iWARP shares port numbers with TCP traffic and can't listen on the same port with TCP and RDMACM
            auto listener = msgr.rdmacm_listen(bind_address, rdmacm_port, &rdmacm_port, log_level);
            if (listener)
                rdmacm_listeners.push_back(listener);
        }
        if (!rdmacm_listeners.size() && disable_tcp)
        {
            fprintf(stderr, "Failed to create RDMA-CM listeners, exiting\n");
            force_stop(1);
        }
    }
#endif
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
        cur_op->req.hdr.opcode != OSD_OP_SEC_LOCK &&
        cur_op->req.hdr.opcode != OSD_OP_SEC_LIST &&
        cur_op->req.hdr.opcode != OSD_OP_READ &&
        cur_op->req.hdr.opcode != OSD_OP_SEC_READ_BMP &&
        cur_op->req.hdr.opcode != OSD_OP_SCRUB &&
        cur_op->req.hdr.opcode != OSD_OP_DESCRIBE &&
        cur_op->req.hdr.opcode != OSD_OP_SHOW_CONFIG)
    {
        // Readonly mode
        finish_op(cur_op, -EROFS);
        return;
    }
    if (cur_op->req.hdr.opcode == OSD_OP_SHOW_CONFIG)
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
    else if (cur_op->req.hdr.opcode == OSD_OP_SCRUB)
    {
        continue_primary_scrub(cur_op);
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_DESCRIBE)
    {
        continue_primary_describe(cur_op);
    }
    else
    {
        exec_secondary(cur_op);
    }
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
                    "[OSD %ju] avg latency for op %d (%s): %ju us, B/W: %.2f %s\n", osd_num, i, osd_op_names[i], avg,
                    (bw > 1024*1024*1024 ? bw/1024.0/1024/1024 : (bw > 1024*1024 ? bw/1024.0/1024 : bw/1024.0)),
                    (bw > 1024*1024*1024 ? "GB/s" : (bw > 1024*1024 ? "MB/s" : "KB/s"))
                );
            }
            else
            {
                printf("[OSD %ju] avg latency for op %d (%s): %ju us\n", osd_num, i, osd_op_names[i], avg);
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
            printf("[OSD %ju] avg latency for subop %d (%s): %jd us\n", osd_num, i, osd_op_names[i], avg);
            prev_stats.subop_stat_count[i] = msgr.stats.subop_stat_count[i];
            prev_stats.subop_stat_sum[i] = msgr.stats.subop_stat_sum[i];
        }
    }
    for (int i = 0; i < 2; i++)
    {
        if (recovery_stat[i].count > recovery_print_prev[i].count)
        {
            uint64_t bw = (recovery_stat[i].bytes - recovery_print_prev[i].bytes) / print_stats_interval;
            printf(
                "[OSD %ju] %s recovery: %.1f op/s, B/W: %.2f %s, avg latency %jd us, delay %jd us\n", osd_num, recovery_stat_names[i],
                (recovery_stat[i].count - recovery_print_prev[i].count) * 1.0 / print_stats_interval,
                (bw > 1024*1024*1024 ? bw/1024.0/1024/1024 : (bw > 1024*1024 ? bw/1024.0/1024 : bw/1024.0)),
                (bw > 1024*1024*1024 ? "GB/s" : (bw > 1024*1024 ? "MB/s" : "KB/s")),
                (recovery_stat[i].usec - recovery_print_prev[i].usec) / (recovery_stat[i].count - recovery_print_prev[i].count),
                recovery_target_sleep_us
            );
        }
    }
    memcpy(recovery_print_prev, recovery_stat, sizeof(recovery_stat));
    if (corrupted_objects > 0)
    {
        printf("[OSD %ju] %ju object(s) corrupted\n", osd_num, corrupted_objects);
    }
    if (incomplete_objects > 0)
    {
        printf("[OSD %ju] %ju object(s) incomplete\n", osd_num, incomplete_objects);
    }
    if (degraded_objects > 0)
    {
        printf("[OSD %ju] %ju object(s) degraded\n", osd_num, degraded_objects);
    }
    if (misplaced_objects > 0)
    {
        printf("[OSD %ju] %ju object(s) misplaced\n", osd_num, misplaced_objects);
    }
}

void osd_t::print_slow()
{
    cur_slow_op_primary = 0;
    cur_slow_op_secondary = 0;
    char alloc[1024];
    timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    // FIXME: Also track slow local blockstore ops and recovery/flush/scrub ops
    for (auto & kv: msgr.clients)
    {
        for (auto op: kv.second->received_ops)
        {
            if ((now.tv_sec - op->tv_begin.tv_sec) >= slow_log_interval)
            {
                int l = sizeof(alloc), n;
                char *buf = alloc;
#define bufprintf(s, ...) { n = snprintf(buf, l, s, __VA_ARGS__); n = n < 0 ? 0 : n; buf += n; l -= n; }
                bufprintf("[OSD %ju] Slow op %jx", osd_num, (uint64_t)op);
                if (kv.second->osd_num)
                {
                    bufprintf(" from peer OSD %ju (client %d)", kv.second->osd_num, kv.second->peer_fd);
                }
                else
                {
                    bufprintf(" from client %d", kv.second->peer_fd);
                }
                bufprintf(": %s id=%ju", osd_op_names[op->req.hdr.opcode], op->req.hdr.id);
                if (op->req.hdr.opcode == OSD_OP_SEC_READ || op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
                    op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE || op->req.hdr.opcode == OSD_OP_SEC_DELETE)
                {
                    bufprintf(" %jx:%jx v", op->req.sec_rw.oid.inode, op->req.sec_rw.oid.stripe);
                    if (op->req.sec_rw.version == UINT64_MAX)
                    {
                        bufprintf("%s", "max");
                    }
                    else
                    {
                        bufprintf("%ju", op->req.sec_rw.version);
                    }
                    if (op->req.hdr.opcode != OSD_OP_SEC_DELETE)
                    {
                        bufprintf(" offset=%x len=%x", op->req.sec_rw.offset, op->req.sec_rw.len);
                    }
                }
                else if (op->req.hdr.opcode == OSD_OP_SEC_STABILIZE || op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK)
                {
                    for (uint64_t i = 0; i < op->req.sec_stab.len && i < sizeof(obj_ver_id)*12; i += sizeof(obj_ver_id))
                    {
                        obj_ver_id *ov = (obj_ver_id*)((uint8_t*)op->buf + i);
                        bufprintf(i == 0 ? " %jx:%jx v%ju" : ", %jx:%jx v%ju", ov->oid.inode, ov->oid.stripe, ov->version);
                    }
                    if (op->req.sec_stab.len > sizeof(obj_ver_id)*12)
                    {
                        bufprintf(", ... (%ju items)", op->req.sec_stab.len/sizeof(obj_ver_id));
                    }
                }
                else if (op->req.hdr.opcode == OSD_OP_SEC_LIST)
                {
                    bufprintf(
                        " oid=%jx/%jx-%jx/%jx pg=%u/%u, stripe=%ju, limit=%u",
                        op->req.sec_list.min_inode, op->req.sec_list.min_stripe,
                        op->req.sec_list.max_inode, op->req.sec_list.max_stripe,
                        op->req.sec_list.list_pg, op->req.sec_list.pg_count,
                        op->req.sec_list.pg_stripe_size, op->req.sec_list.stable_limit
                    );
                }
                else if (op->req.hdr.opcode == OSD_OP_READ || op->req.hdr.opcode == OSD_OP_WRITE ||
                    op->req.hdr.opcode == OSD_OP_DELETE)
                {
                    bufprintf(" inode=%jx offset=%jx len=%x", op->req.rw.inode, op->req.rw.offset, op->req.rw.len);
                }
                if (op->req.hdr.opcode == OSD_OP_SEC_READ || op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
                    op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE || op->req.hdr.opcode == OSD_OP_SEC_DELETE ||
                    op->req.hdr.opcode == OSD_OP_SEC_SYNC || op->req.hdr.opcode == OSD_OP_SEC_LIST ||
                    op->req.hdr.opcode == OSD_OP_SEC_STABILIZE || op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK ||
                    op->req.hdr.opcode == OSD_OP_SEC_READ_BMP)
                {
                    cur_slow_op_secondary++;
                    if (op->bs_op)
                    {
                        auto diag = bs->get_op_diag(op->bs_op);
                        if (diag != "")
                            bufprintf(" %s", diag.c_str());
                    }
                }
                else if (op->req.hdr.opcode == OSD_OP_READ || op->req.hdr.opcode == OSD_OP_WRITE ||
                    op->req.hdr.opcode == OSD_OP_SYNC || op->req.hdr.opcode == OSD_OP_DELETE)
                {
                    cur_slow_op_primary++;
                    bufprintf(" state=%d", !op->op_data ? -1 : op->op_data->st);
                }
                else
                {
                    cur_slow_op_primary++;
                }
#undef bufprintf
                printf("%s\n", alloc);
            }
        }
    }
    if ((cur_slow_op_primary+cur_slow_op_secondary) > 0 && bs)
    {
        bs->dump_diagnostics();
    }
}
