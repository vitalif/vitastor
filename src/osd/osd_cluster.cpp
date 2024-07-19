// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "osd.h"
#include "str_util.h"
#include "etcd_state_client.h"
#include "http_client.h"
#include "osd_rmw.h"
#include "addr_util.h"

// Startup sequence:
//   Start etcd watcher -> Load global OSD configuration -> Bind socket -> Acquire lease -> Report&lock OSD state
//   -> Load PG config -> Report&lock PG states -> Load peers -> Connect to peers -> Peer PGs
// Event handling
//   Wait for PG changes -> Start/Stop PGs when requested
//   Peer connection is lost -> Reload connection data -> Try to reconnect
void osd_t::init_cluster()
{
    if (!st_cli.address_count())
    {
        if (run_primary)
        {
            // Test version of clustering code with 1 pool, 1 PG and 2 peers
            // Example: peers = 2:127.0.0.1:11204,3:127.0.0.1:11205
            std::string peerstr = config["peers"].string_value();
            while (peerstr.size())
            {
                int pos = peerstr.find(',');
                parse_test_peer(pos < 0 ? peerstr : peerstr.substr(0, pos));
                peerstr = pos < 0 ? std::string("") : peerstr.substr(pos+1);
            }
            if (st_cli.peer_states.size() < 2)
            {
                throw std::runtime_error("run_primary requires at least 2 peers");
            }
            pgs[{ 1, 1 }] = (pg_t){
                .state = PG_PEERING,
                .scheme = POOL_SCHEME_XOR,
                .pg_cursize = 0,
                .pg_size = 3,
                .pg_minsize = 2,
                .pg_data_size = 2,
                .pool_id = 1,
                .pg_num = 1,
                .target_set = { 1, 2, 3 },
                .cur_set = { 0, 0, 0 },
            };
            st_cli.pool_config[1] = (pool_config_t){
                .exists = true,
                .id = 1,
                .name = "testpool",
                .scheme = POOL_SCHEME_XOR,
                .pg_size = 3,
                .pg_minsize = 2,
                .pg_count = 1,
                .real_pg_count = 1,
            };
            report_pg_state(pgs[{ 1, 1 }]);
            pg_counts[1] = 1;
        }
        bind_socket();
    }
    else
    {
        st_cli.tfd = tfd;
        st_cli.log_level = log_level;
        st_cli.on_change_osd_state_hook = [this](osd_num_t peer_osd) { on_change_osd_state_hook(peer_osd); };
        st_cli.on_change_pg_history_hook = [this](pool_id_t pool_id, pg_num_t pg_num) { on_change_pg_history_hook(pool_id, pg_num); };
        st_cli.on_change_hook = [this](std::map<std::string, etcd_kv_t> & changes) { on_change_etcd_state_hook(changes); };
        st_cli.on_load_config_hook = [this](json11::Json::object & cfg) { on_load_config_hook(cfg); };
        st_cli.load_pgs_checks_hook = [this]() { return on_load_pgs_checks_hook(); };
        st_cli.on_load_pgs_hook = [this](bool success) { on_load_pgs_hook(success); };
        st_cli.on_reload_hook = [this]() { st_cli.load_global_config(); };
        peering_state = OSD_LOADING_PGS;
        st_cli.load_global_config();
    }
    if (run_primary && autosync_interval > 0)
    {
        autosync_timer_id = this->tfd->set_timer(autosync_interval*1000, true, [this](int timer_id)
        {
            autosync();
        });
    }
}

void osd_t::parse_test_peer(std::string peer)
{
    // OSD_NUM:IP:PORT
    int pos1 = peer.find(':');
    int pos2 = peer.find(':', pos1+1);
    if (pos1 < 0 || pos2 < 0)
        throw new std::runtime_error("OSD peer string must be in the form OSD_NUM:IP:PORT");
    std::string addr = peer.substr(pos1+1, pos2-pos1-1);
    std::string osd_num_str = peer.substr(0, pos1);
    std::string port_str = peer.substr(pos2+1);
    osd_num_t peer_osd = strtoull(osd_num_str.c_str(), NULL, 10);
    if (!peer_osd)
        throw new std::runtime_error("Could not parse OSD peer osd_num");
    else if (st_cli.peer_states.find(peer_osd) != st_cli.peer_states.end())
        throw std::runtime_error("Same osd number "+std::to_string(peer_osd)+" specified twice in peers");
    int port = strtoull(port_str.c_str(), NULL, 10);
    if (!port)
        throw new std::runtime_error("Could not parse OSD peer port");
    st_cli.peer_states[peer_osd] = json11::Json::object {
        { "state", "up" },
        { "addresses", json11::Json::array { addr } },
        { "port", port },
    };
    msgr.connect_peer(peer_osd, st_cli.peer_states[peer_osd]);
}

bool osd_t::check_peer_config(osd_client_t *cl, json11::Json conf)
{
    // Check block_size, bitmap_granularity and immediate_commit of the peer
    if (conf["block_size"].is_null() ||
        conf["bitmap_granularity"].is_null() ||
        conf["immediate_commit"].is_null())
    {
        printf(
            "[OSD %ju] Warning: peer OSD %ju does not report block_size/bitmap_granularity/immediate_commit."
            " Is it older than 0.6.3?\n", this->osd_num, cl->osd_num
        );
    }
    else
    {
        int peer_immediate_commit = (conf["immediate_commit"].string_value() == "all"
            ? IMMEDIATE_ALL : (conf["immediate_commit"].string_value() == "small" ? IMMEDIATE_SMALL : IMMEDIATE_NONE));
        if (immediate_commit == IMMEDIATE_ALL && peer_immediate_commit != IMMEDIATE_ALL ||
            immediate_commit == IMMEDIATE_SMALL && peer_immediate_commit == IMMEDIATE_NONE)
        {
            printf(
                "[OSD %ju] My immediate_commit is \"%s\", but peer OSD %ju has \"%s\". We can't work together\n",
                this->osd_num, immediate_commit == IMMEDIATE_ALL ? "all" : "small",
                cl->osd_num, conf["immediate_commit"].string_value().c_str()
            );
            return false;
        }
        else if (conf["block_size"].uint64_value() != (uint64_t)this->bs_block_size)
        {
            printf(
                "[OSD %ju] My block_size is %u, but peer OSD %ju has %ju. We can't work together\n",
                this->osd_num, this->bs_block_size, cl->osd_num, conf["block_size"].uint64_value()
            );
            return false;
        }
        else if (conf["bitmap_granularity"].uint64_value() != (uint64_t)this->bs_bitmap_granularity)
        {
            printf(
                "[OSD %ju] My bitmap_granularity is %u, but peer OSD %ju has %ju. We can't work together\n",
                this->osd_num, this->bs_bitmap_granularity, cl->osd_num, conf["bitmap_granularity"].uint64_value()
            );
            return false;
        }
    }
    return true;
}

json11::Json osd_t::get_osd_state()
{
    std::vector<char> hostname;
    hostname.resize(1024);
    while (gethostname(hostname.data(), hostname.size()) < 0 && errno == ENAMETOOLONG)
        hostname.resize(hostname.size()+1024);
    hostname.resize(strnlen(hostname.data(), hostname.size()));
    json11::Json::object st;
    st["state"] = "up";
    if (bind_address != "0.0.0.0")
        st["addresses"] = json11::Json::array { bind_address };
    else
        st["addresses"] = getifaddr_list();
    st["host"] = std::string(hostname.data(), hostname.size());
    st["port"] = listening_port;
    st["primary_enabled"] = run_primary;
    st["blockstore_enabled"] = bs ? true : false;
    return st;
}

json11::Json osd_t::get_statistics()
{
    json11::Json::object st;
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    uint64_t ts_diff = 0;
    if (report_stats_ts.tv_sec != 0)
        ts_diff = (ts.tv_sec - report_stats_ts.tv_sec + (ts.tv_nsec - report_stats_ts.tv_nsec) / 1000000000);
    if (!ts_diff)
        ts_diff = 1;
    report_stats_ts = ts;
    char time_str[50] = { 0 };
    sprintf(time_str, "%jd.%03ld", (uint64_t)ts.tv_sec, ts.tv_nsec/1000000);
    st["time"] = time_str;
    if (bs)
    {
        st["blockstore_ready"] = bs->is_started();
        st["size"] = bs->get_block_count() * bs->get_block_size();
        st["free"] = bs->get_free_block_count() * bs->get_block_size();
    }
    st["data_block_size"] = (uint64_t)bs_block_size;
    st["bitmap_granularity"] = (uint64_t)bs_bitmap_granularity;
    st["immediate_commit"] = immediate_commit == IMMEDIATE_ALL ? "all" : (immediate_commit == IMMEDIATE_SMALL ? "small" : "none");
    st["host"] = self_state["host"];
    json11::Json::object op_stats, subop_stats;
    for (int i = OSD_OP_MIN; i <= OSD_OP_MAX; i++)
    {
        auto n = (msgr.stats.op_stat_count[i] - prev_report_stats.op_stat_count[i]);
        op_stats[osd_op_names[i]] = json11::Json::object {
            { "count", msgr.stats.op_stat_count[i] },
            { "usec", msgr.stats.op_stat_sum[i] },
            { "bytes", msgr.stats.op_stat_bytes[i] },
            { "lat", (msgr.stats.op_stat_sum[i] - prev_report_stats.op_stat_sum[i]) / (n < 1 ? 1 : n) },
            { "bps", (msgr.stats.op_stat_bytes[i] - prev_report_stats.op_stat_bytes[i]) / ts_diff },
            { "iops", n / ts_diff },
        };
    }
    for (int i = OSD_OP_MIN; i <= OSD_OP_MAX; i++)
    {
        auto n = (msgr.stats.subop_stat_count[i] - prev_report_stats.subop_stat_count[i]);
        subop_stats[osd_op_names[i]] = json11::Json::object {
            { "count", msgr.stats.subop_stat_count[i] },
            { "usec", msgr.stats.subop_stat_sum[i] },
            { "lat", (msgr.stats.subop_stat_sum[i] - prev_report_stats.subop_stat_sum[i]) / (n < 1 ? 1 : n) },
            { "iops", n / ts_diff },
        };
    }
    st["op_stats"] = op_stats;
    st["subop_stats"] = subop_stats;
    auto n0 = recovery_stat[0].count - recovery_report_prev[0].count;
    auto n1 = recovery_stat[1].count - recovery_report_prev[1].count;
    st["recovery_stats"] = json11::Json::object {
        { recovery_stat_names[0], json11::Json::object {
            { "count", recovery_stat[0].count },
            { "bytes", recovery_stat[0].bytes },
            { "usec", recovery_stat[0].usec },
            { "lat", (recovery_stat[0].usec - recovery_report_prev[0].usec) / (n0 < 1 ? 1 : n0) },
            { "bps", (recovery_stat[0].bytes - recovery_report_prev[0].bytes) / ts_diff },
            { "iops", n0 / ts_diff },
        } },
        { recovery_stat_names[1], json11::Json::object {
            { "count", recovery_stat[1].count },
            { "bytes", recovery_stat[1].bytes },
            { "usec", recovery_stat[1].usec },
            { "lat", (recovery_stat[1].usec - recovery_report_prev[1].usec) / (n1 < 1 ? 1 : n1) },
            { "bps", (recovery_stat[1].bytes - recovery_report_prev[1].bytes) / ts_diff },
            { "iops", n1 / ts_diff },
        } },
    };
    prev_report_stats = msgr.stats;
    memcpy(recovery_report_prev, recovery_stat, sizeof(recovery_stat));
    return st;
}

void osd_t::report_statistics()
{
    if (etcd_reporting_stats)
    {
        return;
    }
    etcd_reporting_stats = true;
    // Report space usage statistics as a whole
    // Maybe we'll report it using deltas if we tune for a lot of inodes at some point
    json11::Json::object inode_space;
    json11::Json::object last_stat;
    pool_id_t last_pool = 0;
    std::map<uint64_t, uint64_t> bs_empty_space;
    auto & bs_inode_space = bs ? bs->get_inode_space_stats() : bs_empty_space;
    for (auto kv: bs_inode_space)
    {
        pool_id_t pool_id = INODE_POOL(kv.first);
        uint64_t only_inode_num = INODE_NO_POOL(kv.first);
        if (!last_pool || pool_id != last_pool)
        {
            if (last_pool)
                inode_space[std::to_string(last_pool)] = last_stat;
            last_stat = json11::Json::object();
            last_pool = pool_id;
        }
        last_stat[std::to_string(only_inode_num)] = kv.second;
    }
    if (last_pool)
        inode_space[std::to_string(last_pool)] = last_stat;
    last_stat = json11::Json::object();
    last_pool = 0;
    json11::Json::object inode_ops;
    timespec tv_now;
    for (auto st_it = inode_stats.begin(); st_it != inode_stats.end(); )
    {
        auto & kv = *st_it;
        auto spc_it = bs_inode_space.find(kv.first);
        if (spc_it == bs_inode_space.end() || !spc_it->second) // prevent autovivification
        {
            // Is it an empty inode?
            if (!tv_now.tv_sec)
                clock_gettime(CLOCK_REALTIME, &tv_now);
            auto & tv_van = vanishing_inodes[kv.first];
            if (!tv_van.tv_sec)
                tv_van = tv_now;
            else if (tv_van.tv_sec < tv_now.tv_sec-inode_vanish_time)
            {
                // Inode vanished <inode_vanish_time> seconds ago, remove it from stats
                vanishing_inodes.erase(kv.first);
                inode_stats.erase(st_it++);
                continue;
            }
        }
        pool_id_t pool_id = INODE_POOL(kv.first);
        uint64_t only_inode_num = (kv.first & (((uint64_t)1 << (64-POOL_ID_BITS)) - 1));
        if (!last_pool || pool_id != last_pool)
        {
            if (last_pool)
                inode_ops[std::to_string(last_pool)] = last_stat;
            last_stat = json11::Json::object();
            last_pool = pool_id;
        }
        last_stat[std::to_string(only_inode_num)] = json11::Json::object {
            { "read", json11::Json::object {
                { "count", kv.second.op_count[INODE_STATS_READ] },
                { "usec", kv.second.op_sum[INODE_STATS_READ] },
                { "bytes", kv.second.op_bytes[INODE_STATS_READ] },
            } },
            { "write", json11::Json::object {
                { "count", kv.second.op_count[INODE_STATS_WRITE] },
                { "usec", kv.second.op_sum[INODE_STATS_WRITE] },
                { "bytes", kv.second.op_bytes[INODE_STATS_WRITE] },
            } },
            { "delete", json11::Json::object {
                { "count", kv.second.op_count[INODE_STATS_DELETE] },
                { "usec", kv.second.op_sum[INODE_STATS_DELETE] },
                { "bytes", kv.second.op_bytes[INODE_STATS_DELETE] },
            } },
        };
        st_it++;
    }
    if (last_pool)
        inode_ops[std::to_string(last_pool)] = last_stat;
    json11::Json::array txn = {
        json11::Json::object {
            { "request_put", json11::Json::object {
                { "key", base64_encode(st_cli.etcd_prefix+"/osd/stats/"+std::to_string(osd_num)) },
                { "value", base64_encode(get_statistics().dump()) },
            } },
        },
        json11::Json::object {
            { "request_put", json11::Json::object {
                { "key", base64_encode(st_cli.etcd_prefix+"/osd/space/"+std::to_string(osd_num)) },
                { "value", base64_encode(json11::Json(inode_space).dump()) },
            } },
        },
        json11::Json::object {
            { "request_put", json11::Json::object {
                { "key", base64_encode(st_cli.etcd_prefix+"/osd/inodestats/"+std::to_string(osd_num)) },
                { "value", base64_encode(json11::Json(inode_ops).dump()) },
            } },
        },
    };
    for (auto & p: pgs)
    {
        auto & pg = p.second;
        if (pg.state & (PG_OFFLINE | PG_STARTING))
        {
            // Don't report statistics for offline PGs
            continue;
        }
        json11::Json::object pg_stats;
        pg_stats["object_count"] = pg.total_count;
        pg_stats["clean_count"] = pg.clean_count;
        pg_stats["misplaced_count"] = pg.misplaced_objects.size();
        pg_stats["degraded_count"] = pg.degraded_objects.size();
        pg_stats["incomplete_count"] = pg.incomplete_objects.size();
        if (pg.corrupted_count)
            pg_stats["corrupted_count"] = pg.corrupted_count;
        pg_stats["write_osd_set"] = pg.cur_set;
        txn.push_back(json11::Json::object {
            { "request_put", json11::Json::object {
                { "key", base64_encode(st_cli.etcd_prefix+"/pgstats/"+std::to_string(pg.pool_id)+"/"+std::to_string(pg.pg_num)) },
                { "value", base64_encode(json11::Json(pg_stats).dump()) },
            } }
        });
    }
    st_cli.etcd_txn_slow(json11::Json::object { { "success", txn } }, [this](std::string err, json11::Json res)
    {
        etcd_reporting_stats = false;
        if (err != "")
        {
            printf("[OSD %ju] Error reporting state to etcd: %s\n", this->osd_num, err.c_str());
            // Retry indefinitely
            tfd->set_timer(st_cli.etcd_slow_timeout, false, [this](int timer_id)
            {
                report_statistics();
            });
        }
        else if (res["error"].string_value() != "")
        {
            printf("[OSD %ju] Error reporting state to etcd: %s\n", this->osd_num, res["error"].string_value().c_str());
            force_stop(1);
        }
    });
}

void osd_t::on_change_osd_state_hook(osd_num_t peer_osd)
{
    if (msgr.wanted_peers.find(peer_osd) != msgr.wanted_peers.end())
    {
        msgr.connect_peer(peer_osd, st_cli.peer_states[peer_osd]);
    }
}

void osd_t::on_change_etcd_state_hook(std::map<std::string, etcd_kv_t> & changes)
{
    if (changes.find(st_cli.etcd_prefix+"/config/global") != changes.end())
    {
        etcd_global_config = changes[st_cli.etcd_prefix+"/config/global"].value.object_items();
        parse_config(false);
    }
    bool pools = changes.find(st_cli.etcd_prefix+"/config/pools") != changes.end();
    if (pools)
    {
        apply_no_inode_stats();
    }
    if (run_primary)
    {
        bool pgs = changes.find(st_cli.etcd_prefix+"/pg/config") != changes.end();
        if (pools || pgs)
        {
            apply_pg_count();
        }
        apply_pg_config();
    }
}

void osd_t::on_load_config_hook(json11::Json::object & global_config)
{
    etcd_global_config = global_config;
    parse_config(true);
    bind_socket();
    acquire_lease();
    st_cli.on_load_config_hook = [this](json11::Json::object & cfg) { on_reload_config_hook(cfg); };
}

void osd_t::on_reload_config_hook(json11::Json::object & global_config)
{
    etcd_global_config = global_config;
    parse_config(false);
    renew_lease(true);
}

// Acquire lease
void osd_t::acquire_lease()
{
    // Apply no_inode_stats before the first statistics report
    apply_no_inode_stats();
    // Maximum lease TTL is (report interval) + retries * (timeout + repeat interval)
    st_cli.etcd_call("/lease/grant", json11::Json::object {
        { "TTL", etcd_report_interval+(st_cli.max_etcd_attempts*(2*st_cli.etcd_quick_timeout)+999)/1000 }
    }, st_cli.etcd_quick_timeout, 0, 0, [this](std::string err, json11::Json data)
    {
        if (err != "" || data["ID"].string_value() == "")
        {
            printf("Error acquiring a lease from etcd: %s, retrying\n", err.c_str());
            tfd->set_timer(st_cli.etcd_quick_timeout, false, [this](int timer_id)
            {
                acquire_lease();
            });
            return;
        }
        etcd_lease_id = data["ID"].string_value();
        create_osd_state();
    });
    printf(
        "[OSD %ju] reporting to etcd at %s every %d seconds (statistics every %d seconds)\n", this->osd_num,
        (config["etcd_address"].is_string() ? config["etcd_address"].string_value() : config["etcd_address"].dump()).c_str(),
        etcd_report_interval, etcd_stats_interval
    );
    tfd->set_timer(etcd_report_interval*1000, true, [this](int timer_id)
    {
        renew_lease(false);
    });
    tfd->set_timer(etcd_stats_interval*1000, true, [this](int timer_id)
    {
        report_statistics();
    });
}

// Report "up" state once, then keep it alive using the lease
// Do it first to allow "monitors" check it when moving PGs
void osd_t::create_osd_state()
{
    std::string state_key = base64_encode(st_cli.etcd_prefix+"/osd/state/"+std::to_string(osd_num));
    self_state = get_osd_state();
    st_cli.etcd_txn(json11::Json::object {
        // Check that the state key does not exist
        { "compare", json11::Json::array {
            json11::Json::object {
                { "target", "CREATE" },
                { "create_revision", 0 },
                { "key", state_key },
            }
        } },
        { "success", json11::Json::array {
            json11::Json::object {
                { "request_put", json11::Json::object {
                    { "key", state_key },
                    { "value", base64_encode(self_state.dump()) },
                    { "lease", etcd_lease_id },
                } }
            },
        } },
        { "failure", json11::Json::array {
            json11::Json::object {
                { "request_range", json11::Json::object {
                    { "key", state_key },
                } }
            },
        } },
    }, st_cli.etcd_quick_timeout, 0, 0, [this](std::string err, json11::Json data)
    {
        if (err != "")
        {
            etcd_failed_attempts++;
            printf("Error creating OSD state key: %s\n", err.c_str());
            if (etcd_failed_attempts > st_cli.max_etcd_attempts)
            {
                // Die
                throw std::runtime_error("Cluster connection failed");
            }
            // Retry
            tfd->set_timer(st_cli.etcd_quick_timeout, false, [this](int timer_id)
            {
                create_osd_state();
            });
            return;
        }
        if (!data["succeeded"].bool_value())
        {
            // OSD is already up
            auto kv = st_cli.parse_etcd_kv(data["responses"][0]["response_range"]["kvs"][0]);
            printf("Key %s already exists in etcd, OSD %ju is still up\n", kv.key.c_str(), this->osd_num);
            int64_t port = kv.value["port"].int64_value();
            for (auto & addr: kv.value["addresses"].array_items())
            {
                printf("  listening at: %s:%jd\n", addr.string_value().c_str(), port);
            }
            force_stop(0);
            return;
        }
        if (run_primary)
        {
            st_cli.load_pgs();
        }
        report_statistics();
    });
}

// Renew lease
void osd_t::renew_lease(bool reload)
{
    st_cli.etcd_call("/lease/keepalive", json11::Json::object {
        { "ID", etcd_lease_id }
    }, st_cli.etcd_quick_timeout, 0, 0, [this, reload](std::string err, json11::Json data)
    {
        if (err == "" && data["result"]["TTL"].uint64_value() == 0)
        {
            // Die
            fprintf(stderr, "Error refreshing etcd lease\n");
            force_stop(1);
        }
        if (err != "")
        {
            etcd_failed_attempts++;
            printf("Error renewing etcd lease: %s\n", err.c_str());
            if (etcd_failed_attempts > st_cli.max_etcd_attempts)
            {
                // Die
                fprintf(stderr, "Cluster connection failed\n");
                force_stop(1);
            }
            // Retry
            tfd->set_timer(st_cli.etcd_quick_timeout, false, [this, reload](int timer_id)
            {
                renew_lease(reload);
            });
        }
        else
        {
            etcd_failed_attempts = 0;
            // Reload PGs
            if (reload && run_primary)
            {
                st_cli.load_pgs();
            }
        }
    });
}

void osd_t::force_stop(int exitcode)
{
    if (etcd_lease_id != "")
    {
        st_cli.etcd_call("/kv/lease/revoke", json11::Json::object {
            { "ID", etcd_lease_id }
        }, st_cli.etcd_quick_timeout, st_cli.max_etcd_attempts, 0, [this, exitcode](std::string err, json11::Json data)
        {
            if (err != "")
            {
                printf("Error revoking etcd lease: %s\n", err.c_str());
            }
            printf("[OSD %ju] Force stopping\n", this->osd_num);
            exit(exitcode);
        });
    }
    else
    {
        printf("[OSD %ju] Force stopping\n", this->osd_num);
        exit(exitcode);
    }
}

json11::Json osd_t::on_load_pgs_checks_hook()
{
    json11::Json::array checks = {
        json11::Json::object {
            { "target", "LEASE" },
            { "lease", etcd_lease_id },
            { "key", base64_encode(st_cli.etcd_prefix+"/osd/state/"+std::to_string(osd_num)) },
        }
    };
    return checks;
}

void osd_t::on_load_pgs_hook(bool success)
{
    if (!success)
    {
        printf("Error loading PGs from etcd: lease expired\n");
        force_stop(1);
    }
    else
    {
        peering_state &= ~OSD_LOADING_PGS;
        apply_no_inode_stats();
        if (run_primary)
        {
            apply_pg_count();
            apply_pg_config();
        }
    }
}

void osd_t::apply_no_inode_stats()
{
    if (!bs)
    {
        return;
    }
    std::vector<uint64_t> no_inode_stats;
    for (auto & pool_item: st_cli.pool_config)
    {
        if (!pool_item.second.used_for_fs.empty())
        {
            no_inode_stats.push_back(pool_item.first);
        }
    }
    bs->set_no_inode_stats(no_inode_stats);
}

void osd_t::apply_pg_count()
{
    for (auto & pool_item: st_cli.pool_config)
    {
        if (pool_item.second.real_pg_count != 0 &&
            pool_item.second.real_pg_count != pg_counts[pool_item.first])
        {
            // Check that all pool PGs are offline. It is not allowed to change PG count when any PGs are online
            // The external tool must wait for all PGs to come down before changing PG count
            // If it doesn't wait, a restarted OSD may apply the new count immediately which will lead to bugs
            // So an OSD just dies if it detects PG count change while there are active PGs
            int still_active = 0;
            for (auto & kv: pgs)
            {
                if (kv.first.pool_id == pool_item.first && (kv.second.state & PG_ACTIVE))
                {
                    still_active++;
                }
            }
            if (still_active > 0)
            {
                printf(
                    "[OSD %ju] PG count change detected for pool %u (new is %ju, old is %u),"
                    " but %u PG(s) are still active. This is not allowed. Exiting\n",
                    this->osd_num, pool_item.first, pool_item.second.real_pg_count, pg_counts[pool_item.first], still_active
                );
                force_stop(1);
                return;
            }
        }
        this->pg_counts[pool_item.first] = pool_item.second.real_pg_count;
    }
}

void osd_t::apply_pg_config()
{
    bool all_applied = true;
    for (auto & pool_item: st_cli.pool_config)
    {
        bool warned_block_size = false;
        auto pool_id = pool_item.first;
        for (auto & kv: pool_item.second.pg_config)
        {
            pg_num_t pg_num = kv.first;
            auto & pg_cfg = kv.second;
            bool take = pg_cfg.config_exists && pg_cfg.primary == this->osd_num &&
                !pg_cfg.pause && (!pg_cfg.cur_primary || pg_cfg.cur_primary == this->osd_num);
            auto pg_it = this->pgs.find({ .pool_id = pool_id, .pg_num = pg_num });
            bool currently_taken = pg_it != this->pgs.end() && pg_it->second.state != PG_OFFLINE;
            // Check pool block size and bitmap granularity
            if (take && this->bs_block_size != pool_item.second.data_block_size ||
                this->bs_bitmap_granularity != pool_item.second.bitmap_granularity)
            {
                if (!warned_block_size)
                {
                    printf(
                        "[OSD %ju] My block_size and bitmap_granularity are %u/%u"
                        ", but pool %u has %u/%u. Refusing to start PGs of this pool\n",
                        this->osd_num, bs_block_size, bs_bitmap_granularity,
                        pool_id, pool_item.second.data_block_size, pool_item.second.bitmap_granularity
                    );
                }
                warned_block_size = true;
                take = false;
            }
            if (currently_taken && !take)
            {
                // Stop this PG
                stop_pg(pg_it->second);
            }
            else if (take)
            {
                // Take this PG
                std::set<osd_num_t> all_peers;
                for (osd_num_t pg_osd: pg_cfg.target_set)
                {
                    if (pg_osd != 0)
                    {
                        all_peers.insert(pg_osd);
                    }
                }
                for (osd_num_t pg_osd: pg_cfg.all_peers)
                {
                    if (pg_osd != 0)
                    {
                        all_peers.insert(pg_osd);
                    }
                }
                for (auto & hist_item: pg_cfg.target_history)
                {
                    for (auto pg_osd: hist_item)
                    {
                        if (pg_osd != 0)
                        {
                            all_peers.insert(pg_osd);
                        }
                    }
                }
                auto vec_all_peers = std::vector<osd_num_t>(all_peers.begin(), all_peers.end());
                if (currently_taken)
                {
                    if (pg_it->second.state & (PG_ACTIVE | PG_INCOMPLETE | PG_PEERING | PG_REPEERING))
                    {
                        if (pg_it->second.target_set == pg_cfg.target_set &&
                            pg_it->second.target_history == pg_cfg.target_history &&
                            pg_it->second.all_peers == vec_all_peers)
                        {
                            // No change in osd_set and history
                            if (pg_it->second.next_scrub != pg_cfg.next_scrub)
                            {
                                pg_it->second.next_scrub = pg_cfg.next_scrub;
                                schedule_scrub(pg_it->second);
                            }
                            continue;
                        }
                        else
                        {
                            // Stop PG, reapply change after stopping
                            stop_pg(pg_it->second);
                            all_applied = false;
                            continue;
                        }
                    }
                    else if (pg_it->second.state & PG_STOPPING)
                    {
                        // Reapply change after stopping
                        all_applied = false;
                        continue;
                    }
                    else if (pg_it->second.state & PG_STARTING)
                    {
                        if (pg_cfg.cur_primary == this->osd_num)
                        {
                            // PG locked, continue
                        }
                        else
                        {
                            // Reapply change after locking the PG
                            all_applied = false;
                            continue;
                        }
                    }
                    else
                    {
                        throw std::runtime_error(
                            "Unexpected PG "+std::to_string(pool_id)+"/"+std::to_string(pg_num)+
                            " state: "+std::to_string(pg_it->second.state)
                        );
                    }
                }
                auto & pg = this->pgs[{ .pool_id = pool_id, .pg_num = pg_num }];
                pg = (pg_t){
                    .state = pg_cfg.cur_primary == this->osd_num ? PG_PEERING : PG_STARTING,
                    .scheme = pool_item.second.scheme,
                    .pg_cursize = 0,
                    .pg_size = pool_item.second.pg_size,
                    .pg_minsize = pool_item.second.pg_minsize,
                    .pg_data_size = pool_item.second.scheme == POOL_SCHEME_REPLICATED
                         ? 1 : pool_item.second.pg_size - pool_item.second.parity_chunks,
                    .pool_id = pool_id,
                    .pg_num = pg_num,
                    .reported_epoch = pg_cfg.epoch,
                    .target_history = pg_cfg.target_history,
                    .all_peers = vec_all_peers,
                    .next_scrub = pg_cfg.next_scrub,
                    .target_set = pg_cfg.target_set,
                };
                if (pg.scheme == POOL_SCHEME_EC)
                {
                    use_ec(pg.pg_size, pg.pg_data_size, true);
                }
                this->pg_state_dirty.insert({ .pool_id = pool_id, .pg_num = pg_num });
                pg.print_state();
                if (pg_cfg.cur_primary == this->osd_num)
                {
                    // Add peers
                    for (auto pg_osd: all_peers)
                    {
                        if (pg_osd != this->osd_num && msgr.osd_peer_fds.find(pg_osd) == msgr.osd_peer_fds.end())
                        {
                            msgr.connect_peer(pg_osd, st_cli.peer_states[pg_osd]);
                        }
                    }
                    start_pg_peering(pg);
                }
                else
                {
                    // Reapply change after locking the PG
                    all_applied = false;
                }
            }
        }
    }
    report_pg_states();
    this->pg_config_applied = all_applied;
}

struct reporting_pg_t
{
    pool_pg_num_t pool_pg_num;
    bool history_changed;
};

void osd_t::report_pg_states()
{
    if (etcd_reporting_pg_state || !this->pg_state_dirty.size() || !st_cli.address_count())
    {
        return;
    }
    std::vector<reporting_pg_t> reporting_pgs;
    json11::Json::array checks;
    json11::Json::array success;
    json11::Json::array failure;
    for (auto it = pg_state_dirty.begin(); it != pg_state_dirty.end(); it++)
    {
        auto pg_it = this->pgs.find(*it);
        if (pg_it == this->pgs.end())
        {
            continue;
        }
        auto & pg = pg_it->second;
        reporting_pgs.push_back((reporting_pg_t){ *it, pg.history_changed });
        std::string state_key_base64 = base64_encode(st_cli.etcd_prefix+"/pg/state/"+std::to_string(pg.pool_id)+"/"+std::to_string(pg.pg_num));
        bool pg_state_exists = false;
        if (pg.state != PG_STARTING)
        {
            auto pool_it = st_cli.pool_config.find(pg.pool_id);
            if (pool_it != st_cli.pool_config.end())
            {
                auto pg_it = pool_it->second.pg_config.find(pg.pg_num);
                if (pg_it != pool_it->second.pg_config.end() &&
                    pg_it->second.cur_state != 0)
                {
                    pg_state_exists = true;
                    if (pg.state == PG_OFFLINE && pg_it->second.cur_primary != this->osd_num)
                    {
                        // Nothing to report, PG is already taken over by another OSD
                        checks.push_back(json11::Json::object {
                            { "target", "MOD" },
                            { "key", state_key_base64 },
                            { "result", "LESS" },
                            { "mod_revision", st_cli.etcd_watch_revision+1 },
                        });
                        continue;
                    }
                }
            }
        }
        if (!pg_state_exists)
        {
            // Check that the PG key does not exist
            // Failed check indicates an unsuccessful PG lock attempt in this case
            checks.push_back(json11::Json::object {
                { "target", "VERSION" },
                { "version", 0 },
                { "key", state_key_base64 },
            });
        }
        else
        {
            // Check that the key is ours if it already exists
            checks.push_back(json11::Json::object {
                { "target", "LEASE" },
                { "lease", etcd_lease_id },
                { "key", state_key_base64 },
            });
        }
        if (pg.state == PG_OFFLINE)
        {
            success.push_back(json11::Json::object {
                { "request_delete_range", json11::Json::object {
                    { "key", state_key_base64 },
                } }
            });
        }
        else
        {
            json11::Json::array pg_state_keywords;
            for (int i = 0; i < pg_state_bit_count; i++)
            {
                if (pg.state & pg_state_bits[i])
                {
                    pg_state_keywords.push_back(pg_state_names[i]);
                }
            }
            success.push_back(json11::Json::object {
                { "request_put", json11::Json::object {
                    { "key", state_key_base64 },
                    { "value", base64_encode(json11::Json(json11::Json::object {
                        { "primary", this->osd_num },
                        { "state", pg_state_keywords },
                        { "peers", pg.cur_peers },
                    }).dump()) },
                    { "lease", etcd_lease_id },
                } }
            });
            if (pg.history_changed)
            {
                // Prevent race conditions (for the case when the monitor is updating this key at the same time)
                pg.history_changed = false;
                std::string history_key = base64_encode(st_cli.etcd_prefix+"/pg/history/"+std::to_string(pg.pool_id)+"/"+std::to_string(pg.pg_num));
                json11::Json::object history_value = {
                    { "epoch", pg.epoch },
                    { "all_peers", pg.all_peers },
                    { "osd_sets", pg.target_history },
                };
                if (pg.next_scrub)
                    history_value["next_scrub"] = pg.next_scrub;
                checks.push_back(json11::Json::object {
                    { "target", "MOD" },
                    { "key", history_key },
                    { "result", "LESS" },
                    { "mod_revision", st_cli.etcd_watch_revision+1 },
                });
                success.push_back(json11::Json::object {
                    { "request_put", json11::Json::object {
                        { "key", history_key },
                        { "value", base64_encode(json11::Json(history_value).dump()) },
                    } }
                });
            }
        }
        failure.push_back(json11::Json::object {
            { "request_range", json11::Json::object {
                { "key", state_key_base64 },
            } }
        });
    }
    pg_state_dirty.clear();
    etcd_reporting_pg_state = true;
    st_cli.etcd_txn(json11::Json::object {
        { "compare", checks }, { "success", success }, { "failure", failure }
    }, st_cli.etcd_quick_timeout, 0, 0, [this, reporting_pgs](std::string err, json11::Json data)
    {
        etcd_reporting_pg_state = false;
        if (!data["succeeded"].bool_value())
        {
            std::string rpgnames = "";
            for (auto pp: reporting_pgs)
            {
                rpgnames += (rpgnames.size() ? ", " : "")+std::to_string(pp.pool_pg_num.pool_id)+"/"+std::to_string(pp.pool_pg_num.pg_num);
            }
            printf("Error reporting PG %s states, will repeat the attempt: %s\n", rpgnames.c_str(), err.c_str());
        }
        if (!data["succeeded"].bool_value())
        {
            // One of PG state updates failed, put dirty flags back
            for (auto pp: reporting_pgs)
            {
                this->pg_state_dirty.insert(pp.pool_pg_num);
                if (pp.history_changed)
                {
                    auto pg_it = this->pgs.find(pp.pool_pg_num);
                    if (pg_it != this->pgs.end())
                    {
                        pg_it->second.history_changed = true;
                    }
                }
            }
            for (auto & res: data["responses"].array_items())
            {
                if (res["response_range"]["kvs"].array_items().size())
                {
                    auto kv = st_cli.parse_etcd_kv(res["response_range"]["kvs"][0]);
                    if (kv.key.substr(0, st_cli.etcd_prefix.length()+10) == st_cli.etcd_prefix+"/pg/state/")
                    {
                        pool_id_t pool_id = 0;
                        pg_num_t pg_num = 0;
                        char null_byte = 0;
                        int scanned = sscanf(kv.key.c_str() + st_cli.etcd_prefix.length()+10, "%u/%u%c", &pool_id, &pg_num, &null_byte);
                        if (scanned == 2)
                        {
                            auto pg_it = pgs.find({ .pool_id = pool_id, .pg_num = pg_num });
                            if (pg_it != pgs.end() && pg_it->second.state != PG_OFFLINE && pg_it->second.state != PG_STARTING &&
                                kv.value["primary"].uint64_value() != 0 &&
                                kv.value["primary"].uint64_value() != this->osd_num)
                            {
                                // PG is somehow captured by another OSD
                                printf("BUG: OSD %ju captured our PG %u/%u. Race condition detected, exiting\n",
                                    kv.value["primary"].uint64_value(), pool_id, pg_num);
                                force_stop(1);
                                return;
                            }
                        }
                    }
                }
            }
            // Retry after a short pause (hope we'll get some updates and update PG states accordingly)
            tfd->set_timer(500, false, [this](int) { report_pg_states(); });
        }
        else
        {
            // Success. We'll get our changes back via the watcher and react to them
            for (auto pp: reporting_pgs)
            {
                auto pg_it = this->pgs.find(pp.pool_pg_num);
                if (pg_it != this->pgs.end() &&
                    pg_state_dirty.find(pp.pool_pg_num) == pg_state_dirty.end())
                {
                    if (pg_it->second.state == PG_OFFLINE)
                    {
                        // Forget offline PGs after reporting their state
                        // (if the state wasn't changed again)
                        if (pg_it->second.scheme == POOL_SCHEME_EC)
                        {
                            use_ec(pg_it->second.pg_size, pg_it->second.pg_data_size, false);
                        }
                        this->pgs.erase(pg_it);
                    }
                }
            }
            // Push other PG state updates, if any
            report_pg_states();
            if (!this->pg_state_dirty.size())
            {
                // Update statistics
                report_statistics();
            }
        }
    });
}
