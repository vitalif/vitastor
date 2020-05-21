#include "osd.h"
#include "base64.h"

#define ETCD_CONFIG_WATCH_ID 1
#define ETCD_PG_STATE_WATCH_ID 2
#define ETCD_PG_HISTORY_WATCH_ID 3
#define ETCD_OSD_STATE_WATCH_ID 4

#define MAX_ETCD_ATTEMPTS 5
#define ETCD_SLOW_TIMEOUT 5000
#define ETCD_QUICK_TIMEOUT 1000

json_kv_t osd_t::parse_etcd_kv(const json11::Json & kv_json)
{
    json_kv_t kv;
    kv.key = base64_decode(kv_json["key"].string_value());
    std::string json_err, json_text = base64_decode(kv_json["value"].string_value());
    kv.value = json_text == "" ? json11::Json() : json11::Json::parse(json_text, json_err);
    if (json_err != "")
    {
        printf("Bad JSON in etcd key %s: %s (value: %s)\n", kv.key.c_str(), json_err.c_str(), json_text.c_str());
        kv.key = "";
    }
    return kv;
}

void osd_t::etcd_txn(json11::Json txn, int timeout, std::function<void(std::string, json11::Json)> callback)
{
    etcd_call("/kv/txn", txn, timeout, callback);
}

void osd_t::etcd_call(std::string api, json11::Json payload, int timeout, std::function<void(std::string, json11::Json)> callback)
{
    std::string req = payload.dump();
    req = "POST "+etcd_api_path+api+" HTTP/1.1\r\n"
        "Host: "+etcd_address+"\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: "+std::to_string(req.size())+"\r\n"
        "Connection: close\r\n"
        "\r\n"+req;
    http_request_json(tfd, etcd_address, req, timeout, callback);
}

// Startup sequence:
//   Start etcd watcher -> Load global OSD configuration -> Bind socket -> Acquire lease -> Report&lock OSD state
//   -> Load PG config -> Report&lock PG states -> Load peers -> Connect to peers -> Peer PGs
// Event handling
//   Wait for PG changes -> Start/Stop PGs when requested
//   Peer connection is lost -> Reload connection data -> Try to reconnect
void osd_t::init_cluster()
{
    if (etcd_address == "")
    {
        if (run_primary)
        {
            // Test version of clustering code with 1 PG and 2 peers
            // Example: peers = 2:127.0.0.1:11204,3:127.0.0.1:11205
            std::string peerstr = config["peers"];
            while (peerstr.size())
            {
                int pos = peerstr.find(',');
                parse_test_peer(pos < 0 ? peerstr : peerstr.substr(0, pos));
                peerstr = pos < 0 ? std::string("") : peerstr.substr(pos+1);
            }
            if (peer_states.size() < 2)
            {
                throw std::runtime_error("run_primary requires at least 2 peers");
            }
            pgs[1] = (pg_t){
                .state = PG_PEERING,
                .pg_cursize = 0,
                .pg_num = 1,
                .target_set = { 1, 2, 3 },
                .cur_set = { 0, 0, 0 },
            };
            report_pg_state(pgs[1]);
            pg_count = 1;
            peering_state = OSD_CONNECTING_PEERS;
        }
        bind_socket();
    }
    else
    {
        peering_state = OSD_LOADING_PGS;
        load_global_config();
    }
    if (run_primary && autosync_interval > 0)
    {
        this->tfd->set_timer(autosync_interval*1000, true, [this](int timer_id)
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
    else if (peer_states.find(peer_osd) != peer_states.end())
        throw std::runtime_error("Same osd number "+std::to_string(peer_osd)+" specified twice in peers");
    int port = strtoull(port_str.c_str(), NULL, 10);
    if (!port)
        throw new std::runtime_error("Could not parse OSD peer port");
    peer_states[peer_osd] = json11::Json::object {
        { "state", "up" },
        { "addresses", json11::Json::array { addr } },
        { "port", port },
    };
    wanted_peers[peer_osd] = { 0 };
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
        st["addresses"] = { bind_address };
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
    char time_str[50] = { 0 };
    sprintf(time_str, "%ld.%03ld", ts.tv_sec, ts.tv_nsec/1000000);
    st["time"] = time_str;
    st["blockstore_ready"] = bs->is_started();
    if (bs)
    {
        st["size"] = bs->get_block_count() * bs->get_block_size();
        st["free"] = bs->get_free_block_count() * bs->get_block_size();
    }
    st["host"] = self_state["host"];
    json11::Json::object op_stats, subop_stats;
    for (int i = 0; i <= OSD_OP_MAX; i++)
    {
        op_stats[osd_op_names[i]] = json11::Json::object {
            { "count", op_stat_count[0][i] },
            { "usec", op_stat_sum[0][i] },
            { "bytes", op_stat_bytes[0][i] },
        };
    }
    for (int i = 0; i <= OSD_OP_MAX; i++)
    {
        subop_stats[osd_op_names[i]] = json11::Json::object {
            { "count", subop_stat_count[0][i] },
            { "usec", subop_stat_sum[0][i] },
        };
    }
    st["op_stats"] = op_stats;
    st["subop_stats"] = subop_stats;
    st["recovery_stats"] = json11::Json::object {
        { recovery_stat_names[0], json11::Json::object {
            { "count", recovery_stat_count[0][0] },
            { "bytes", recovery_stat_bytes[0][0] },
        } },
        { recovery_stat_names[1], json11::Json::object {
            { "count", recovery_stat_count[0][1] },
            { "bytes", recovery_stat_bytes[0][1] },
        } },
    };
    return st;
}

void osd_t::report_statistics()
{
    if (etcd_reporting_stats)
    {
        return;
    }
    etcd_reporting_stats = true;
    json11::Json::array txn = { json11::Json::object {
        { "request_put", json11::Json::object {
            { "key", base64_encode(etcd_prefix+"/osd/stats/"+std::to_string(osd_num)) },
            { "value", base64_encode(get_statistics().dump()) },
        } }
    } };
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
        pg_stats["write_osd_set"] = pg.cur_set;
        txn.push_back(json11::Json::object {
            { "request_put", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/pg/stats/"+std::to_string(pg.pg_num)) },
                { "value", base64_encode(json11::Json(pg_stats).dump()) },
            } }
        });
    }
    etcd_txn(json11::Json::object { { "success", txn } }, ETCD_SLOW_TIMEOUT, [this](std::string err, json11::Json res)
    {
        etcd_reporting_stats = false;
        if (err != "")
        {
            printf("[OSD %lu] Error reporting state to etcd: %s\n", this->osd_num, err.c_str());
            // Retry indefinitely
            tfd->set_timer(ETCD_SLOW_TIMEOUT, false, [this](int timer_id)
            {
                report_statistics();
            });
        }
        else if (res["error"].string_value() != "")
        {
            printf("[OSD %lu] Error reporting state to etcd: %s\n", this->osd_num, res["error"].string_value().c_str());
            force_stop(1);
        }
    });
}

void osd_t::start_etcd_watcher()
{
    etcd_watches_initialised = 0;
    etcd_watch_ws = open_websocket(tfd, etcd_address, etcd_api_path+"/watch", ETCD_SLOW_TIMEOUT, [this](const http_response_t *msg)
    {
        if (msg->body.length())
        {
            std::string json_err;
            json11::Json data = json11::Json::parse(msg->body, json_err);
            if (json_err != "")
            {
                printf("Bad JSON in etcd event: %s, ignoring event\n", json_err.c_str());
            }
            else
            {
                if (data["result"]["created"].bool_value())
                {
                    etcd_watches_initialised++;
                }
                if (etcd_watches_initialised == 4)
                {
                    etcd_watch_revision = data["result"]["header"]["revision"].uint64_value();
                }
                // First gather all changes into a hash to remove multiple overwrites
                json11::Json::object changes;
                for (auto & ev: data["result"]["events"].array_items())
                {
                    auto kv = parse_etcd_kv(ev["kv"]);
                    if (kv.key != "")
                    {
                        changes[kv.key] = kv.value;
                    }
                }
                for (auto & kv: changes)
                {
                    if (this->log_level > 0)
                    {
                        printf("Incoming event: %s -> %s\n", kv.first.c_str(), kv.second.dump().c_str());
                    }
                    if (kv.first.substr(0, etcd_prefix.length()+11) == etcd_prefix+"/osd/state/")
                    {
                        parse_etcd_osd_state(kv.first, kv.second);
                    }
                    else
                    {
                        parse_pg_state(kv.first, kv.second);
                    }
                }
                apply_pg_count();
                apply_pg_config();
            }
        }
        if (msg->eof)
        {
            etcd_watch_ws = NULL;
            if (etcd_watches_initialised == 0)
            {
                // Connection not established, retry in <ETCD_SLOW_TIMEOUT>
                tfd->set_timer(ETCD_SLOW_TIMEOUT, false, [this](int)
                {
                    start_etcd_watcher();
                });
            }
            else
            {
                // Connection was live, retry immediately
                start_etcd_watcher();
            }
        }
    });
    // FIXME apply config changes in runtime
    etcd_watch_ws->post_message(WS_TEXT, json11::Json(json11::Json::object {
        { "create_request", json11::Json::object {
            { "key", base64_encode(etcd_prefix+"/config/") },
            { "range_end", base64_encode(etcd_prefix+"/config0") },
            { "start_revision", etcd_watch_revision+1 },
            { "watch_id", ETCD_CONFIG_WATCH_ID },
        } }
    }).dump());
    etcd_watch_ws->post_message(WS_TEXT, json11::Json(json11::Json::object {
        { "create_request", json11::Json::object {
            { "key", base64_encode(etcd_prefix+"/osd/state/") },
            { "range_end", base64_encode(etcd_prefix+"/osd/state0") },
            { "start_revision", etcd_watch_revision+1 },
            { "watch_id", ETCD_OSD_STATE_WATCH_ID },
        } }
    }).dump());
    etcd_watch_ws->post_message(WS_TEXT, json11::Json(json11::Json::object {
        { "create_request", json11::Json::object {
            { "key", base64_encode(etcd_prefix+"/pg/state/") },
            { "range_end", base64_encode(etcd_prefix+"/pg/state0") },
            { "start_revision", etcd_watch_revision+1 },
            { "watch_id", ETCD_PG_STATE_WATCH_ID },
        } }
    }).dump());
    etcd_watch_ws->post_message(WS_TEXT, json11::Json(json11::Json::object {
        { "create_request", json11::Json::object {
            { "key", base64_encode(etcd_prefix+"/pg/history/") },
            { "range_end", base64_encode(etcd_prefix+"/pg/history0") },
            { "start_revision", etcd_watch_revision+1 },
            { "watch_id", ETCD_PG_HISTORY_WATCH_ID },
        } }
    }).dump());
}

void osd_t::load_global_config()
{
    etcd_call("/kv/range", json11::Json::object {
        { "key", base64_encode(etcd_prefix+"/config/global") }
    }, ETCD_SLOW_TIMEOUT, [this](std::string err, json11::Json data)
    {
        if (err != "")
        {
            printf("Error reading OSD configuration from etcd: %s\n", err.c_str());
            tfd->set_timer(ETCD_SLOW_TIMEOUT, false, [this](int timer_id)
            {
                load_global_config();
            });
            return;
        }
        if (!etcd_watch_revision)
        {
            etcd_watch_revision = data["header"]["revision"].uint64_value();
        }
        if (data["kvs"].array_items().size() > 0)
        {
            auto kv = parse_etcd_kv(data["kvs"][0]);
            if (kv.value.is_object())
            {
                blockstore_config_t osd_config = this->config;
                for (auto & cfg_var: kv.value.object_items())
                {
                    if (this->config.find(cfg_var.first) == this->config.end())
                    {
                        osd_config[cfg_var.first] = cfg_var.second.string_value();
                    }
                }
                parse_config(osd_config);
            }
        }
        bind_socket();
        start_etcd_watcher();
        acquire_lease();
    });
}

// Acquire lease
void osd_t::acquire_lease()
{
    // Maximum lease TTL is (report interval) + retries * (timeout + repeat interval)
    etcd_call("/lease/grant", json11::Json::object {
        { "TTL", etcd_report_interval+(MAX_ETCD_ATTEMPTS*(2*ETCD_QUICK_TIMEOUT)+999)/1000 }
    }, ETCD_SLOW_TIMEOUT, [this](std::string err, json11::Json data)
    {
        if (err != "" || data["ID"].string_value() == "")
        {
            printf("Error acquiring a lease from etcd: %s\n", err.c_str());
            tfd->set_timer(ETCD_SLOW_TIMEOUT, false, [this](int timer_id)
            {
                acquire_lease();
            });
            return;
        }
        etcd_lease_id = data["ID"].string_value();
        create_osd_state();
    });
    printf("[OSD %lu] reporting to etcd at %s every %d seconds\n", this->osd_num, etcd_address.c_str(), etcd_report_interval);
    tfd->set_timer(etcd_report_interval*1000, true, [this](int timer_id)
    {
        renew_lease();
    });
}

// Report "up" state once, then keep it alive using the lease
// Do it first to allow "monitors" check it when moving PGs
void osd_t::create_osd_state()
{
    std::string state_key = base64_encode(etcd_prefix+"/osd/state/"+std::to_string(osd_num));
    self_state = get_osd_state();
    etcd_txn(json11::Json::object {
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
    }, ETCD_QUICK_TIMEOUT, [this](std::string err, json11::Json data)
    {
        if (err != "")
        {
            // FIXME Retry? But etcd should be already UP because we've just got the lease
            printf("Error reporting OSD state to etcd: %s\n", err.c_str());
            force_stop(1);
            return;
        }
        if (!data["succeeded"].bool_value())
        {
            // OSD is already up
            auto kv = parse_etcd_kv(data["responses"][0]["response_range"]["kvs"][0]);
            printf("Key %s already exists in etcd, OSD %lu is still up\n", kv.key.c_str(), this->osd_num);
            int64_t port = kv.value["port"].int64_value();
            for (auto & addr: kv.value["addresses"].array_items())
            {
                printf("  listening at: %s:%ld\n", addr.string_value().c_str(), port);
            }
            force_stop(0);
            return;
        }
        if (run_primary)
        {
            load_pgs();
        }
    });
}

// Renew lease
void osd_t::renew_lease()
{
    etcd_call("/lease/keepalive", json11::Json::object {
        { "ID", etcd_lease_id }
    }, ETCD_QUICK_TIMEOUT, [this](std::string err, json11::Json data)
    {
        if (err == "" && data["result"]["TTL"].string_value() == "")
        {
            // Die
            throw std::runtime_error("etcd lease has expired");
        }
        if (err != "")
        {
            etcd_failed_attempts++;
            printf("Error renewing etcd lease: %s\n", err.c_str());
            if (etcd_failed_attempts > MAX_ETCD_ATTEMPTS)
            {
                // Die
                throw std::runtime_error("Cluster connection failed");
            }
            // Retry
            tfd->set_timer(ETCD_QUICK_TIMEOUT, false, [this](int timer_id)
            {
                renew_lease();
            });
        }
        else
        {
            etcd_failed_attempts = 0;
            report_statistics();
        }
    });
}

void osd_t::force_stop(int exitcode)
{
    if (etcd_lease_id != "")
    {
        etcd_call("/kv/lease/revoke", json11::Json::object {
            { "ID", etcd_lease_id }
        }, ETCD_QUICK_TIMEOUT, [this, exitcode](std::string err, json11::Json data)
        {
            if (err != "")
            {
                printf("Error revoking etcd lease: %s\n", err.c_str());
            }
            printf("[OSD %lu] Force stopping\n", this->osd_num);
            exit(exitcode);
        });
    }
    else
    {
        printf("[OSD %lu] Force stopping\n", this->osd_num);
        exit(exitcode);
    }
}

void osd_t::load_pgs()
{
    assert(this->pgs.size() == 0);
    json11::Json::array checks = {
        json11::Json::object {
            { "target", "LEASE" },
            { "lease", etcd_lease_id },
            { "key", base64_encode(etcd_prefix+"/osd/state/"+std::to_string(osd_num)) },
        }
    };
    json11::Json::array txn = {
        json11::Json::object {
            { "request_range", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/config/pgs") },
            } }
        },
        json11::Json::object {
            { "request_range", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/pg/history/") },
                { "range_end", base64_encode(etcd_prefix+"/pg/history0") },
            } }
        },
        json11::Json::object {
            { "request_range", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/pg/state/") },
                { "range_end", base64_encode(etcd_prefix+"/pg/state0") },
            } }
        },
    };
    etcd_txn(json11::Json::object {
        { "compare", checks }, { "success", txn }
    }, ETCD_SLOW_TIMEOUT, [this](std::string err, json11::Json data)
    {
        if (err != "")
        {
            printf("Error loading PGs from etcd: %s\n", err.c_str());
            tfd->set_timer(ETCD_SLOW_TIMEOUT, false, [this](int timer_id)
            {
                load_pgs();
            });
            return;
        }
        if (!data["succeeded"].bool_value())
        {
            printf("Error loading PGs from etcd: lease expired\n");
            force_stop(1);
            return;
        }
        peering_state &= ~OSD_LOADING_PGS;
        for (auto & res: data["responses"].array_items())
        {
            for (auto & kv_json: res["response_range"]["kvs"].array_items())
            {
                auto kv = parse_etcd_kv(kv_json);
                parse_pg_state(kv.key, kv.value);
            }
        }
        apply_pg_count();
        apply_pg_config();
    });
}

void osd_t::parse_pg_state(const std::string & key, const json11::Json & value)
{
    if (key == etcd_prefix+"/config/pgs")
    {
        for (auto & pg_item: this->pg_config)
        {
            pg_item.second.exists = false;
        }
        for (auto & pg_item: value["items"].object_items())
        {
            pg_num_t pg_num = stoull_full(pg_item.first);
            if (!pg_num)
            {
                printf("Bad key in PG configuration: %s (must be a number), skipped\n", pg_item.first.c_str());
                continue;
            }
            this->pg_config[pg_num].exists = true;
            this->pg_config[pg_num].pause = pg_item.second["pause"].bool_value();
            this->pg_config[pg_num].primary = pg_item.second["primary"].uint64_value();
            this->pg_config[pg_num].target_set.clear();
            for (auto pg_osd: pg_item.second["osd_set"].array_items())
            {
                this->pg_config[pg_num].target_set.push_back(pg_osd.uint64_value());
            }
            if (this->pg_config[pg_num].target_set.size() != 3)
            {
                printf("Bad PG %u config format: incorrect osd_set = %s\n", pg_num, pg_item.second["osd_set"].dump().c_str());
                this->pg_config[pg_num].target_set.resize(3);
                this->pg_config[pg_num].pause = true;
            }
        }
    }
    else if (key.substr(0, etcd_prefix.length()+12) == etcd_prefix+"/pg/history/")
    {
        // <etcd_prefix>/pg/history/%d
        pg_num_t pg_num = stoull_full(key.substr(etcd_prefix.length()+12));
        if (!pg_num)
        {
            printf("Bad etcd key %s, ignoring\n", key.c_str());
        }
        else
        {
            auto & pg_cfg = this->pg_config[pg_num];
            pg_cfg.target_history.clear();
            pg_cfg.all_peers.clear();
            // Refuse to start PG if any set of the <osd_sets> has no live OSDs
            for (auto hist_item: value["osd_sets"].array_items())
            {
                std::vector<osd_num_t> history_set;
                for (auto pg_osd: hist_item.array_items())
                {
                    history_set.push_back(pg_osd.uint64_value());
                }
                pg_cfg.target_history.push_back(history_set);
            }
            // Include these additional OSDs when peering the PG
            for (auto pg_osd: value["all_peers"].array_items())
            {
                pg_cfg.all_peers.push_back(pg_osd.uint64_value());
            }
        }
    }
    else if (key.substr(0, etcd_prefix.length()+10) == etcd_prefix+"/pg/state/")
    {
        // <etcd_prefix>/pg/state/%d
        pg_num_t pg_num = stoull_full(key.substr(etcd_prefix.length()+10));
        if (!pg_num)
        {
            printf("Bad etcd key %s, ignoring\n", key.c_str());
        }
        else if (value.is_null())
        {
            this->pg_config[pg_num].cur_primary = 0;
            this->pg_config[pg_num].cur_state = 0;
        }
        else
        {
            osd_num_t cur_primary = value["primary"].uint64_value();
            int state = 0;
            for (auto & e: value["state"].array_items())
            {
                int i;
                for (i = 0; i < pg_state_bit_count; i++)
                {
                    if (e.string_value() == pg_state_names[i])
                    {
                        state = state | pg_state_bits[i];
                        break;
                    }
                }
                if (i >= pg_state_bit_count)
                {
                    printf("Unexpected PG %u state keyword in etcd: %s\n", pg_num, e.dump().c_str());
                    force_stop(1);
                    return;
                }
            }
            if (!cur_primary || !value["state"].is_array() || !state ||
                (state & PG_OFFLINE) && state != PG_OFFLINE ||
                (state & PG_PEERING) && state != PG_PEERING ||
                (state & PG_INCOMPLETE) && state != PG_INCOMPLETE)
            {
                printf("Unexpected PG %u state in etcd: primary=%lu, state=%s\n", pg_num, cur_primary, value["state"].dump().c_str());
                force_stop(1);
                return;
            }
            this->pg_config[pg_num].cur_primary = cur_primary;
            this->pg_config[pg_num].cur_state = state;
        }
    }
}

void osd_t::apply_pg_count()
{
    pg_num_t pg_count = pg_config.size();
    if (pg_count > 0 && (pg_config.begin()->first != 1 || std::prev(pg_config.end())->first != pg_count))
    {
        printf("Invalid PG configuration: PG numbers don't cover the whole 1..%d range\n", pg_count);
        force_stop(1);
        return;
    }
    if (this->pg_count != 0 && this->pg_count != pg_count)
    {
        // Check that all PGs are offline. It is not allowed to change PG count when any PGs are online
        // The external tool must wait for all PGs to come down before changing PG count
        // If it doesn't wait, a restarted OSD may apply the new count immediately which will lead to bugs
        // So an OSD just dies if it detects PG count change while there are active PGs
        int still_active = 0;
        for (auto & kv: pgs)
        {
            if (kv.second.state & PG_ACTIVE)
            {
                still_active++;
            }
        }
        if (still_active > 0)
        {
            printf("[OSD %lu] PG count change detected, but %d PG(s) are still active. This is not allowed. Exiting\n", this->osd_num, still_active);
            force_stop(1);
            return;
        }
    }
    this->pg_count = pg_count;
}

void osd_t::apply_pg_config()
{
    bool all_applied = true;
    for (auto & kv: pg_config)
    {
        pg_num_t pg_num = kv.first;
        auto & pg_cfg = kv.second;
        bool take = pg_cfg.exists && pg_cfg.primary == this->osd_num &&
            !pg_cfg.pause && (!pg_cfg.cur_primary || pg_cfg.cur_primary == this->osd_num);
        bool currently_taken = this->pgs.find(pg_num) != this->pgs.end() &&
            this->pgs[pg_num].state != PG_OFFLINE;
        if (currently_taken && !take)
        {
            // Stop this PG
            stop_pg(pg_num);
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
            if (currently_taken)
            {
                if (this->pgs[pg_num].state & (PG_ACTIVE | PG_INCOMPLETE | PG_PEERING))
                {
                    if (this->pgs[pg_num].target_set == pg_cfg.target_set)
                    {
                        // No change in osd_set; history changes are ignored
                        continue;
                    }
                    else
                    {
                        // Stop PG, reapply change after stopping
                        stop_pg(pg_num);
                        all_applied = false;
                        continue;
                    }
                }
                else if (this->pgs[pg_num].state & PG_STOPPING)
                {
                    // Reapply change after stopping
                    all_applied = false;
                    continue;
                }
                else if (this->pgs[pg_num].state & PG_STARTING)
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
                    throw std::runtime_error("Unexpected PG "+std::to_string(pg_num)+" state: "+std::to_string(this->pgs[pg_num].state));
                }
            }
            this->pgs[pg_num] = (pg_t){
                .state = pg_cfg.cur_primary == this->osd_num ? PG_PEERING : PG_STARTING,
                .pg_cursize = 0,
                .pg_num = pg_num,
                .target_history = pg_cfg.target_history,
                .all_peers = std::vector<osd_num_t>(all_peers.begin(), all_peers.end()),
                .target_set = pg_cfg.target_set,
            };
            this->pg_state_dirty.insert(pg_num);
            this->pgs[pg_num].print_state();
            if (pg_cfg.cur_primary == this->osd_num)
            {
                // Add peers
                for (auto pg_osd: all_peers)
                {
                    if (pg_osd != this->osd_num && osd_peer_fds.find(pg_osd) == osd_peer_fds.end())
                    {
                        wanted_peers[pg_osd] = { 0 };
                    }
                }
                start_pg_peering(pg_num);
            }
            else
            {
                // Reapply change after locking the PG
                all_applied = false;
            }
        }
    }
    if (wanted_peers.size() > 0)
    {
        peering_state |= OSD_CONNECTING_PEERS;
    }
    report_pg_states();
    this->pg_config_applied = all_applied;
}

void osd_t::report_pg_states()
{
    if (etcd_reporting_pg_state || !this->pg_state_dirty.size() || !etcd_address.length())
    {
        return;
    }
    etcd_reporting_pg_state = true;
    std::vector<std::pair<pg_num_t,bool>> reporting_pgs;
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
        reporting_pgs.push_back({ pg.pg_num, pg.history_changed });
        std::string state_key_base64 = base64_encode(etcd_prefix+"/pg/state/"+std::to_string(pg.pg_num));
        if (pg.state == PG_STARTING)
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
            // Check that the key is ours
            // Failed check indicates success for OFFLINE pgs (PG lock is already deleted)
            // and an unexpected race condition for started pgs (PG lock is held by someone else)
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
                    { "key", base64_encode(etcd_prefix+"/pg/state/"+std::to_string(pg.pg_num)) },
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
                pg.history_changed = false;
                if (pg.state == PG_ACTIVE)
                {
                    success.push_back(json11::Json::object {
                        { "request_delete_range", json11::Json::object {
                            { "key", base64_encode(etcd_prefix+"/pg/history/"+std::to_string(pg.pg_num)) },
                        } }
                    });
                }
                else if (pg.state == (PG_ACTIVE|PG_LEFT_ON_DEAD))
                {
                    success.push_back(json11::Json::object {
                        { "request_put", json11::Json::object {
                            { "key", base64_encode(etcd_prefix+"/pg/history/"+std::to_string(pg.pg_num)) },
                            { "value", base64_encode(json11::Json(json11::Json::object {
                                { "all_peers", pg.all_peers },
                            }).dump()) },
                        } }
                    });
                }
            }
        }
        failure.push_back(json11::Json::object {
            { "request_range", json11::Json::object {
                { "key", state_key_base64 },
            } }
        });
    }
    pg_state_dirty.clear();
    etcd_txn(json11::Json::object {
        { "compare", checks }, { "success", success }, { "failure", failure }
    }, ETCD_QUICK_TIMEOUT, [this, reporting_pgs](std::string err, json11::Json data)
    {
        etcd_reporting_pg_state = false;
        if (!data["succeeded"].bool_value())
        {
            // One of PG state updates failed, put dirty flags back
            for (auto pp: reporting_pgs)
            {
                this->pg_state_dirty.insert(pp.first);
                if (pp.second)
                {
                    auto pg_it = this->pgs.find(pp.first);
                    if (pg_it != this->pgs.end())
                    {
                        pg_it->second.history_changed = true;
                    }
                }
            }
            for (auto & res: data["responses"].array_items())
            {
                auto kv = parse_etcd_kv(res["kvs"][0]);
                pg_num_t pg_num = stoull_full(kv.key.substr(etcd_prefix.length()+10));
                auto pg_it = pgs.find(pg_num);
                if (pg_it != pgs.end() && pg_it->second.state != PG_OFFLINE && pg_it->second.state != PG_STARTING)
                {
                    // Live PG state update failed
                    printf("Failed to report state of PG %u which is live. Race condition detected, exiting\n", pg_num);
                    force_stop(1);
                    return;
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
                auto pg_it = this->pgs.find(pp.first);
                if (pg_it != this->pgs.end())
                {
                    if (pg_it->second.state == PG_OFFLINE)
                    {
                        // Remove offline PGs after reporting their state
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

void osd_t::load_and_connect_peers()
{
    json11::Json::array load_peer_txn;
    for (auto wp_it = wanted_peers.begin(); wp_it != wanted_peers.end();)
    {
        osd_num_t peer_osd = wp_it->first;
        if (osd_peer_fds.find(peer_osd) != osd_peer_fds.end())
        {
            // It shouldn't be here
            wanted_peers.erase(wp_it++);
            if (!wanted_peers.size())
            {
                // Connected to all peers
                peering_state = peering_state & ~OSD_CONNECTING_PEERS;
            }
        }
        else if (peer_states.find(peer_osd) == peer_states.end())
        {
            if (!loading_peer_config && (time(NULL) - wp_it->second.last_load_attempt >= peer_connect_interval))
            {
                // (Re)load OSD state from etcd
                wp_it->second.last_load_attempt = time(NULL);
                load_peer_txn.push_back(json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(etcd_prefix+"/osd/state/"+std::to_string(peer_osd)) },
                    } }
                });
            }
            wp_it++;
        }
        else if (!wp_it->second.connecting &&
            time(NULL) - wp_it->second.last_connect_attempt >= peer_connect_interval)
        {
            // Try to connect
            wp_it->second.connecting = true;
            const std::string addr = peer_states[peer_osd]["addresses"][wp_it->second.address_index].string_value();
            int64_t peer_port = peer_states[peer_osd]["port"].int64_value();
            wp_it++;
            connect_peer(peer_osd, addr.c_str(), peer_port, [this](osd_num_t peer_osd, int peer_fd)
            {
                wanted_peers[peer_osd].connecting = false;
                if (peer_fd < 0)
                {
                    int64_t peer_port = peer_states[peer_osd]["port"].int64_value();
                    auto & addrs = peer_states[peer_osd]["addresses"].array_items();
                    const char *addr = addrs[wanted_peers[peer_osd].address_index].string_value().c_str();
                    printf("Failed to connect to peer OSD %lu address %s port %ld: %s\n", peer_osd, addr, peer_port, strerror(-peer_fd));
                    if (wanted_peers[peer_osd].address_index < addrs.size()-1)
                    {
                        // Try all addresses
                        wanted_peers[peer_osd].address_index++;
                    }
                    else
                    {
                        wanted_peers[peer_osd].last_connect_attempt = time(NULL);
                        peer_states.erase(peer_osd);
                    }
                    return;
                }
                printf("Connected with peer OSD %lu (fd %d)\n", clients[peer_fd].osd_num, peer_fd);
                wanted_peers.erase(peer_osd);
                if (!wanted_peers.size())
                {
                    // Connected to all peers
                    printf("Connected to all peers\n");
                    peering_state = peering_state & ~OSD_CONNECTING_PEERS;
                }
                repeer_pgs(peer_osd);
            });
        }
        else
        {
            // Skip
            wp_it++;
        }
    }
    if (load_peer_txn.size() > 0)
    {
        etcd_txn(json11::Json::object { { "success", load_peer_txn } }, ETCD_QUICK_TIMEOUT, [this](std::string err, json11::Json data)
        {
            // Ugly, but required to wake up the loop and retry connecting after <peer_connect_interval> seconds
            tfd->set_timer(peer_connect_interval*1000, false, [](int timer_id){});
            loading_peer_config = false;
            if (err != "")
            {
                printf("Failed to load peer states from etcd: %s\n", err.c_str());
                return;
            }
            for (auto & res: data["responses"].array_items())
            {
                if (res["response_range"]["kvs"].array_items().size())
                {
                    auto kv = parse_etcd_kv(res["response_range"]["kvs"][0]);
                    parse_etcd_osd_state(kv.key, kv.value);
                }
            }
        });
    }
}

void osd_t::parse_etcd_osd_state(const std::string & key, const json11::Json & value)
{
    // <etcd_prefix>/osd/state/<osd_num>
    osd_num_t peer_osd = std::stoull(key.substr(etcd_prefix.length()+11));
    if (peer_osd > 0 && value.is_object() && value["state"] == "up" &&
        value["addresses"].is_array() &&
        value["port"].int64_value() > 0 && value["port"].int64_value() < 65536)
    {
        peer_states[peer_osd] = value;
    }
}
