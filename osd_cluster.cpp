#include "osd.h"
#include "osd_http.h"
#include "base64.h"

void osd_t::init_cluster()
{
    if (run_primary)
    {
        init_primary();
    }
    if (etcd_address != "")
    {
        if (!run_primary)
        {
            report_status();
        }
        printf("[OSD %lu] reporting to etcd at %s each %d seconds\n", osd_num, etcd_address.c_str(), etcd_report_interval);
        tfd->set_timer(etcd_report_interval*1000, true, [this](int timer_id)
        {
            report_status();
        });
    }
    else
    {
        bind_socket();
    }
}

void osd_t::init_primary()
{
    if (etcd_address == "")
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
        pgs[1].print_state();
        pg_count = 1;
        peering_state = OSD_CONNECTING_PEERS;
    }
    else
    {
        peering_state = OSD_LOADING_PGS;
        load_pgs();
    }
    if (autosync_interval > 0)
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

json11::Json osd_t::get_status()
{
    json11::Json::object st;
    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    st["time"] = std::to_string(ts.tv_sec)+"."+std::to_string(ts.tv_nsec/1000000);
    st["state"] = "up";
    if (bind_address != "0.0.0.0")
        st["addresses"] = { bind_address };
    else
    {
        if (bind_addresses.size() == 0)
            bind_addresses = getifaddr_list();
        st["addresses"] = bind_addresses;
    }
    st["port"] = listening_port;
    st["primary_enabled"] = run_primary;
    st["blockstore_ready"] = bs->is_started();
    st["blockstore_enabled"] = bs ? true : false;
    if (bs)
    {
        st["size"] = bs->get_block_count() * bs->get_block_size();
        st["free"] = bs->get_free_block_count() * bs->get_block_size();
    }
    json11::Json::object op_stats, subop_stats;
    for (int i = 0; i <= OSD_OP_MAX; i++)
    {
        op_stats[osd_op_names[i]] = json11::Json::object {
            { "count", op_stat_count[0][i] },
            { "sum", op_stat_sum[0][i] },
        };
    }
    for (int i = 0; i <= OSD_OP_MAX; i++)
    {
        subop_stats[osd_op_names[i]] = json11::Json::object {
            { "count", subop_stat_count[0][i] },
            { "sum", subop_stat_sum[0][i] },
        };
    }
    st["op_latency"] = op_stats;
    st["subop_latency"] = subop_stats;
    return st;
}

void osd_t::report_status()
{
    std::string st = get_status().dump();
    // (!) Keys end with . to allow "select /osd/state/123. by prefix"
    // because etcd transactions fail if you try to read non-existing keys
    json11::Json::array txn = { json11::Json::object {
        { "requestPut", json11::Json::object {
            { "key", base64_encode(etcd_prefix+"/osd/state/"+std::to_string(osd_num)) },
            { "value", base64_encode(st) },
        } }
    } };
    for (auto & p: pgs)
    {
        auto & pg = p.second;
        json11::Json::object pg_st;
        json11::Json::array pg_state;
        for (int i = 0; i < pg_state_bit_count; i++)
            if (pg.state & pg_state_bits[i])
                pg_state.push_back(pg_state_names[i]);
        pg_st["state"] = pg_state;
        pg_st["object_count"] = pg.total_count;
        pg_st["clean_count"] = pg.clean_count;
        pg_st["misplaced_count"] = pg.misplaced_objects.size();
        pg_st["degraded_count"] = pg.degraded_objects.size();
        pg_st["incomplete_count"] = pg.incomplete_objects.size();
        pg_st["write_osd_set"] = pg.cur_set;
        txn.push_back(json11::Json::object {
            { "requestPut", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/pg/state/"+std::to_string(pg.pg_num)) },
                { "value", base64_encode(json11::Json(pg_st).dump()) },
            } }
        });
        if (pg.state == PG_ACTIVE && pg.target_history.size())
        {
            pg.target_history.clear();
            pg.all_peers = pg.target_set;
            txn.push_back(json11::Json::object {
                { "requestDeleteRange", json11::Json::object {
                    { "key", base64_encode(etcd_prefix+"/pg/history/"+std::to_string(pg.pg_num)) },
                } }
            });
        }
    }
    etcd_txn(json11::Json::object { { "success", txn } }, [this](std::string err, json11::Json res)
    {
        if (err != "")
        {
            etcd_failed_attempts++;
            printf("Error reporting state to etcd: %s\n", err.c_str());
            if (etcd_failed_attempts > MAX_ETCD_ATTEMPTS)
            {
                throw std::runtime_error("Cluster connection failed");
            }
            // Retry
            tfd->set_timer(ETCD_RETRY_INTERVAL, false, [this](int timer_id)
            {
                report_status();
            });
        }
        else
        {
            etcd_failed_attempts = 0;
        }
    });
}

void osd_t::etcd_txn(json11::Json txn, std::function<void(std::string, json11::Json)> callback)
{
    std::string req = txn.dump();
    req = "POST "+etcd_api_path+"/kv/txn HTTP/1.1\r\n"
        "Host: "+etcd_host+"\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: "+std::to_string(req.size())+"\r\n"
        "Connection: close\r\n"
        "\r\n"+req;
    http_request_json(etcd_address, req, callback);
}

// Start -> Load config & PGs -> Load peers -> Connect to peers -> Peer PGs
// Wait for PG changes -> Start/Stop PGs when requested
// Peer connection is lost -> Reload connection data -> Try to reconnect -> Repeat
void osd_t::load_pgs()
{
    assert(this->pgs.size() == 0);
    json11::Json::array txn = {
        // Update OSD state when loading PGs to allow "monitors" do CAS transactions when moving PGs
        json11::Json::object {
            { "requestPut", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/osd/state/"+std::to_string(osd_num)) },
                { "value", base64_encode(get_status().dump()) },
            } }
        },
        json11::Json::object {
            { "requestRange", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/config/osd/all") },
            } }
        },
        json11::Json::object {
            { "requestRange", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/config/pgs") },
            } }
        },
        json11::Json::object {
            { "requestRange", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/pg/history/") },
                { "range_end", base64_encode(etcd_prefix+"/pg/history0") },
            } }
        },
    };
    etcd_txn(json11::Json::object { { "success", txn } }, [this](std::string err, json11::Json data)
    {
        if (err != "")
        {
            printf("Error loading PGs from etcd: %s\n", err.c_str());
            tfd->set_timer(ETCD_START_INTERVAL, false, [this](int timer_id)
            {
                load_pgs();
            });
            return;
        }
        peering_state &= ~OSD_LOADING_PGS;
        blockstore_config_t osd_config = this->config;
        json11::Json pg_config;
        std::map<pg_num_t, json11::Json> pg_history;
        for (auto & res: data["responses"].array_items())
        {
            if (!res["response_range"].is_object())
            {
                continue;
            }
            for (auto & kvs: res["response_range"]["kvs"].array_items())
            {
                std::string key = base64_decode(kvs["key"].string_value());
                std::string json_err, json_text = base64_decode(kvs["value"].string_value());
                json11::Json value = json11::Json::parse(json_text, json_err);
                if (json_err != "")
                {
                    printf("Bad JSON in etcd key %s: %s (value: %s)\n", key.c_str(), json_err.c_str(), json_text.c_str());
                }
                if (key == etcd_prefix+"/config/osd/all")
                {
                    for (auto & cfg_var: value.object_items())
                    {
                        if (this->config.find(cfg_var.first) == this->config.end())
                        {
                            osd_config[cfg_var.first] = cfg_var.second.string_value();
                        }
                    }
                }
                else if (key == etcd_prefix+"/config/pgs")
                {
                    pg_config = value;
                }
                else if (key.substr(0, etcd_prefix.length()+12) == etcd_prefix+"/pg/history/")
                {
                    // <etcd_prefix>/pg/history/%d.
                    pg_num_t pg_num = stoull_full(key.substr(etcd_prefix.length()+12));
                    if (pg_num)
                    {
                        pg_history[pg_num] = value;
                    }
                }
            }
        }
        parse_config(osd_config);
        bind_socket();
        parse_pgs(pg_config, pg_history);
        report_status();
    });
}

void osd_t::parse_pgs(const json11::Json & pg_config, const std::map<pg_num_t, json11::Json> & pg_history)
{
    uint64_t pg_count = 0;
    for (auto pg_item: pg_config.object_items())
    {
        pg_num_t pg_num = stoull_full(pg_item.first);
        if (!pg_num)
        {
            throw std::runtime_error("Bad key in PG hash: "+pg_item.first);
        }
        auto & pg_json = pg_item.second;
        osd_num_t primary_osd = pg_json["primary"].uint64_value();
        if (primary_osd != 0 && primary_osd == this->osd_num)
        {
            // Take this PG
            std::set<osd_num_t> all_peers;
            std::vector<osd_num_t> target_set;
            for (auto pg_osd_num: pg_json["osd_set"].array_items())
            {
                osd_num_t pg_osd = pg_osd_num.uint64_value();
                target_set.push_back(pg_osd);
                if (pg_osd != 0)
                {
                    all_peers.insert(pg_osd);
                }
            }
            if (target_set.size() != 3)
            {
                throw std::runtime_error("Bad PG "+std::to_string(pg_num)+" config format: incorrect osd_set");
            }
            std::vector<std::vector<osd_num_t>> target_history;
            auto hist_it = pg_history.find(pg_num);
            if (hist_it != pg_history.end())
            {
                for (auto hist_item: hist_it->second.array_items())
                {
                    std::vector<osd_num_t> history_set;
                    for (auto pg_osd_num: hist_item["osd_set"].array_items())
                    {
                        osd_num_t pg_osd = pg_osd_num.uint64_value();
                        history_set.push_back(pg_osd);
                        if (pg_osd != 0)
                        {
                            all_peers.insert(pg_osd);
                        }
                    }
                    target_history.push_back(history_set);
                }
            }
            this->pgs[pg_num] = (pg_t){
                .state = PG_PEERING,
                .pg_cursize = 0,
                .pg_num = pg_num,
                .all_peers = std::vector<osd_num_t>(all_peers.begin(), all_peers.end()),
                .target_history = target_history,
                .target_set = target_set,
            };
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
        pg_count++;
    }
    this->pg_count = pg_count;
    if (wanted_peers.size() > 0)
    {
        peering_state |= OSD_CONNECTING_PEERS;
    }
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
                    { "requestRange", json11::Json::object {
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
        etcd_txn(json11::Json::object { { "success", load_peer_txn } }, [this](std::string err, json11::Json data)
        {
            // Ugly, but required to wake up the loop
            tfd->set_timer(peer_connect_interval*1000, false, [](int timer_id){});
            loading_peer_config = false;
            if (err != "")
            {
                printf("Failed to load peer configuration from etcd: %s\n", err.c_str());
                return;
            }
            for (auto & res: data["responses"].array_items())
            {
                if (res["response_range"]["kvs"].array_items().size())
                {
                    std::string key = base64_decode(res["response_range"]["kvs"][0]["key"].string_value());
                    // <etcd_prefix>/osd/state/<osd_num>
                    osd_num_t peer_osd = std::stoull(key.substr(etcd_prefix.length()+11));
                    std::string json_err;
                    std::string json_text = base64_decode(res["response_range"]["kvs"][0]["value"].string_value());
                    json11::Json st = json11::Json::parse(json_text, json_err);
                    if (json_err != "")
                    {
                        printf("Bad JSON in etcd key %s: %s (value: %s)\n", key.c_str(), json_err.c_str(), json_text.c_str());
                    }
                    if (peer_osd > 0 && st.is_object() && st["state"] == "up" &&
                        st["addresses"].is_array() && st["port"].int64_value() > 0 && st["port"].int64_value() < 65536)
                    {
                        peer_states[peer_osd] = st;
                    }
                }
            }
        });
    }
}
