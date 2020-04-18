#include "osd.h"
#include "osd_http.h"
#include "base64.h"

void osd_t::init_cluster()
{
    if (consul_address != "")
    {
        printf("OSD %lu reporting to Consul at %s each %d seconds\n", osd_num, consul_address.c_str(), consul_report_interval);
        report_status();
        this->consul_tfd = new timerfd_interval(ringloop, consul_report_interval, [this]()
        {
            report_status();
        });
    }
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
    st["port"] = bind_port;
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

/*
    json11::Json::object pg_status;
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
        pg_status[std::to_string(pg.pg_num)] = pg_st;
    }
    st["pgs"] = pg_status;
*/

void osd_t::report_status()
{
    std::string st = get_status().dump();
    // (!) Keys end with . to allow "select /osd/state/123. by prefix"
    // because Consul transactions fail if you try to read non-existing keys
    std::string req = "PUT /v1/kv/"+consul_prefix+"/osd/state/"+std::to_string(osd_num)+". HTTP/1.1\r\n"+
        "Host: "+consul_host+"\r\n"+
        "Content-Length: "+std::to_string(st.size())+"\r\n"+
        "Connection: close\r\n"+
        "\r\n"+st;
    http_request(consul_address, req, [this](int err, std::string res)
    {
        int pos = res.find("\r\n\r\n");
        if (pos >= 0)
        {
            res = res.substr(pos+4);
        }
        if (err != 0 || res != "true")
        {
            consul_failed_attempts++;
            printf("Error reporting state to Consul: code %d (%s), response text: %s\n", err, strerror(err), res.c_str());
            if (consul_failed_attempts > MAX_CONSUL_ATTEMPTS)
            {
                throw std::runtime_error("Cluster connection failed");
            }
            // Retry
            tfd->set_timer(CONSUL_RETRY_INTERVAL, false, [this](int timer_id)
            {
                report_status();
            });
        }
        else
        {
            consul_failed_attempts = 0;
        }
    });
}

void osd_t::consul_txn(json11::Json txn, std::function<void(std::string, json11::Json)> callback)
{
    std::string req = txn.dump();
    req = "PUT /v1/txn HTTP/1.1\r\n"
        "Host: "+consul_host+"\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: "+std::to_string(req.size())+"\r\n"
        "Connection: close\r\n"
        "\r\n"+req;
    http_request_json(consul_address, req, callback);
}

uint64_t stoull_full(std::string str, int base = 10)
{
    if (isspace(str[0]))
    {
        return 0;
    }
    size_t end = -1;
    uint64_t r = std::stoull(str, &end, base);
    if (end < str.length())
    {
        return 0;
    }
    return r;
}

// Start -> Load PGs -> Load peers -> Connect to peers -> Peer PGs
// Wait for PG changes -> Start/Stop PGs when requested
// Peer connection is lost -> Reload connection data -> Try to reconnect -> Repeat
void osd_t::load_pgs()
{
    assert(this->pgs.size() == 0);
    json11::Json::array txn = {
        json11::Json::object {
            { "KV", json11::Json::object {
                { "Verb", "get" },
                { "Key", consul_prefix+"/config/pgs" },
            } }
        },
        json11::Json::object {
            { "KV", json11::Json::object {
                { "Verb", "get-tree" },
                { "Key", consul_prefix+"/pg/history/" },
            } }
        },
    };
    consul_txn(txn, [this](std::string err, json11::Json data)
    {
        if (err != "")
        {
            printf("Error loading PGs from Consul: %s\n", err.c_str());
            tfd->set_timer(CONSUL_START_INTERVAL, false, [this](int timer_id)
            {
                load_pgs();
            });
            return;
        }
        json11::Json pg_config;
        std::map<pg_num_t, json11::Json> pg_history;
        for (auto & res: data["Results"].array_items())
        {
            std::string key = res["KV"]["Key"].string_value();
            std::string json_err;
            json11::Json value = json11::Json::parse(base64_decode(res["KV"]["Value"].string_value()), json_err);
            if (json_err != "")
            {
                printf("Bad JSON in Consul key %s: %s\n", key.c_str(), json_err.c_str());
            }
            if (key == consul_prefix+"/config/pgs")
            {
                pg_config = value;
            }
            else
            {
                // <consul_prefix>/pg/history/%d.
                pg_num_t pg_num = stoull_full(key.substr(consul_prefix.length()+13, key.length()-consul_prefix.length()-14));
                if (pg_num)
                {
                    pg_history[pg_num] = value;
                }
            }
        }
        parse_pgs(pg_config, pg_history);
        peering_state = OSD_CONNECTING_PEERS;
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
        if (primary_osd == this->osd_num)
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
            this->pgs[pg_num].print_state();
            // Add peers
            for (auto pg_osd: all_peers)
            {
                if (pg_osd != this->osd_num && osd_peer_fds.find(pg_osd) == osd_peer_fds.end())
                {
                    wanted_peers[pg_osd] = { 0 };
                }
            }
        }
        pg_count++;
    }
    this->pg_count = pg_count;
}

void osd_t::load_and_connect_peers()
{
    json11::Json::array load_peer_txn;
    for (auto wp_it = wanted_peers.begin(); wp_it != wanted_peers.end();)
    {
        osd_num_t osd_num = wp_it->first;
        if (osd_peer_fds.find(osd_num) != osd_peer_fds.end())
        {
            // It shouldn't be here
            wanted_peers.erase(wp_it++);
            if (!wanted_peers.size())
            {
                // Connected to all peers
                peering_state = peering_state & ~OSD_CONNECTING_PEERS;
            }
        }
        else if (peer_states.find(osd_num) == peer_states.end())
        {
            if (!loading_peer_config && (time(NULL) - wp_it->second.last_load_attempt >= peer_connect_interval))
            {
                // (Re)load OSD state from Consul
                wp_it->second.last_load_attempt = time(NULL);
                load_peer_txn.push_back(json11::Json::object {
                    { "KV", json11::Json::object {
                        { "Verb", "get-tree" },
                        { "Key", consul_prefix+"/osd/state/"+std::to_string(osd_num)+"." },
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
            const std::string addr = peer_states[osd_num]["addresses"][wp_it->second.address_index].string_value();
            int64_t port = peer_states[osd_num]["port"].int64_value();
            wp_it++;
            connect_peer(osd_num, addr.c_str(), port, [this](osd_num_t osd_num, int peer_fd)
            {
                wanted_peers[osd_num].connecting = false;
                if (peer_fd < 0)
                {
                    auto & addrs = peer_states[osd_num]["addresses"].array_items();
                    const char *addr = addrs[wanted_peers[osd_num].address_index].string_value().c_str();
                    printf("Failed to connect to peer OSD %lu address %s: %s\n", osd_num, addr, strerror(-peer_fd));
                    if (wanted_peers[osd_num].address_index < addrs.size()-1)
                    {
                        // Try all addresses
                        wanted_peers[osd_num].address_index++;
                    }
                    else
                    {
                        wanted_peers[osd_num].last_connect_attempt = time(NULL);
                        peer_states.erase(osd_num);
                    }
                    return;
                }
                printf("Connected with peer OSD %lu (fd %d)\n", clients[peer_fd].osd_num, peer_fd);
                wanted_peers.erase(osd_num);
                if (!wanted_peers.size())
                {
                    // Connected to all peers
                    printf("Connected to all peers\n");
                    peering_state = peering_state & ~OSD_CONNECTING_PEERS;
                }
                repeer_pgs(osd_num);
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
        consul_txn(load_peer_txn, [this](std::string err, json11::Json data)
        {
            loading_peer_config = false;
            if (err != "")
            {
                printf("Failed to load peer configuration from Consul: %s\n", err.c_str());
                return;
            }
            for (auto & res: data["Results"].array_items())
            {
                std::string key = res["KV"]["Key"].string_value();
                // <consul_prefix>/osd/state/<osd_num>.
                osd_num_t peer_osd = std::stoull(key.substr(consul_prefix.length()+11, key.length()-consul_prefix.length()-12));
                std::string json_err;
                json11::Json st = json11::Json::parse(base64_decode(res["KV"]["Value"].string_value()), json_err);
                if (json_err != "")
                {
                    printf("Bad JSON in Consul key %s: %s\n", key.c_str(), json_err.c_str());
                }
                if (peer_osd > 0 && st.is_object() && st["state"] == "up" &&
                    st["addresses"].is_array() && st["port"].is_number())
                {
                    peer_states[peer_osd] = st;
                }
            }
        });
    }
}
