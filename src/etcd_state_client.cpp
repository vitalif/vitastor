// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include "osd_ops.h"
#include "pg_states.h"
#include "etcd_state_client.h"
#ifndef __MOCK__
#include "addr_util.h"
#include "http_client.h"
#include "base64.h"
#endif

etcd_state_client_t::~etcd_state_client_t()
{
    for (auto watch: watches)
    {
        delete watch;
    }
    watches.clear();
    etcd_watches_initialised = -1;
    if (ws_keepalive_timer >= 0)
    {
        tfd->clear_timer(ws_keepalive_timer);
        ws_keepalive_timer = -1;
    }
#ifndef __MOCK__
    if (etcd_watch_ws)
    {
        http_close(etcd_watch_ws);
        etcd_watch_ws = NULL;
    }
    if (keepalive_client)
    {
        http_close(keepalive_client);
        keepalive_client = NULL;
    }
#endif
}

#ifndef __MOCK__
etcd_kv_t etcd_state_client_t::parse_etcd_kv(const json11::Json & kv_json)
{
    etcd_kv_t kv;
    kv.key = base64_decode(kv_json["key"].string_value());
    std::string json_err, json_text = base64_decode(kv_json["value"].string_value());
    kv.value = json_text == "" ? json11::Json() : json11::Json::parse(json_text, json_err);
    if (json_err != "")
    {
        fprintf(stderr, "Bad JSON in etcd key %s: %s (value: %s)\n", kv.key.c_str(), json_err.c_str(), json_text.c_str());
        kv.key = "";
    }
    else
        kv.mod_revision = kv_json["mod_revision"].uint64_value();
    return kv;
}

void etcd_state_client_t::etcd_txn(json11::Json txn, int timeout, int retries, int interval, std::function<void(std::string, json11::Json)> callback)
{
    etcd_call("/kv/txn", txn, timeout, retries, interval, callback);
}

void etcd_state_client_t::etcd_txn_slow(json11::Json txn, std::function<void(std::string, json11::Json)> callback)
{
    etcd_call("/kv/txn", txn, etcd_slow_timeout, max_etcd_attempts, 0, callback);
}

std::vector<std::string> etcd_state_client_t::get_addresses()
{
    auto addrs = etcd_local;
    addrs.insert(addrs.end(), etcd_addresses.begin(), etcd_addresses.end());
    return addrs;
}

void etcd_state_client_t::etcd_call_oneshot(std::string etcd_address, std::string api, json11::Json payload,
    int timeout, std::function<void(std::string, json11::Json)> callback)
{
    std::string etcd_api_path;
    int pos = etcd_address.find('/');
    if (pos >= 0)
    {
        etcd_api_path = etcd_address.substr(pos);
        etcd_address = etcd_address.substr(0, pos);
    }
    std::string req = payload.dump();
    req = "POST "+etcd_api_path+api+" HTTP/1.1\r\n"
        "Host: "+etcd_address+"\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: "+std::to_string(req.size())+"\r\n"
        "Connection: close\r\n"
        "\r\n"+req;
    auto http_cli = http_init(tfd);
    auto cb = [http_cli, callback](const http_response_t *response)
    {
        std::string err;
        json11::Json data;
        response->parse_json_response(err, data);
        callback(err, data);
        http_close(http_cli);
    };
    http_request(http_cli, etcd_address, req, { .timeout = timeout }, cb);
}

void etcd_state_client_t::etcd_call(std::string api, json11::Json payload, int timeout,
    int retries, int interval, std::function<void(std::string, json11::Json)> callback)
{
    if (!etcd_addresses.size() && !etcd_local.size())
    {
        fprintf(stderr, "etcd_address is missing in Vitastor configuration\n");
        exit(1);
    }
    pick_next_etcd();
    std::string etcd_address = selected_etcd_address;
    std::string etcd_api_path;
    int pos = etcd_address.find('/');
    if (pos >= 0)
    {
        etcd_api_path = etcd_address.substr(pos);
        etcd_address = etcd_address.substr(0, pos);
    }
    std::string req = payload.dump();
    req = "POST "+etcd_api_path+api+" HTTP/1.1\r\n"
        "Host: "+etcd_address+"\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: "+std::to_string(req.size())+"\r\n"
        "Connection: keep-alive\r\n"
        "Keep-Alive: timeout="+std::to_string(etcd_keepalive_timeout)+"\r\n"
        "\r\n"+req;
    auto cb = [this, api, payload, timeout, retries, interval, callback,
        cur_addr = selected_etcd_address](const http_response_t *response)
    {
        std::string err;
        json11::Json data;
        response->parse_json_response(err, data);
        if (err != "")
        {
            if (cur_addr == selected_etcd_address)
                selected_etcd_address = "";
            if (retries > 0)
            {
                if (this->log_level > 0)
                {
                    printf(
                        "Warning: etcd request failed: %s, retrying %d more times\n",
                        err.c_str(), retries
                    );
                }
                if (interval > 0)
                {
                    tfd->set_timer(interval, false, [this, api, payload, timeout, retries, interval, callback](int)
                    {
                        etcd_call(api, payload, timeout, retries-1, interval, callback);
                    });
                }
                else
                    etcd_call(api, payload, timeout, retries-1, interval, callback);
            }
            else
                callback(err, data);
        }
        else
            callback(err, data);
    };
    if (!keepalive_client)
    {
        keepalive_client = http_init(tfd);
    }
    http_request(keepalive_client, etcd_address, req, { .timeout = timeout, .keepalive = true }, cb);
}

void etcd_state_client_t::add_etcd_url(std::string addr)
{
    if (addr.length() > 0)
    {
        if (strtolower(addr.substr(0, 7)) == "http://")
            addr = addr.substr(7);
        else if (strtolower(addr.substr(0, 8)) == "https://")
        {
            fprintf(stderr, "HTTPS is unsupported for etcd. Either use plain HTTP or setup a local proxy for etcd interaction\n");
            exit(1);
        }
        if (!local_ips.size())
            local_ips = getifaddr_list();
        std::string check_addr;
        int pos = addr.find('/');
        int pos2 = addr.find(':');
        if (pos2 >= 0)
            check_addr = addr.substr(0, pos2);
        else if (pos >= 0)
            check_addr = addr.substr(0, pos);
        else
            check_addr = addr;
        if (pos == std::string::npos)
            addr += "/v3";
        int i;
        for (i = 0; i < local_ips.size(); i++)
        {
            if (local_ips[i] == check_addr)
            {
                this->etcd_local.push_back(addr);
                break;
            }
        }
        if (i >= local_ips.size())
            this->etcd_addresses.push_back(addr);
    }
}

void etcd_state_client_t::parse_config(const json11::Json & config)
{
    this->etcd_addresses.clear();
    if (config["etcd_address"].is_string())
    {
        std::string ea = config["etcd_address"].string_value();
        while (1)
        {
            int pos = ea.find(',');
            add_etcd_url(pos >= 0 ? ea.substr(0, pos) : ea);
            if (pos >= 0)
                ea = ea.substr(pos+1);
            else
                break;
        }
    }
    else if (config["etcd_address"].array_items().size())
    {
        for (auto & ea: config["etcd_address"].array_items())
        {
            add_etcd_url(ea.string_value());
        }
    }
    this->etcd_prefix = config["etcd_prefix"].string_value();
    if (this->etcd_prefix == "")
    {
        this->etcd_prefix = "/vitastor";
    }
    else if (this->etcd_prefix[0] != '/')
    {
        this->etcd_prefix = "/"+this->etcd_prefix;
    }
    this->log_level = config["log_level"].int64_value();
    this->etcd_keepalive_timeout = config["etcd_keepalive_timeout"].uint64_value();
    if (this->etcd_keepalive_timeout <= 0)
    {
        this->etcd_keepalive_timeout = config["etcd_report_interval"].uint64_value() * 2;
        if (this->etcd_keepalive_timeout < 30)
            this->etcd_keepalive_timeout = 30;
    }
    this->etcd_ws_keepalive_interval = config["etcd_ws_keepalive_interval"].uint64_value();
    if (this->etcd_ws_keepalive_interval <= 0)
    {
        this->etcd_ws_keepalive_interval = 30;
    }
    this->max_etcd_attempts = config["max_etcd_attempts"].uint64_value();
    if (this->max_etcd_attempts <= 0)
    {
        this->max_etcd_attempts = 5;
    }
    this->etcd_slow_timeout = config["etcd_slow_timeout"].uint64_value();
    if (this->etcd_slow_timeout <= 0)
    {
        this->etcd_slow_timeout = 5000;
    }
    this->etcd_quick_timeout = config["etcd_quick_timeout"].uint64_value();
    if (this->etcd_quick_timeout <= 0)
    {
        this->etcd_quick_timeout = 1000;
    }
}

void etcd_state_client_t::pick_next_etcd()
{
    if (selected_etcd_address != "")
        return;
    if (addresses_to_try.size() == 0)
    {
        // Prefer local etcd, if any
        for (int i = 0; i < etcd_local.size(); i++)
            addresses_to_try.push_back(etcd_local[i]);
        std::vector<int> ns;
        for (int i = 0; i < etcd_addresses.size(); i++)
            ns.push_back(i);
        if (!rand_initialized)
        {
            timespec tv;
            clock_gettime(CLOCK_REALTIME, &tv);
            srand48(tv.tv_sec*1000000000 + tv.tv_nsec);
            rand_initialized = true;
        }
        while (ns.size())
        {
            int i = lrand48() % ns.size();
            addresses_to_try.push_back(etcd_addresses[ns[i]]);
            ns.erase(ns.begin()+i, ns.begin()+i+1);
        }
    }
    selected_etcd_address = addresses_to_try[0];
    addresses_to_try.erase(addresses_to_try.begin(), addresses_to_try.begin()+1);
}

void etcd_state_client_t::start_etcd_watcher()
{
    if (!etcd_addresses.size() && !etcd_local.size())
    {
        fprintf(stderr, "etcd_address is missing in Vitastor configuration\n");
        exit(1);
    }
    pick_next_etcd();
    std::string etcd_address = selected_etcd_address;
    std::string etcd_api_path;
    int pos = etcd_address.find('/');
    if (pos >= 0)
    {
        etcd_api_path = etcd_address.substr(pos);
        etcd_address = etcd_address.substr(0, pos);
    }
    etcd_watches_initialised = 0;
    ws_alive = 1;
    if (etcd_watch_ws)
    {
        http_close(etcd_watch_ws);
        etcd_watch_ws = NULL;
    }
    if (this->log_level > 1)
        printf("Trying to connect to etcd websocket at %s\n", etcd_address.c_str());
    etcd_watch_ws = open_websocket(tfd, etcd_address, etcd_api_path+"/watch", etcd_slow_timeout,
        [this, cur_addr = selected_etcd_address](const http_response_t *msg)
    {
        if (msg->body.length())
        {
            ws_alive = 1;
            std::string json_err;
            json11::Json data = json11::Json::parse(msg->body, json_err);
            if (json_err != "")
            {
                fprintf(stderr, "Bad JSON in etcd event: %s, ignoring event\n", json_err.c_str());
            }
            else
            {
                if (data["result"]["created"].bool_value())
                {
                    uint64_t watch_id = data["result"]["watch_id"].uint64_value();
                    if (watch_id == ETCD_CONFIG_WATCH_ID ||
                        watch_id == ETCD_PG_STATE_WATCH_ID ||
                        watch_id == ETCD_PG_HISTORY_WATCH_ID ||
                        watch_id == ETCD_OSD_STATE_WATCH_ID)
                        etcd_watches_initialised++;
                    if (etcd_watches_initialised == 4 && this->log_level > 0)
                        fprintf(stderr, "Successfully subscribed to etcd at %s\n", selected_etcd_address.c_str());
                }
                if (data["result"]["canceled"].bool_value())
                {
                    // etcd watch canceled, maybe because the revision was compacted
                    if (data["result"]["compact_revision"].uint64_value())
                    {
                        // we may miss events if we proceed
                        // so we should restart from the beginning if we can
                        if (on_reload_hook != NULL)
                        {
                            fprintf(stderr, "Revisions before %lu were compacted by etcd, reloading state\n",
                                data["result"]["compact_revision"].uint64_value());
                            if (etcd_watch_ws)
                            {
                                http_close(etcd_watch_ws);
                                etcd_watch_ws = NULL;
                            }
                            etcd_watch_revision = 0;
                            on_reload_hook();
                        }
                        else
                        {
                            fprintf(stderr, "Revisions before %lu were compacted by etcd, exiting\n",
                                data["result"]["compact_revision"].uint64_value());
                            exit(1);
                        }
                    }
                    else
                    {
                        fprintf(stderr, "Watch canceled by etcd, reason: %s, exiting\n", data["result"]["cancel_reason"].string_value().c_str());
                        exit(1);
                    }
                }
                if (etcd_watches_initialised == 4)
                {
                    etcd_watch_revision = data["result"]["header"]["revision"].uint64_value()+1;
                    addresses_to_try.clear();
                }
                // First gather all changes into a hash to remove multiple overwrites
                std::map<std::string, etcd_kv_t> changes;
                for (auto & ev: data["result"]["events"].array_items())
                {
                    auto kv = parse_etcd_kv(ev["kv"]);
                    if (kv.key != "")
                    {
                        changes[kv.key] = kv;
                    }
                }
                for (auto & kv: changes)
                {
                    if (this->log_level > 3)
                    {
                        fprintf(stderr, "Incoming event: %s -> %s\n", kv.first.c_str(), kv.second.value.dump().c_str());
                    }
                    parse_state(kv.second);
                }
                // React to changes
                if (on_change_hook != NULL)
                {
                    on_change_hook(changes);
                }
            }
        }
        if (msg->eof)
        {
            if (cur_addr == selected_etcd_address)
            {
                fprintf(stderr, "Disconnected from etcd %s\n", selected_etcd_address.c_str());
                selected_etcd_address = "";
            }
            else
                fprintf(stderr, "Disconnected from etcd\n");
            if (etcd_watch_ws)
            {
                http_close(etcd_watch_ws);
                etcd_watch_ws = NULL;
            }
            if (etcd_watches_initialised == 0)
            {
                // Connection not established, retry in <etcd_quick_timeout>
                tfd->set_timer(etcd_quick_timeout, false, [this](int)
                {
                    start_etcd_watcher();
                });
            }
            else if (etcd_watches_initialised > 0)
            {
                // Connection was live, retry immediately
                start_etcd_watcher();
            }
        }
    });
    http_post_message(etcd_watch_ws, WS_TEXT, json11::Json(json11::Json::object {
        { "create_request", json11::Json::object {
            { "key", base64_encode(etcd_prefix+"/config/") },
            { "range_end", base64_encode(etcd_prefix+"/config0") },
            { "start_revision", etcd_watch_revision },
            { "watch_id", ETCD_CONFIG_WATCH_ID },
            { "progress_notify", true },
        } }
    }).dump());
    http_post_message(etcd_watch_ws, WS_TEXT, json11::Json(json11::Json::object {
        { "create_request", json11::Json::object {
            { "key", base64_encode(etcd_prefix+"/osd/state/") },
            { "range_end", base64_encode(etcd_prefix+"/osd/state0") },
            { "start_revision", etcd_watch_revision },
            { "watch_id", ETCD_OSD_STATE_WATCH_ID },
            { "progress_notify", true },
        } }
    }).dump());
    http_post_message(etcd_watch_ws, WS_TEXT, json11::Json(json11::Json::object {
        { "create_request", json11::Json::object {
            { "key", base64_encode(etcd_prefix+"/pg/state/") },
            { "range_end", base64_encode(etcd_prefix+"/pg/state0") },
            { "start_revision", etcd_watch_revision },
            { "watch_id", ETCD_PG_STATE_WATCH_ID },
            { "progress_notify", true },
        } }
    }).dump());
    http_post_message(etcd_watch_ws, WS_TEXT, json11::Json(json11::Json::object {
        { "create_request", json11::Json::object {
            { "key", base64_encode(etcd_prefix+"/pg/history/") },
            { "range_end", base64_encode(etcd_prefix+"/pg/history0") },
            { "start_revision", etcd_watch_revision },
            { "watch_id", ETCD_PG_HISTORY_WATCH_ID },
            { "progress_notify", true },
        } }
    }).dump());
    if (on_start_watcher_hook)
    {
        on_start_watcher_hook(etcd_watch_ws);
    }
    if (ws_keepalive_timer < 0)
    {
        ws_keepalive_timer = tfd->set_timer(etcd_ws_keepalive_interval*1000, true, [this](int)
        {
            if (!etcd_watch_ws)
            {
                // Do nothing
            }
            else if (!ws_alive)
            {
                if (this->log_level > 0)
                {
                    fprintf(stderr, "Websocket ping failed, disconnecting from etcd %s\n", selected_etcd_address.c_str());
                }
                if (etcd_watch_ws)
                {
                    http_close(etcd_watch_ws);
                    etcd_watch_ws = NULL;
                }
                start_etcd_watcher();
            }
            else
            {
                ws_alive = 0;
                http_post_message(etcd_watch_ws, WS_TEXT, json11::Json(json11::Json::object {
                    { "progress_request", json11::Json::object { } }
                }).dump());
            }
        });
    }
}

void etcd_state_client_t::load_global_config()
{
    etcd_call("/kv/range", json11::Json::object {
        { "key", base64_encode(etcd_prefix+"/config/global") }
    }, etcd_slow_timeout, max_etcd_attempts, 0, [this](std::string err, json11::Json data)
    {
        if (err != "")
        {
            fprintf(stderr, "Error reading OSD configuration from etcd: %s\n", err.c_str());
            tfd->set_timer(etcd_slow_timeout, false, [this](int timer_id)
            {
                load_global_config();
            });
            return;
        }
        json11::Json::object global_config;
        if (data["kvs"].array_items().size() > 0)
        {
            auto kv = parse_etcd_kv(data["kvs"][0]);
            if (kv.value.is_object())
            {
                global_config = kv.value.object_items();
            }
        }
        bs_block_size = global_config["block_size"].uint64_value();
        if (!bs_block_size)
        {
            bs_block_size = DEFAULT_BLOCK_SIZE;
        }
        on_load_config_hook(global_config);
    });
}

void etcd_state_client_t::load_pgs()
{
    json11::Json::array txn = {
        json11::Json::object {
            { "request_range", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/config/pools") },
            } }
        },
        json11::Json::object {
            { "request_range", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/config/pgs") },
            } }
        },
        json11::Json::object {
            { "request_range", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/config/inode/") },
                { "range_end", base64_encode(etcd_prefix+"/config/inode0") },
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
        json11::Json::object {
            { "request_range", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/osd/state/") },
                { "range_end", base64_encode(etcd_prefix+"/osd/state0") },
            } }
        },
    };
    json11::Json::object req = { { "success", txn } };
    json11::Json checks = load_pgs_checks_hook != NULL ? load_pgs_checks_hook() : json11::Json();
    if (checks.array_items().size() > 0)
    {
        req["compare"] = checks;
    }
    etcd_txn_slow(req, [this](std::string err, json11::Json data)
    {
        if (err != "")
        {
            // Retry indefinitely
            fprintf(stderr, "Error loading PGs from etcd: %s\n", err.c_str());
            tfd->set_timer(etcd_slow_timeout, false, [this](int timer_id)
            {
                load_pgs();
            });
            return;
        }
        if (!data["succeeded"].bool_value())
        {
            on_load_pgs_hook(false);
            return;
        }
        if (!etcd_watch_revision)
        {
            etcd_watch_revision = data["header"]["revision"].uint64_value()+1;
        }
        for (auto & res: data["responses"].array_items())
        {
            for (auto & kv_json: res["response_range"]["kvs"].array_items())
            {
                auto kv = parse_etcd_kv(kv_json);
                parse_state(kv);
            }
        }
        on_load_pgs_hook(true);
        start_etcd_watcher();
    });
}
#else
void etcd_state_client_t::parse_config(const json11::Json & config)
{
}

void etcd_state_client_t::load_global_config()
{
    json11::Json::object global_config;
    on_load_config_hook(global_config);
}

void etcd_state_client_t::load_pgs()
{
}
#endif

void etcd_state_client_t::parse_state(const etcd_kv_t & kv)
{
    const std::string & key = kv.key;
    const json11::Json & value = kv.value;
    if (key == etcd_prefix+"/config/pools")
    {
        for (auto & pool_item: this->pool_config)
        {
            pool_item.second.exists = false;
        }
        for (auto & pool_item: value.object_items())
        {
            pool_config_t pc;
            // ID
            pool_id_t pool_id;
            char null_byte = 0;
            sscanf(pool_item.first.c_str(), "%u%c", &pool_id, &null_byte);
            if (!pool_id || pool_id >= POOL_ID_MAX || null_byte != 0)
            {
                fprintf(stderr, "Pool ID %s is invalid (must be a number less than 0x%x), skipping pool\n", pool_item.first.c_str(), POOL_ID_MAX);
                continue;
            }
            pc.id = pool_id;
            // Pool Name
            pc.name = pool_item.second["name"].string_value();
            if (pc.name == "")
            {
                fprintf(stderr, "Pool %u has empty name, skipping pool\n", pool_id);
                continue;
            }
            // Failure Domain
            pc.failure_domain = pool_item.second["failure_domain"].string_value();
            // Coding Scheme
            if (pool_item.second["scheme"] == "replicated")
                pc.scheme = POOL_SCHEME_REPLICATED;
            else if (pool_item.second["scheme"] == "xor")
                pc.scheme = POOL_SCHEME_XOR;
            else if (pool_item.second["scheme"] == "jerasure")
                pc.scheme = POOL_SCHEME_JERASURE;
            else
            {
                fprintf(stderr, "Pool %u has invalid coding scheme (one of \"xor\", \"replicated\" or \"jerasure\" required), skipping pool\n", pool_id);
                continue;
            }
            // PG Size
            pc.pg_size = pool_item.second["pg_size"].uint64_value();
            if (pc.pg_size < 1 ||
                pool_item.second["pg_size"].uint64_value() < 3 &&
                (pc.scheme == POOL_SCHEME_XOR || pc.scheme == POOL_SCHEME_JERASURE) ||
                pool_item.second["pg_size"].uint64_value() > 256)
            {
                fprintf(stderr, "Pool %u has invalid pg_size, skipping pool\n", pool_id);
                continue;
            }
            // Parity Chunks
            pc.parity_chunks = pool_item.second["parity_chunks"].uint64_value();
            if (pc.scheme == POOL_SCHEME_XOR)
            {
                if (pc.parity_chunks > 1)
                {
                    fprintf(stderr, "Pool %u has invalid parity_chunks (must be 1), skipping pool\n", pool_id);
                    continue;
                }
                pc.parity_chunks = 1;
            }
            if (pc.scheme == POOL_SCHEME_JERASURE &&
                (pc.parity_chunks < 1 || pc.parity_chunks > pc.pg_size-2))
            {
                fprintf(stderr, "Pool %u has invalid parity_chunks (must be between 1 and pg_size-2), skipping pool\n", pool_id);
                continue;
            }
            // PG MinSize
            pc.pg_minsize = pool_item.second["pg_minsize"].uint64_value();
            if (pc.pg_minsize < 1 || pc.pg_minsize > pc.pg_size ||
                (pc.scheme == POOL_SCHEME_XOR || pc.scheme == POOL_SCHEME_JERASURE) &&
                pc.pg_minsize < (pc.pg_size-pc.parity_chunks))
            {
                fprintf(stderr, "Pool %u has invalid pg_minsize, skipping pool\n", pool_id);
                continue;
            }
            // PG Count
            pc.pg_count = pool_item.second["pg_count"].uint64_value();
            if (pc.pg_count < 1)
            {
                fprintf(stderr, "Pool %u has invalid pg_count, skipping pool\n", pool_id);
                continue;
            }
            // Max OSD Combinations
            pc.max_osd_combinations = pool_item.second["max_osd_combinations"].uint64_value();
            if (!pc.max_osd_combinations)
                pc.max_osd_combinations = 10000;
            if (pc.max_osd_combinations > 0 && pc.max_osd_combinations < 100)
            {
                fprintf(stderr, "Pool %u has invalid max_osd_combinations (must be at least 100), skipping pool\n", pool_id);
                continue;
            }
            // PG Stripe Size
            pc.pg_stripe_size = pool_item.second["pg_stripe_size"].uint64_value();
            uint64_t min_stripe_size = bs_block_size * (pc.scheme == POOL_SCHEME_REPLICATED ? 1 : (pc.pg_size-pc.parity_chunks));
            if (pc.pg_stripe_size < min_stripe_size)
                pc.pg_stripe_size = min_stripe_size;
            // Save
            pc.real_pg_count = this->pool_config[pool_id].real_pg_count;
            std::swap(pc.pg_config, this->pool_config[pool_id].pg_config);
            std::swap(this->pool_config[pool_id], pc);
            auto & parsed_cfg = this->pool_config[pool_id];
            parsed_cfg.exists = true;
            for (auto & pg_item: parsed_cfg.pg_config)
            {
                if (pg_item.second.target_set.size() != parsed_cfg.pg_size)
                {
                    fprintf(stderr, "Pool %u PG %u configuration is invalid: osd_set size %lu != pool pg_size %lu\n",
                        pool_id, pg_item.first, pg_item.second.target_set.size(), parsed_cfg.pg_size);
                    pg_item.second.pause = true;
                }
            }
        }
    }
    else if (key == etcd_prefix+"/config/pgs")
    {
        for (auto & pool_item: this->pool_config)
        {
            for (auto & pg_item: pool_item.second.pg_config)
            {
                pg_item.second.exists = false;
            }
        }
        for (auto & pool_item: value["items"].object_items())
        {
            pool_id_t pool_id;
            char null_byte = 0;
            sscanf(pool_item.first.c_str(), "%u%c", &pool_id, &null_byte);
            if (!pool_id || pool_id >= POOL_ID_MAX || null_byte != 0)
            {
                fprintf(stderr, "Pool ID %s is invalid in PG configuration (must be a number less than 0x%x), skipping pool\n", pool_item.first.c_str(), POOL_ID_MAX);
                continue;
            }
            for (auto & pg_item: pool_item.second.object_items())
            {
                pg_num_t pg_num = 0;
                sscanf(pg_item.first.c_str(), "%u%c", &pg_num, &null_byte);
                if (!pg_num || null_byte != 0)
                {
                    fprintf(stderr, "Bad key in pool %u PG configuration: %s (must be a number), skipped\n", pool_id, pg_item.first.c_str());
                    continue;
                }
                auto & parsed_cfg = this->pool_config[pool_id].pg_config[pg_num];
                parsed_cfg.exists = true;
                parsed_cfg.pause = pg_item.second["pause"].bool_value();
                parsed_cfg.primary = pg_item.second["primary"].uint64_value();
                parsed_cfg.target_set.clear();
                for (auto & pg_osd: pg_item.second["osd_set"].array_items())
                {
                    parsed_cfg.target_set.push_back(pg_osd.uint64_value());
                }
                if (parsed_cfg.target_set.size() != pool_config[pool_id].pg_size)
                {
                    fprintf(stderr, "Pool %u PG %u configuration is invalid: osd_set size %lu != pool pg_size %lu\n",
                        pool_id, pg_num, parsed_cfg.target_set.size(), pool_config[pool_id].pg_size);
                    parsed_cfg.pause = true;
                }
            }
        }
        for (auto & pool_item: this->pool_config)
        {
            int n = 0;
            for (auto pg_it = pool_item.second.pg_config.begin(); pg_it != pool_item.second.pg_config.end(); pg_it++)
            {
                if (pg_it->second.exists && pg_it->first != ++n)
                {
                    fprintf(
                        stderr, "Invalid pool %u PG configuration: PG numbers don't cover whole 1..%lu range\n",
                        pool_item.second.id, pool_item.second.pg_config.size()
                    );
                    for (pg_it = pool_item.second.pg_config.begin(); pg_it != pool_item.second.pg_config.end(); pg_it++)
                    {
                        pg_it->second.exists = false;
                    }
                    n = 0;
                    break;
                }
            }
            pool_item.second.real_pg_count = n;
        }
    }
    else if (key.substr(0, etcd_prefix.length()+12) == etcd_prefix+"/pg/history/")
    {
        // <etcd_prefix>/pg/history/%d/%d
        pool_id_t pool_id = 0;
        pg_num_t pg_num = 0;
        char null_byte = 0;
        sscanf(key.c_str() + etcd_prefix.length()+12, "%u/%u%c", &pool_id, &pg_num, &null_byte);
        if (!pool_id || pool_id >= POOL_ID_MAX || !pg_num || null_byte != 0)
        {
            fprintf(stderr, "Bad etcd key %s, ignoring\n", key.c_str());
        }
        else
        {
            auto & pg_cfg = this->pool_config[pool_id].pg_config[pg_num];
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
            // Read epoch
            pg_cfg.epoch = value["epoch"].uint64_value();
            if (on_change_pg_history_hook != NULL)
            {
                on_change_pg_history_hook(pool_id, pg_num);
            }
        }
    }
    else if (key.substr(0, etcd_prefix.length()+10) == etcd_prefix+"/pg/state/")
    {
        // <etcd_prefix>/pg/state/%d/%d
        pool_id_t pool_id = 0;
        pg_num_t pg_num = 0;
        char null_byte = 0;
        sscanf(key.c_str() + etcd_prefix.length()+10, "%u/%u%c", &pool_id, &pg_num, &null_byte);
        if (!pool_id || pool_id >= POOL_ID_MAX || !pg_num || null_byte != 0)
        {
            fprintf(stderr, "Bad etcd key %s, ignoring\n", key.c_str());
        }
        else if (value.is_null())
        {
            this->pool_config[pool_id].pg_config[pg_num].cur_primary = 0;
            this->pool_config[pool_id].pg_config[pg_num].cur_state = 0;
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
                    fprintf(stderr, "Unexpected pool %u PG %u state keyword in etcd: %s\n", pool_id, pg_num, e.dump().c_str());
                    return;
                }
            }
            if (!cur_primary || !value["state"].is_array() || !state ||
                (state & PG_OFFLINE) && state != PG_OFFLINE ||
                (state & PG_PEERING) && state != PG_PEERING ||
                (state & PG_INCOMPLETE) && state != PG_INCOMPLETE)
            {
                fprintf(stderr, "Unexpected pool %u PG %u state in etcd: primary=%lu, state=%s\n", pool_id, pg_num, cur_primary, value["state"].dump().c_str());
                return;
            }
            this->pool_config[pool_id].pg_config[pg_num].cur_primary = cur_primary;
            this->pool_config[pool_id].pg_config[pg_num].cur_state = state;
        }
    }
    else if (key.substr(0, etcd_prefix.length()+11) == etcd_prefix+"/osd/state/")
    {
        // <etcd_prefix>/osd/state/%d
        osd_num_t peer_osd = std::stoull(key.substr(etcd_prefix.length()+11));
        if (peer_osd > 0)
        {
            if (value.is_object() && value["state"] == "up" &&
                value["addresses"].is_array() &&
                value["port"].int64_value() > 0 && value["port"].int64_value() < 65536)
            {
                this->peer_states[peer_osd] = value;
            }
            else
            {
                this->peer_states.erase(peer_osd);
            }
            if (on_change_osd_state_hook != NULL)
            {
                on_change_osd_state_hook(peer_osd);
            }
        }
    }
    else if (key.substr(0, etcd_prefix.length()+14) == etcd_prefix+"/config/inode/")
    {
        // <etcd_prefix>/config/inode/%d/%d
        uint64_t pool_id = 0;
        uint64_t inode_num = 0;
        char null_byte = 0;
        sscanf(key.c_str() + etcd_prefix.length()+14, "%lu/%lu%c", &pool_id, &inode_num, &null_byte);
        if (!pool_id || pool_id >= POOL_ID_MAX || !inode_num || (inode_num >> (64-POOL_ID_BITS)) || null_byte != 0)
        {
            fprintf(stderr, "Bad etcd key %s, ignoring\n", key.c_str());
        }
        else
        {
            inode_num |= (pool_id << (64-POOL_ID_BITS));
            auto it = this->inode_config.find(inode_num);
            if (it != this->inode_config.end() && it->second.name != "")
            {
                auto n_it = this->inode_by_name.find(it->second.name);
                if (n_it->second == inode_num)
                {
                    this->inode_by_name.erase(n_it);
                    for (auto w: watches)
                    {
                        if (w->name == it->second.name)
                        {
                            w->cfg = { 0 };
                        }
                    }
                }
            }
            if (!value.is_object())
            {
                if (on_inode_change_hook != NULL)
                {
                    on_inode_change_hook(inode_num, true);
                }
                this->inode_config.erase(inode_num);
            }
            else
            {
                inode_t parent_inode_num = value["parent_id"].uint64_value();
                if (parent_inode_num && !(parent_inode_num >> (64-POOL_ID_BITS)))
                {
                    uint64_t parent_pool_id = value["parent_pool"].uint64_value();
                    if (!parent_pool_id)
                        parent_inode_num |= pool_id << (64-POOL_ID_BITS);
                    else if (parent_pool_id >= POOL_ID_MAX)
                    {
                        fprintf(
                            stderr, "Inode %lu/%lu parent_pool value is invalid, ignoring parent setting\n",
                            inode_num >> (64-POOL_ID_BITS), inode_num & (((uint64_t)1 << (64-POOL_ID_BITS)) - 1)
                        );
                        parent_inode_num = 0;
                    }
                    else
                        parent_inode_num |= parent_pool_id << (64-POOL_ID_BITS);
                }
                insert_inode_config((inode_config_t){
                    .num = inode_num,
                    .name = value["name"].string_value(),
                    .size = value["size"].uint64_value(),
                    .parent_id = parent_inode_num,
                    .readonly = value["readonly"].bool_value(),
                    .meta = value["meta"],
                    .mod_revision = kv.mod_revision,
                });
            }
        }
    }
}

void etcd_state_client_t::insert_inode_config(const inode_config_t & cfg)
{
    this->inode_config[cfg.num] = cfg;
    if (cfg.name != "")
    {
        this->inode_by_name[cfg.name] = cfg.num;
        for (auto w: watches)
        {
            if (w->name == cfg.name)
            {
                w->cfg = cfg;
            }
        }
    }
    if (on_inode_change_hook != NULL)
    {
        on_inode_change_hook(cfg.num, false);
    }
}

inode_watch_t* etcd_state_client_t::watch_inode(std::string name)
{
    inode_watch_t *watch = new inode_watch_t;
    watch->name = name;
    watches.push_back(watch);
    auto it = inode_by_name.find(name);
    if (it != inode_by_name.end())
    {
        watch->cfg = inode_config[it->second];
    }
    return watch;
}

void etcd_state_client_t::close_watch(inode_watch_t* watch)
{
    for (int i = 0; i < watches.size(); i++)
    {
        if (watches[i] == watch)
        {
            watches.erase(watches.begin()+i, watches.begin()+i+1);
            break;
        }
    }
    delete watch;
}

json11::Json::object etcd_state_client_t::serialize_inode_cfg(inode_config_t *cfg)
{
    json11::Json::object new_cfg = json11::Json::object {
        { "name", cfg->name },
        { "size", cfg->size },
    };
    if (cfg->parent_id)
    {
        if (INODE_POOL(cfg->num) != INODE_POOL(cfg->parent_id))
            new_cfg["parent_pool"] = (uint64_t)INODE_POOL(cfg->parent_id);
        new_cfg["parent_id"] = (uint64_t)INODE_NO_POOL(cfg->parent_id);
    }
    if (cfg->readonly)
    {
        new_cfg["readonly"] = true;
    }
    if (cfg->meta.is_object())
    {
        new_cfg["meta"] = cfg->meta;
    }
    return new_cfg;
}

int etcd_state_client_t::address_count()
{
    return etcd_addresses.size() + etcd_local.size();
}
