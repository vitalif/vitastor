// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 or GNU GPL-2.0+ (see README.md for details)

#include "osd_ops.h"
#include "pg_states.h"
#include "etcd_state_client.h"
#include "http_client.h"
#include "base64.h"

json_kv_t etcd_state_client_t::parse_etcd_kv(const json11::Json & kv_json)
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

void etcd_state_client_t::etcd_txn(json11::Json txn, int timeout, std::function<void(std::string, json11::Json)> callback)
{
    etcd_call("/kv/txn", txn, timeout, callback);
}

void etcd_state_client_t::etcd_call(std::string api, json11::Json payload, int timeout, std::function<void(std::string, json11::Json)> callback)
{
    std::string etcd_address = etcd_addresses[rand() % etcd_addresses.size()];
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
    http_request_json(tfd, etcd_address, req, timeout, callback);
}

void etcd_state_client_t::parse_config(json11::Json & config)
{
    this->etcd_addresses.clear();
    if (config["etcd_address"].is_string())
    {
        std::string ea = config["etcd_address"].string_value();
        while (1)
        {
            int pos = ea.find(',');
            std::string addr = pos >= 0 ? ea.substr(0, pos) : ea;
            if (addr.length() > 0)
            {
                if (addr.find('/') < 0)
                    addr += "/v3";
                this->etcd_addresses.push_back(addr);
            }
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
            std::string addr = ea.string_value();
            if (addr != "")
            {
                if (addr.find('/') < 0)
                    addr += "/v3";
                this->etcd_addresses.push_back(addr);
            }
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
}

void etcd_state_client_t::start_etcd_watcher()
{
    std::string etcd_address = etcd_addresses[rand() % etcd_addresses.size()];
    std::string etcd_api_path;
    int pos = etcd_address.find('/');
    if (pos >= 0)
    {
        etcd_api_path = etcd_address.substr(pos);
        etcd_address = etcd_address.substr(0, pos);
    }
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
                    parse_state(kv.first, kv.second);
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

void etcd_state_client_t::load_global_config()
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
        json11::Json::object global_config;
        if (data["kvs"].array_items().size() > 0)
        {
            auto kv = parse_etcd_kv(data["kvs"][0]);
            if (kv.value.is_object())
            {
                global_config = kv.value.object_items();
            }
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
    etcd_txn(req, ETCD_SLOW_TIMEOUT, [this](std::string err, json11::Json data)
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
            on_load_pgs_hook(false);
            return;
        }
        for (auto & res: data["responses"].array_items())
        {
            for (auto & kv_json: res["response_range"]["kvs"].array_items())
            {
                auto kv = parse_etcd_kv(kv_json);
                parse_state(kv.key, kv.value);
            }
        }
        on_load_pgs_hook(true);
    });
}

void etcd_state_client_t::parse_state(const std::string & key, const json11::Json & value)
{
    if (key == etcd_prefix+"/config/pools")
    {
        for (auto & pool_item: this->pool_config)
        {
            pool_item.second.exists = false;
        }
        for (auto & pool_item: value.object_items())
        {
            pool_id_t pool_id = stoull_full(pool_item.first);
            if (!pool_id || pool_id >= POOL_ID_MAX)
            {
                printf("Pool ID %s is invalid (must be a number less than 0x%x), skipping pool\n", pool_item.first.c_str(), POOL_ID_MAX);
                continue;
            }
            if (pool_item.second["pg_size"].uint64_value() < 1 ||
                pool_item.second["scheme"] == "xor" && pool_item.second["pg_size"].uint64_value() < 3)
            {
                printf("Pool %u has invalid pg_size, skipping pool\n", pool_id);
                continue;
            }
            if (pool_item.second["pg_minsize"].uint64_value() < 1 ||
                pool_item.second["pg_minsize"].uint64_value() > pool_item.second["pg_size"].uint64_value() ||
                pool_item.second["pg_minsize"].uint64_value() < (pool_item.second["pg_size"].uint64_value() - 1))
            {
                printf("Pool %u has invalid pg_minsize, skipping pool\n", pool_id);
                continue;
            }
            if (pool_item.second["pg_count"].uint64_value() < 1)
            {
                printf("Pool %u has invalid pg_count, skipping pool\n", pool_id);
                continue;
            }
            if (pool_item.second["name"].string_value() == "")
            {
                printf("Pool %u has empty name, skipping pool\n", pool_id);
                continue;
            }
            if (pool_item.second["scheme"] != "replicated" && pool_item.second["scheme"] != "xor")
            {
                printf("Pool %u has invalid coding scheme (only \"xor\" and \"replicated\" are allowed), skipping pool\n", pool_id);
                continue;
            }
            if (pool_item.second["max_osd_combinations"].uint64_value() > 0 &&
                pool_item.second["max_osd_combinations"].uint64_value() < 100)
            {
                printf("Pool %u has invalid max_osd_combinations (must be at least 100), skipping pool\n", pool_id);
                continue;
            }
            auto & parsed_cfg = this->pool_config[pool_id];
            parsed_cfg.exists = true;
            parsed_cfg.id = pool_id;
            parsed_cfg.name = pool_item.second["name"].string_value();
            parsed_cfg.scheme = pool_item.second["scheme"] == "replicated" ? POOL_SCHEME_REPLICATED : POOL_SCHEME_XOR;
            parsed_cfg.pg_size = pool_item.second["pg_size"].uint64_value();
            parsed_cfg.pg_minsize = pool_item.second["pg_minsize"].uint64_value();
            parsed_cfg.pg_count = pool_item.second["pg_count"].uint64_value();
            parsed_cfg.failure_domain = pool_item.second["failure_domain"].string_value();
            parsed_cfg.pg_stripe_size = pool_item.second["pg_stripe_size"].uint64_value();
            if (!parsed_cfg.pg_stripe_size)
            {
                parsed_cfg.pg_stripe_size = DEFAULT_PG_STRIPE_SIZE;
            }
            parsed_cfg.max_osd_combinations = pool_item.second["max_osd_combinations"].uint64_value();
            if (!parsed_cfg.max_osd_combinations)
            {
                parsed_cfg.max_osd_combinations = 10000;
            }
            for (auto & pg_item: parsed_cfg.pg_config)
            {
                if (pg_item.second.target_set.size() != parsed_cfg.pg_size)
                {
                    printf("Pool %u PG %u configuration is invalid: osd_set size %lu != pool pg_size %lu\n",
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
            pool_id_t pool_id = stoull_full(pool_item.first);
            if (!pool_id || pool_id >= POOL_ID_MAX)
            {
                printf("Pool ID %s is invalid in PG configuration (must be a number less than 0x%x), skipping pool\n", pool_item.first.c_str(), POOL_ID_MAX);
                continue;
            }
            for (auto & pg_item: pool_item.second.object_items())
            {
                pg_num_t pg_num = stoull_full(pg_item.first);
                if (!pg_num)
                {
                    printf("Bad key in pool %u PG configuration: %s (must be a number), skipped\n", pool_id, pg_item.first.c_str());
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
                    printf("Pool %u PG %u configuration is invalid: osd_set size %lu != pool pg_size %lu\n",
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
                    printf(
                        "Invalid pool %u PG configuration: PG numbers don't cover whole 1..%lu range\n",
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
            printf("Bad etcd key %s, ignoring\n", key.c_str());
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
            printf("Bad etcd key %s, ignoring\n", key.c_str());
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
                    printf("Unexpected pool %u PG %u state keyword in etcd: %s\n", pool_id, pg_num, e.dump().c_str());
                    return;
                }
            }
            if (!cur_primary || !value["state"].is_array() || !state ||
                (state & PG_OFFLINE) && state != PG_OFFLINE ||
                (state & PG_PEERING) && state != PG_PEERING ||
                (state & PG_INCOMPLETE) && state != PG_INCOMPLETE)
            {
                printf("Unexpected pool %u PG %u state in etcd: primary=%lu, state=%s\n", pool_id, pg_num, cur_primary, value["state"].dump().c_str());
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
}
