// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include "malloc_or_die.h"
#include "osd_ops.h"
#include "msgr_op.h"
#include "pg_states.h"
#include "etcd_state_client.h"
#include "addr_util.h"
#include "str_util.h"
#include "json_util.h"

etcd_state_client_t::~etcd_state_client_t()
{
    for (auto watch: watches)
    {
        delete watch;
    }
    watches.clear();
}

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

std::shared_ptr<user_info_t> etcd_state_client_t::get_user(const std::string & username)
{
    auto user_it = user_info.find(username);
    if (user_it != user_info.end())
    {
        return user_it->second;
    }
    auto inf = std::make_shared<user_info_t>();
    inf->name = username;
    return inf;
}

bool etcd_state_client_t::check_image_perm(const std::shared_ptr<user_info_t> & user_info, inode_t inode_num, bool write)
{
    if (user_info->type == user_type_t::ADMIN)
    {
        return true;
    }
    auto cache_it = user_info->perm_cache.find(inode_num);
    if (cache_it != user_info->perm_cache.end() &&
        cache_it->second.mod_revision == user_perm_cache_revision)
    {
        return write ? (cache_it->second.perm == user_perm_t::OWNER) : (cache_it->second.perm != user_perm_t::DENY);
    }
    auto inode_it = inode_config.find(inode_num);
    if (inode_it == inode_config.end())
    {
        return false;
    }
    // FIXME Implement cache reset after reworking etcd interaction to not keep everything in memory
    auto & perm_item = user_info->perm_cache[inode_num];
    perm_item.mod_revision = user_perm_cache_revision;
    perm_item.perm = (user_info->name == inode_it->second.owner || inode_it->second.owner_group != "" &&
        user_info->groups.find(inode_it->second.owner_group) != user_info->groups.end()
        ? user_perm_t::OWNER : (inode_it->second.reader_group != "" &&
            user_info->groups.find(inode_it->second.reader_group) != user_info->groups.end()
            ? user_perm_t::READER : user_perm_t::DENY));
    return write ? (perm_item.perm == user_perm_t::OWNER) : (perm_item.perm != user_perm_t::DENY);
}

void etcd_state_client_t::add_etcd_url(std::string etcd_address)
{
    if (etcd_address.size() > 0)
    {
        if (!local_ips.size())
        {
            // Fill local_ips
            for (auto & ip: getifaddr_list(std::vector<addr_mask_t>(), true))
                local_ips.insert(ip);
        }
        std::string etcd_api_path;
        bool ssl = false;
        if (etcd_address.substr(0, 8) == "https://")
        {
            ssl = true;
            etcd_address = etcd_address.substr(8);
        }
        else if (etcd_address.substr(0, 7) == "http://")
            etcd_address = etcd_address.substr(7);
        auto pos = etcd_address.find('/');
        if (pos != std::string::npos)
        {
            etcd_api_path = etcd_address.substr(pos);
            etcd_address = etcd_address.substr(0, pos);
        }
        else
            etcd_api_path = "/v3";
        pos = etcd_address.find(':');
        auto check_addr = (pos != std::string::npos ? etcd_address.substr(0, pos) : etcd_address);
        bool is_local = local_ips.find(check_addr) != local_ips.end();
        auto & to = (is_local ? etcd_local : etcd_addresses);
        check_addr = (ssl ? "https://" : "http://") + etcd_address + etcd_api_path;
        size_t i;
        for (i = 0; i < to.size(); i++)
        {
            if (to[i] == check_addr)
                break;
        }
        if (i >= to.size())
        {
            to.push_back(check_addr);
            // Check if it's a domain name
            sockaddr_storage ss;
            bool is_name = !is_local && !string_to_addr(etcd_address, true, 0, &ss);
            auto & to_addr = (is_local ? etcd_local_addr_urls : (is_name ? etcd_name_urls : etcd_nonlocal_addr_urls));
            to_addr.push_back((http_url_t){ .ssl = ssl, .addr = etcd_address, .hostname = etcd_address, .path = etcd_api_path });
        }
    }
}

void etcd_state_client_t::parse_config(const json11::Json & config)
{
    this->etcd_local.clear();
    this->etcd_addresses.clear();
    this->etcd_local_addr_urls.clear();
    this->etcd_nonlocal_addr_urls.clear();
    this->etcd_name_urls.clear();
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
    if (this->osd_num)
    {
        this->etcd_client_cert = config["osd_cert"].string_value();
        this->etcd_client_key = config["osd_pkey"].string_value();
    }
    else
    {
        this->etcd_client_cert = config["cert"].string_value();
        this->etcd_client_key = config["pkey"].string_value();
    }
    if (this->etcd_client_cert == "")
    {
        this->etcd_client_cert = config["etcd_client_cert"].string_value();
        this->etcd_client_key = config["etcd_client_key"].string_value();
    }
    this->etcd_ca = config["etcd_ca"].string_value();
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
        this->etcd_ws_keepalive_interval = 5;
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
    this->etcd_min_reload_interval = config["etcd_min_reload_interval"].uint64_value();
    if (this->etcd_min_reload_interval <= 0)
    {
        this->etcd_min_reload_interval = 50;
    }
}

void etcd_state_client_t::load_global_config(std::function<void(const std::string & error)> cb)
{
    json11::Json::object req = { { "success", json11::Json::array {
        json11::Json::object {
            { "request_range", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/config/global") },
            } }
        },
        json11::Json::object {
            { "request_range", json11::Json::object {
                { "key", base64_encode(etcd_prefix+"/config/pools") },
            } }
        },
    } } };
    etcd_txn(req, etcd_quick_timeout, max_etcd_attempts, 0, [this, cb](std::string err, json11::Json data)
    {
        if (err != "")
        {
            fprintf(stderr, "Error reading configuration from etcd: %s\n", err.c_str());
            cb(err);
            return;
        }
        json11::Json config_kv = data["responses"][0]["response_range"]["kvs"][0];
        json11::Json pools_kv = data["responses"][1]["response_range"]["kvs"][0];
        json11::Json::object global_config;
        if (!config_kv.is_null())
        {
            auto kv = parse_etcd_kv(config_kv);
            if (kv.value.is_object())
            {
                global_config = kv.value.object_items();
            }
        }
        global_block_size = global_config["block_size"].uint64_value();
        if (!global_block_size)
        {
            global_block_size = DEFAULT_BLOCK_SIZE;
        }
        global_bitmap_granularity = global_config["bitmap_granularity"].uint64_value();
        if (!global_bitmap_granularity)
        {
            global_bitmap_granularity = DEFAULT_BITMAP_GRANULARITY;
        }
        global_immediate_commit = parse_immediate_commit(global_config["immediate_commit"].string_value(), IMMEDIATE_ALL);
        if (!pools_kv.is_null())
        {
            auto kv = parse_etcd_kv(pools_kv);
            parse_state(kv);
        }
        on_load_config_hook(global_config);
        cb("");
    });
}

void etcd_state_client_t::load_pgs(std::function<void(const std::string &)> cb)
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
                { "key", base64_encode(etcd_prefix+"/pg/config") },
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
    etcd_txn_slow(req, [this, cb](std::string err, json11::Json data)
    {
        if (err != "")
        {
            // Retry indefinitely
            fprintf(stderr, "Error loading PGs from etcd: %s\n", err.c_str());
            cb(err);
            return;
        }
        if (!data["succeeded"].bool_value())
        {
            on_load_pgs_hook(false);
            return;
        }
        reset_pg_exists();
        etcd_watch_revision_config = etcd_watch_revision_osd = etcd_watch_revision_pg = data["header"]["revision"].uint64_value()+1;
        if (this->log_level > 3)
        {
            fprintf(stderr, "Loaded revision %ju of PG configuration\n", etcd_watch_revision_pg-1);
        }
        for (auto & res: data["responses"].array_items())
        {
            for (auto & kv_json: res["response_range"]["kvs"].array_items())
            {
                auto kv = parse_etcd_kv(kv_json);
                if (this->log_level > 3)
                {
                    fprintf(stderr, "Loaded key: %s -> %s\n", kv.key.c_str(), kv.value.dump().c_str());
                }
                parse_state(kv);
            }
        }
        clean_nonexistent_pgs();
        on_load_pgs_hook(true);
        cb("");
    });
}

void etcd_state_client_t::reset_pg_exists()
{
    for (auto & pool_item: pool_config)
    {
        for (auto & pg_item: pool_item.second.pg_config)
        {
            pg_item.second.state_exists = false;
            pg_item.second.history_exists = false;
        }
    }
    seen_peers.clear();
}

void etcd_state_client_t::clean_nonexistent_pgs()
{
    for (auto & pool_item: pool_config)
    {
        for (auto pg_it = pool_item.second.pg_config.begin(); pg_it != pool_item.second.pg_config.end(); )
        {
            auto & pg_cfg = pg_it->second;
            if (!pg_cfg.config_exists && !pg_cfg.state_exists && !pg_cfg.history_exists)
            {
                if (this->log_level > 3)
                {
                    fprintf(stderr, "PG %u/%u disappeared after reload, forgetting it\n", pool_item.first, pg_it->first);
                }
                pool_item.second.pg_config.erase(pg_it++);
            }
            else
            {
                if (!pg_cfg.state_exists)
                {
                    if (this->log_level > 3 && (pg_cfg.cur_primary || pg_cfg.cur_state))
                    {
                        fprintf(stderr, "PG %u/%u primary OSD disappeared after reload, forgetting it\n", pool_item.first, pg_it->first);
                    }
                    parse_state((etcd_kv_t){
                        .key = etcd_prefix+"/pg/state/"+std::to_string(pool_item.first)+"/"+std::to_string(pg_it->first),
                    });
                }
                if (!pg_cfg.history_exists)
                {
                    if (this->log_level > 3 && (pg_cfg.target_history.size() || pg_cfg.all_peers.size() || pg_cfg.epoch || pg_cfg.next_scrub))
                    {
                        fprintf(stderr, "PG %u/%u history disappeared after reload, forgetting it\n", pool_item.first, pg_it->first);
                    }
                    parse_state((etcd_kv_t){
                        .key = etcd_prefix+"/pg/history/"+std::to_string(pool_item.first)+"/"+std::to_string(pg_it->first),
                    });
                }
                pg_it++;
            }
        }
    }
    std::vector<osd_num_t> stale_peers;
    for (auto & peer_item: peer_states)
    {
        if (seen_peers.find(peer_item.first) == seen_peers.end())
            stale_peers.push_back(peer_item.first);
    }
    for (auto & stale_peer: stale_peers)
    {
        fprintf(stderr, "OSD %ju state disappeared after reload, forgetting it\n", stale_peer);
        parse_state((etcd_kv_t){
            .key = etcd_prefix+"/osd/state/"+std::to_string(stale_peer),
        });
    }
    seen_peers.clear();
}

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
            pool_config_t pc = {};
            // ID
            pool_id_t pool_id;
            char null_byte = 0;
            int scanned = sscanf(pool_item.first.c_str(), "%u%c", &pool_id, &null_byte);
            if (scanned != 1 || !pool_id || pool_id >= POOL_ID_MAX)
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
            pc.scheme = parse_scheme(pool_item.second["scheme"].string_value());
            if (!pc.scheme)
            {
                fprintf(stderr, "Pool %u has invalid coding scheme (one of \"xor\", \"replicated\", \"ec\" or \"jerasure\" required), skipping pool\n", pool_id);
                continue;
            }
            // PG Size
            pc.pg_size = pool_item.second["pg_size"].uint64_value();
            if (pc.pg_size < 1 ||
                pc.pg_size < 3 && (pc.scheme == POOL_SCHEME_XOR || pc.scheme == POOL_SCHEME_EC) ||
                // limit is 64 because osd_peering_pg.cpp uses a 64-bit mask for has_roles
                pc.pg_size > 64)
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
            if (pc.scheme == POOL_SCHEME_EC &&
                (pc.parity_chunks < 1 || pc.parity_chunks > pc.pg_size-2))
            {
                fprintf(stderr, "Pool %u has invalid parity_chunks (must be between 1 and pg_size-2), skipping pool\n", pool_id);
                continue;
            }
            // PG MinSize
            pc.pg_minsize = pool_item.second["pg_minsize"].uint64_value();
            if (pc.pg_minsize < 1 || pc.pg_minsize > pc.pg_size ||
                (pc.scheme == POOL_SCHEME_XOR || pc.scheme == POOL_SCHEME_EC) &&
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
            // Data Block Size
            uint64_t data_block_size = pool_item.second["block_size"].uint64_value();
            if (!data_block_size)
                data_block_size = global_block_size;
            if ((data_block_size & (data_block_size-1)) ||
                data_block_size < MIN_DATA_BLOCK_SIZE || data_block_size > MAX_DATA_BLOCK_SIZE ||
                pc.scheme != POOL_SCHEME_REPLICATED && data_block_size*pc.pg_size > UINT32_MAX)
            {
                fprintf(stderr, "Pool %u has invalid block_size (must be a power of two between %u and %u; must not exceed 2^32-1 when multiplied by pg_size), skipping pool\n",
                    pool_id, MIN_DATA_BLOCK_SIZE, MAX_DATA_BLOCK_SIZE);
                continue;
            }
            pc.data_block_size = data_block_size;
            // Bitmap Granularity
            pc.bitmap_granularity = pool_item.second["bitmap_granularity"].uint64_value();
            if (!pc.bitmap_granularity)
                pc.bitmap_granularity = global_bitmap_granularity;
            if (!pc.bitmap_granularity || pc.data_block_size % pc.bitmap_granularity)
            {
                fprintf(stderr, "Pool %u has invalid bitmap_granularity (must divide block_size), skipping pool\n", pool_id);
                continue;
            }
            // Scrub Interval
            pc.scrub_interval = parse_time(pool_item.second["scrub_interval"].string_value());
            if (!pc.scrub_interval)
                pc.scrub_interval = 0;
            // Mark pool as VitastorFS pool (disable per-inode stats and block volume creation)
            pc.used_for_app = pool_item.second["used_for_fs"].as_string();
            if (pc.used_for_app != "")
                pc.used_for_app = "fs:"+pc.used_for_app;
            else
                pc.used_for_app = pool_item.second["used_for_app"].as_string();
            // Create group permission
            pc.creator_group = pool_item.second["creator_group"].string_value();
            // Local Read Configuration
            std::string local_reads = pool_item.second["local_reads"].string_value();
            if (local_reads == "nearest")
                pc.local_reads = POOL_LOCAL_READ_NEAREST;
            else if (local_reads == "random")
                pc.local_reads = POOL_LOCAL_READ_RANDOM;
            else if (local_reads == "" || local_reads == "primary")
                pc.local_reads = POOL_LOCAL_READ_PRIMARY;
            else
            {
                pc.local_reads = POOL_LOCAL_READ_PRIMARY;
                fprintf(stderr, "Warning: Pool %u has invalid local_reads, using 'primary'\n", pool_id);
            }
            // Immediate Commit Mode
            pc.immediate_commit = pool_item.second["immediate_commit"].is_string()
                ? parse_immediate_commit(pool_item.second["immediate_commit"].string_value(), IMMEDIATE_ALL)
                : global_immediate_commit;
            // PG Stripe Size
            pc.pg_stripe_size = pool_item.second["pg_stripe_size"].uint64_value();
            uint64_t min_stripe_size = pc.data_block_size * (pc.scheme == POOL_SCHEME_REPLICATED ? 1 : (pc.pg_size-pc.parity_chunks));
            if (pc.pg_stripe_size < min_stripe_size)
                pc.pg_stripe_size = min_stripe_size;
            // Save
            auto & old_pc = this->pool_config[pool_id];
            pc.real_pg_count = old_pc.real_pg_count;
            pc.applied_pg_count = old_pc.applied_pg_count;
            pc.applied_pg_stripe_size = old_pc.applied_pg_stripe_size;
            pc.reshard_state = old_pc.reshard_state;
            std::swap(pc.pg_config, old_pc.pg_config);
            std::swap(this->pool_config[pool_id], pc);
            auto & parsed_cfg = this->pool_config[pool_id];
            parsed_cfg.exists = true;
            for (auto & pg_item: parsed_cfg.pg_config)
            {
                if (pg_item.second.target_set.size() != parsed_cfg.pg_size)
                {
                    fprintf(stderr, "Pool %u PG %u configuration is invalid: osd_set size %zu != pool pg_size %ju\n",
                        pool_id, pg_item.first, pg_item.second.target_set.size(), parsed_cfg.pg_size);
                    pg_item.second.pause = true;
                }
            }
        }
        if (on_change_pool_config_hook)
        {
            on_change_pool_config_hook();
        }
    }
    else if (key == etcd_prefix+"/pg/config" || key == etcd_prefix+"/config/pgs")
    {
        if (key == etcd_prefix+"/pg/config")
        {
            new_pg_config = !value.is_null();
        }
        else if (new_pg_config)
        {
            // Ignore old key if the new one is present
            return;
        }
        for (auto & pool_id_json: value["backfillfull_pools"].array_items())
        {
            auto pool_id = pool_id_json.uint64_value();
            auto pool_it = this->pool_config.find(pool_id);
            if (pool_it != this->pool_config.end())
            {
                pool_it->second.backfillfull |= 2;
            }
        }
        for (auto & pool_item: this->pool_config)
        {
            for (auto & pg_item: pool_item.second.pg_config)
            {
                pg_item.second.config_exists = false;
            }
            // 3 = was 1 and became 1, 0 = was 0 and became 0
            if (pool_item.second.backfillfull == 2 || pool_item.second.backfillfull == 1)
            {
                if (on_change_backfillfull_hook)
                    on_change_backfillfull_hook(pool_item.first);
            }
            pool_item.second.backfillfull = pool_item.second.backfillfull >> 1;
        }
        for (auto & pool_item: value["items"].object_items())
        {
            pool_id_t pool_id;
            char null_byte = 0;
            int scanned = sscanf(pool_item.first.c_str(), "%u%c", &pool_id, &null_byte);
            if (scanned != 1 || !pool_id || pool_id >= POOL_ID_MAX)
            {
                fprintf(stderr, "Pool ID %s is invalid in PG configuration (must be a number less than 0x%x), skipping pool\n", pool_item.first.c_str(), POOL_ID_MAX);
                continue;
            }
            for (auto & pg_item: pool_item.second.object_items())
            {
                pg_num_t pg_num = 0;
                int scanned = sscanf(pg_item.first.c_str(), "%u%c", &pg_num, &null_byte);
                if (scanned != 1 || !pg_num)
                {
                    fprintf(stderr, "Bad key in pool %u PG configuration: %s (must be a number), skipped\n", pool_id, pg_item.first.c_str());
                    continue;
                }
                auto & parsed_cfg = this->pool_config[pool_id].pg_config[pg_num];
                parsed_cfg.config_exists = true;
                parsed_cfg.pause = pg_item.second["pause"].bool_value();
                parsed_cfg.primary = pg_item.second["primary"].uint64_value();
                parsed_cfg.target_set.clear();
                for (auto & pg_osd: pg_item.second["osd_set"].array_items())
                {
                    parsed_cfg.target_set.push_back(pg_osd.uint64_value());
                }
                if (parsed_cfg.target_set.size() != pool_config[pool_id].pg_size)
                {
                    fprintf(stderr, "Pool %u PG %u configuration is invalid: osd_set size %zu != pool pg_size %ju\n",
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
                if (pg_it->second.config_exists && pg_it->first != ++n)
                {
                    fprintf(
                        stderr, "Invalid pool %u PG configuration: PG numbers don't cover whole 1..%zu range\n",
                        pool_item.second.id, pool_item.second.pg_config.size()
                    );
                    for (pg_it = pool_item.second.pg_config.begin(); pg_it != pool_item.second.pg_config.end(); pg_it++)
                    {
                        pg_it->second.config_exists = false;
                    }
                    n = 0;
                    break;
                }
            }
            pool_item.second.real_pg_count = n;
        }
        if (on_change_pg_config_hook)
        {
            on_change_pg_config_hook();
        }
    }
    else if (key.substr(0, etcd_prefix.length()+12) == etcd_prefix+"/pg/history/")
    {
        // <etcd_prefix>/pg/history/%d/%d
        pool_id_t pool_id = 0;
        pg_num_t pg_num = 0;
        char null_byte = 0;
        int scanned = sscanf(key.c_str() + etcd_prefix.length()+12, "%u/%u%c", &pool_id, &pg_num, &null_byte);
        if (scanned != 2 || !pool_id || pool_id >= POOL_ID_MAX || !pg_num)
        {
            fprintf(stderr, "Bad etcd key %s, ignoring\n", key.c_str());
        }
        else
        {
            auto & pg_cfg = this->pool_config[pool_id].pg_config[pg_num];
            pg_cfg.target_history.clear();
            pg_cfg.all_peers.clear();
            pg_cfg.history_exists = !value.is_null();
            // Refuse to start PG if any set of the <osd_sets> has no live OSDs
            for (auto & hist_item: value["osd_sets"].array_items())
            {
                std::vector<osd_num_t> history_set;
                for (auto & pg_osd: hist_item.array_items())
                {
                    osd_num_t pg_osd_num = pg_osd.uint64_value();
                    if (pg_osd_num != 0)
                    {
                        auto it = std::lower_bound(history_set.begin(), history_set.end(), pg_osd_num);
                        if (it == history_set.end() || *it != pg_osd_num)
                            history_set.insert(it, pg_osd_num);
                    }
                }
                auto it = std::lower_bound(pg_cfg.target_history.begin(), pg_cfg.target_history.end(), history_set);
                if (it == pg_cfg.target_history.end() || *it != history_set)
                    pg_cfg.target_history.insert(it, history_set);
            }
            // Include these additional OSDs when peering the PG
            for (auto pg_osd: value["all_peers"].array_items())
            {
                osd_num_t pg_osd_num = pg_osd.uint64_value();
                if (pg_osd_num != 0)
                {
                    auto it = std::lower_bound(pg_cfg.all_peers.begin(), pg_cfg.all_peers.end(), pg_osd_num);
                    if (it == pg_cfg.all_peers.end() || *it != pg_osd_num)
                        pg_cfg.all_peers.insert(it, pg_osd_num);
                }
            }
            // Read epoch
            pg_cfg.epoch = value["epoch"].uint64_value();
            // Next scrub timestamp (0 or empty = scrub is not needed)
            pg_cfg.next_scrub = value["next_scrub"].uint64_value();
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
        int scanned = sscanf(key.c_str() + etcd_prefix.length()+10, "%u/%u%c", &pool_id, &pg_num, &null_byte);
        if (scanned != 2 || !pool_id || pool_id >= POOL_ID_MAX || !pg_num)
        {
            fprintf(stderr, "Bad etcd key %s, ignoring\n", key.c_str());
        }
        else if (value.is_null())
        {
            auto & pg_cfg = this->pool_config[pool_id].pg_config[pg_num];
            auto prev_primary = pg_cfg.cur_primary;
            pg_cfg.state_exists = false;
            pg_cfg.cur_primary = 0;
            pg_cfg.cur_state = 0;
            if (on_change_pg_state_hook)
            {
                on_change_pg_state_hook(pool_id, pg_num, prev_primary);
            }
        }
        else
        {
            auto & pg_cfg = this->pool_config[pool_id].pg_config[pg_num];
            auto prev_primary = pg_cfg.cur_primary;
            pg_cfg.state_exists = true;
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
                }
            }
            if (!cur_primary || !value["state"].is_array() || !state ||
                (state & PG_OFFLINE) && state != PG_OFFLINE ||
                (state & PG_PEERING) && state != PG_PEERING ||
                (state & PG_INCOMPLETE) && state != PG_INCOMPLETE && state != (PG_INCOMPLETE|PG_HAS_INVALID))
            {
                fprintf(stderr, "Unexpected pool %u PG %u state in etcd: primary=%ju, state=%s\n", pool_id, pg_num, cur_primary, value["state"].dump().c_str());
            }
            pg_cfg.cur_primary = cur_primary;
            pg_cfg.cur_state = state;
            if (on_change_pg_state_hook)
            {
                on_change_pg_state_hook(pool_id, pg_num, prev_primary);
            }
        }
    }
    else if (key.substr(0, etcd_prefix.length()+11) == etcd_prefix+"/osd/state/")
    {
        // <etcd_prefix>/osd/state/%d
        osd_num_t peer_osd = 0;
        char null_byte = 0;
        int scanned = sscanf(key.c_str() + etcd_prefix.length()+11, "%ju%c", &peer_osd, &null_byte);
        if (scanned != 1 || !peer_osd)
        {
            fprintf(stderr, "Bad etcd key %s, ignoring\n", key.c_str());
        }
        else
        {
            if (value.is_object() && value["state"] == "up")
            {
                this->peer_states[peer_osd] = value;
                this->seen_peers.insert(peer_osd);
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
        int scanned = sscanf(key.c_str() + etcd_prefix.length()+14, "%ju/%ju%c", &pool_id, &inode_num, &null_byte);
        if (scanned != 2 || !pool_id || pool_id >= POOL_ID_MAX || !inode_num || (inode_num >> (64-POOL_ID_BITS)))
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
                if (this->inode_config.find(inode_num) != this->inode_config.end())
                {
                    user_perm_cache_revision = kv.mod_revision;
                }
                this->inode_config.erase(inode_num);
            }
            else
            {
                insert_inode_config(deserialize_inode_cfg(inode_num, kv.value, kv.mod_revision));
            }
        }
    }
    else if (key == etcd_prefix+"/config/node_placement")
    {
        // <etcd_prefix>/config/node_placement
        node_placement = value;
        if (on_change_node_placement_hook)
            on_change_node_placement_hook();
    }
    else if (key.substr(0, etcd_prefix.length()+13) == etcd_prefix+"/config/user/")
    {
        // <etcd_prefix>/config/user/<username>
        auto name = key.substr(etcd_prefix.length()+13);
        auto & inf = user_info[name];
        if (!value.is_object())
        {
            if (inf)
            {
                inf->type = user_type_t::CLIENT;
                inf->groups.clear();
                inf->perm_cache.clear();
            }
            user_info.erase(name);
        }
        else
        {
            if (!inf)
            {
                inf = std::make_shared<user_info_t>();
                inf->name = name;
            }
            inf->type = value["type"] == "admin" ? user_type_t::ADMIN : user_type_t::CLIENT;
            inf->groups.clear();
            for (auto & group: value["groups"].array_items())
            {
                if (group.string_value() != "")
                    inf->groups.insert(group.string_value());
            }
            inf->perm_cache.clear();
        }
    }
}

uint32_t etcd_state_client_t::parse_immediate_commit(const std::string & immediate_commit_str, uint32_t default_value)
{
    return (immediate_commit_str == "all" ? IMMEDIATE_ALL :
        (immediate_commit_str == "small" ? IMMEDIATE_SMALL :
        (immediate_commit_str == "none" ? IMMEDIATE_NONE : default_value)));
}

uint32_t etcd_state_client_t::parse_scheme(const std::string & scheme)
{
    if (scheme == "replicated")
        return POOL_SCHEME_REPLICATED;
    else if (scheme == "xor")
        return POOL_SCHEME_XOR;
    else if (scheme == "ec" || scheme == "jerasure")
        return POOL_SCHEME_EC;
    return 0;
}

void etcd_state_client_t::insert_inode_config(const inode_config_t & cfg)
{
    auto & cfg_ref = this->inode_config[cfg.num];
    if (cfg_ref.mod_revision != cfg.mod_revision)
    {
        user_perm_cache_revision = cfg.mod_revision;
    }
    cfg_ref = cfg;
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
    if (!cfg->enc_key.empty())
    {
        new_cfg["enc_key"] = cfg->enc_key;
    }
    if (cfg->readonly)
    {
        new_cfg["readonly"] = true;
    }
    if (cfg->deleted)
    {
        new_cfg["deleted"] = true;
    }
    if (!cfg->owner.empty())
    {
        new_cfg["owner"] = cfg->owner;
    }
    if (!cfg->owner_group.empty())
    {
        new_cfg["owner_group"] = cfg->owner_group;
    }
    if (!cfg->reader_group.empty())
    {
        new_cfg["reader_group"] = cfg->reader_group;
    }
    if (cfg->meta.is_object())
    {
        new_cfg["meta"] = cfg->meta;
    }
    return new_cfg;
}

inode_config_t etcd_state_client_t::deserialize_inode_cfg(uint64_t inode_num, json11::Json value, uint64_t mod_revision)
{
    inode_t parent_inode_num = value["parent_id"].uint64_value();
    if (parent_inode_num && !INODE_POOL(parent_inode_num))
    {
        uint64_t parent_pool_id = value["parent_pool"].uint64_value();
        if (!parent_pool_id)
            parent_inode_num = INODE_WITH_POOL(INODE_POOL(inode_num), parent_inode_num);
        else if (parent_pool_id >= POOL_ID_MAX)
        {
            fprintf(
                stderr, "Inode %u/%ju parent_pool value is invalid, ignoring parent setting\n",
                INODE_POOL(inode_num), INODE_NO_POOL(inode_num)
            );
            parent_inode_num = 0;
        }
        else
            parent_inode_num |= parent_pool_id << (64-POOL_ID_BITS);
    }
    std::string enc_key;
    if (!value["enc_key"].is_null())
    {
        enc_key = value["enc_key"].string_value();
        if (enc_key.substr(0, strlen(VAULT_KEY_PREFIX)) != VAULT_KEY_PREFIX &&
            (enc_key.size() != 2*AES_256_XTS_KEY_SIZE || !ishexstr(enc_key)))
        {
            enc_key = "";
            fprintf(stderr, "Inode %u/%ju has invalid enc_key, should be %u bit hex string or Vault key reference\n",
                INODE_POOL(inode_num), INODE_NO_POOL(inode_num), AES_256_XTS_KEY_SIZE);
        }
    }
    return (inode_config_t){
        .num = inode_num,
        .name = value["name"].string_value(),
        .size = value["size"].uint64_value(),
        .parent_id = parent_inode_num,
        .readonly = value["readonly"].bool_value(),
        .deleted = value["deleted"].bool_value(),
        .enc_key = std::move(enc_key),
        .owner = value["owner"].string_value(),
        .owner_group = value["owner_group"].string_value(),
        .reader_group = value["reader_group"].string_value(),
        .meta = value["meta"],
        .mod_revision = mod_revision,
    };
}

int etcd_state_client_t::address_count()
{
    return etcd_addresses.size() + etcd_local.size();
}
