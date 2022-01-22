// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include "base64.h"

// List pools with space statistics
struct pool_lister_t
{
    cli_tool_t *parent;

    int state = 0;
    json11::Json space_info;
    std::map<pool_id_t, json11::Json::object> pool_stats;

    bool is_done()
    {
        return state == 100;
    }

    void get_stats()
    {
        if (state == 1)
            goto resume_1;
        // Space statistics - pool/stats/<pool>
        parent->waiting++;
        parent->cli->st_cli.etcd_txn(json11::Json::object {
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/pool/stats/"
                        ) },
                        { "range_end", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/pool/stats0"
                        ) },
                    } },
                },
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/osd/stats/"
                        ) },
                        { "range_end", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/osd/stats0"
                        ) },
                    } },
                },
            } },
        }, parent->cli->st_cli.etcd_slow_timeout, [this](std::string err, json11::Json res)
        {
            parent->waiting--;
            if (err != "")
            {
                fprintf(stderr, "Error reading from etcd: %s\n", err.c_str());
                exit(1);
            }
            space_info = res;
            parent->ringloop->wakeup();
        });
        state = 1;
resume_1:
        if (parent->waiting > 0)
            return;
        std::map<pool_id_t, uint64_t> osd_free;
        for (auto & kv_item: space_info["responses"][0]["response_range"]["kvs"].array_items())
        {
            auto kv = parent->cli->st_cli.parse_etcd_kv(kv_item);
            // pool ID
            pool_id_t pool_id;
            char null_byte = 0;
            sscanf(kv.key.substr(parent->cli->st_cli.etcd_prefix.length()).c_str(), "/pool/stats/%u%c", &pool_id, &null_byte);
            if (!pool_id || pool_id >= POOL_ID_MAX || null_byte != 0)
            {
                fprintf(stderr, "Invalid key in etcd: %s\n", kv.key.c_str());
                continue;
            }
            // pool/stats/<N>
            pool_stats[pool_id] = kv.value.object_items();
        }
        for (auto & kv_item: space_info["responses"][1]["response_range"]["kvs"].array_items())
        {
            auto kv = parent->cli->st_cli.parse_etcd_kv(kv_item);
            // osd ID
            osd_num_t osd_num;
            char null_byte = 0;
            sscanf(kv.key.substr(parent->cli->st_cli.etcd_prefix.length()).c_str(), "/osd/stats/%lu%c", &osd_num, &null_byte);
            if (!osd_num || osd_num >= POOL_ID_MAX || null_byte != 0)
            {
                fprintf(stderr, "Invalid key in etcd: %s\n", kv.key.c_str());
                continue;
            }
            // osd/stats/<N>::free
            osd_free[osd_num] = kv.value["free"].uint64_value();
        }
        // Calculate max_avail for each pool
        for (auto & pp: parent->cli->st_cli.pool_config)
        {
            auto & pool_cfg = pp.second;
            uint64_t pool_avail = UINT64_MAX;
            std::map<osd_num_t, uint64_t> pg_per_osd;
            for (auto & pgp: pool_cfg.pg_config)
            {
                for (auto pg_osd: pgp.second.target_set)
                {
                    if (pg_osd != 0)
                    {
                        pg_per_osd[pg_osd]++;
                    }
                }
            }
            for (auto pg_per_pair: pg_per_osd)
            {
                uint64_t pg_free = osd_free[pg_per_pair.first] * pool_cfg.pg_count / pg_per_pair.second;
                if (pool_avail > pg_free)
                {
                    pool_avail = pg_free;
                }
            }
            if (pool_cfg.scheme != POOL_SCHEME_REPLICATED)
            {
                pool_avail = pool_avail * (pool_cfg.pg_size - pool_cfg.parity_chunks) / pool_stats[pool_cfg.id]["pg_real_size"].uint64_value();
            }
            pool_stats[pool_cfg.id] = json11::Json::object {
                { "name", pool_cfg.name },
                { "pg_count", pool_cfg.pg_count },
                { "scheme", pool_cfg.scheme == POOL_SCHEME_REPLICATED ? "replicated" : "jerasure" },
                { "scheme_name", pool_cfg.scheme == POOL_SCHEME_REPLICATED
                    ? std::to_string(pool_cfg.pg_size)+"/"+std::to_string(pool_cfg.pg_minsize)
                    : "EC "+std::to_string(pool_cfg.pg_size-pool_cfg.parity_chunks)+"+"+std::to_string(pool_cfg.parity_chunks) },
                { "used_raw", (uint64_t)(pool_stats[pool_cfg.id]["used_raw_tb"].number_value() * (1l<<40)) },
                { "total_raw", (uint64_t)(pool_stats[pool_cfg.id]["total_raw_tb"].number_value() * (1l<<40)) },
                { "max_available", pool_avail },
                { "raw_to_usable", pool_stats[pool_cfg.id]["raw_to_usable"].number_value() },
                { "space_efficiency", pool_stats[pool_cfg.id]["space_efficiency"].number_value() },
                { "pg_real_size", pool_stats[pool_cfg.id]["pg_real_size"].uint64_value() },
                { "failure_domain", pool_cfg.failure_domain },
            };
        }
    }

    json11::Json::array to_list()
    {
        json11::Json::array list;
        for (auto & kv: pool_stats)
        {
            list.push_back(kv.second);
        }
        return list;
    }

    void loop()
    {
        get_stats();
        if (parent->waiting > 0)
            return;
        if (parent->json_output)
        {
            // JSON output
            printf("%s\n", json11::Json(to_list()).dump().c_str());
            state = 100;
            return;
        }
        // Table output: name, scheme_name, pg_count, total, used, max_avail, used%, efficiency
        json11::Json::array cols;
        cols.push_back(json11::Json::object{
            { "key", "name" },
            { "title", "NAME" },
        });
        cols.push_back(json11::Json::object{
            { "key", "scheme_name" },
            { "title", "SCHEME" },
        });
        cols.push_back(json11::Json::object{
            { "key", "pg_count" },
            { "title", "PGS" },
        });
        cols.push_back(json11::Json::object{
            { "key", "total_fmt" },
            { "title", "TOTAL" },
        });
        cols.push_back(json11::Json::object{
            { "key", "used_fmt" },
            { "title", "USED" },
        });
        cols.push_back(json11::Json::object{
            { "key", "max_avail_fmt" },
            { "title", "AVAILABLE" },
        });
        cols.push_back(json11::Json::object{
            { "key", "used_pct" },
            { "title", "USED%" },
        });
        cols.push_back(json11::Json::object{
            { "key", "eff_fmt" },
            { "title", "EFFICIENCY" },
        });
        json11::Json::array list;
        for (auto & kv: pool_stats)
        {
            kv.second["total_fmt"] = format_size(kv.second["total_raw"].uint64_value() / kv.second["raw_to_usable"].number_value());
            kv.second["used_fmt"] = format_size(kv.second["used_raw"].uint64_value() / kv.second["raw_to_usable"].number_value());
            kv.second["max_avail_fmt"] = format_size(kv.second["max_available"].uint64_value());
            kv.second["used_pct"] = format_q(100 - 100*kv.second["max_available"].uint64_value() *
                kv.second["raw_to_usable"].number_value() / kv.second["total_raw"].uint64_value())+"%";
            kv.second["eff_fmt"] = format_q(kv.second["space_efficiency"].number_value()*100)+"%";
        }
        printf("%s", print_table(to_list(), cols, parent->color).c_str());
        state = 100;
    }
};

std::function<bool(void)> cli_tool_t::start_df(json11::Json cfg)
{
    json11::Json::array cmd = cfg["command"].array_items();
    auto lister = new pool_lister_t();
    lister->parent = this;
    return [lister]()
    {
        lister->loop();
        if (lister->is_done())
        {
            delete lister;
            return true;
        }
        return false;
    };
}
