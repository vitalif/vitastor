// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include "base64.h"
#include "pg_states.h"

// Print cluster status:
// etcd, mon, osd states
// raw/used space, object states, pool states, pg states
// client io, recovery io, rebalance io
struct status_printer_t
{
    cli_tool_t *parent;

    int state = 0;
    json11::Json::array mon_members, osd_stats;
    json11::Json agg_stats;
    std::map<pool_id_t, json11::Json::object> pool_stats;
    json11::Json::array etcd_states;

    bool is_done()
    {
        return state == 100;
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        else if (state == 2)
            goto resume_2;
        // etcd states
        {
            auto addrs = parent->cli->st_cli.get_addresses();
            etcd_states.resize(addrs.size());
            for (int i = 0; i < etcd_states.size(); i++)
            {
                parent->waiting++;
                parent->cli->st_cli.etcd_call_oneshot(
                    addrs[i], "/maintenance/status", json11::Json::object(),
                    parent->cli->st_cli.etcd_quick_timeout, [this, i](std::string err, json11::Json res)
                    {
                        parent->waiting--;
                        etcd_states[i] = err != "" ? json11::Json::object{ { "error", err } } : res;
                        parent->ringloop->wakeup();
                    }
                );
            }
        }
        state = 1;
resume_1:
        if (parent->waiting > 0)
            return;
        // Monitors, OSD states
        parent->etcd_txn(json11::Json::object {
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/mon/") },
                        { "range_end", base64_encode(parent->cli->st_cli.etcd_prefix+"/mon0") },
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
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/stats") },
                    } },
                },
            } },
        });
        state = 2;
resume_2:
        if (parent->waiting > 0)
            return;
        mon_members = parent->etcd_result["responses"][0]["response_range"]["kvs"].array_items();
        osd_stats = parent->etcd_result["responses"][1]["response_range"]["kvs"].array_items();
        if (parent->etcd_result["responses"][2]["response_range"]["kvs"].array_items().size() > 0)
        {
            agg_stats = parent->cli->st_cli.parse_etcd_kv(parent->etcd_result["responses"][2]["response_range"]["kvs"][0]).value;
        }
        int etcd_alive = 0;
        uint64_t etcd_db_size = 0;
        std::string etcd_detail;
        for (int i = 0; i < etcd_states.size(); i++)
        {
            if (etcd_states[i]["error"].is_null())
            {
                etcd_alive++;
                etcd_db_size = etcd_states[i]["dbSizeInUse"].uint64_value();
            }
        }
        int mon_count = 0;
        std::string mon_master;
        for (int i = 0; i < mon_members.size(); i++)
        {
            auto kv = parent->cli->st_cli.parse_etcd_kv(mon_members[i]);
            kv.key = kv.key.substr(parent->cli->st_cli.etcd_prefix.size());
            if (kv.key.substr(0, 12) == "/mon/member/")
                mon_count++;
            else if (kv.key == "/mon/master")
            {
                if (kv.value["hostname"].is_string())
                    mon_master = kv.value["hostname"].string_value();
                else
                    mon_master = kv.value["ip"][0].string_value();
            }
        }
        int osd_count = 0, osd_up = 0;
        uint64_t total_raw = 0, free_raw = 0, free_down_raw = 0, down_raw = 0;
        for (int i = 0; i < osd_stats.size(); i++)
        {
            auto kv = parent->cli->st_cli.parse_etcd_kv(osd_stats[i]);
            osd_num_t stat_osd_num = 0;
            char null_byte = 0;
            sscanf(kv.key.c_str() + parent->cli->st_cli.etcd_prefix.size(), "/osd/stats/%lu%c", &stat_osd_num, &null_byte);
            if (!stat_osd_num || null_byte != 0)
            {
                fprintf(stderr, "Invalid key in etcd: %s\n", kv.key.c_str());
                continue;
            }
            osd_count++;
            total_raw += kv.value["size"].uint64_value();
            free_raw += kv.value["free"].uint64_value();
            auto peer_it = parent->cli->st_cli.peer_states.find(stat_osd_num);
            if (peer_it != parent->cli->st_cli.peer_states.end())
            {
                osd_up++;
            }
            else
            {
                down_raw += kv.value["size"].uint64_value();
                free_down_raw += kv.value["size"].uint64_value();
            }
        }
        int pool_count = 0, pools_active = 0;
        std::map<std::string, int> pgs_by_state;
        std::string pgs_by_state_str;
        for (auto & pool_pair: parent->cli->st_cli.pool_config)
        {
            auto & pool_cfg = pool_pair.second;
            bool active = true;
            if (pool_cfg.pg_config.size() != pool_cfg.pg_count)
            {
                active = false;
                pgs_by_state["offline"] += pool_cfg.pg_count-pool_cfg.pg_config.size();
            }
            pool_count++;
            for (auto pg_it = pool_cfg.pg_config.begin(); pg_it != pool_cfg.pg_config.end(); pg_it++)
            {
                if (!(pg_it->second.cur_state & PG_ACTIVE))
                {
                    active = false;
                }
                std::string pg_state_str;
                for (int i = 0; i < pg_state_bit_count; i++)
                {
                    if (pg_it->second.cur_state & pg_state_bits[i])
                    {
                        pg_state_str += "+";
                        pg_state_str += pg_state_names[i];
                    }
                }
                if (pg_state_str.size())
                    pgs_by_state[pg_state_str.substr(1)]++;
                else
                    pgs_by_state["offline"]++;
            }
            if (active)
            {
                pools_active++;
            }
        }
        for (auto & kv: pgs_by_state)
        {
            if (pgs_by_state_str.size())
            {
                pgs_by_state_str += "\n           ";
            }
            pgs_by_state_str += std::to_string(kv.second)+" "+kv.first;
        }
        uint64_t object_size = parent->cli->get_bs_block_size();
        std::string more_states;
        uint64_t obj_n;
        obj_n = agg_stats["object_counts"]["misplaced"].uint64_value();
        if (obj_n > 0)
            more_states += ", "+format_size(obj_n*object_size)+" misplaced";
        obj_n = agg_stats["object_counts"]["degraded"].uint64_value();
        if (obj_n > 0)
            more_states += ", "+format_size(obj_n*object_size)+" degraded";
        obj_n = agg_stats["object_counts"]["incomplete"].uint64_value();
        if (obj_n > 0)
            more_states += ", "+format_size(obj_n*object_size)+" incomplete";
        std::string recovery_io;
        {
            uint64_t deg_bps = agg_stats["recovery_stats"]["degraded"]["bps"].uint64_value();
            uint64_t deg_iops = agg_stats["recovery_stats"]["degraded"]["iops"].uint64_value();
            uint64_t misp_bps = agg_stats["recovery_stats"]["misplaced"]["bps"].uint64_value();
            uint64_t misp_iops = agg_stats["recovery_stats"]["misplaced"]["iops"].uint64_value();
            if (deg_iops > 0 || deg_bps > 0)
                recovery_io += "    recovery:  "+format_size(deg_bps)+"/s, "+format_size(deg_iops, true)+" op/s\n";
            if (misp_iops > 0 || misp_bps > 0)
                recovery_io += "    rebalance: "+format_size(misp_bps)+"/s, "+format_size(misp_iops, true)+" op/s\n";
        }
        if (parent->json_output)
        {
            // JSON output
            printf("%s\n", json11::Json(json11::Json::object {
                { "etcd_alive", etcd_alive },
                { "etcd_count", (uint64_t)etcd_states.size() },
                { "etcd_db_size", etcd_db_size },
                { "mon_count", mon_count },
                { "mon_master", mon_master },
                { "osd_up", osd_up },
                { "osd_count", osd_count },
                { "total_raw", total_raw },
                { "free_raw", free_raw },
                { "down_raw", down_raw },
                { "free_down_raw", free_down_raw },
                { "clean_data", agg_stats["object_counts"]["clean"].uint64_value() * object_size },
                { "misplaced_data", agg_stats["object_counts"]["misplaced"].uint64_value() * object_size },
                { "degraded_data", agg_stats["object_counts"]["degraded"].uint64_value() * object_size },
                { "incomplete_data", agg_stats["object_counts"]["incomplete"].uint64_value() * object_size },
                { "pool_count", pool_count },
                { "active_pool_count", pools_active },
                { "pg_states", pgs_by_state },
                { "op_stats", agg_stats["op_stats"] },
                { "recovery_stats", agg_stats["recovery_stats"] },
                { "object_counts", agg_stats["object_counts"] },
            }).dump().c_str());
            state = 100;
            return;
        }
        printf(
            "  cluster:\n"
            "    etcd: %d / %ld up, %s database size\n"
            "    mon:  %d up%s\n"
            "    osd:  %d / %d up\n"
            "  \n"
            "  data:\n"
            "    raw:   %s used, %s / %s available%s\n"
            "    state: %s clean%s\n"
            "    pools: %d / %d active\n"
            "    pgs:   %s\n"
            "  \n"
            "  io:\n"
            "    client:%s %s/s rd, %s op/s rd, %s/s wr, %s op/s wr\n"
            "%s",
            etcd_alive, etcd_states.size(), format_size(etcd_db_size).c_str(),
            mon_count, mon_master == "" ? "" : (", master "+mon_master).c_str(),
            osd_up, osd_count,
            format_size(total_raw-free_raw).c_str(),
            format_size(free_raw-free_down_raw).c_str(),
            format_size(total_raw-down_raw).c_str(),
            (down_raw > 0 ? (", "+format_size(down_raw)+" down").c_str() : ""),
            format_size(agg_stats["object_counts"]["clean"].uint64_value() * object_size).c_str(), more_states.c_str(),
            pools_active, pool_count,
            pgs_by_state_str.c_str(),
            recovery_io.size() > 0 ? "   " : "",
            format_size(agg_stats["op_stats"]["primary_read"]["bps"].uint64_value()).c_str(),
            format_size(agg_stats["op_stats"]["primary_read"]["iops"].uint64_value(), true).c_str(),
            format_size(agg_stats["op_stats"]["primary_write"]["bps"].uint64_value()).c_str(),
            format_size(agg_stats["op_stats"]["primary_write"]["iops"].uint64_value(), true).c_str(),
            recovery_io.c_str()
        );
        state = 100;
    }
};

std::function<bool(void)> cli_tool_t::start_status(json11::Json cfg)
{
    json11::Json::array cmd = cfg["command"].array_items();
    auto printer = new status_printer_t();
    printer->parent = this;
    return [printer]()
    {
        printer->loop();
        if (printer->is_done())
        {
            delete printer;
            return true;
        }
        return false;
    };
}
