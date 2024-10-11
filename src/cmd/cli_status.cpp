// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include "str_util.h"
#include "json_util.h"
#include "pg_states.h"
#include "http_client.h"

static const char *obj_states[] = { "clean", "misplaced", "degraded", "incomplete" };

// Print cluster status:
// etcd, mon, osd states
// raw/used space, object states, pool states, pg states
// client io, recovery io, rebalance io, scrub io
struct status_printer_t
{
    cli_tool_t *parent;

    int state = 0;
    json11::Json::array mon_members;
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
        if (parent->etcd_err.err)
        {
            fprintf(stderr, "%s\n", parent->etcd_err.text.c_str());
            state = 100;
            return;
        }
        mon_members = parent->etcd_result["responses"][0]["response_range"]["kvs"].array_items();
        auto osd_stats = parent->etcd_result["responses"][1]["response_range"]["kvs"];
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
                etcd_db_size = etcd_states[i]["dbSize"].uint64_value();
            }
        }
        int mon_count = 0;
        int osds_full = 0, osds_nearfull = 0;
        double osd_nearfull_ratio = parent->cli->config["osd_nearfull_ratio"].number_value();
        if (!osd_nearfull_ratio)
        {
            osd_nearfull_ratio = 0.95;
        }
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
        parent->iterate_kvs_1(osd_stats, "/osd/stats/", [&](uint64_t stat_osd_num, json11::Json value)
        {
            osd_count++;
            auto osd_size = value["size"].uint64_value();
            auto osd_free = value["free"].uint64_value();
            total_raw += osd_size;
            free_raw += osd_free;
            if (!osd_free)
            {
                osds_full++;
            }
            else if (osd_free < (uint64_t)(osd_size*(1-osd_nearfull_ratio)))
            {
                osds_nearfull++;
            }
            auto peer_it = parent->cli->st_cli.peer_states.find(stat_osd_num);
            if (peer_it != parent->cli->st_cli.peer_states.end())
            {
                osd_up++;
            }
            else
            {
                down_raw += value["size"].uint64_value();
                free_down_raw += value["free"].uint64_value();
            }
        });
        int pool_count = 0, pools_active = 0;
        std::map<std::string, int> pgs_by_state;
        std::string pgs_by_state_str;
        for (auto & pool_pair: parent->cli->st_cli.pool_config)
        {
            auto & pool_cfg = pool_pair.second;
            bool active = pool_cfg.real_pg_count > 0;
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
        bool readonly = json_is_true(parent->cli->config["readonly"]);
        bool no_recovery = json_is_true(parent->cli->config["no_recovery"]);
        bool no_rebalance = json_is_true(parent->cli->config["no_rebalance"]);
        bool no_scrub = json_is_true(parent->cli->config["no_scrub"]);
        if (parent->json_output)
        {
            // JSON output
            auto json_status = json11::Json::object {
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
                { "readonly", readonly },
                { "no_recovery", no_recovery },
                { "no_rebalance", no_rebalance },
                { "no_scrub", no_scrub },
                { "pool_count", pool_count },
                { "active_pool_count", pools_active },
                { "pg_states", pgs_by_state },
                { "op_stats", agg_stats["op_stats"] },
                { "recovery_stats", agg_stats["recovery_stats"] },
                { "object_counts", agg_stats["object_counts"] },
            };
            for (int i = 0; i < sizeof(obj_states)/sizeof(obj_states[0]); i++)
            {
                std::string str(obj_states[i]);
                uint64_t obj_n = agg_stats["object_bytes"][str].uint64_value();
                if (!obj_n)
                    obj_n = agg_stats["object_counts"][str].uint64_value() * parent->cli->st_cli.global_block_size;
                json_status[str+"_data"] = obj_n;
            }
            printf("%s\n", json11::Json(json_status).dump().c_str());
            state = 100;
            return;
        }
        std::string more_states;
        for (int i = 0; i < sizeof(obj_states)/sizeof(obj_states[0]); i++)
        {
            std::string str(obj_states[i]);
            uint64_t obj_n = agg_stats["object_bytes"][str].uint64_value();
            if (!obj_n)
                obj_n = agg_stats["object_counts"][str].uint64_value() * parent->cli->st_cli.global_block_size;
            if (!i || obj_n > 0)
                more_states += format_size(obj_n)+" "+str+", ";
        }
        more_states.resize(more_states.size()-2);
        std::string recovery_io;
        int io_indent = 0;
        {
            uint64_t deg_bps = agg_stats["recovery_stats"]["degraded"]["bps"].uint64_value();
            uint64_t deg_iops = agg_stats["recovery_stats"]["degraded"]["iops"].uint64_value();
            uint64_t misp_bps = agg_stats["recovery_stats"]["misplaced"]["bps"].uint64_value();
            uint64_t misp_iops = agg_stats["recovery_stats"]["misplaced"]["iops"].uint64_value();
            uint64_t scrub_bps = agg_stats["op_stats"]["scrub"]["bps"].uint64_value();
            uint64_t scrub_iops = agg_stats["op_stats"]["scrub"]["iops"].uint64_value();
            if (misp_iops > 0 || misp_bps > 0 || no_rebalance)
                io_indent = 3;
            else if (deg_iops > 0 || deg_bps > 0 || no_recovery)
                io_indent = 2;
            if (deg_iops > 0 || deg_bps > 0)
            {
                recovery_io += "    recovery: "+str_repeat(" ", io_indent-2)+std::string(no_recovery ? "disabled, " : "")+
                    format_size(deg_bps)+"/s, "+format_size(deg_iops, true)+" op/s\n";
            }
            else if (no_recovery)
                recovery_io += "    recovery: disabled\n";
            if (misp_iops > 0 || misp_bps > 0)
            {
                recovery_io += "    rebalance: "+std::string(no_rebalance ? "disabled, " : "")+
                    format_size(misp_bps)+"/s, "+format_size(misp_iops, true)+" op/s\n";
            }
            else if (no_rebalance)
                recovery_io += "    rebalance: disabled\n";
            if (scrub_iops > 0 || scrub_bps > 0)
            {
                recovery_io += "    scrub: "+str_repeat(" ", io_indent+1)+std::string(no_scrub ? "disabled, " : "")+
                    format_size(scrub_bps)+"/s, "+format_size(scrub_iops, true)+" op/s\n";
            }
            else if (no_scrub)
                recovery_io += "    scrub: "+str_repeat(" ", io_indent+1)+"disabled\n";
        }
        std::string warning_str;
        if (osds_full)
        {
            warning_str += "    "+std::to_string(osds_full)+
                (osds_full > 1 ? " osds are full\n" : " osd is full\n");
        }
        if (osds_nearfull)
        {
            warning_str += "    "+std::to_string(osds_nearfull)+
                (osds_nearfull > 1 ? " osds are almost full\n" : " osd is almost full\n");
        }
        if (warning_str != "")
        {
            warning_str = "\n  warning:\n"+warning_str;
        }
        printf(
            "  cluster:\n"
            "    etcd: %d / %zd up, %s database size\n"
            "    mon:  %d up%s\n"
            "    osd:  %d / %d up\n"
            "%s"
            "  \n"
            "  data:\n"
            "    raw:   %s used, %s / %s available%s\n"
            "    state: %s\n"
            "    pools: %d / %d active\n"
            "    pgs:   %s\n"
            "  \n"
            "  io%s:\n"
            "    client:%s %s/s rd, %s op/s rd, %s/s wr, %s op/s wr\n"
            "%s",
            etcd_alive, etcd_states.size(), format_size(etcd_db_size).c_str(),
            mon_count, mon_master == "" ? "" : (", master "+mon_master).c_str(),
            osd_up, osd_count, warning_str.c_str(),
            format_size(total_raw-free_raw).c_str(),
            format_size(free_raw-free_down_raw).c_str(),
            format_size(total_raw-down_raw).c_str(),
            (down_raw > 0 ? (", "+format_size(down_raw)+" down").c_str() : ""),
            more_states.c_str(),
            pools_active, pool_count,
            pgs_by_state_str.c_str(),
            readonly ? " (read-only mode)" : "",
            str_repeat(" ", io_indent).c_str(),
            format_size(agg_stats["op_stats"]["primary_read"]["bps"].uint64_value()).c_str(),
            format_size(agg_stats["op_stats"]["primary_read"]["iops"].uint64_value(), true).c_str(),
            format_size(agg_stats["op_stats"]["primary_write"]["bps"].uint64_value()).c_str(),
            format_size(agg_stats["op_stats"]["primary_write"]["iops"].uint64_value(), true).c_str(),
            recovery_io.c_str()
        );
        state = 100;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_status(json11::Json cfg)
{
    auto printer = new status_printer_t();
    printer->parent = this;
    return [printer](cli_result_t & result)
    {
        printer->loop();
        if (printer->is_done())
        {
            result = { .err = 0 };
            delete printer;
            return true;
        }
        return false;
    };
}
