// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <ctype.h>
#include "cli.h"
#include "cluster_client.h"
#include "str_util.h"

#include <algorithm>

// Delete OSD metadata from etcd
struct rm_osd_t
{
    cli_tool_t *parent;

    bool dry_run, force_warning, force_dataloss;
    std::vector<uint64_t> osd_ids;

    int state = 0;
    cli_result_t result;

    std::set<uint64_t> to_remove;
    json11::Json::array pool_effects;
    bool is_warning, is_dataloss;

    bool is_done()
    {
        return state == 100;
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        if (!osd_ids.size())
        {
            result = (cli_result_t){ .err = EINVAL, .text = "OSD numbers are not specified" };
            state = 100;
            return;
        }
        for (auto osd_id: osd_ids)
        {
            if (!osd_id)
            {
                result = (cli_result_t){ .err = EINVAL, .text = "OSD number can't be zero" };
                state = 100;
                return;
            }
            to_remove.insert(osd_id);
        }
        // Check if OSDs are still used in data distribution
        is_warning = is_dataloss = false;
        for (auto & pp: parent->cli->st_cli.pool_config)
        {
            // Will OSD deletion make pool incomplete / down / degraded?
            bool pool_incomplete = false, pool_down = false, pool_degraded = false;
            bool hist_incomplete = false, hist_degraded = false;
            auto & pool_cfg = pp.second;
            uint64_t pg_data_size = (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks);
            for (auto & pgp: pool_cfg.pg_config)
            {
                auto & pg_cfg = pgp.second;
                int pg_cursize = 0, pg_rm = 0;
                for (auto pg_osd: pg_cfg.target_set)
                {
                    if (pg_osd != 0)
                    {
                        pg_cursize++;
                        if (to_remove.find(pg_osd) != to_remove.end())
                            pg_rm++;
                    }
                }
                for (auto & hist_item: pg_cfg.target_history)
                {
                    int hist_size = 0, hist_rm = 0;
                    for (auto & old_osd: hist_item)
                    {
                        if (old_osd != 0)
                        {
                            hist_size++;
                            if (to_remove.find(old_osd) != to_remove.end())
                                hist_rm++;
                        }
                    }
                    if (hist_rm > 0)
                    {
                        hist_degraded = true;
                        if (hist_size-hist_rm == 0)
                            pool_incomplete = true;
                        else if (hist_size-hist_rm < pg_data_size)
                            hist_incomplete = true;
                    }
                }
                if (pg_rm > 0)
                {
                    pool_degraded = true;
                    if (pg_cursize-pg_rm < pg_data_size)
                        pool_incomplete = true;
                    else if (pg_cursize-pg_rm < pool_cfg.pg_minsize)
                        pool_down = true;
                }
            }
            if (pool_incomplete || pool_down || pool_degraded || hist_incomplete || hist_degraded)
            {
                pool_effects.push_back(json11::Json::object {
                    { "pool_id", (uint64_t)pool_cfg.id },
                    { "pool_name", pool_cfg.name },
                    { "effect", (pool_incomplete
                        ? "incomplete"
                        : (hist_incomplete
                            ? "has_incomplete"
                            : (pool_down
                                ? "offline"
                                : (pool_degraded
                                    ? "degraded"
                                    : (hist_degraded ? "has_degraded" : "?")
                                )
                            )
                        )
                    ) },
                });
                is_warning = true;
                if (pool_incomplete || hist_incomplete)
                    is_dataloss = true;
            }
        }
        result.data = json11::Json::object {
            { "osd_ids", osd_ids },
            { "pool_errors", pool_effects },
        };
        if (is_dataloss || is_warning || dry_run)
        {
            std::string error;
            for (auto & e: pool_effects)
            {
                error += "Pool "+e["pool_name"].string_value()+" (ID "+e["pool_id"].as_string()+") will have "+(
                    e["effect"] == "has_incomplete"
                        ? std::string("INCOMPLETE objects (DATA LOSS)")
                        : (e["effect"] == "incomplete"
                            ? std::string("INCOMPLETE PGs (DATA LOSS)")
                            : (e["effect"] == "has_degraded"
                                ? std::string("DEGRADED objects")
                                : strtoupper(e["effect"].string_value())+" PGs"))
                )+" after deleting OSD(s).\n";
            }
            if (is_dataloss && !force_dataloss && !dry_run)
                error += "OSDs not deleted. Please move data to other OSDs or bypass this check with --allow-data-loss if you know what you are doing.\n";
            else if (is_warning && !force_warning && !dry_run)
                error += "OSDs not deleted. Please move data to other OSDs or bypass this check with --force if you know what you are doing.\n";
            else if (!is_dataloss && !is_warning && dry_run)
                error += "OSDs can be deleted without data loss.\n";
            result.text = error;
            if (is_dataloss && !force_dataloss || is_warning && !force_warning)
            {
                result.err = EBUSY;
                state = 100;
                return;
            }
            if (dry_run)
            {
                result.err = 0;
                state = 100;
                return;
            }
        }
        // Remove keys from etcd
        {
            json11::Json::array rm_items;
            for (auto osd_id: osd_ids)
            {
                rm_items.push_back("/config/osd/"+std::to_string(osd_id));
                rm_items.push_back("/osd/stats/"+std::to_string(osd_id));
                rm_items.push_back("/osd/state/"+std::to_string(osd_id));
                rm_items.push_back("/osd/inodestats/"+std::to_string(osd_id));
                rm_items.push_back("/osd/space/"+std::to_string(osd_id));
            }
            for (int i = 0; i < rm_items.size(); i++)
            {
                rm_items[i] = json11::Json::object {
                    { "request_delete_range", json11::Json::object {
                        { "key", base64_encode(
                            parent->cli->st_cli.etcd_prefix+rm_items[i].string_value()
                        ) },
                    } },
                };
            }
            parent->etcd_txn(json11::Json::object { { "success", rm_items } });
        }
    resume_1:
        state = 1;
        if (parent->waiting > 0)
            return;
        if (parent->etcd_err.err)
        {
            result = parent->etcd_err;
            state = 100;
            return;
        }
        std::string ids = "";
        for (auto osd_id: osd_ids)
        {
            ids += (ids.size() ? ", " : "")+std::to_string(osd_id);
        }
        ids = (osd_ids.size() > 1 ? "OSDs " : "OSD ")+ids+(osd_ids.size() > 1 ? " are" : " is")+" removed from etcd";
        state = 100;
        result.text = (result.text != "" ? ids+"\n"+result.text : ids);
        result.err = 0;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_rm_osd(json11::Json cfg)
{
    auto rm_osd = new rm_osd_t();
    rm_osd->parent = this;
    rm_osd->dry_run = cfg["dry_run"].bool_value();
    rm_osd->force_dataloss = cfg["allow_data_loss"].bool_value();
    rm_osd->force_warning = rm_osd->force_dataloss || cfg["force"].bool_value();
    if (cfg["osd_id"].is_number() || cfg["osd_id"].is_string())
        rm_osd->osd_ids.push_back(cfg["osd_id"].uint64_value());
    else
    {
        for (auto & id: cfg["osd_id"].array_items())
            rm_osd->osd_ids.push_back(id.uint64_value());
    }
    return [rm_osd](cli_result_t & result)
    {
        rm_osd->loop();
        if (rm_osd->is_done())
        {
            result = rm_osd->result;
            delete rm_osd;
            return true;
        }
        return false;
    };
}
