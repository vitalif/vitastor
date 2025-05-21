// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <ctype.h>
#include "cli.h"
#include "cluster_client.h"
#include "str_util.h"
#include "json_util.h"
#include "epoll_manager.h"

#include <algorithm>

// Delete OSD metadata from etcd
struct rm_osd_t
{
    cli_tool_t *parent;

    bool dry_run, force_warning, force_dataloss, allow_up;
    uint64_t etcd_tx_retry_ms = 500;
    uint64_t etcd_tx_retries = 10000;
    std::vector<uint64_t> osd_ids;

    int state = 0;
    cli_result_t result;

    std::set<osd_num_t> to_remove;
    std::vector<osd_num_t> still_up;
    json11::Json::array pool_effects;
    json11::Json::array history_updates, history_checks;
    json11::Json new_pgs, new_clean_pgs;
    uint64_t new_pgs_mod_rev, new_clean_pgs_mod_rev;
    uint64_t cur_retry = 0;
    uint64_t retry_wait = 0;
    bool is_warning, is_dataloss;

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
        else if (state == 3)
            goto resume_3;
        else if (state == 4)
            goto resume_4;
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
        is_warning = is_dataloss = false;
        // Check if OSDs are still up
        for (auto osd_id: to_remove)
        {
            if (parent->cli->st_cli.peer_states.find(osd_id) != parent->cli->st_cli.peer_states.end())
            {
                is_warning = !allow_up;
                still_up.push_back(osd_id);
            }
        }
        // Check if OSDs are still used in data distribution
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
            if (still_up.size() && !allow_up)
                error += (still_up.size() == 1 ? "OSD " : "OSDs ") + implode(", ", still_up) +
                    (still_up.size() == 1 ? "is" : "are") + " still up. Use `vitastor-disk purge` to delete them.\n";
            if (is_dataloss && !force_dataloss && !dry_run)
                error += "OSDs not deleted. Please move data to other OSDs or bypass this check with --allow-data-loss if you know what you are doing.\n";
            else if (is_warning && !force_warning && !dry_run)
                error += "OSDs not deleted. Please move data to other OSDs or bypass this check with --force if you know what you are doing.\n";
            else if (!is_dataloss && !is_warning && dry_run)
                error += "OSDs can be deleted without data loss.\n";
            result.text = error;
            if (dry_run || is_dataloss && !force_dataloss || is_warning && !force_warning)
            {
                result.err = is_dataloss && !force_dataloss || is_warning && !force_warning ? EBUSY : 0;
                state = 100;
                return;
            }
        }
        parent->etcd_txn(json11::Json::object { { "success", json11::Json::array {
            json11::Json::object {
                { "request_range", json11::Json::object {
                    { "key", base64_encode(
                        parent->cli->st_cli.etcd_prefix+"/pg/config"
                    ) },
                } },
            },
            json11::Json::object {
                { "request_range", json11::Json::object {
                    { "key", base64_encode(
                        parent->cli->st_cli.etcd_prefix+"/history/last_clean_pgs"
                    ) },
                } },
            },
        } } });
    resume_4:
        state = 4;
        if (parent->waiting > 0)
            return;
        if (parent->etcd_err.err)
        {
            result = parent->etcd_err;
            state = 100;
            return;
        }
        {
            auto kv = parent->cli->st_cli.parse_etcd_kv(parent->etcd_result["responses"][0]["response_range"]["kvs"][0]);
            new_pgs = remove_osds_from_pgs(kv);
            new_pgs_mod_rev = kv.mod_revision;
            kv = parent->cli->st_cli.parse_etcd_kv(parent->etcd_result["responses"][1]["response_range"]["kvs"][0]);
            new_clean_pgs = remove_osds_from_pgs(kv);
            new_clean_pgs_mod_rev = kv.mod_revision;
        }
        // Remove keys from etcd
        {
            json11::Json::array rm_items, rm_checks;
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
            if (!new_pgs.is_null())
            {
                auto pgs_key = base64_encode(parent->cli->st_cli.etcd_prefix+"/pg/config");
                rm_items.push_back(json11::Json::object {
                    { "request_put", json11::Json::object {
                        { "key", pgs_key },
                        { "value", base64_encode(new_pgs.dump()) },
                    } },
                });
                rm_checks.push_back(json11::Json::object {
                    { "target", "MOD" },
                    { "key", pgs_key },
                    { "result", "LESS" },
                    { "mod_revision", new_pgs_mod_rev+1 },
                });
            }
            if (!new_clean_pgs.is_null())
            {
                auto pgs_key = base64_encode(parent->cli->st_cli.etcd_prefix+"/history/last_clean_pgs");
                rm_items.push_back(json11::Json::object {
                    { "request_put", json11::Json::object {
                        { "key", pgs_key },
                        { "value", base64_encode(new_clean_pgs.dump()) },
                    } },
                });
                rm_checks.push_back(json11::Json::object {
                    { "target", "MOD" },
                    { "key", pgs_key },
                    { "result", "LESS" },
                    { "mod_revision", new_clean_pgs_mod_rev+1 },
                });
            }
            parent->etcd_txn(json11::Json::object { { "success", rm_items }, { "checks", rm_checks } });
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
        // Remove old OSD from PG all_peers to prevent left_on_dead and from
        // target_history to prevent INCOMPLETE if --allow-data-loss is specified
        for (auto & rsp: parent->etcd_result["responses"].array_items())
        {
            if (rsp["response_delete_range"]["deleted"].uint64_value() > 0)
            {
                // Wait for mon_change_timeout before updating PG history, or the monitor's change will likely interfere with ours
                retry_wait = parent->cli->config["mon_change_timeout"].uint64_value();
                if (!retry_wait)
                    retry_wait = 1000;
                retry_wait += etcd_tx_retry_ms;
            }
        }
        while (1)
        {
    resume_2:
            if (!remove_osds_from_history(2))
                return;
    resume_3:
            state = 3;
            if (parent->waiting > 0)
                return;
            if (parent->etcd_err.err)
            {
                result = parent->etcd_err;
                state = 100;
                return;
            }
            if (parent->etcd_result["succeeded"].bool_value())
                break;
            if ((++cur_retry) >= etcd_tx_retries)
            {
                result.err = EAGAIN;
                result.text += "Failed to remove OSDs from PG history due to update conflicts."
                    " Some PGs may remain left_on_dead or incomplete. Please retry later\n";
                state = 100;
                return;
            }
            retry_wait = etcd_tx_retry_ms;
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

    json11::Json remove_osds_from_pgs(const etcd_kv_t & kv)
    {
        if (kv.value.is_null())
        {
            return kv.value;
        }
        json11::Json::object new_pgs;
        for (auto & pp: kv.value["items"].object_items())
        {
            if (pp.second.is_object())
            {
                json11::Json::object new_pool;
                for (auto & pgp: pp.second.object_items())
                {
                    json11::Json::array osd_set;
                    for (auto & osd_json: pgp.second["osd_set"].array_items())
                    {
                        uint64_t osd_num = osd_json.uint64_value();
                        osd_set.push_back(osd_num == 0 || to_remove.find(osd_num) != to_remove.end() ? 0 : osd_num);
                    }
                    json11::Json::object new_pg = pgp.second.object_items();
                    new_pg["osd_set"] = osd_set;
                    new_pool[pgp.first] = new_pg;
                }
                new_pgs[pp.first] = new_pool;
            }
            else
                new_pgs[pp.first] = pp.second;
        }
        auto res = kv.value.object_items();
        res["items"] = new_pgs;
        return res;
    }

    bool remove_osds_from_history(int base_state)
    {
        if (state == base_state+0)
            goto resume_0;
        history_updates.clear();
        history_checks.clear();
        for (auto & pp: parent->cli->st_cli.pool_config)
        {
            bool update_pg_history = false;
            auto & pool_cfg = pp.second;
            for (auto & pgp: pool_cfg.pg_config)
            {
                auto pg_num = pgp.first;
                auto & pg_cfg = pgp.second;
                for (int i = 0; i < pg_cfg.all_peers.size(); i++)
                {
                    if (to_remove.find(pg_cfg.all_peers[i]) != to_remove.end())
                    {
                        update_pg_history = true;
                        pg_cfg.all_peers.erase(pg_cfg.all_peers.begin()+i, pg_cfg.all_peers.begin()+i+1);
                        i--;
                    }
                }
                for (int i = 0; i < pg_cfg.target_history.size(); i++)
                {
                    int hist_size = 0, hist_rm = 0;
                    for (auto & old_osd: pg_cfg.target_history[i])
                    {
                        if (old_osd != 0)
                        {
                            hist_size++;
                            if (to_remove.find(old_osd) != to_remove.end())
                            {
                                hist_rm++;
                                old_osd = 0;
                            }
                        }
                    }
                    if (hist_rm > 0)
                    {
                        if (hist_size-hist_rm == 0)
                        {
                            pg_cfg.target_history.erase(pg_cfg.target_history.begin()+i, pg_cfg.target_history.begin()+i+1);
                            i--;
                        }
                        update_pg_history = true;
                    }
                }
                if (update_pg_history)
                {
                    std::string history_key = base64_encode(
                        parent->cli->st_cli.etcd_prefix+"/pg/history/"+
                        std::to_string(pool_cfg.id)+"/"+std::to_string(pg_num)
                    );
                    auto hist = json11::Json::object {
                        { "epoch", pg_cfg.epoch },
                        { "all_peers", pg_cfg.all_peers },
                        { "osd_sets", pg_cfg.target_history },
                    };
                    if (pg_cfg.next_scrub)
                        hist["next_scrub"] = pg_cfg.next_scrub;
                    history_updates.push_back(json11::Json::object {
                        { "request_put", json11::Json::object {
                            { "key", history_key },
                            { "value", base64_encode(json11::Json(hist).dump()) },
                        } },
                    });
                    history_checks.push_back(json11::Json::object {
                        { "target", "MOD" },
                        { "key", history_key },
                        { "result", "LESS" },
                        { "mod_revision", parent->cli->st_cli.etcd_watch_revision_pg+1 },
                    });
                }
            }
        }
        if (history_updates.size())
        {
            if (retry_wait)
            {
                parent->waiting++;
                parent->epmgr->tfd->set_timer(retry_wait, false, [this](int timer_id)
                {
                    parent->waiting--;
                    parent->ringloop->wakeup();
                });
    resume_0:
                state = base_state+0;
                if (parent->waiting > 0)
                    return false;
            }
            parent->etcd_txn(json11::Json::object {
                { "success", history_updates },
                { "compare", history_checks },
            });
        }
        else
            parent->etcd_result = json11::Json::object{ { "succeeded", true } };
        return true;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_rm_osd(json11::Json cfg)
{
    auto rm_osd = new rm_osd_t();
    rm_osd->parent = this;
    rm_osd->dry_run = cfg["dry_run"].bool_value();
    rm_osd->allow_up = cfg["allow_up"].bool_value();
    rm_osd->force_dataloss = cfg["allow_data_loss"].bool_value();
    rm_osd->force_warning = rm_osd->force_dataloss || cfg["force"].bool_value();
    if (!cfg["etcd_tx_retries"].is_null())
        rm_osd->etcd_tx_retries = cfg["etcd_tx_retries"].uint64_value();
    if (!cfg["etcd_tx_retry_ms"].is_null())
    {
        rm_osd->etcd_tx_retry_ms = cfg["etcd_tx_retry_ms"].uint64_value();
        if (rm_osd->etcd_tx_retry_ms < 100)
            rm_osd->etcd_tx_retry_ms = 100;
    }
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
