// Copyright (c) Vitaliy Filippov, 2024
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include "pg_states.h"
#include "str_util.h"

struct pg_lister_t
{
    cli_tool_t *parent;

    uint64_t pool_id = 0;
    std::set<osd_num_t> osd_nums;
    std::string pool_name;
    std::vector<std::string> pg_state;
    uint64_t min_pg_num = 0;
    uint64_t max_pg_num = 0;

    std::map<pool_pg_num_t, json11::Json> pg_stats;

    int state = 0;
    cli_result_t result;

    bool is_done() { return state == 100; }

    void load_pg_stats()
    {
        if (state == 1)
            goto resume_1;
        if (pool_name != "")
        {
            pool_id = 0;
            for (auto & pp: parent->cli->st_cli.pool_config)
            {
                if (pp.second.name == pool_name)
                {
                    pool_id = pp.first;
                    break;
                }
            }
            if (!pool_id)
            {
                result = (cli_result_t){ .err = ENOENT, .text = "Pool "+pool_name+" not found" };
                state = 100;
                return;
            }
        }
        parent->etcd_txn(json11::Json::object {
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/pgstats"+(pool_id ? "/"+std::to_string(pool_id)+"/" : "/")) },
                        { "range_end", base64_encode(parent->cli->st_cli.etcd_prefix+"/pgstats"+(pool_id ? "/"+std::to_string(pool_id)+"0" : "0")) },
                    } },
                },
            } },
        });
        state = 1;
resume_1:
        if (parent->waiting > 0)
            return;
        if (parent->etcd_err.err)
        {
            result = parent->etcd_err;
            state = 100;
            return;
        }
        parent->iterate_kvs_2(parent->etcd_result["responses"][0]["response_range"]["kvs"], "/pgstats/", [&](pool_id_t pool_id, uint64_t pg_num, json11::Json value)
        {
            pg_stats[(pool_pg_num_t){ .pool_id = pool_id, .pg_num = (pg_num_t)pg_num }] = value;
        });
    }

    void format_pgs()
    {
        uint64_t is_not = ((uint64_t)1 << 63);
        std::vector<uint64_t> masks;
        if (pg_state.size())
        {
            for (auto & st: pg_state)
            {
                if (st.size())
                {
                    uint64_t mask = 0;
                    size_t pos = 0;
                    if (st[0] == '!' || st[0] == '^')
                    {
                        mask |= is_not;
                        pos++;
                    }
                    size_t prev = pos;
                    while (true)
                    {
                        if (pos < st.size() && (st[pos] >= 'a' && st[pos] <= 'z' || st[pos] == '_'))
                            pos++;
                        else
                        {
                            if (pos > prev)
                            {
                                std::string bit = st.substr(prev, pos-prev);
                                bool found = false;
                                for (int i = 0; i < pg_state_bit_count; i++)
                                {
                                    if (pg_state_names[i] == bit)
                                    {
                                        mask |= (uint64_t)1 << i;
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found)
                                {
                                    result = (cli_result_t){ .err = EINVAL, .text = "Unknown PG state "+bit };
                                    state = 100;
                                    return;
                                }
                            }
                            while (pos < st.size() && !(st[pos] >= 'a' && st[pos] <= 'z' || st[pos] == '_'))
                                pos++;
                            prev = pos;
                            if (pos >= st.size())
                                break;
                        }
                    }
                    masks.push_back(mask);
                }
            }
        }
        json11::Json::array pgs;
        for (auto & pp: parent->cli->st_cli.pool_config)
        {
            if ((!pool_id || pp.first == pool_id) && (pool_name == "" || pp.second.name == pool_name))
            {
                for (auto & pgp: pp.second.pg_config)
                {
                    if (min_pg_num && pgp.first < min_pg_num || max_pg_num && pgp.first > max_pg_num)
                    {
                        continue;
                    }
                    if (osd_nums.size())
                    {
                        bool found = false;
                        for (int i = 0; !found && i < pgp.second.target_set.size(); i++)
                            if (osd_nums.find(pgp.second.target_set[i]) != osd_nums.end())
                                found = true;
                        for (int i = 0; !found && i < pgp.second.target_history.size(); i++)
                            for (int j = 0; !found && j < pgp.second.target_history[i].size(); j++)
                                if (osd_nums.find(pgp.second.target_history[i][j]) != osd_nums.end())
                                    found = true;
                        for (int i = 0; !found && i < pgp.second.all_peers.size(); i++)
                            if (osd_nums.find(pgp.second.all_peers[i]) != osd_nums.end())
                                found = true;
                        if (!found)
                            continue;
                    }
                    if (masks.size())
                    {
                        bool found = false;
                        for (auto mask: masks)
                        {
                            if ((mask & is_not)
                                ? (pgp.second.cur_state & (mask & ~is_not)) != (mask & ~is_not)
                                : ((pgp.second.cur_state & mask) == mask))
                            {
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                            continue;
                    }
                    json11::Json::array state_names;
                    for (int i = 0; i < pg_state_bit_count; i++)
                    {
                        if (pgp.second.cur_state & (1 << i))
                        {
                            state_names.push_back(std::string(pg_state_names[i]));
                        }
                    }
                    if (!pgp.second.cur_state)
                    {
                        state_names.push_back("offline");
                    }
                    auto stat = pg_stats[(pool_pg_num_t){ .pool_id = pp.first, .pg_num = pgp.first }].object_items();
                    stat.erase("write_osd_set");
                    stat["pool_id"] = (uint64_t)pp.first;
                    stat["pool_name"] = pp.second.name;
                    stat["pg_num"] = (uint64_t)pgp.first;
                    stat["pause"] = pgp.second.pause;
                    stat["state"] = state_names;
                    stat["cur_primary"] = pgp.second.cur_primary;
                    stat["target_primary"] = pgp.second.primary;
                    stat["target_set"] = pgp.second.target_set;
                    stat["target_history"] = pgp.second.target_history;
                    stat["all_peers"] = pgp.second.all_peers;
                    stat["epoch"] = pgp.second.epoch;
                    stat["next_scrub"] = pgp.second.next_scrub;
                    if (!parent->json_output)
                    {
                        stat["fmt_state"] = implode("+", state_names);
                        stat["fmt_primary"] = (!pgp.second.primary && !pgp.second.cur_primary
                            ? "-"
                            : (std::to_string(pgp.second.cur_primary) + (pgp.second.primary == pgp.second.cur_primary
                                ? ""
                                : "->"+std::to_string(pgp.second.primary))));
                        stat["fmt_target_set"] = implode(",", stat["target_set"]);
                        uint64_t pg_block = pp.second.data_block_size * (pp.second.scheme == POOL_SCHEME_REPLICATED
                            ? 1 : (pp.second.pg_size-pp.second.parity_chunks));
                        stat["fmt_clean"] = format_size(stat["clean_count"].uint64_value() * pg_block);
                        stat["fmt_misplaced"] = format_size(stat["misplaced_count"].uint64_value() * pg_block);
                        stat["fmt_degraded"] = format_size(stat["degraded_count"].uint64_value() * pg_block);
                        stat["fmt_incomplete"] = format_size(stat["incomplete_count"].uint64_value() * pg_block);
                    }
                    pgs.push_back(stat);
                }
            }
        }
        if (parent->json_output)
        {
            result.data = pgs;
            return;
        }
        json11::Json::array cols;
        if (!pool_id)
        {
            cols.push_back(json11::Json::object{
                { "key", "pool_name" },
                { "title", "POOL" },
            });
        }
        cols.push_back(json11::Json::object{
            { "key", "pg_num" },
            { "title", "NUM" },
        });
        cols.push_back(json11::Json::object{
            { "key", "fmt_target_set" },
            { "title", "OSD SET" },
        });
        cols.push_back(json11::Json::object{
            { "key", "fmt_primary" },
            { "title", "PRIMARY" },
        });
        cols.push_back(json11::Json::object{
            { "key", "fmt_clean" },
            { "title", "DATA CLEAN" },
        });
        cols.push_back(json11::Json::object{
            { "key", "fmt_misplaced" },
            { "title", "MISPLACED" },
        });
        cols.push_back(json11::Json::object{
            { "key", "fmt_misplaced" },
            { "title", "DEGRADED" },
        });
        cols.push_back(json11::Json::object{
            { "key", "fmt_incomplete" },
            { "title", "INCOMPLETE" },
        });
        cols.push_back(json11::Json::object{
            { "key", "fmt_state" },
            { "title", "STATE" },
        });
        result.text = print_table(pgs, cols, parent->color);
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
resume_1:
        load_pg_stats();
        if (parent->waiting > 0)
            return;
        format_pgs();
        state = 100;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_pg_list(json11::Json cfg)
{
    auto pg_lister = new pg_lister_t();
    pg_lister->parent = this;
    if (cfg["pool"].uint64_value())
        pg_lister->pool_id = cfg["pool"].uint64_value();
    else
        pg_lister->pool_name = cfg["pool"].string_value();
    for (auto & st: cfg["pg_state"].array_items())
        pg_lister->pg_state.push_back(st.string_value());
    if (cfg["pg_state"].is_string())
        pg_lister->pg_state.push_back(cfg["pg_state"].string_value());
    pg_lister->min_pg_num = cfg["min"].uint64_value();
    pg_lister->max_pg_num = cfg["max"].uint64_value();
    if (cfg["osd"].is_array())
        for (auto & osd_num_json: cfg["osd"].array_items())
            pg_lister->osd_nums.insert(osd_num_json.uint64_value());
    else if (cfg["osd"].is_string())
        for (auto & osd_num_str: explode(",", cfg["osd"].string_value(), true))
            pg_lister->osd_nums.insert(stoull_full(osd_num_str));
    else if (cfg["osd"].uint64_value())
        pg_lister->osd_nums.insert(cfg["osd"].uint64_value());
    return [pg_lister](cli_result_t & result)
    {
        pg_lister->loop();
        if (pg_lister->is_done())
        {
            result = pg_lister->result;
            delete pg_lister;
            return true;
        }
        return false;
    };
}
