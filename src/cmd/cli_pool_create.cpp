// Copyright (c) MIND Software LLC, 2023 (info@mindsw.io)
// I accept Vitastor CLA: see CLA-en.md for details
// Copyright (c) Vitaliy Filippov, 2024
// License: VNPL-1.1 (see README.md for details)

#include <ctype.h>
#include "cli.h"
#include "cli_pool_cfg.h"
#include "cluster_client.h"
#include "epoll_manager.h"
#include "pg_states.h"
#include "str_util.h"

struct pool_creator_t
{
    cli_tool_t *parent;
    json11::Json cfg;

    bool force = false;
    bool wait = false;

    uint64_t block_size = 0, bitmap_granularity = 0;
    uint32_t immediate_commit = 0;

    int state = 0;
    cli_result_t result;

    struct {
        uint32_t retries = 5;
        uint32_t interval = 0;
        bool passed = false;
    } create_check;

    uint64_t new_id = 1;
    uint64_t new_pools_mod_rev;
    json11::Json state_node_tree;
    json11::Json new_pools;
    std::map<osd_num_t, json11::Json> osd_stats;

    bool is_done() { return state == 100; }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        else if (state == 2)
            goto resume_2;
        else if (state == 3)
            goto resume_3;
        else if (state == 5)
            goto resume_5;
        else if (state == 6)
            goto resume_6;
        else if (state == 7)
            goto resume_7;
        else if (state == 8)
            goto resume_8;

        // Validate pool parameters
        {
            auto new_cfg = cfg.object_items();
            result.text = validate_pool_config(new_cfg, json11::Json(), parent->cli->st_cli.global_block_size,
                parent->cli->st_cli.global_bitmap_granularity, force);
            cfg = new_cfg;
        }
        if (result.text != "")
        {
            result.err = EINVAL;
            state = 100;
            return;
        }

        // Validate pool name
        for (auto & pp: parent->cli->st_cli.pool_config)
        {
            if (pp.second.name == cfg["name"].string_value())
            {
                result = (cli_result_t){
                    .err = EAGAIN,
                    .text = "Pool "+cfg["name"].string_value()+" already exists",
                };
                state = 100;
                return;
            }
        }

        state = 1;
resume_1:
        // If not forced, check that we have enough osds for pg_size
        if (!force)
        {
            // Get node_placement configuration from etcd and OSD stats
            parent->etcd_txn(json11::Json::object {
                { "success", json11::Json::array {
                    json11::Json::object {
                        { "request_range", json11::Json::object {
                            { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/node_placement") },
                        } },
                    },
                    json11::Json::object {
                        { "request_range", json11::Json::object {
                            { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/osd/stats/") },
                            { "range_end", base64_encode(parent->cli->st_cli.etcd_prefix+"/osd/stats0") },
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
                result = parent->etcd_err;
                state = 100;
                return;
            }

            // Get state_node_tree based on node_placement and osd stats
            {
                auto node_placement_kv = parent->cli->st_cli.parse_etcd_kv(parent->etcd_result["responses"][0]["response_range"]["kvs"][0]);
                timespec tv_now;
                clock_gettime(CLOCK_REALTIME, &tv_now);
                uint64_t osd_out_time = parent->cli->config["osd_out_time"].uint64_value();
                if (!osd_out_time)
                    osd_out_time = 600;
                osd_stats.clear();
                parent->iterate_kvs_1(parent->etcd_result["responses"][1]["response_range"]["kvs"], "/osd/stats/", [&](uint64_t cur_osd, json11::Json value)
                {
                    if ((uint64_t)value["time"].number_value()+osd_out_time >= tv_now.tv_sec)
                        osd_stats[cur_osd] = value;
                });
                state_node_tree = get_state_node_tree(node_placement_kv.value.object_items(), osd_stats);
            }

            // Skip tag checks, if pool has none
            if (cfg["osd_tags"].array_items().size())
            {
                // Get osd configs (for tags) of osds in state_node_tree
                {
                    json11::Json::array osd_configs;
                    for (auto osd_num: state_node_tree["osds"].array_items())
                    {
                        osd_configs.push_back(json11::Json::object {
                            { "request_range", json11::Json::object {
                                { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/osd/"+osd_num.as_string()) },
                            } }
                        });
                    }
                    parent->etcd_txn(json11::Json::object{ { "success", osd_configs } });
                }

                state = 3;
resume_3:
                if (parent->waiting > 0)
                    return;
                if (parent->etcd_err.err)
                {
                    result = parent->etcd_err;
                    state = 100;
                    return;
                }

                // Filter out osds from state_node_tree based on pool/osd tags
                {
                    std::vector<json11::Json> osd_configs;
                    for (auto & ocr: parent->etcd_result["responses"].array_items())
                    {
                        auto kv = parent->cli->st_cli.parse_etcd_kv(ocr["response_range"]["kvs"][0]);
                        osd_configs.push_back(kv.value);
                    }
                    state_node_tree = filter_state_node_tree_by_tags(state_node_tree, osd_configs);
                }
            }

            // Filter osds from state_node_tree based on pool parameters and osd stats
            {
                std::vector<json11::Json> filtered_osd_stats;
                for (auto & osd_num: state_node_tree["osds"].array_items())
                {
                    auto st_it = osd_stats.find(osd_num.uint64_value());
                    if (st_it != osd_stats.end())
                    {
                        filtered_osd_stats.push_back(st_it->second);
                    }
                }
                guess_block_size(filtered_osd_stats);
                state_node_tree = filter_state_node_tree_by_stats(state_node_tree, osd_stats);
            }

            // Check that pg_size <= max_pg_size
            {
                auto failure_domain = cfg["failure_domain"].string_value() == ""
                    ? "host" : cfg["failure_domain"].string_value();
                uint64_t max_pg_size = get_max_pg_size(state_node_tree, failure_domain, cfg["root_node"].string_value());

                if (cfg["pg_size"].uint64_value() > max_pg_size)
                {
                    std::string pool_err = "Not enough matching OSDs to create pool."
                        " Change parameters or add --force to create a degraded pool."
                        "\n\nAt least "+std::to_string(cfg["pg_size"].uint64_value())+
                        " (pg_size="+std::to_string(cfg["pg_size"].uint64_value())+") OSDs should have:"
                        "\n- block_size "+format_size(block_size, false, true)+
                        "\n- bitmap_granularity "+format_size(bitmap_granularity, false, true);
                    if (immediate_commit == IMMEDIATE_ALL)
                        pool_err += "\n- immediate_commit all";
                    else if (immediate_commit == IMMEDIATE_SMALL)
                        pool_err += "\n- immediate_commit all or small";
                    if (cfg["osd_tags"].array_items().size())
                        pool_err += "\n- '"+implode("', '", cfg["osd_tags"])+(cfg["osd_tags"].array_items().size() > 1 ? "' tags" : "' tag");
                    if (failure_domain != "osd")
                        pool_err += "\n- different parent '"+failure_domain+"' nodes";
                    result = (cli_result_t){
                        .err = EINVAL,
                        .text = pool_err,
                    };
                    state = 100;
                    return;
                }
            }
        }
        // Create pool
        state = 5;
resume_5:
        // Get pools from etcd
        parent->etcd_txn(json11::Json::object {
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/pools") },
                    } }
                },
            } },
        });
        state = 6;
resume_6:
        if (parent->waiting > 0)
            return;
        if (parent->etcd_err.err)
        {
            result = parent->etcd_err;
            state = 100;
            return;
        }
        {
            // Add new pool
            auto kv = parent->cli->st_cli.parse_etcd_kv(parent->etcd_result["responses"][0]["response_range"]["kvs"][0]);
            new_pools = create_pool(kv);
            if (new_pools.is_string())
            {
                result = (cli_result_t){ .err = EEXIST, .text = new_pools.string_value() };
                state = 100;
                return;
            }
            new_pools_mod_rev = kv.mod_revision;
        }
        // Update pools in etcd
        parent->etcd_txn(json11::Json::object {
            { "compare", json11::Json::array {
                json11::Json::object {
                    { "target", "MOD" },
                    { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/pools") },
                    { "result", "LESS" },
                    { "mod_revision", new_pools_mod_rev+1 },
                }
            } },
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_put", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/pools") },
                        { "value", base64_encode(new_pools.dump()) },
                    } },
                },
            } },
        });
        state = 7;
resume_7:
        if (parent->waiting > 0)
            return;
        if (parent->etcd_err.err)
        {
            result = parent->etcd_err;
            state = 100;
            return;
        }

        // Perform final create-check
        create_check.interval = parent->cli->config["mon_change_timeout"].uint64_value();
        if (!create_check.interval)
            create_check.interval = 1000;

        state = 8;
resume_8:
        if (parent->waiting > 0)
            return;

        // Unless forced, check that pool was created and is active
        if (!wait)
        {
            create_check.passed = true;
        }
        else if (create_check.retries)
        {
            create_check.retries--;
            parent->waiting++;
            parent->epmgr->tfd->set_timer(create_check.interval, false, [this](int timer_id)
            {
                if (parent->cli->st_cli.pool_config.find(new_id) != parent->cli->st_cli.pool_config.end())
                {
                    auto & pool_cfg = parent->cli->st_cli.pool_config[new_id];
                    create_check.passed = pool_cfg.real_pg_count > 0;
                    for (auto pg_it = pool_cfg.pg_config.begin(); pg_it != pool_cfg.pg_config.end(); pg_it++)
                    {
                        if (!(pg_it->second.cur_state & PG_ACTIVE))
                        {
                            create_check.passed = false;
                            break;
                        }
                    }
                    if (create_check.passed)
                        create_check.retries = 0;
                }
                parent->waiting--;
                parent->ringloop->wakeup();
            });
            return;
        }

        if (!create_check.passed)
        {
            result = (cli_result_t){
                .err = EAGAIN,
                .text = "Pool "+cfg["name"].string_value()+" was created, but failed to become active."
                    " This may indicate that cluster state has changed while the pool was being created."
                    " Please check the current state and adjust the pool configuration if necessary.",
            };
        }
        else
        {
            result = (cli_result_t){
                .err = 0,
                .text = "Pool "+cfg["name"].string_value()+" created",
                .data = new_pools[std::to_string(new_id)],
            };
        }
        state = 100;
    }

    // Returns a JSON object of form {"nodes": {...}, "osds": [...]} that
    // contains: all nodes (osds, hosts, ...) based on node_placement config
    // and current osd stats.
    json11::Json get_state_node_tree(json11::Json::object node_placement, std::map<osd_num_t, json11::Json> & osd_stats)
    {
        // Erase non-existing osd nodes from node_placement
        for (auto np_it = node_placement.begin(); np_it != node_placement.end();)
        {
            // Numeric nodes are osds
            osd_num_t osd_num = stoull_full(np_it->first);

            // If node is osd and its stats do not exist, erase it
            if (osd_num > 0 && osd_stats.find(osd_num) == osd_stats.end())
                node_placement.erase(np_it++);
            else
                np_it++;
        }

        // List of osds
        std::vector<std::string> existing_osds;

        // Record osds and add missing osds/hosts to np
        for (auto & ps: osd_stats)
        {
            std::string osd_num = std::to_string(ps.first);

            // Record osd
            existing_osds.push_back(osd_num);

            // Add host if necessary
            std::string osd_host = ps.second["host"].as_string();
            if (node_placement.find(osd_host) == node_placement.end())
            {
                node_placement[osd_host] = json11::Json::object {
                    { "level", "host" }
                };
            }

            // Add osd
            node_placement[osd_num] = json11::Json::object {
                { "parent", node_placement[osd_num]["parent"].is_null() ? osd_host : node_placement[osd_num]["parent"] },
                { "level", "osd" },
            };
        }

        return json11::Json::object { { "osds", existing_osds }, { "nodes", node_placement } };
    }

    // Returns new state_node_tree based on given state_node_tree with osds
    // filtered out by tags in given osd_configs and current pool config.
    // Requires: state_node_tree["osds"] must match osd_configs 1-1
    json11::Json filter_state_node_tree_by_tags(const json11::Json & state_node_tree, std::vector<json11::Json> & osd_configs)
    {
        auto & osds = state_node_tree["osds"].array_items();

        // Accepted state_node_tree nodes
        auto accepted_nodes = state_node_tree["nodes"].object_items();

        // List of accepted osds
        std::vector<std::string> accepted_osds;

        for (size_t i = 0; i < osd_configs.size(); i++)
        {
            auto & oc = osd_configs[i].object_items();

            // Get osd number
            auto osd_num = osds[i].as_string();

            // We need tags in config to check against pool tags
            if (oc.find("tags") == oc.end())
            {
                // Exclude osd from state_node_tree nodes
                accepted_nodes.erase(osd_num);
                continue;
            }
            else
            {
                // If all pool tags are in osd tags, accept osd
                if (all_in_tags(osd_configs[i]["tags"], cfg["osd_tags"]))
                {
                    accepted_osds.push_back(osd_num);
                }
                // Otherwise, exclude osd
                else
                {
                    // Exclude osd from state_node_tree nodes
                    accepted_nodes.erase(osd_num);
                }
            }
        }

        return json11::Json::object { { "osds", accepted_osds }, { "nodes", accepted_nodes } };
    }

    // Autodetect block size for the pool if not specified
    void guess_block_size(std::vector<json11::Json> & osd_stats)
    {
        json11::Json::object upd;
        if (!cfg["block_size"].uint64_value())
        {
            uint64_t osd_bs = 0;
            for (auto & os: osd_stats)
            {
                if (!os["data_block_size"].is_null())
                {
                    if (osd_bs == 0)
                        osd_bs = os["data_block_size"].uint64_value();
                    else if (osd_bs != os["data_block_size"].uint64_value())
                        osd_bs = UINT32_MAX;
                }
            }
            if (osd_bs && osd_bs != UINT32_MAX && osd_bs != parent->cli->st_cli.global_block_size)
            {
                fprintf(stderr, "Auto-selecting block_size=%s because all pool OSDs use it\n", format_size(osd_bs).c_str());
                upd["block_size"] = osd_bs;
            }
        }
        if (!cfg["bitmap_granularity"].uint64_value())
        {
            uint64_t osd_bg = 0;
            for (auto & os: osd_stats)
            {
                if (!os["bitmap_granularity"].is_null())
                {
                    if (osd_bg == 0)
                        osd_bg = os["bitmap_granularity"].uint64_value();
                    else if (osd_bg != os["bitmap_granularity"].uint64_value())
                        osd_bg = UINT32_MAX;
                }
            }
            if (osd_bg && osd_bg != UINT32_MAX && osd_bg != parent->cli->st_cli.global_bitmap_granularity)
            {
                fprintf(stderr, "Auto-selecting bitmap_granularity=%s because all pool OSDs use it\n", format_size(osd_bg).c_str());
                upd["bitmap_granularity"] = osd_bg;
            }
        }
        if (cfg["immediate_commit"].is_null())
        {
            uint32_t osd_imm = UINT32_MAX;
            for (auto & os: osd_stats)
            {
                if (!os["immediate_commit"].is_null())
                {
                    uint32_t imm = etcd_state_client_t::parse_immediate_commit(os["immediate_commit"].string_value(), IMMEDIATE_NONE);
                    if (osd_imm == UINT32_MAX)
                        osd_imm = imm;
                    else if (osd_imm != imm)
                        osd_imm = UINT32_MAX-1;
                }
            }
            if (osd_imm < UINT32_MAX-1 && osd_imm != parent->cli->st_cli.global_immediate_commit)
            {
                const char *imm_str = osd_imm == IMMEDIATE_NONE ? "none" : (osd_imm == IMMEDIATE_ALL ? "all" : "small");
                fprintf(stderr, "Auto-selecting immediate_commit=%s because all pool OSDs use it\n", imm_str);
                upd["immediate_commit"] = imm_str;
            }
        }
        if (upd.size())
        {
            json11::Json::object cfg_obj = cfg.object_items();
            for (auto & kv: upd)
            {
                cfg_obj[kv.first] = kv.second;
            }
            cfg = cfg_obj;
        }
    }

    // Returns new state_node_tree based on given state_node_tree with osds
    // filtered out by stats parameters (block_size, bitmap_granularity) in
    // given osd_stats and current pool config.
    // Requires: state_node_tree["osds"] must match osd_stats 1-1
    json11::Json filter_state_node_tree_by_stats(const json11::Json & state_node_tree, std::map<osd_num_t, json11::Json> & osd_stats)
    {
        // Accepted state_node_tree nodes
        auto accepted_nodes = state_node_tree["nodes"].object_items();

        // List of accepted osds
        json11::Json::array accepted_osds;

        block_size = cfg["block_size"].uint64_value()
            ? cfg["block_size"].uint64_value()
            : parent->cli->st_cli.global_block_size;
        bitmap_granularity = cfg["bitmap_granularity"].uint64_value()
            ? cfg["bitmap_granularity"].uint64_value()
            : parent->cli->st_cli.global_bitmap_granularity;
        immediate_commit = cfg["immediate_commit"].is_string()
            ? etcd_state_client_t::parse_immediate_commit(cfg["immediate_commit"].string_value(), IMMEDIATE_ALL)
            : parent->cli->st_cli.global_immediate_commit;

        for (auto osd_num_json: state_node_tree["osds"].array_items())
        {
            auto osd_num = osd_num_json.uint64_value();
            auto os_it = osd_stats.find(osd_num);
            if (os_it == osd_stats.end())
            {
                continue;
            }
            auto & os = os_it->second;
            if (!os["data_block_size"].is_null() && os["data_block_size"] != block_size ||
                !os["bitmap_granularity"].is_null() && os["bitmap_granularity"] != bitmap_granularity ||
                !os["immediate_commit"].is_null() &&
                etcd_state_client_t::parse_immediate_commit(os["immediate_commit"].string_value(), IMMEDIATE_NONE) < immediate_commit)
            {
                accepted_nodes.erase(osd_num_json.as_string());
            }
            else
            {
                accepted_osds.push_back(osd_num_json);
            }
        }

        return json11::Json::object { { "osds", accepted_osds }, { "nodes", accepted_nodes } };
    }

    // Returns maximum pg_size possible for given node_tree and failure_domain, starting at parent_node
    uint64_t get_max_pg_size(json11::Json state_node_tree, const std::string & level, const std::string & root_node)
    {
        std::set<std::string> level_seen;
        for (auto & osd: state_node_tree["osds"].array_items())
        {
            // find OSD parent at <level>, but stop at <root_node>
            auto cur_id = osd.string_value();
            auto cur = state_node_tree["nodes"][cur_id];
            while (!cur.is_null())
            {
                if (cur["level"] == level)
                {
                    level_seen.insert(cur_id);
                    break;
                }
                if (cur_id == root_node)
                    break;
                cur_id = cur["parent"].string_value();
                cur = state_node_tree["nodes"][cur_id];
            }
        }
        return level_seen.size();
    }

    json11::Json create_pool(const etcd_kv_t & kv)
    {
        for (auto & p: kv.value.object_items())
        {
            // ID
            uint64_t pool_id = stoull_full(p.first);
            new_id = std::max(pool_id+1, new_id);
            // Name
            if (p.second["name"].string_value() == cfg["name"].string_value())
            {
                return "Pool with name \""+cfg["name"].string_value()+"\" already exists (ID "+std::to_string(pool_id)+")";
            }
        }
        auto res = kv.value.object_items();
        res[std::to_string(new_id)] = cfg;
        return res;
    }

    // Checks whether tags2 tags are all in tags1 tags
    bool all_in_tags(json11::Json tags1, json11::Json tags2)
    {
        if (!tags2.is_array())
        {
            tags2 = json11::Json::array{ tags2.string_value() };
        }
        if (!tags1.is_array())
        {
            tags1 = json11::Json::array{ tags1.string_value() };
        }
        for (auto & tag2: tags2.array_items())
        {
            bool found = false;
            for (auto & tag1: tags1.array_items())
            {
                if (tag1 == tag2)
                {
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                return false;
            }
        }
        return true;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_pool_create(json11::Json cfg)
{
    auto pool_creator = new pool_creator_t();
    pool_creator->parent = this;
    pool_creator->cfg = cfg;
    pool_creator->force = cfg["force"].bool_value();
    pool_creator->wait = cfg["wait"].bool_value();
    return [pool_creator](cli_result_t & result)
    {
        pool_creator->loop();
        if (pool_creator->is_done())
        {
            result = pool_creator->result;
            delete pool_creator;
            return true;
        }
        return false;
    };
}
