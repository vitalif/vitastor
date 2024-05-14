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

    bool is_done() { return state == 100; }

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
        state = 1;
resume_1:
        // If not forced, check that we have enough osds for pg_size
        if (!force)
        {
            // Get node_placement configuration from etcd
            parent->etcd_txn(json11::Json::object {
                { "success", json11::Json::array {
                    json11::Json::object {
                        { "request_range", json11::Json::object {
                            { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/node_placement") },
                        } }
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

            // Get state_node_tree based on node_placement and osd peer states
            {
                auto kv = parent->cli->st_cli.parse_etcd_kv(parent->etcd_result["responses"][0]["response_range"]["kvs"][0]);
                state_node_tree = get_state_node_tree(kv.value.object_items());
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
                    parent->etcd_txn(json11::Json::object { { "success", osd_configs, }, });
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

            // Get stats (for block_size, bitmap_granularity, ...) of osds in state_node_tree
            {
                json11::Json::array osd_stats;

                for (auto osd_num: state_node_tree["osds"].array_items())
                {
                    osd_stats.push_back(json11::Json::object {
                        { "request_range", json11::Json::object {
                            { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/osd/stats/"+osd_num.as_string()) },
                        } }
                    });
                }

                parent->etcd_txn(json11::Json::object { { "success", osd_stats, }, });
            }

            state = 4;
resume_4:
            if (parent->waiting > 0)
                return;
            if (parent->etcd_err.err)
            {
                result = parent->etcd_err;
                state = 100;
                return;
            }

            // Filter osds from state_node_tree based on pool parameters and osd stats
            {
                std::vector<json11::Json> osd_stats;
                for (auto & ocr: parent->etcd_result["responses"].array_items())
                {
                    auto kv = parent->cli->st_cli.parse_etcd_kv(ocr["response_range"]["kvs"][0]);
                    osd_stats.push_back(kv.value);
                }
                state_node_tree = filter_state_node_tree_by_stats(state_node_tree, osd_stats);
            }

            // Check that pg_size <= max_pg_size
            {
                auto failure_domain = cfg["failure_domain"].string_value() == ""
                    ? "host" : cfg["failure_domain"].string_value();
                uint64_t max_pg_size = get_max_pg_size(state_node_tree["nodes"].object_items(),
                    failure_domain, cfg["root_node"].string_value());

                if (cfg["pg_size"].uint64_value() > max_pg_size)
                {
                    result = (cli_result_t){
                        .err = EINVAL,
                        .text =
                            "There are "+std::to_string(max_pg_size)+" \""+failure_domain+"\" failure domains with OSDs matching tags and"
                            " block_size/bitmap_granularity/immediate_commit parameters, but you want to create a"
                            " pool with "+cfg["pg_size"].as_string()+" OSDs from different failure domains in a PG."
                            " Change parameters or add --force if you want to create a degraded pool and add OSDs later."
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
            result = (cli_result_t) {
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
    // and current peer state, and a list of active peer osds.
    json11::Json get_state_node_tree(json11::Json::object node_placement)
    {
        // Erase non-peer osd nodes from node_placement
        for (auto np_it = node_placement.begin(); np_it != node_placement.end();)
        {
            // Numeric nodes are osds
            osd_num_t osd_num = stoull_full(np_it->first);

            // If node is osd and it is not in peer states, erase it
            if (osd_num > 0 &&
                parent->cli->st_cli.peer_states.find(osd_num) == parent->cli->st_cli.peer_states.end())
            {
                node_placement.erase(np_it++);
            }
            else
                np_it++;
        }

        // List of peer osds
        std::vector<std::string> peer_osds;

        // Record peer osds and add missing osds/hosts to np
        for (auto & ps: parent->cli->st_cli.peer_states)
        {
            std::string osd_num = std::to_string(ps.first);

            // Record peer osd
            peer_osds.push_back(osd_num);

            // Add osd, if necessary
            if (node_placement.find(osd_num) == node_placement.end())
            {
                std::string osd_host = ps.second["host"].as_string();

                // Add host, if necessary
                if (node_placement.find(osd_host) == node_placement.end())
                {
                    node_placement[osd_host] = json11::Json::object {
                        { "level", "host" }
                    };
                }

                node_placement[osd_num] = json11::Json::object {
                    { "parent", osd_host }
                };
            }
        }

        return json11::Json::object { { "osds", peer_osds }, { "nodes", node_placement } };
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

    // Returns new state_node_tree based on given state_node_tree with osds
    // filtered out by stats parameters (block_size, bitmap_granularity) in
    // given osd_stats and current pool config.
    // Requires: state_node_tree["osds"] must match osd_stats 1-1
    json11::Json filter_state_node_tree_by_stats(const json11::Json & state_node_tree, std::vector<json11::Json> & osd_stats)
    {
        auto & osds = state_node_tree["osds"].array_items();

        // Accepted state_node_tree nodes
        auto accepted_nodes = state_node_tree["nodes"].object_items();

        // List of accepted osds
        std::vector<std::string> accepted_osds;

        uint64_t p_block_size = cfg["block_size"].uint64_value()
            ? cfg["block_size"].uint64_value()
            : parent->cli->st_cli.global_block_size;
        uint64_t p_bitmap_granularity = cfg["bitmap_granularity"].uint64_value()
            ? cfg["bitmap_granularity"].uint64_value()
            : parent->cli->st_cli.global_bitmap_granularity;
        uint32_t p_immediate_commit = cfg["immediate_commit"].is_string()
            ? etcd_state_client_t::parse_immediate_commit(cfg["immediate_commit"].string_value())
            : parent->cli->st_cli.global_immediate_commit;

        for (size_t i = 0; i < osd_stats.size(); i++)
        {
            auto & os = osd_stats[i];
            // Get osd number
            auto osd_num = osds[i].as_string();
            if (!os["data_block_size"].is_null() && os["data_block_size"] != p_block_size ||
                !os["bitmap_granularity"].is_null() && os["bitmap_granularity"] != p_bitmap_granularity ||
                !os["immediate_commit"].is_null() &&
                etcd_state_client_t::parse_immediate_commit(os["immediate_commit"].string_value()) < p_immediate_commit)
            {
                accepted_nodes.erase(osd_num);
            }
            else
            {
                accepted_osds.push_back(osd_num);
            }
        }

        return json11::Json::object { { "osds", accepted_osds }, { "nodes", accepted_nodes } };
    }

    // Returns maximum pg_size possible for given node_tree and failure_domain, starting at parent_node
    uint64_t get_max_pg_size(json11::Json::object node_tree, const std::string & level, const std::string & parent_node)
    {
        uint64_t max_pg_sz = 0;

        std::vector<std::string> nodes;

        // Check if parent node is an osd (numeric)
        if (parent_node != "" && stoull_full(parent_node))
        {
            // Add it to node list if osd is in node tree
            if (node_tree.find(parent_node) != node_tree.end())
                nodes.push_back(parent_node);
        }
        // If parent node given, ...
        else if (parent_node != "")
        {
            // ... look for children nodes of this parent
            for (auto & sn: node_tree)
            {
                auto & props = sn.second.object_items();

                auto parent_prop = props.find("parent");
                if (parent_prop != props.end() && (parent_prop->second.as_string() == parent_node))
                {
                    nodes.push_back(sn.first);

                    // If we're not looking for all osds, we only need a single
                    // child osd node
                    if (level != "osd" && stoull_full(sn.first))
                        break;
                }
            }
        }
        // No parent node given, and we're not looking for all osds
        else if (level != "osd")
        {
            // ... look for all level nodes
            for (auto & sn: node_tree)
            {
                auto & props = sn.second.object_items();

                auto level_prop = props.find("level");
                if (level_prop != props.end() && (level_prop->second.as_string() == level))
                {
                    nodes.push_back(sn.first);
                }
            }
        }
        // Otherwise, ...
        else
        {
            // ... we're looking for osd nodes only
            for (auto & sn: node_tree)
            {
                if (stoull_full(sn.first))
                {
                    nodes.push_back(sn.first);
                }
            }
        }

        // Process gathered nodes
        for (auto & node: nodes)
        {
            // Check for osd node, return constant max size
            if (stoull_full(node))
            {
                max_pg_sz += 1;
            }
            // Otherwise, ...
            else
            {
                // ... exclude parent node from tree, and ...
                node_tree.erase(parent_node);

                // ... descend onto the resulting tree
                max_pg_sz += get_max_pg_size(node_tree, level, node);
            }
        }

        return max_pg_sz;
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
