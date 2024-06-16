// Copyright (c) Vitaliy Filippov, 2024
// License: VNPL-1.1 (see README.md for details)

#include <ctype.h>
#include "cli.h"
#include "cluster_client.h"
#include "epoll_manager.h"
#include "pg_states.h"
#include "str_util.h"

struct placement_osd_t
{
    osd_num_t num;
    std::string parent;
    std::vector<std::string> tags;
    uint64_t size;
    uint64_t free;
    bool up;
    double reweight;
    uint32_t block_size, bitmap_granularity, immediate_commit;
};

struct placement_node_t
{
    std::string name;
    std::string parent;
    std::string level;
    std::vector<std::string> child_nodes;
    std::vector<osd_num_t> child_osds;
};

struct placement_tree_t
{
    std::map<std::string, placement_node_t> nodes;
    std::map<osd_num_t, placement_osd_t> osds;
};

struct osd_tree_printer_t
{
    cli_tool_t *parent;
    json11::Json cfg;
    bool flat = false;

    int state = 0;
    cli_result_t result;

    std::shared_ptr<placement_tree_t> placement_tree;

    bool is_done() { return state == 100; }

    void load_osd_tree()
    {
        if (state == 1)
            goto resume_1;
        parent->etcd_txn(json11::Json::object {
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/node_placement") },
                    } },
                },
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/osd/") },
                        { "range_end", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/osd0") },
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
        json11::Json node_placement;
        for (auto & item: parent->etcd_result["responses"][0]["response_range"]["kvs"].array_items())
        {
            node_placement = parent->cli->st_cli.parse_etcd_kv(item).value;
        }
        std::map<uint64_t, json11::Json> osd_config;
        parent->iterate_kvs_1(parent->etcd_result["responses"][1]["response_range"]["kvs"], "/config/osd/", [&](uint64_t cur_osd, json11::Json value)
        {
            osd_config[cur_osd] = value;
        });
        std::map<uint64_t, json11::Json> osd_stats;
        parent->iterate_kvs_1(parent->etcd_result["responses"][2]["response_range"]["kvs"], "/osd/stats/", [&](uint64_t cur_osd, json11::Json value)
        {
            osd_stats[cur_osd] = value;
        });
        placement_tree = make_osd_tree(node_placement, osd_config, osd_stats);
    }

    std::shared_ptr<placement_tree_t> make_osd_tree(json11::Json node_placement_json,
        std::map<uint64_t, json11::Json> osd_config, std::map<uint64_t, json11::Json> osd_stats)
    {
        auto node_placement = node_placement_json.object_items();
        auto tree = std::make_shared<placement_tree_t>();
        tree->nodes[""] = (placement_node_t){};
        // Add non-OSD items
        for (auto & kv: node_placement)
        {
            auto osd_num = stoull_full(kv.first);
            if (!osd_num)
            {
                auto level = kv.second["level"].string_value();
                tree->nodes[kv.first] = (placement_node_t){
                    .name = kv.first,
                    .parent = kv.second["parent"].string_value(),
                    .level = level == "" ? "unknown" : level,
                };
            }
        }
        // Add OSDs
        for (auto & kv: osd_stats)
        {
            auto & osd = tree->osds[kv.first] = (placement_osd_t){
                .num = kv.first,
                .parent = kv.second["host"].string_value(),
                .size = kv.second["size"].uint64_value(),
                .free = kv.second["free"].uint64_value(),
                .up = parent->cli->st_cli.peer_states.find(kv.first) != parent->cli->st_cli.peer_states.end(),
                .reweight = 1,
                .block_size = (uint32_t)kv.second["data_block_size"].uint64_value(),
                .bitmap_granularity = (uint32_t)kv.second["bitmap_granularity"].uint64_value(),
                .immediate_commit = etcd_state_client_t::parse_immediate_commit(kv.second["immediate_commit"].string_value()),
            };
            if (tree->nodes.find(osd.parent) == tree->nodes.end())
            {
                // Autocreate all hosts
                tree->nodes[osd.parent] = (placement_node_t){
                    .name = osd.parent,
                    .level = "host",
                };
            }
            auto cfg_it = osd_config.find(osd.num);
            if (cfg_it != osd_config.end())
            {
                auto & osd_cfg = cfg_it->second;
                osd.reweight = osd_cfg["reweight"].is_number() ? osd_cfg["reweight"].number_value() : 1;
                if (osd_cfg["tags"].is_array())
                {
                    for (auto & jtag: osd_cfg["tags"].array_items())
                        osd.tags.push_back(jtag.string_value());
                }
            }
            auto np_it = node_placement.find(std::to_string(osd.num));
            if (np_it != node_placement.end())
            {
                osd.parent = np_it->second["parent"].string_value();
            }
            tree->nodes[osd.parent].child_osds.push_back(osd.num);
        }
        // Fill child_nodes
        for (auto & ip: tree->nodes)
        {
            if (tree->nodes.find(ip.second.parent) == tree->nodes.end())
            {
                ip.second.parent = "";
            }
            if (ip.first != "")
            {
                tree->nodes[ip.second.parent].child_nodes.push_back(ip.first);
            }
        }
        // FIXME: Maybe filter out loops here
        return tree;
    }

    std::string format_tree()
    {
        std::vector<std::string> node_seq = { "" };
        std::vector<int> indents = { -1 };
        std::map<std::string, bool> seen;
        for (int i = 0; i < node_seq.size(); i++)
        {
            if (seen[node_seq[i]])
            {
                continue;
            }
            seen[node_seq[i]] = true;
            auto & child_nodes = placement_tree->nodes.at(node_seq[i]).child_nodes;
            if (child_nodes.size())
            {
                node_seq.insert(node_seq.begin()+i+1, child_nodes.begin(), child_nodes.end());
                indents.insert(indents.begin()+i+1, child_nodes.size(), indents[i]+1);
            }
        }
        json11::Json::array fmt_items;
        for (int i = 1; i < node_seq.size(); i++)
        {
            auto & node = placement_tree->nodes.at(node_seq[i]);
            if (!flat)
            {
                fmt_items.push_back(json11::Json::object{
                    { "type", str_repeat("  ", indents[i]) + node.level },
                    { "name", node.name },
                });
            }
            std::string parent = node.name;
            if (flat)
            {
                auto cur = &placement_tree->nodes.at(node.name);
                while (cur->parent != "" && cur->parent != node.name)
                {
                    parent = cur->parent+"/"+parent;
                    cur = &placement_tree->nodes.at(cur->parent);
                }
            }
            for (uint64_t osd_num: node.child_osds)
            {
                auto & osd = placement_tree->osds.at(osd_num);
                fmt_items.push_back(json11::Json::object{
                    { "type", (flat ? "osd" : str_repeat("  ", indents[i]+1) + "osd") },
                    { "name", osd.num },
                    { "parent", parent },
                    { "up", osd.up ? "up" : "down" },
                    { "size", format_size(osd.size, false, true) },
                    { "used", format_q(100.0*(osd.size - osd.free)/osd.size)+" %" },
                    { "reweight", format_q(osd.reweight) },
                    { "tags", implode(",", osd.tags) },
                    { "block", format_size(osd.block_size, false, true) },
                    { "bitmap", format_size(osd.bitmap_granularity, false, true) },
                    { "commit", osd.immediate_commit == IMMEDIATE_NONE ? "none" : (osd.immediate_commit == IMMEDIATE_ALL ? "all" : "small") },
                });
            }
        }
        json11::Json::array cols;
        if (!flat)
        {
            cols.push_back(json11::Json::object{
                { "key", "type" },
                { "title", "TYPE" },
            });
        }
        cols.push_back(json11::Json::object{
            { "key", "name" },
            { "title", flat ? "OSD" : "NAME" },
        });
        if (flat)
        {
            cols.push_back(json11::Json::object{
                { "key", "parent" },
                { "title", "PARENT" },
            });
        }
        cols.push_back(json11::Json::object{
            { "key", "up" },
            { "title", "UP" },
        });
        cols.push_back(json11::Json::object{
            { "key", "size" },
            { "title", "SIZE" },
        });
        cols.push_back(json11::Json::object{
            { "key", "used" },
            { "title", "USED%" },
        });
        cols.push_back(json11::Json::object{
            { "key", "tags" },
            { "title", "TAGS" },
        });
        cols.push_back(json11::Json::object{
            { "key", "reweight" },
            { "title", "WEIGHT" },
        });
        cols.push_back(json11::Json::object{
            { "key", "block" },
            { "title", "BLOCK" },
        });
        cols.push_back(json11::Json::object{
            { "key", "bitmap" },
            { "title", "BITMAP" },
        });
        cols.push_back(json11::Json::object{
            { "key", "commit" },
            { "title", "IMM" },
        });
        return print_table(fmt_items, cols, parent->color);
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
resume_1:
        load_osd_tree();
        if (parent->waiting > 0)
            return;
        result.text = format_tree();
        state = 100;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_osd_tree(json11::Json cfg)
{
    auto osd_tree_printer = new osd_tree_printer_t();
    osd_tree_printer->parent = this;
    osd_tree_printer->cfg = cfg;
    osd_tree_printer->flat = cfg["flat"].bool_value();
    return [osd_tree_printer](cli_result_t & result)
    {
        osd_tree_printer->loop();
        if (osd_tree_printer->is_done())
        {
            result = osd_tree_printer->result;
            delete osd_tree_printer;
            return true;
        }
        return false;
    };
}
