// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <algorithm>
#include "cli.h"
#include "cluster_client.h"
#include "base64.h"

// List existing images
//
// Again, you can just look into etcd, but this console tool incapsulates it
struct image_lister_t
{
    cli_tool_t *parent;

    pool_id_t list_pool_id = 0;
    std::string list_pool_name;
    std::string sort_field;
    std::set<std::string> only_names;
    bool reverse = false;
    int max_count = 0;
    bool show_stats = false, show_delete = false;

    int state = 0;
    std::map<inode_t, json11::Json::object> stats;
    json11::Json space_info;

    bool is_done()
    {
        return state == 100;
    }

    void get_list()
    {
        if (list_pool_name != "")
        {
            for (auto & ic: parent->cli->st_cli.pool_config)
            {
                if (ic.second.name == list_pool_name)
                {
                    list_pool_id = ic.first;
                    break;
                }
            }
            if (!list_pool_id)
            {
                fprintf(stderr, "Pool %s does not exist\n", list_pool_name.c_str());
                exit(1);
            }
        }
        for (auto & ic: parent->cli->st_cli.inode_config)
        {
            if (list_pool_id && INODE_POOL(ic.second.num) != list_pool_id)
            {
                continue;
            }
            auto & pool_cfg = parent->cli->st_cli.pool_config.at(INODE_POOL(ic.second.num));
            auto item = json11::Json::object {
                { "name", ic.second.name },
                { "size", ic.second.size },
                { "used_size", 0 },
                { "readonly", ic.second.readonly },
                { "pool_id", (uint64_t)INODE_POOL(ic.second.num) },
                { "pool_name", pool_cfg.name },
                { "inode_num", INODE_NO_POOL(ic.second.num) },
                { "inode_id", ic.second.num },
            };
            if (ic.second.parent_id)
            {
                auto p_it = parent->cli->st_cli.inode_config.find(ic.second.parent_id);
                item["parent_name"] = p_it != parent->cli->st_cli.inode_config.end()
                    ? p_it->second.name : "";
                item["parent_pool_id"] = (uint64_t)INODE_POOL(ic.second.parent_id);
                item["parent_inode_num"] = INODE_NO_POOL(ic.second.parent_id);
            }
            stats[ic.second.num] = item;
        }
    }

    void get_stats()
    {
        if (state == 1)
            goto resume_1;
        // Space statistics
        // inode/stats/<pool>/<inode>::raw_used divided by pool/stats/<pool>::pg_real_size
        // multiplied by 1 or number of data drives
        parent->etcd_txn(json11::Json::object {
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/pool/stats"+
                            (list_pool_id ? "/"+std::to_string(list_pool_id) : "")+"/"
                        ) },
                        { "range_end", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/pool/stats"+
                            (list_pool_id ? "/"+std::to_string(list_pool_id) : "")+"0"
                        ) },
                    } },
                },
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/inode/stats"+
                            (list_pool_id ? "/"+std::to_string(list_pool_id) : "")+"/"
                        ) },
                        { "range_end", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/inode/stats"+
                            (list_pool_id ? "/"+std::to_string(list_pool_id) : "")+"0"
                        ) },
                    } },
                },
            } },
        });
        state = 1;
resume_1:
        if (parent->waiting > 0)
            return;
        space_info = parent->etcd_result;
        std::map<pool_id_t, uint64_t> pool_pg_real_size;
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
            // pg_real_size
            pool_pg_real_size[pool_id] = kv.value["pg_real_size"].uint64_value();
        }
        for (auto & kv_item: space_info["responses"][1]["response_range"]["kvs"].array_items())
        {
            auto kv = parent->cli->st_cli.parse_etcd_kv(kv_item);
            // pool ID & inode number
            pool_id_t pool_id;
            inode_t only_inode_num;
            char null_byte = 0;
            sscanf(kv.key.substr(parent->cli->st_cli.etcd_prefix.length()).c_str(),
                "/inode/stats/%u/%lu%c", &pool_id, &only_inode_num, &null_byte);
            if (!pool_id || pool_id >= POOL_ID_MAX || INODE_POOL(only_inode_num) != 0 || null_byte != 0)
            {
                fprintf(stderr, "Invalid key in etcd: %s\n", kv.key.c_str());
                continue;
            }
            inode_t inode_num = INODE_WITH_POOL(pool_id, only_inode_num);
            uint64_t used_size = kv.value["raw_used"].uint64_value();
            // save stats
            auto pool_it = parent->cli->st_cli.pool_config.find(pool_id);
            if (pool_it != parent->cli->st_cli.pool_config.end())
            {
                auto & pool_cfg = pool_it->second;
                used_size = used_size / (pool_pg_real_size[pool_id] ? pool_pg_real_size[pool_id] : 1)
                    * (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks);
            }
            auto stat_it = stats.find(inode_num);
            if (stat_it == stats.end())
            {
                stats[inode_num] = json11::Json::object {
                    { "name", "Pool:"+std::to_string(pool_id)+",ID:"+std::to_string(only_inode_num) },
                    { "size", 0 },
                    { "readonly", false },
                    { "pool_id", (uint64_t)INODE_POOL(inode_num) },
                    { "pool_name", pool_it == parent->cli->st_cli.pool_config.end()
                        ? (pool_it->second.name == "" ? "<Unnamed>" : pool_it->second.name) : "?" },
                    { "inode_num", INODE_NO_POOL(inode_num) },
                    { "inode_id", inode_num },
                };
                stat_it = stats.find(inode_num);
            }
            stat_it->second["used_size"] = used_size;
            stat_it->second["read_iops"] = kv.value["read"]["iops"];
            stat_it->second["read_bps"] = kv.value["read"]["bps"];
            stat_it->second["read_lat"] = kv.value["read"]["lat"];
            stat_it->second["read_queue"] = kv.value["read"]["iops"].number_value() * kv.value["read"]["lat"].number_value() / 1000000;
            stat_it->second["write_iops"] = kv.value["write"]["iops"];
            stat_it->second["write_bps"] = kv.value["write"]["bps"];
            stat_it->second["write_lat"] = kv.value["write"]["lat"];
            stat_it->second["write_queue"] = kv.value["write"]["iops"].number_value() * kv.value["write"]["lat"].number_value() / 1000000;
            stat_it->second["delete_iops"] = kv.value["delete"]["iops"];
            stat_it->second["delete_bps"] = kv.value["delete"]["bps"];
            stat_it->second["delete_lat"] = kv.value["delete"]["lat"];
            stat_it->second["delete_queue"] = kv.value["delete"]["iops"].number_value() * kv.value["delete"]["lat"].number_value() / 1000000;
        }
    }

    json11::Json::array to_list()
    {
        json11::Json::array list;
        for (auto & kv: stats)
        {
            if (!only_names.size())
            {
                list.push_back(kv.second);
            }
            else
            {
                for (auto glob: only_names)
                {
                    if (stupid_glob(kv.second["name"].string_value(), glob))
                    {
                        list.push_back(kv.second);
                        break;
                    }
                }
            }
        }
        if (sort_field == "name" || sort_field == "pool_name")
        {
            std::sort(list.begin(), list.end(), [this](json11::Json a, json11::Json b)
            {
                auto av = a[sort_field].as_string();
                auto bv = b[sort_field].as_string();
                return reverse ? av > bv : av < bv;
            });
        }
        else
        {
            std::sort(list.begin(), list.end(), [this](json11::Json a, json11::Json b)
            {
                auto av = a[sort_field].number_value();
                auto bv = b[sort_field].number_value();
                return reverse ? av > bv : av < bv;
            });
        }
        if (max_count > 0 && list.size() > max_count)
        {
            list.resize(max_count);
        }
        return list;
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        get_list();
        if (show_stats)
        {
resume_1:
            get_stats();
            if (parent->waiting > 0)
                return;
        }
        if (parent->json_output)
        {
            // JSON output
            printf("%s\n", json11::Json(to_list()).dump().c_str());
            state = 100;
            return;
        }
        // Table output: name, size_fmt, [used_size_fmt], ro, parent_name
        json11::Json::array cols;
        cols.push_back(json11::Json::object{
            { "key", "name" },
            { "title", "NAME" },
        });
        if (!list_pool_id)
        {
            cols.push_back(json11::Json::object{
                { "key", "pool_name" },
                { "title", "POOL" },
            });
        }
        cols.push_back(json11::Json::object{
            { "key", "size_fmt" },
            { "title", "SIZE" },
        });
        if (show_stats)
        {
            cols.push_back(json11::Json::object{
                { "key", "used_size_fmt" },
                { "title", "USED" },
            });
            cols.push_back(json11::Json::object{
                { "key", "read_bw" },
                { "title", "READ" },
            });
            cols.push_back(json11::Json::object{
                { "key", "read_iops" },
                { "title", "IOPS" },
            });
            cols.push_back(json11::Json::object{
                { "key", "read_q" },
                { "title", "QUEUE" },
            });
            cols.push_back(json11::Json::object{
                { "key", "read_lat_f" },
                { "title", "LAT" },
            });
            cols.push_back(json11::Json::object{
                { "key", "write_bw" },
                { "title", "WRITE" },
            });
            cols.push_back(json11::Json::object{
                { "key", "write_iops" },
                { "title", "IOPS" },
            });
            cols.push_back(json11::Json::object{
                { "key", "write_q" },
                { "title", "QUEUE" },
            });
            cols.push_back(json11::Json::object{
                { "key", "write_lat_f" },
                { "title", "LAT" },
            });
            if (show_delete)
            {
                cols.push_back(json11::Json::object{
                    { "key", "delete_bw" },
                    { "title", "DEL" },
                });
                cols.push_back(json11::Json::object{
                    { "key", "delete_iops" },
                    { "title", "IOPS" },
                });
                cols.push_back(json11::Json::object{
                    { "key", "delete_q" },
                    { "title", "QUEUE" },
                });
                cols.push_back(json11::Json::object{
                    { "key", "delete_lat_f" },
                    { "title", "LAT" },
                });
            }
        }
        cols.push_back(json11::Json::object{
            { "key", "ro" },
            { "title", "FLAGS" },
            { "right", true },
        });
        cols.push_back(json11::Json::object{
            { "key", "parent_name" },
            { "title", "PARENT" },
        });
        json11::Json::array list;
        for (auto & kv: stats)
        {
            if (show_stats)
            {
                kv.second["used_size_fmt"] = format_size(kv.second["used_size"].uint64_value());
                kv.second["read_bw"] = format_size(kv.second["read_bps"].uint64_value())+"/s";
                kv.second["write_bw"] = format_size(kv.second["write_bps"].uint64_value())+"/s";
                kv.second["delete_bw"] = format_size(kv.second["delete_bps"].uint64_value())+"/s";
                kv.second["read_iops"] = format_q(kv.second["read_iops"].number_value());
                kv.second["write_iops"] = format_q(kv.second["write_iops"].number_value());
                kv.second["delete_iops"] = format_q(kv.second["delete_iops"].number_value());
                kv.second["read_lat_f"] = format_lat(kv.second["read_lat"].uint64_value());
                kv.second["write_lat_f"] = format_lat(kv.second["write_lat"].uint64_value());
                kv.second["delete_lat_f"] = format_lat(kv.second["delete_lat"].uint64_value());
                kv.second["read_q"] = format_q(kv.second["read_queue"].number_value());
                kv.second["write_q"] = format_q(kv.second["write_queue"].number_value());
                kv.second["delete_q"] = format_q(kv.second["delete_queue"].number_value());
            }
            kv.second["size_fmt"] = format_size(kv.second["size"].uint64_value());
            kv.second["ro"] = kv.second["readonly"].bool_value() ? "RO" : "-";
        }
        printf("%s", print_table(to_list(), cols, parent->color).c_str());
        state = 100;
    }
};

std::string print_table(json11::Json items, json11::Json header, bool use_esc)
{
    std::vector<int> sizes;
    for (int i = 0; i < header.array_items().size(); i++)
    {
        sizes.push_back(header[i]["title"].string_value().length());
    }
    for (auto & item: items.array_items())
    {
        for (int i = 0; i < header.array_items().size(); i++)
        {
            int l = item[header[i]["key"].string_value()].as_string().length();
            sizes[i] = sizes[i] < l ? l : sizes[i];
        }
    }
    std::string str = use_esc ? "\033[1m" : "";
    for (int i = 0; i < header.array_items().size(); i++)
    {
        if (i > 0)
        {
            // Separator
            str += "  ";
        }
        int pad = sizes[i]-header[i]["title"].string_value().length();
        if (header[i]["right"].bool_value())
        {
            // Align right
            for (int j = 0; j < pad; j++)
                str += ' ';
            str += header[i]["title"].string_value();
        }
        else
        {
            // Align left
            str += header[i]["title"].string_value();
            for (int j = 0; j < pad; j++)
                str += ' ';
        }
    }
    if (use_esc)
        str += "\033[0m";
    str += "\n";
    for (auto & item: items.array_items())
    {
        for (int i = 0; i < header.array_items().size(); i++)
        {
            if (i > 0)
            {
                // Separator
                str += "  ";
            }
            int pad = sizes[i] - item[header[i]["key"].string_value()].as_string().length();
            if (header[i]["right"].bool_value())
            {
                // Align right
                for (int j = 0; j < pad; j++)
                    str += ' ';
                str += item[header[i]["key"].string_value()].as_string();
            }
            else
            {
                // Align left
                str += item[header[i]["key"].string_value()].as_string();
                for (int j = 0; j < pad; j++)
                    str += ' ';
            }
        }
        str += "\n";
    }
    return str;
}

static uint64_t size_thresh[] = { 1024l*1024*1024*1024, 1024l*1024*1024, 1024l*1024, 1024, 0 };
static const char *size_unit = "TGMKB";

std::string format_size(uint64_t size)
{
    char buf[256];
    for (int i = 0; i < sizeof(size_thresh)/sizeof(size_thresh[0]); i++)
    {
        if (size >= size_thresh[i] || i >= sizeof(size_thresh)/sizeof(size_thresh[0])-1)
        {
            double value = size_thresh[i] ? (double)size/size_thresh[i] : size;
            int l = snprintf(buf, sizeof(buf), "%.1f", value);
            assert(l < sizeof(buf)-2);
            if (buf[l-1] == '0')
                l -= 2;
            buf[l] = ' ';
            buf[l+1] = size_unit[i];
            buf[l+2] = 0;
            break;
        }
    }
    return std::string(buf);
}

std::string format_lat(uint64_t lat)
{
    char buf[256];
    int l = 0;
    if (lat < 100)
        l = snprintf(buf, sizeof(buf), "%lu us", lat);
    else if (lat < 500000)
        l = snprintf(buf, sizeof(buf), "%.2f ms", (double)lat/1000);
    else
        l = snprintf(buf, sizeof(buf), "%.2f s", (double)lat/1000000);
    assert(l < sizeof(buf));
    return std::string(buf);
}

std::string format_q(double depth)
{
    char buf[256];
    int l = snprintf(buf, sizeof(buf), "%.2f", depth);
    assert(l < sizeof(buf));
    if (buf[l-1] == '0')
        l--;
    if (buf[l-1] == '0')
        l -= 2;
    buf[l] = 0;
    return std::string(buf);
}

struct glob_stack_t
{
    int glob_pos;
    int str_pos;
};

// Yes I know I could do it by translating the pattern to std::regex O:-)
bool stupid_glob(const std::string str, const std::string glob)
{
    std::vector<glob_stack_t> wildcards;
    int pos = 0, gp = 0;
    bool m;
back:
    while (true)
    {
        if (gp >= glob.length())
        {
            if (pos >= str.length())
                return true;
            m = false;
        }
        else if (glob[gp] == '*')
        {
            wildcards.push_back((glob_stack_t){ .glob_pos = ++gp, .str_pos = pos });
            continue;
        }
        else if (glob[gp] == '?')
            m = pos < str.size();
        else
        {
            if (glob[gp] == '\\' && gp < glob.length()-1)
                gp++;
            m = pos < str.size() && str[pos] == glob[gp];
        }
        if (!m)
        {
            while (wildcards.size() > 0)
            {
                // Backtrack
                pos = (++wildcards[wildcards.size()-1].str_pos);
                if (pos > str.size())
                    wildcards.pop_back();
                else
                {
                    gp = wildcards[wildcards.size()-1].glob_pos;
                    goto back;
                }
            }
            return false;
        }
        pos++;
        gp++;
    }
    return true;
}

std::function<bool(void)> cli_tool_t::start_ls(json11::Json cfg)
{
    json11::Json::array cmd = cfg["command"].array_items();
    auto lister = new image_lister_t();
    lister->parent = this;
    lister->list_pool_id = cfg["pool"].uint64_value();
    lister->list_pool_name = lister->list_pool_id ? "" : cfg["pool"].as_string();
    lister->show_stats = cfg["long"].bool_value();
    lister->show_delete = cfg["del"].bool_value();
    lister->sort_field = cfg["sort"].string_value();
    lister->reverse = cfg["reverse"].bool_value();
    lister->max_count = cfg["count"].uint64_value();
    for (int i = 1; i < cmd.size(); i++)
    {
        lister->only_names.insert(cmd[i].string_value());
    }
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
