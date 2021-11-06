// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include "base64.h"

std::string print_table(json11::Json items, json11::Json header);

std::string format_size(uint64_t size);

// List existing images
//
// Again, you can just look into etcd, but this console tool incapsulates it
struct image_lister_t
{
    cli_tool_t *parent;

    pool_id_t list_pool_id = 0;
    std::string list_pool_name;
    bool detailed = false;

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
            };
            if (ic.second.parent_id)
            {
                auto p_it = parent->cli->st_cli.inode_config.find(ic.second.parent_id);
                item["parent_name"] = p_it != parent->cli->st_cli.inode_config.end()
                    ? p_it->second.name : "";
                item["parent_pool_id"] = (uint64_t)INODE_POOL(ic.second.parent_id);
                item["parent_inode_num"] = INODE_NO_POOL(ic.second.parent_id);
            }
            if (!parent->json_output)
            {
                item["used_size_fmt"] = format_size(0);
                item["size_fmt"] = format_size(ic.second.size);
                item["ro"] = ic.second.readonly ? "RO" : "-";
            }
            stats[ic.second.num] = item;
        }
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        get_list();
        if (detailed)
        {
            // Space statistics
            // inode/stats/<pool>/<inode>::raw_used divided by pool/stats/<pool>::pg_real_size
            // multiplied by 1 or number of data drives
            parent->waiting++;
            parent->cli->st_cli.etcd_txn(json11::Json::object {
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
            }, ETCD_SLOW_TIMEOUT, [this](std::string err, json11::Json res)
            {
                parent->waiting--;
                if (err != "")
                {
                    fprintf(stderr, "Error reading from etcd: %s\n", err.c_str());
                    exit(1);
                }
                space_info = res;
            });
            state = 1;
resume_1:
            if (parent->waiting > 0)
                return;
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
                inode_t inode_num = INODE_WITH_POOL(pool_id, inode_num);
                uint64_t used_size = kv.value["raw_used"].uint64_value();
                // save stats
                auto pool_it = parent->cli->st_cli.pool_config.find(pool_id);
                if (pool_it == parent->cli->st_cli.pool_config.end())
                {
                    auto & pool_cfg = pool_it->second;
                    used_size = used_size / pool_pg_real_size[pool_id]
                        * (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks);
                }
                auto stat_it = stats.find(inode_num);
                if (stat_it == stats.end())
                {
                    stats[inode_num] = json11::Json::object {
                        { "name", "UnknownID:"+std::to_string(pool_id)+"/"+std::to_string(only_inode_num)+")" },
                        { "size", 0 },
                        { "readonly", false },
                        { "pool_id", (uint64_t)INODE_POOL(inode_num) },
                        { "pool_name", pool_it == parent->cli->st_cli.pool_config.end()
                            ? (pool_it->second.name == "" ? "<Unnamed>" : pool_it->second.name) : "?" },
                        { "inode_num", INODE_NO_POOL(inode_num) },
                    };
                    if (!parent->json_output)
                    {
                        stats[inode_num]["size_fmt"] = format_size(0);
                        stats[inode_num]["ro"] = "-";
                    }
                    stat_it = stats.find(inode_num);
                }
                stat_it->second["used_size"] = used_size;
                if (!parent->json_output)
                {
                    stat_it->second["used_size_fmt"] = format_size(used_size);
                }
            }
        }
        json11::Json::array list;
        for (auto & kv: stats)
        {
            list.push_back(kv.second);
        }
        if (parent->json_output)
        {
            // JSON output
            printf("%s\n", json11::Json(list).dump().c_str());
            state = 100;
            return;
        }
        // Table output: name, size_fmt, [used_size_fmt], ro, parent_name
        json11::Json::array cols;
        cols.push_back(json11::Json::object{
            { "key", "name" },
            { "title", "NAME" },
        });
        if (!list_pool_id && parent->cli->st_cli.pool_config.size() > 1)
        {
            cols.push_back(json11::Json::object{
                { "key", "used_size_fmt" },
                { "title", "USED" },
                { "right", true },
            });
        }
        cols.push_back(json11::Json::object{
            { "key", "size_fmt" },
            { "title", "SIZE" },
            { "right", true },
        });
        if (detailed)
        {
            cols.push_back(json11::Json::object{
                { "key", "used_size_fmt" },
                { "title", "USED" },
                { "right", true },
            });
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
        printf("%s", print_table(list, cols).c_str());
        state = 100;
    }
};

std::string print_table(json11::Json items, json11::Json header)
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
            int l = item[header[i]["key"].string_value()].string_value().length();
            sizes[i] = sizes[i] < l ? l : sizes[i];
        }
    }
    std::string str = "";
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
            int pad = sizes[i] - item[header[i]["key"].string_value()].string_value().length();
            if (header[i]["right"].bool_value())
            {
                // Align right
                for (int j = 0; j < pad; j++)
                    str += ' ';
                str += item[header[i]["key"].string_value()].string_value();
            }
            else
            {
                // Align left
                str += item[header[i]["key"].string_value()].string_value();
                for (int j = 0; j < pad; j++)
                    str += ' ';
            }
        }
        str += "\n";
    }
    return str;
}

static uint64_t size_thresh[] = { 1024l*1024*1024*1024, 1024l*1024*1024, 1024l*1024, 1024 };
static const char *size_unit = "TGMK";

std::string format_size(uint64_t size)
{
    char buf[256];
    for (int i = 0; i < sizeof(size_thresh)/sizeof(size_thresh[0]); i++)
    {
        if (size >= size_thresh[i] || i >= sizeof(size_thresh)/sizeof(size_thresh[0])-1)
        {
            double value = (double)size/size_thresh[i];
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

std::function<bool(void)> cli_tool_t::start_ls(json11::Json cfg)
{
    json11::Json::array cmd = cfg["command"].array_items();
    auto lister = new image_lister_t();
    lister->parent = this;
    lister->list_pool_id = cfg["pool"].uint64_value();
    lister->list_pool_name = lister->list_pool_id ? "" : cfg["pool"].string_value();
    lister->detailed = cfg["long"].bool_value();
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
