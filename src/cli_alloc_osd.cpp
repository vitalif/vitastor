// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <ctype.h>
#include "cli.h"
#include "cluster_client.h"
#include "base64.h"

#include <algorithm>

// Safely allocate an OSD number
struct alloc_osd_t
{
    cli_tool_t *parent;

    json11::Json result;
    uint64_t new_id = 1;

    int state = 0;

    bool is_done()
    {
        return state == 100;
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        do
        {
            etcd_txn(json11::Json::object {
                { "compare", json11::Json::array {
                    json11::Json::object {
                        { "target", "VERSION" },
                        { "version", 0 },
                        { "key", base64_encode(
                            parent->cli->st_cli.etcd_prefix+"/osd/stats/"+std::to_string(new_id)
                        ) },
                    },
                } },
                { "success", json11::Json::array {
                    json11::Json::object {
                        { "request_put", json11::Json::object {
                            { "key", base64_encode(
                                parent->cli->st_cli.etcd_prefix+"/osd/stats/"+std::to_string(new_id)
                            ) },
                            { "value", base64_encode("{}") },
                        } },
                    },
                } },
                { "failure", json11::Json::array {
                    json11::Json::object {
                        { "request_range", json11::Json::object {
                            { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/osd/stats/") },
                            { "range_end", base64_encode(parent->cli->st_cli.etcd_prefix+"/osd/stats0") },
                            { "keys_only", true },
                        } },
                    },
                } },
            });
        resume_1:
            state = 1;
            if (parent->waiting > 0)
                return;
            if (!result["succeeded"].bool_value())
            {
                std::vector<osd_num_t> used;
                for (auto kv: result["responses"][0]["response_range"]["kvs"].array_items())
                {
                    std::string key = base64_decode(kv["key"].string_value());
                    osd_num_t cur_osd;
                    char null_byte = 0;
                    sscanf(key.c_str() + parent->cli->st_cli.etcd_prefix.length(), "/osd/stats/%lu%c", &cur_osd, &null_byte);
                    if (!cur_osd || null_byte != 0)
                    {
                        fprintf(stderr, "Invalid key in etcd: %s\n", key.c_str());
                        continue;
                    }
                    used.push_back(cur_osd);
                }
                std::sort(used.begin(), used.end());
                if (used[used.size()-1] == used.size())
                {
                    new_id = used.size()+1;
                }
                else
                {
                    int s = 0, e = used.size();
                    while (e > s+1)
                    {
                        int c = (s+e)/2;
                        if (used[c] == c+1)
                            s = c;
                        else
                            e = c;
                    }
                    new_id = used[e-1]+1;
                }
            }
        } while (!result["succeeded"].bool_value());
        state = 100;
    }

    void etcd_txn(json11::Json txn)
    {
        parent->waiting++;
        parent->cli->st_cli.etcd_txn(txn, parent->cli->st_cli.etcd_slow_timeout, [this](std::string err, json11::Json res)
        {
            parent->waiting--;
            if (err != "")
            {
                fprintf(stderr, "Error reading from etcd: %s\n", err.c_str());
                exit(1);
            }
            this->result = res;
            parent->ringloop->wakeup();
        });
    }
};

std::function<bool(void)> cli_tool_t::start_alloc_osd(json11::Json cfg, uint64_t *out)
{
    json11::Json::array cmd = cfg["command"].array_items();
    auto alloc_osd = new alloc_osd_t();
    alloc_osd->parent = this;
    return [alloc_osd, out]()
    {
        alloc_osd->loop();
        if (alloc_osd->is_done())
        {
            if (out)
                *out = alloc_osd->new_id;
            else if (alloc_osd->new_id)
                printf("%lu\n", alloc_osd->new_id);
            delete alloc_osd;
            return true;
        }
        return false;
    };
}
