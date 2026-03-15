// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <algorithm>
#include "cli.h"
#include "cluster_client.h"
#include "str_util.h"
#include "json_util.h"

// List users
struct user_lister_t
{
    cli_tool_t *parent;
    std::vector<std::string> only_names;

    int state = 0;
    cli_result_t result;
    json11::Json::array users;

    bool is_done()
    {
        return state == 100;
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        if (state == 100)
            return;
        {
            json11::Json::array select;
            if (!only_names.size())
            {
                select.push_back(json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(
                            parent->cli->st_cli->etcd_prefix+"/config/user/"
                        ) },
                        { "range_end", base64_encode(
                            parent->cli->st_cli->etcd_prefix+"/config/user0"
                        ) },
                    } },
                });
            }
            else
            {
                for (auto & name: only_names)
                {
                    select.push_back(json11::Json::object {
                        { "request_range", json11::Json::object {
                            { "key", base64_encode(parent->cli->st_cli->etcd_prefix+"/config/user/"+name) },
                        } }
                    });
                }
            }
            parent->etcd_txn(json11::Json::object {
                { "success", select },
            });
        }
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
        for (auto & response: parent->etcd_result["responses"].array_items())
        {
            for (auto & kv_item: response["response_range"]["kvs"].array_items())
            {
                auto kv = parent->cli->st_cli->parse_etcd_kv(kv_item);
                auto user = kv.value.object_items();
                user["name"] = kv.key.substr(parent->cli->st_cli->etcd_prefix.size()+13);
                if (!parent->json_output)
                    user["groups_fmt"] = implode(",", user["groups"].array_items());
                users.push_back(std::move(user));
            }
        }
        if (parent->json_output)
        {
            // JSON output
            result.data = users;
            state = 100;
            return;
        }
        // Table output: name, type, groups
        json11::Json::array cols;
        cols.push_back(json11::Json::object{
            { "key", "name" },
            { "title", "NAME" },
        });
        cols.push_back(json11::Json::object{
            { "key", "type" },
            { "title", "TYPE" },
        });
        cols.push_back(json11::Json::object{
            { "key", "groups_fmt" },
            { "title", "GROUPS" },
        });
        result.text = print_table(users, cols, parent->color);
        state = 100;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_user_ls(json11::Json cfg)
{
    auto lister = new user_lister_t();
    lister->parent = this;
    if (cfg["names"].is_string())
        lister->only_names.push_back(cfg["names"].string_value());
    for (auto & item: cfg["names"].array_items())
        lister->only_names.push_back(item.string_value());
    return [lister](cli_result_t & result)
    {
        lister->loop();
        if (lister->is_done())
        {
            result = lister->result;
            delete lister;
            return true;
        }
        return false;
    };
}
