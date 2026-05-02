// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include "str_util.h"

// Create/update/delete a user
struct cli_modify_user_t
{
    cli_tool_t *parent;

    std::string user_name;
    std::string user_type;
    json11::Json groups;
    bool del = false;

    int state = 0;
    cli_result_t result;
    etcd_kv_t kv;
    json11::Json::object new_cfg;

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
        if (user_type != "client" && user_type != "admin")
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Unknown user type: "+user_type };
            state = 100;
            return;
        }
        if (groups.is_string())
        {
            groups = groups == "" ? std::vector<std::string>() : explode(",", groups.string_value(), true);
        }
        else if (groups.is_array())
        {
            for (auto & gr: groups.array_items())
            {
                if (!gr.is_string())
                {
                    result = (cli_result_t){ .err = EINVAL, .text = "Group names must be strings" };
                    state = 100;
                    return;
                }
            }
        }
        else if (!groups.is_null())
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Group names must be strings" };
            state = 100;
            return;
        }
        if (user_name == "")
        {
            result = (cli_result_t){ .err = EINVAL, .text = "User name must not be empty" };
            state = 100;
            return;
        }
        parent->etcd_txn(json11::Json::object {
            { "success", json11::Json::array { json11::Json::object {
                { "request_range", json11::Json::object {
                    { "key", base64_encode(parent->cli->st_cli->etcd_prefix+"/config/user/"+user_name) },
                } },
            } } }
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
        kv = parent->cli->st_cli->parse_etcd_kv(parent->etcd_result["responses"][0]["response_range"]["kvs"][0]);
        while (true)
        {
            new_cfg = kv.value.object_items();
            if (!kv.mod_revision && del)
            {
                result = (cli_result_t){ .err = ENOENT, .text = "User "+user_name+" does not exist" };
                state = 100;
                break;
            }
            if (!groups.is_null())
                new_cfg["groups"] = groups;
            if (user_type != "")
                new_cfg["type"] = user_type;
            if (!new_cfg["type"].is_string())
                new_cfg["type"] = "client";
            parent->etcd_txn(json11::Json::object {
                { "compare", json11::Json::array { json11::Json::object {
                    { "key", base64_encode(parent->cli->st_cli->etcd_prefix+"/config/user/"+user_name) },
                    { "target", kv.mod_revision ? "MOD" : "VERSION" },
                    { kv.mod_revision ? "mod_revision" : "version", kv.mod_revision },
                } } },
                { "success", json11::Json::array {
                    del ? json11::Json::object { { "request_delete_range", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli->etcd_prefix+"/config/user/"+user_name) },
                    } } } : json11::Json::object { { "request_put", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli->etcd_prefix+"/config/user/"+user_name) },
                        { "value", base64_encode(json11::Json(new_cfg).dump()) }
                    } } },
                } },
                { "failure", json11::Json::array { json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli->etcd_prefix+"/config/user/"+user_name) },
                    } },
                } } },
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
            if (parent->etcd_result["succeeded"].bool_value())
                break;
            kv = parent->cli->st_cli->parse_etcd_kv(parent->etcd_result["responses"][0]["response_range"]["kvs"][0]);
        }
        state = 100;
        result.text = del ? "User "+user_name+" removed" : "User "+user_name+" modified";
        new_cfg["name"] = user_name;
        result.data = del ? json11::Json::object{ { "ok", true } } : new_cfg;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_modify_user(json11::Json cfg)
{
    auto creator = new cli_modify_user_t();
    creator->parent = this;
    creator->user_name = cfg["name"].string_value();
    creator->user_type = cfg["type"].string_value();
    creator->groups = cfg["groups"];
    creator->del = cfg["remove"].bool_value();
    return [creator](cli_result_t & result)
    {
        creator->loop();
        if (creator->is_done())
        {
            result = creator->result;
            delete creator;
            return true;
        }
        return false;
    };
}
