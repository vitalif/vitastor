// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include "str_util.h"
#include "http_client.h"

// Reweight OSD, change tags or set noout flag
struct osd_changer_t
{
    cli_tool_t *parent;

    uint64_t osd_num = 0;
    bool set_tags = false;
    std::vector<std::string> new_tags;
    bool set_reweight = false;
    double new_reweight = 1;
    bool set_noout = false;
    double new_noout = false;
    bool force = false;

    json11::Json::object osd_cfg;
    uint64_t osd_cfg_mod_rev = 0;
    json11::Json::array compare, success;

    int state = 0;
    std::function<bool(cli_result_t &)> cb;
    cli_result_t result;

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
        if (!osd_num)
        {
            result = (cli_result_t){ .err = EINVAL, .text = "OSD number is missing" };
            state = 100;
            return;
        }
        if (!set_tags && !set_reweight && !set_noout)
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Nothing to update" };
            state = 100;
            return;
        }
        if (set_reweight && new_reweight < 0)
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Reweight can't be negative" };
            state = 100;
            return;
        }
        parent->etcd_txn(json11::Json::object {
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/osd/stats/"+std::to_string(osd_num)) },
                    } },
                },
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/osd/"+std::to_string(osd_num)) },
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
        {
            auto osd_stats = parent->cli->st_cli.parse_etcd_kv(parent->etcd_result["responses"][0]["response_range"]["kvs"][0]).value;
            if (!osd_stats.is_object() && !force)
            {
                result = (cli_result_t){ .err = ENOENT, .text = "OSD "+std::to_string(osd_num)+" does not exist. Use --force to set configuration anyway" };
                state = 100;
                return;
            }
            auto kv = parent->cli->st_cli.parse_etcd_kv(parent->etcd_result["responses"][1]["response_range"]["kvs"][0]);
            osd_cfg_mod_rev = kv.mod_revision;
            osd_cfg = kv.value.object_items();
            if (set_reweight)
            {
                if (new_reweight != 1)
                    osd_cfg["reweight"] = new_reweight;
                else
                    osd_cfg.erase("reweight");
            }
            if (set_tags)
            {
                if (new_tags.size())
                    osd_cfg["tags"] = new_tags;
                else
                    osd_cfg.erase("tags");
            }
            if (set_noout)
            {
                if (new_noout)
                    osd_cfg["noout"] = true;
                else
                    osd_cfg.erase("noout");
            }
            compare.push_back(json11::Json::object {
                { "target", "MOD" },
                { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/osd/"+std::to_string(osd_num)) },
                { "result", "LESS" },
                { "mod_revision", osd_cfg_mod_rev+1 },
            });
            if (osd_cfg.size())
            {
                success.push_back(json11::Json::object {
                    { "request_delete_range", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/osd/"+std::to_string(osd_num)) },
                    } },
                });
            }
            else
            {
                success.push_back(json11::Json::object {
                    { "request_put", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/osd/"+std::to_string(osd_num)) },
                        { "value", base64_encode(json11::Json(osd_cfg).dump()) },
                    } },
                });
            }
        }
        parent->etcd_txn(json11::Json::object {
            { "compare", compare },
            { "success", success },
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
        if (!parent->etcd_result["succeeded"].bool_value())
        {
            result = (cli_result_t){ .err = EAGAIN, .text = "OSD "+std::to_string(osd_num)+" configuration was modified by someone else, please repeat your request" };
            state = 100;
            return;
        }
        result = (cli_result_t){
            .err = 0,
            .text = "OSD "+std::to_string(osd_num)+" configuration modified",
            .data = osd_cfg,
        };
        state = 100;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_modify_osd(json11::Json cfg)
{
    auto changer = new osd_changer_t();
    changer->parent = this;
    changer->osd_num = cfg["osd_num"].uint64_value();
    if (!cfg["tags"].is_null())
    {
        changer->set_tags = true;
        if (cfg["tags"].is_string())
        {
            if (cfg["tags"].string_value() != "")
                changer->new_tags = explode(",", cfg["tags"].string_value(), true);
        }
        else if (cfg["tags"].is_array())
        {
            for (auto item: cfg["tags"].array_items())
                changer->new_tags.push_back(item.as_string());
        }
    }
    if (!cfg["reweight"].is_null())
    {
        changer->set_reweight = true;
        changer->new_reweight = cfg["reweight"].number_value();
    }
    if (!cfg["noout"].is_null())
    {
        changer->set_noout = true;
        changer->new_noout = json_is_true(cfg["noout"]);
    }
    changer->force = cfg["force"].bool_value();
    return [changer](cli_result_t & result)
    {
        changer->loop();
        if (changer->is_done())
        {
            result = changer->result;
            delete changer;
            return true;
        }
        return false;
    };
}
