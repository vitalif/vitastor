// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <ctype.h>
#include "cli.h"
#include "cluster_client.h"
#include "str_util.h"

#include <algorithm>

// Delete OSD metadata from etcd
struct rm_osd_t
{
    cli_tool_t *parent;

    std::vector<uint64_t> osd_ids;

    int state = 0;
    cli_result_t result;

    bool is_done()
    {
        return state == 100;
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        if (!osd_ids.size())
        {
            result = (cli_result_t){ .err = EINVAL, .text = "OSD numbers are not specified" };
            state = 100;
            return;
        }
        {
            json11::Json::array rm_items;
            for (auto osd_id: osd_ids)
            {
                if (!osd_id)
                {
                    result = (cli_result_t){ .err = EINVAL, .text = "OSD number can't be zero" };
                    state = 100;
                    return;
                }
                rm_items.push_back("/config/osd/"+std::to_string(osd_id));
                rm_items.push_back("/osd/stats/"+std::to_string(osd_id));
                rm_items.push_back("/osd/state/"+std::to_string(osd_id));
                rm_items.push_back("/osd/inodestats/"+std::to_string(osd_id));
                rm_items.push_back("/osd/space/"+std::to_string(osd_id));
            }
            for (int i = 0; i < rm_items.size(); i++)
            {
                rm_items[i] = json11::Json::object {
                    { "request_delete_range", json11::Json::object {
                        { "key", base64_encode(
                            parent->cli->st_cli.etcd_prefix+rm_items[i].string_value()
                        ) },
                    } },
                };
            }
            parent->etcd_txn(json11::Json::object { { "success", rm_items } });
        }
    resume_1:
        state = 1;
        if (parent->waiting > 0)
            return;
        if (parent->etcd_err.err)
        {
            result = parent->etcd_err;
            state = 100;
            return;
        }
        std::string ids = "";
        for (auto osd_id: osd_ids)
        {
            ids += (ids.size() ? ", " : "")+std::to_string(osd_id);
        }
        state = 100;
        result = (cli_result_t){
            .text = (osd_ids.size() > 1 ? "OSDs " : "OSD ")+ids+(osd_ids.size() > 1 ? " are" : " is")+" removed from etcd",
            .data = json11::Json::object{ { "osd_ids", osd_ids } },
        };
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_rm_osd(json11::Json cfg)
{
    auto rm_osd = new rm_osd_t();
    rm_osd->parent = this;
    if (cfg["osd_id"].is_number() || cfg["osd_id"].is_string())
        rm_osd->osd_ids.push_back(cfg["osd_id"].uint64_value());
    else
    {
        for (auto & id: cfg["osd_id"].array_items())
            rm_osd->osd_ids.push_back(id.uint64_value());
    }
    return [rm_osd](cli_result_t & result)
    {
        rm_osd->loop();
        if (rm_osd->is_done())
        {
            result = rm_osd->result;
            delete rm_osd;
            return true;
        }
        return false;
    };
}
