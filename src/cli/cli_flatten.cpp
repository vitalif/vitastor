// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include <sys/stat.h>

// Flatten a layer: merge all parents into a layer and break the connection completely
struct snap_flattener_t
{
    cli_tool_t *parent;

    // target to flatten
    std::string target_name;
    // writers are stopped, we can safely change writable layers
    bool writers_stopped = false;
    // use CAS writes (0 = never, 1 = auto, 2 = always)
    int use_cas = 1;
    // interval between fsyncs
    int fsync_interval = 128;

    std::string top_parent_name;
    inode_t target_id = 0;
    int state = 0;
    std::function<bool(cli_result_t &)> merger_cb;
    cli_result_t result;

    void get_merge_parents()
    {
        // Get all parents of target
        inode_config_t *target_cfg = parent->get_inode_cfg(target_name);
        if (!target_cfg)
        {
            result = (cli_result_t){ .err = ENOENT, .text = "Layer "+target_name+" not found" };
            state = 100;
            return;
        }
        target_id = target_cfg->num;
        std::vector<inode_t> chain_list;
        inode_config_t *cur = target_cfg;
        chain_list.push_back(cur->num);
        while (cur->parent_id != 0 && cur->parent_id != target_cfg->num)
        {
            auto it = parent->cli->st_cli.inode_config.find(cur->parent_id);
            if (it == parent->cli->st_cli.inode_config.end())
            {
                result = (cli_result_t){
                    .err = ENOENT,
                    .text = "Parent inode of layer "+cur->name+" (id "+std::to_string(cur->parent_id)+") does not exist",
                    .data = json11::Json::object {
                        { "error", "parent-not-found" },
                        { "inode_id", cur->num },
                        { "inode_name", cur->name },
                        { "parent_id", cur->parent_id },
                    },
                };
                state = 100;
                return;
            }
            cur = &it->second;
            chain_list.push_back(cur->num);
        }
        if (cur->parent_id != 0)
        {
            result = (cli_result_t){ .err = EBADF, .text = "Layer "+target_name+" has a loop in parents" };
            state = 100;
            return;
        }
        top_parent_name = cur->name;
    }

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
        else if (state == 3)
            goto resume_3;
        if (target_name == "")
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Layer to flatten not specified" };
            state = 100;
            return;
        }
        // Get parent layers
        get_merge_parents();
        if (state == 100)
            return;
        // Start merger
        merger_cb = parent->start_merge(json11::Json::object {
            { "from", top_parent_name },
            { "to", target_name },
            { "target", target_name },
            { "delete-source", false },
            { "cas", use_cas },
            { "fsync-interval", fsync_interval },
        });
        // Wait for it
resume_1:
        while (!merger_cb(result))
        {
            state = 1;
            return;
        }
        merger_cb = NULL;
        if (result.err)
        {
            state = 100;
            return;
        }
        // Change parent
        parent->change_parent(target_id, 0, &result);
        // Wait for it to complete
        state = 2;
resume_2:
        if (parent->waiting > 0)
            return;
        state = 3;
resume_3:
        // Done
        state = 100;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_flatten(json11::Json cfg)
{
    auto flattener = new snap_flattener_t();
    flattener->parent = this;
    flattener->target_name = cfg["image"].string_value();
    flattener->fsync_interval = cfg["fsync_interval"].uint64_value();
    if (!flattener->fsync_interval)
        flattener->fsync_interval = 128;
    if (!cfg["cas"].is_null())
        flattener->use_cas = cfg["cas"].uint64_value() ? 2 : 0;
    return [flattener](cli_result_t & result)
    {
        flattener->loop();
        if (flattener->is_done())
        {
            result = flattener->result;
            delete flattener;
            return true;
        }
        return false;
    };
}
