// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"

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
    std::function<bool(void)> merger_cb;

    void get_merge_parents()
    {
        // Get all parents of target
        inode_config_t *target_cfg = parent->get_inode_cfg(target_name);
        target_id = target_cfg->num;
        std::vector<inode_t> chain_list;
        inode_config_t *cur = target_cfg;
        chain_list.push_back(cur->num);
        while (cur->parent_id != 0 && cur->parent_id != target_cfg->num)
        {
            auto it = parent->cli->st_cli.inode_config.find(cur->parent_id);
            if (it == parent->cli->st_cli.inode_config.end())
            {
                fprintf(stderr, "Parent inode of layer %s (id %ld) not found\n", cur->name.c_str(), cur->parent_id);
                exit(1);
            }
            cur = &it->second;
            chain_list.push_back(cur->num);
        }
        if (cur->parent_id != 0)
        {
            fprintf(stderr, "Layer %s has a loop in parents\n", target_name.c_str());
            exit(1);
        }
        top_parent_name = cur->name;
    }

    bool is_done()
    {
        return state == 5;
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        else if (state == 2)
            goto resume_2;
        else if (state == 3)
            goto resume_3;
        // Get parent layers
        get_merge_parents();
        // Start merger
        merger_cb = parent->start_merge(json11::Json::object {
            { "command", json11::Json::array{ "merge-data", top_parent_name, target_name } },
            { "target", target_name },
            { "delete-source", false },
            { "cas", use_cas },
            { "fsync-interval", fsync_interval },
        });
        // Wait for it
resume_1:
        while (!merger_cb())
        {
            state = 1;
            return;
        }
        merger_cb = NULL;
        // Change parent
        parent->change_parent(target_id, 0);
        // Wait for it to complete
        state = 2;
resume_2:
        if (parent->waiting > 0)
            return;
        state = 3;
resume_3:
        // Done
        return;
    }
};

std::function<bool(void)> cli_tool_t::start_flatten(json11::Json cfg)
{
    json11::Json::array cmd = cfg["command"].array_items();
    auto flattener = new snap_flattener_t();
    flattener->parent = this;
    flattener->target_name = cmd.size() > 1 ? cmd[1].string_value() : "";
    if (flattener->target_name == "")
    {
        fprintf(stderr, "Layer to flatten argument is missing\n");
        exit(1);
    }
    flattener->fsync_interval = cfg["fsync-interval"].uint64_value();
    if (!flattener->fsync_interval)
        flattener->fsync_interval = 128;
    if (!cfg["cas"].is_null())
        flattener->use_cas = cfg["cas"].uint64_value() ? 2 : 0;
    return [flattener]()
    {
        flattener->loop();
        if (flattener->is_done())
        {
            delete flattener;
            return true;
        }
        return false;
    };
}
