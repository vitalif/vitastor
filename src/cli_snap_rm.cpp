// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include "base64.h"

// Remove layer(s): similar to merge, but alters metadata and processes multiple merge targets
// If the requested snapshot chain has only 1 child and --writers-stopped is specified
// then that child can be merged "down" into the snapshot chain.
// Otherwise we iterate over all children of the chain, merge removed parents into them,
// and delete children afterwards.
//
// Example:
//
// <parent> - <from> - <layer 2> - <to> - <child 1>
//                 \           \       \- <child 2>
//                  \           \- <child 3>
//                   \-<child 4>
//
// 1) Merge <from>..<to> to <child 2>
// 2) Set <child 2> parent to <parent>
// 3) Variant #1, trickier, beneficial when <child 1> has less data than <to>
//    (not implemented yet):
//    - Merge <to>..<child 1> to <to>
//    - Rename <to> to <child 1>
//      It can be done without extra precautions if <child 1> is a read-only layer itself
//      Otherwise it should be either done offline or by pausing writers
//    - <to> is now deleted, repeat deletion with <from>..<layer 2>
// 4) Variant #2, simple:
//    - Repeat 1-2 with <child 1>
//    - Delete <to>
// 5) Process all other children
struct snap_remover_t
{
    cli_tool_t *parent;

    // remove from..to
    std::string from_name, to_name;
    // writers are stopped, we can safely change writable layers
    bool writers_stopped = false;
    // use CAS writes (0 = never, 1 = auto, 2 = always)
    int use_cas = 1;
    // interval between fsyncs
    int fsync_interval = 128;

    std::vector<inode_t> merge_children;
    std::vector<inode_t> chain_list;
    inode_t new_parent = 0;
    int state = 0;
    int current_child = 0;
    std::function<bool(void)> cb;

    void get_merge_children()
    {
        // Get all children of from..to
        inode_config_t *from_cfg = parent->get_inode_cfg(from_name);
        inode_config_t *to_cfg = parent->get_inode_cfg(to_name);
        // Check that to_cfg is actually a child of from_cfg
        // FIXME de-copypaste the following piece of code with snap_merger_t
        inode_config_t *cur = to_cfg;
        chain_list.push_back(cur->num);
        while (cur->num != from_cfg->num && cur->parent_id != 0)
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
        if (cur->num != from_cfg->num)
        {
            fprintf(stderr, "Layer %s is not a child of %s\n", to_name.c_str(), from_name.c_str());
            exit(1);
        }
        new_parent = from_cfg->parent_id;
        // Calculate ranks
        std::map<inode_t,int> sources;
        int i = chain_list.size()-1;
        for (inode_t item: chain_list)
        {
            sources[item] = i--;
        }
        for (auto & ic: parent->cli->st_cli.inode_config)
        {
            if (!ic.second.parent_id)
            {
                continue;
            }
            auto it = sources.find(ic.second.parent_id);
            if (it != sources.end() && sources.find(ic.second.num) == sources.end())
            {
                merge_children.push_back(ic.second.num);
            }
        }
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
        else if (state == 4)
            goto resume_4;
        else if (state == 5)
            goto resume_5;
        // Get children to merge
        get_merge_children();
        // Merge children one by one
        for (current_child = 0; current_child < merge_children.size(); current_child++)
        {
            start_merge_child();
resume_1:
            while (!cb())
            {
                state = 1;
                return;
            }
            cb = NULL;
            parent->change_parent(merge_children[current_child], new_parent);
            state = 2;
resume_2:
            if (parent->waiting > 0)
                return;
        }
        // Delete sources
        for (current_child = 0; current_child < chain_list.size(); current_child++)
        {
            start_delete_source();
resume_3:
            while (!cb())
            {
                state = 3;
                return;
            }
            cb = NULL;
            delete_inode_config(chain_list[current_child]);
            state = 4;
resume_4:
            if (parent->waiting > 0)
                return;
        }
        state = 5;
resume_5:
        // Done
        return;
    }

    void delete_inode_config(inode_t cur)
    {
        auto cur_cfg_it = parent->cli->st_cli.inode_config.find(cur);
        if (cur_cfg_it == parent->cli->st_cli.inode_config.end())
        {
            fprintf(stderr, "Inode 0x%lx disappeared\n", cur);
            exit(1);
        }
        inode_config_t *cur_cfg = &cur_cfg_it->second;
        std::string cur_name = cur_cfg->name;
        std::string cur_cfg_key = base64_encode(parent->cli->st_cli.etcd_prefix+
            "/config/inode/"+std::to_string(INODE_POOL(cur))+
            "/"+std::to_string(INODE_NO_POOL(cur)));
        parent->waiting++;
        parent->cli->st_cli.etcd_txn(json11::Json::object {
            { "compare", json11::Json::array {
                json11::Json::object {
                    { "target", "MOD" },
                    { "key", cur_cfg_key },
                    { "result", "LESS" },
                    { "mod_revision", cur_cfg->mod_revision+1 },
                },
            } },
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_delete_range", json11::Json::object {
                        { "key", cur_cfg_key },
                    } }
                },
            } },
        }, ETCD_SLOW_TIMEOUT, [this, cur_name](std::string err, json11::Json res)
        {
            if (err != "")
            {
                fprintf(stderr, "Error deleting %s: %s\n", cur_name.c_str(), err.c_str());
                exit(1);
            }
            if (!res["succeeded"].bool_value())
            {
                fprintf(stderr, "Layer %s configuration was modified during deletion\n", cur_name.c_str());
                exit(1);
            }
            printf("Layer %s deleted\n", cur_name.c_str());
            parent->waiting--;
            parent->ringloop->wakeup();
        });
    }

    void start_merge_child()
    {
        auto target = parent->cli->st_cli.inode_config.find(merge_children[current_child]);
        if (target == parent->cli->st_cli.inode_config.end())
        {
            fprintf(stderr, "Inode %ld disappeared\n", merge_children[current_child]);
            exit(1);
        }
        cb = parent->start_merge(json11::Json::object {
            { "command", json11::Json::array{ "merge", from_name, target->second.name } },
            { "target", target->second.name },
            { "delete-source", false },
            { "cas", use_cas },
            { "fsync-interval", fsync_interval },
        });
    }

    void start_delete_source()
    {
        auto source = parent->cli->st_cli.inode_config.find(chain_list[current_child]);
        if (source == parent->cli->st_cli.inode_config.end())
        {
            fprintf(stderr, "Inode %ld disappeared\n", chain_list[current_child]);
            exit(1);
        }
        cb = parent->start_rm(json11::Json::object {
            { "inode", chain_list[current_child] },
            { "pool", (uint64_t)INODE_POOL(chain_list[current_child]) },
            { "fsync-interval", fsync_interval },
        });
    }
};

std::function<bool(void)> cli_tool_t::start_snap_rm(json11::Json cfg)
{
    json11::Json::array cmd = cfg["command"].array_items();
    auto snap_remover = new snap_remover_t();
    snap_remover->parent = this;
    snap_remover->from_name = cmd.size() > 1 ? cmd[1].string_value() : "";
    snap_remover->to_name = cmd.size() > 2 ? cmd[2].string_value() : "";
    if (snap_remover->from_name == "")
    {
        fprintf(stderr, "Layer to remove argument is missing\n");
        exit(1);
    }
    if (snap_remover->to_name == "")
    {
        snap_remover->to_name = snap_remover->from_name;
    }
    snap_remover->fsync_interval = cfg["fsync-interval"].uint64_value();
    if (!snap_remover->fsync_interval)
        snap_remover->fsync_interval = 128;
    if (!cfg["cas"].is_null())
        snap_remover->use_cas = cfg["cas"].uint64_value() ? 2 : 0;
    return [snap_remover]()
    {
        snap_remover->loop();
        if (snap_remover->is_done())
        {
            delete snap_remover;
            return true;
        }
        return false;
    };
}
