// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include "base64.h"

// Remove layer(s): similar to merge, but alters metadata and processes multiple merge targets
//
// Exactly one child of the requested layers may be merged using the "inverted" workflow,
// where we merge it "down" into one of the "to-be-removed" layers and then rename the
// "to-be-removed" layer to the child. It may be done either if all writers are stopped
// before trying to delete layers (which is signaled by --writers-stopped) or if that child
// is a read-only layer (snapshot) itself.
//
// This "inverted" workflow trades copying data of one of the deleted layers for copying
// data of one child of the chain which is also a child of the "traded" layer. So we
// choose the (parent,child) pair which has the largest difference between "parent" and
// "child" inode sizes.
//
// All other children of the chain are processed by iterating though them, merging removed
// parents into them and rebasing them to the last layer which isn't a member of the removed
// chain.
//
// Example:
//
// <parent> - <from> - <layer 2> - <to> - <child 1>
//                 \           \       \- <child 2>
//                  \           \- <child 3>
//                   \-<child 4>
//
// 1) Find optimal pair for the "reverse" scenario
//    Imagine that it's (<layer 2>, <child 1>) in this example
// 2) Process all children except <child 1>:
//    - Merge <from>..<to> to <child 2>
//    - Set <child 2> parent to <parent>
//    - Repeat for others
// 3) Process <child 1>:
//    - Merge <from>..<child 1> to <layer 2>
//    - Set <layer 2> parent to <parent>
//    - Rename <layer 2> to <child 1>
// 4) Delete other layers of the chain (<from>, <to>)
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

    std::map<inode_t,int> sources;
    std::map<inode_t,uint64_t> inode_used;
    std::vector<inode_t> merge_children;
    std::vector<inode_t> chain_list;
    std::map<inode_t,int> inverse_candidates;
    inode_t inverse_parent = 0, inverse_child = 0;
    inode_t new_parent = 0;
    int state = 0;
    int current_child = 0;
    std::function<bool(void)> cb;

    bool is_done()
    {
        return state == 9;
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
        else if (state == 6)
            goto resume_6;
        else if (state == 7)
            goto resume_7;
        else if (state == 8)
            goto resume_8;
        else if (state == 9)
            goto resume_9;
        // Get children to merge
        get_merge_children();
        // Try to select an inode for the "inverse" optimized scenario
        // Read statistics from etcd to do it
        read_stats();
        state = 1;
resume_1:
        if (parent->waiting > 0)
            return;
        choose_inverse_candidate();
        // Merge children one by one, except our "inverse" child
        for (current_child = 0; current_child < merge_children.size(); current_child++)
        {
            if (merge_children[current_child] == inverse_child)
                continue;
            start_merge_child(merge_children[current_child], merge_children[current_child]);
resume_2:
            while (!cb())
            {
                state = 2;
                return;
            }
            cb = NULL;
            parent->change_parent(merge_children[current_child], new_parent);
            state = 3;
resume_3:
            if (parent->waiting > 0)
                return;
        }
        // Merge our "inverse" child into our "inverse" parent
        if (inverse_child != 0)
        {
            start_merge_child(inverse_child, inverse_parent);
resume_4:
            while (!cb())
            {
                state = 4;
                return;
            }
            cb = NULL;
            // Delete "inverse" child data
            start_delete_source(inverse_child);
resume_5:
            while (!cb())
            {
                state = 5;
                return;
            }
            cb = NULL;
            // Delete "inverse" child metadata and rename parent over it
            rename_inverse_parent();
            state = 6;
resume_6:
            if (parent->waiting > 0)
                return;
        }
        // Delete parents, except the "inverse" one
        for (current_child = 0; current_child < chain_list.size(); current_child++)
        {
            if (chain_list[current_child] == inverse_parent)
                continue;
            start_delete_source(chain_list[current_child]);
resume_7:
            while (!cb())
            {
                state = 7;
                return;
            }
            cb = NULL;
            delete_inode_config(chain_list[current_child]);
            state = 8;
resume_8:
            if (parent->waiting > 0)
                return;
        }
        state = 9;
resume_9:
        // Done
        return;
    }

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
                if (ic.second.readonly || writers_stopped)
                {
                    inverse_candidates[ic.second.num] = it->second;
                }
            }
        }
    }

    void read_stats()
    {
        if (inverse_candidates.size() == 0)
        {
            return;
        }
        json11::Json::array reads;
        for (auto cp: inverse_candidates)
        {
            inode_t inode = cp.first;
            reads.push_back(json11::Json::object {
                { "request_range", json11::Json::object {
                    { "key", base64_encode(
                        parent->cli->st_cli.etcd_prefix+
                        "/inode/stats/"+std::to_string(INODE_POOL(inode))+
                        "/"+std::to_string(INODE_NO_POOL(inode))
                    ) },
                } }
            });
        }
        for (auto cp: sources)
        {
            inode_t inode = cp.first;
            reads.push_back(json11::Json::object {
                { "request_range", json11::Json::object {
                    { "key", base64_encode(
                        parent->cli->st_cli.etcd_prefix+
                        "/inode/stats/"+std::to_string(INODE_POOL(inode))+
                        "/"+std::to_string(INODE_NO_POOL(inode))
                    ) },
                } }
            });
        }
        parent->waiting++;
        parent->cli->st_cli.etcd_txn(json11::Json::object {
            { "success", reads },
        }, ETCD_SLOW_TIMEOUT, [this](std::string err, json11::Json data)
        {
            parent->waiting--;
            if (err != "")
            {
                fprintf(stderr, "Error reading layer statistics from etcd: %s\n", err.c_str());
                exit(1);
            }
            for (auto inode_result: data["responses"].array_items())
            {
                auto kv = parent->cli->st_cli.parse_etcd_kv(inode_result["kvs"][0]);
                pool_id_t pool_id = 0;
                inode_t inode = 0;
                char null_byte = 0;
                sscanf(kv.key.c_str() + parent->cli->st_cli.etcd_prefix.length()+13, "%u/%lu%c", &pool_id, &inode, &null_byte);
                if (!inode || null_byte != 0)
                {
                    fprintf(stderr, "Bad key returned from etcd: %s\n", kv.key.c_str());
                    exit(1);
                }
                auto pool_cfg_it = parent->cli->st_cli.pool_config.find(pool_id);
                if (pool_cfg_it == parent->cli->st_cli.pool_config.end())
                {
                    fprintf(stderr, "Pool %u does not exist\n", pool_id);
                    exit(1);
                }
                inode = INODE_WITH_POOL(pool_id, inode);
                auto & pool_cfg = pool_cfg_it->second;
                uint64_t used_bytes = kv.value["raw_used"].uint64_value() / pool_cfg.pg_size;
                if (pool_cfg.scheme != POOL_SCHEME_REPLICATED)
                {
                    used_bytes *= (pool_cfg.pg_size - pool_cfg.parity_chunks);
                }
                inode_used[inode] = used_bytes;
            }
            parent->ringloop->wakeup();
        });
    }

    void choose_inverse_candidate()
    {
        uint64_t max_diff = 0;
        for (auto cp: inverse_candidates)
        {
            inode_t child = cp.first;
            uint64_t child_used = inode_used[child];
            int rank = cp.second;
            for (int i = chain_list.size()-rank; i < chain_list.size(); i++)
            {
                inode_t parent = chain_list[i];
                uint64_t parent_used = inode_used[parent];
                if (parent_used > child_used && (!max_diff || max_diff < (parent_used-child_used)))
                {
                    max_diff = (parent_used-child_used);
                    inverse_parent = parent;
                    inverse_child = child;
                }
            }
        }
    }

    void rename_inverse_parent()
    {
        auto child_it = parent->cli->st_cli.inode_config.find(inverse_child);
        if (child_it == parent->cli->st_cli.inode_config.end())
        {
            fprintf(stderr, "Inode %ld disappeared\n", inverse_child);
            exit(1);
        }
        auto target_it = parent->cli->st_cli.inode_config.find(inverse_parent);
        if (target_it == parent->cli->st_cli.inode_config.end())
        {
            fprintf(stderr, "Inode %ld disappeared\n", inverse_parent);
            exit(1);
        }
        inode_config_t *child_cfg = &child_it->second;
        inode_config_t *target_cfg = &target_it->second;
        std::string child_name = child_cfg->name;
        std::string target_name = target_cfg->name;
        std::string child_cfg_key = base64_encode(
            parent->cli->st_cli.etcd_prefix+
            "/config/inode/"+std::to_string(INODE_POOL(inverse_child))+
            "/"+std::to_string(INODE_NO_POOL(inverse_child))
        );
        std::string target_cfg_key = base64_encode(
            parent->cli->st_cli.etcd_prefix+
            "/config/inode/"+std::to_string(INODE_POOL(inverse_parent))+
            "/"+std::to_string(INODE_NO_POOL(inverse_parent))
        );
        json11::Json::object new_cfg = json11::Json::object {
            { "name", child_cfg->name },
            { "size", child_cfg->size },
        };
        if (new_parent)
        {
            if (INODE_POOL(inverse_parent) != INODE_POOL(new_parent))
                new_cfg["parent_pool"] = (uint64_t)INODE_POOL(new_parent);
            new_cfg["parent_id"] = (uint64_t)INODE_NO_POOL(new_parent);
        }
        if (child_cfg->readonly)
        {
            new_cfg["readonly"] = true;
        }
        parent->waiting++;
        parent->cli->st_cli.etcd_txn(json11::Json::object {
            { "compare", json11::Json::array {
                json11::Json::object {
                    { "target", "MOD" },
                    { "key", child_cfg_key },
                    { "result", "LESS" },
                    { "mod_revision", child_cfg->mod_revision+1 },
                },
                json11::Json::object {
                    { "target", "MOD" },
                    { "key", target_cfg_key },
                    { "result", "LESS" },
                    { "mod_revision", target_cfg->mod_revision+1 },
                },
            } },
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_delete_range", json11::Json::object {
                        { "key", child_cfg_key },
                    } },
                    { "request_put", json11::Json::object {
                        { "key", target_cfg_key },
                        { "value", base64_encode(json11::Json(new_cfg).dump()) },
                    } }
                },
            } },
        }, ETCD_SLOW_TIMEOUT, [this, target_name, child_name](std::string err, json11::Json res)
        {
            parent->waiting--;
            if (err != "")
            {
                fprintf(stderr, "Error renaming %s to %s: %s\n", target_name.c_str(), child_name.c_str(), err.c_str());
                exit(1);
            }
            if (!res["succeeded"].bool_value())
            {
                fprintf(stderr, "Layer %s or %s configuration was modified during renaming\n", target_name.c_str(), child_name.c_str());
                exit(1);
            }
            printf("Layer %s renamed to %s\n", target_name.c_str(), child_name.c_str());
            parent->ringloop->wakeup();
        });
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
                    } },
                },
            } },
        }, ETCD_SLOW_TIMEOUT, [this, cur_name](std::string err, json11::Json res)
        {
            parent->waiting--;
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
            parent->ringloop->wakeup();
        });
    }

    void start_merge_child(inode_t child_inode, inode_t target_inode)
    {
        auto child_it = parent->cli->st_cli.inode_config.find(child_inode);
        if (child_it == parent->cli->st_cli.inode_config.end())
        {
            fprintf(stderr, "Inode %ld disappeared\n", child_inode);
            exit(1);
        }
        auto target_it = parent->cli->st_cli.inode_config.find(target_inode);
        if (target_it == parent->cli->st_cli.inode_config.end())
        {
            fprintf(stderr, "Inode %ld disappeared\n", target_inode);
            exit(1);
        }
        cb = parent->start_merge(json11::Json::object {
            { "command", json11::Json::array{ "merge", from_name, child_it->second.name } },
            { "target", target_it->second.name },
            { "delete-source", false },
            { "cas", use_cas },
            { "fsync-interval", fsync_interval },
        });
    }

    void start_delete_source(inode_t inode)
    {
        auto source = parent->cli->st_cli.inode_config.find(inode);
        if (source == parent->cli->st_cli.inode_config.end())
        {
            fprintf(stderr, "Inode %ld disappeared\n", inode);
            exit(1);
        }
        cb = parent->start_rm(json11::Json::object {
            { "inode", inode },
            { "pool", (uint64_t)INODE_POOL(inode) },
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
    if (!cfg["writers_stopped"].is_null())
        snap_remover->writers_stopped = true;
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
