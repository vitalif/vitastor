// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <fcntl.h>
#include <algorithm>
#include "cli.h"
#include "cluster_client.h"
#include "str_util.h"

struct inode_rev_t
{
    inode_t inode_num;
    uint64_t meta_rev;
};

// Remove multiple images in correct order
struct wildcard_remover_t
{
    cli_tool_t *parent;

    json11::Json cfg;
    std::vector<std::string> globs;
    bool exact = false;

    json11::Json::array deleted_ids, deleted_images, rebased_images;
    std::map<inode_t, inode_t> chains; // child => parent pairs
    std::vector<std::vector<inode_rev_t>> versioned_chains;
    json11::Json::object sub_cfg;
    size_t i = 0;
    int state = 0;
    std::function<bool(cli_result_t &)> sub_cb;

    cli_result_t result;

    bool is_done()
    {
        return state == 100;
    }

    void join_chains()
    {
        bool changed = true;
        while (changed)
        {
            changed = false;
            auto ino_it = chains.begin();
            while (ino_it != chains.end())
            {
                auto child_id = ino_it->first;
                auto parent_id = ino_it->second;
                auto & parent_cfg = parent->cli->st_cli.inode_config.at(parent_id);
                if (parent_cfg.parent_id)
                {
                    auto chain_it = chains.find(parent_cfg.parent_id);
                    if (chain_it != chains.end())
                    {
                        changed = true;
                        ino_it->second = chain_it->second;
                        chains.erase(chain_it);
                    }
                }
                ino_it = chains.upper_bound(child_id);
            }
        }
        // Remember metadata modification revisions to check for parallel changes
        versioned_chains.clear();
        for (auto cp: chains)
        {
            auto child_id = cp.first;
            auto parent_id = cp.second;
            std::vector<inode_rev_t> ver_chain;
            do
            {
                auto & inode_cfg = parent->cli->st_cli.inode_config.at(child_id);
                ver_chain.push_back((inode_rev_t){ .inode_num = child_id, .meta_rev = inode_cfg.mod_revision });
                child_id = inode_cfg.parent_id;
            } while (child_id && child_id != parent_id);
            versioned_chains.push_back(std::move(ver_chain));
        }
        // Sort chains based on parent inode rank to first delete child-most layers
        std::map<inode_t, uint64_t> ranks;
        for (auto cp: chains)
        {
            auto parent_id = cp.second, cur_id = parent_id;
            uint64_t rank = 0;
            do
            {
                rank++;
                cur_id = parent->cli->st_cli.inode_config.at(cur_id).parent_id;
            } while (cur_id && cur_id != parent_id);
            ranks[parent_id] = rank;
        }
        std::sort(versioned_chains.begin(), versioned_chains.end(), [&](const std::vector<inode_rev_t> & a, const std::vector<inode_rev_t> & b)
        {
            return ranks[a.back().inode_num] > ranks[b.back().inode_num];
        });
    }

    void loop()
    {
        if (state == 0)
            goto resume_0;
        if (state == 1)
            goto resume_1;
        else if (state == 100)
            goto resume_100;
    resume_0:
        state = 0;
        chains.clear();
        // Select images to delete
        for (auto & ic: parent->cli->st_cli.inode_config)
        {
            for (auto & glob: globs)
            {
                if (exact ? (ic.second.name == glob) : stupid_glob(ic.second.name, glob))
                {
                    chains[ic.first] = ic.first;
                    break;
                }
            }
        }
        // Arrange them into chains
        join_chains();
        // Delete each chain
        i = 0;
        while (i < versioned_chains.size())
        {
            // Check for parallel changes
            for (auto & irev: versioned_chains[i])
            {
                auto inode_it = parent->cli->st_cli.inode_config.find(irev.inode_num);
                if (inode_it == parent->cli->st_cli.inode_config.end() ||
                    inode_it->second.mod_revision > irev.meta_rev)
                {
                    if (inode_it != parent->cli->st_cli.inode_config.end())
                        fprintf(stderr, "Warning: image %s modified by someone else during deletion, restarting wildcard deletion\n", inode_it->second.name.c_str());
                    else
                        fprintf(stderr, "Warning: inode %lx modified by someone else during deletion, retrying wildcard deletion\n", irev.inode_num);
                    goto resume_0;
                }
            }
            // Delete
            {
                auto from_cfg = parent->cli->st_cli.inode_config.at(versioned_chains[i].back().inode_num);
                auto to_cfg = parent->cli->st_cli.inode_config.at(versioned_chains[i].front().inode_num);
                sub_cfg = cfg.object_items();
                sub_cfg.erase("globs");
                sub_cfg.erase("exact");
                sub_cfg["from"] = from_cfg.name;
                sub_cfg["to"] = to_cfg.name;
                sub_cb = parent->start_rm(sub_cfg);
            }
resume_1:
            while (!sub_cb(result))
            {
                state = 1;
                return;
            }
            sub_cb = NULL;
            i++;
            merge_result();
            if (result.err)
            {
                break;
            }
        }
        state = 100;
        result.data = json11::Json::object{
            { "deleted_ids", deleted_ids },
            { "deleted_images", deleted_images },
            { "rebased_images", rebased_images },
        };
resume_100:
        // Done
        return;
    }

    void merge_result()
    {
        for (auto & item: result.data["deleted_ids"].array_items())
            deleted_ids.push_back(item);
        for (auto & item: result.data["deleted_images"].array_items())
            deleted_images.push_back(item == result.data["renamed_to"] ? result.data["renamed_from"] : item);
        for (auto & item: result.data["rebased_images"].array_items())
            rebased_images.push_back(item);
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_rm_wildcard(json11::Json cfg)
{
    auto wildcard_remover = new wildcard_remover_t();
    wildcard_remover->parent = this;
    wildcard_remover->cfg = cfg;
    for (auto & glob: cfg["globs"].array_items())
        wildcard_remover->globs.push_back(glob.string_value());
    wildcard_remover->exact = cfg["exact"].bool_value();
    return [wildcard_remover](cli_result_t & result)
    {
        wildcard_remover->loop();
        if (wildcard_remover->is_done())
        {
            result = wildcard_remover->result;
            delete wildcard_remover;
            return true;
        }
        return false;
    };
}
