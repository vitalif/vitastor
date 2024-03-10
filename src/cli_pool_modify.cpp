// Copyright (c) MIND Software LLC, 2023 (info@mindsw.io)
// I accept Vitastor CLA: see CLA-en.md for details
// Copyright (c) Vitaliy Filippov, 2024
// License: VNPL-1.1 (see README.md for details)

#include <ctype.h>
#include "cli.h"
#include "cli_pool_cfg.h"
#include "cluster_client.h"
#include "str_util.h"

struct pool_changer_t
{
    cli_tool_t *parent;

    // Required parameters (id/name)
    pool_id_t pool_id = 0;
    std::string pool_name;
    json11::Json::object cfg;
    json11::Json::object new_cfg;
    bool force = false;

    json11::Json old_cfg;

    int state = 0;
    cli_result_t result;

    // Updated pools
    json11::Json new_pools;

    // Expected pools mod revision
    uint64_t pools_mod_rev;

    bool is_done() { return state == 100; }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        else if (state == 2)
            goto resume_2;
        pool_id = stoull_full(cfg["old_name"].string_value());
        if (!pool_id)
        {
            pool_name = cfg["old_name"].string_value();
            if (pool_name == "")
            {
                result = (cli_result_t){ .err = ENOENT, .text = "Pool ID or name is required to modify it" };
                state = 100;
                return;
            }
        }
resume_0:
        // Get pools from etcd
        parent->etcd_txn(json11::Json::object {
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_range", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/pools") },
                    } }
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
            // Parse received pools from etcd
            auto kv = parent->cli->st_cli.parse_etcd_kv(parent->etcd_result["responses"][0]["response_range"]["kvs"][0]);

            // Get pool by name or ID
            old_cfg = json11::Json();
            if (pool_name != "")
            {
                for (auto & pce: kv.value.object_items())
                {
                    if (pce.second["name"] == pool_name)
                    {
                        pool_id = stoull_full(pce.first);
                        old_cfg = pce.second;
                        break;
                    }
                }
            }
            else
            {
                pool_name = std::to_string(pool_id);
                old_cfg = kv.value[pool_name];
            }
            if (!old_cfg.is_object())
            {
                result = (cli_result_t){ .err = ENOENT, .text = "Pool "+pool_name+" does not exist" };
                state = 100;
                return;
            }

            // Update pool
            new_cfg = cfg;
            result.text = validate_pool_config(new_cfg, old_cfg, parent->cli->st_cli.global_block_size,
                parent->cli->st_cli.global_bitmap_granularity, force);
            if (result.text != "")
            {
                result.err = EINVAL;
                state = 100;
                return;
            }

            if (new_cfg.find("used_for_fs") != new_cfg.end() && !force)
            {
                // Check that pool doesn't have images
                auto img_it = parent->cli->st_cli.inode_config.lower_bound(INODE_WITH_POOL(pool_id, 0));
                if (img_it != parent->cli->st_cli.inode_config.end() && INODE_POOL(img_it->first) == pool_id &&
                    img_it->second.name == new_cfg["used_for_fs"].string_value())
                {
                    // Only allow metadata image to exist in the FS pool
                    img_it++;
                }
                if (img_it != parent->cli->st_cli.inode_config.end() && INODE_POOL(img_it->first) == pool_id)
                {
                    result = (cli_result_t){ .err = ENOENT, .text = "Pool "+pool_name+" has block images, delete them before using it for VitastorFS" };
                    state = 100;
                    return;
                }
            }

            // Update pool
            auto pls = kv.value.object_items();
            pls[std::to_string(pool_id)] = new_cfg;
            new_pools = pls;

            // Expected pools mod revision
            pools_mod_rev = kv.mod_revision;
        }
        // Update pools in etcd
        parent->etcd_txn(json11::Json::object {
            { "compare", json11::Json::array {
                json11::Json::object {
                    { "target", "MOD" },
                    { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/pools") },
                    { "result", "LESS" },
                    { "mod_revision", pools_mod_rev+1 },
                }
            } },
            { "success", json11::Json::array {
                json11::Json::object {
                    { "request_put", json11::Json::object {
                        { "key", base64_encode(parent->cli->st_cli.etcd_prefix+"/config/pools") },
                        { "value", base64_encode(new_pools.dump()) },
                    } },
                },
            } },
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
            // CAS failure - retry
            fprintf(stderr, "Warning: pool configuration was modified in the meantime by someone else\n");
            goto resume_0;
        }
        // Successfully updated pool
        result = (cli_result_t){
            .err = 0,
            .text = "Pool "+pool_name+" updated",
            .data = new_pools,
        };
        state = 100;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_pool_modify(json11::Json cfg)
{
    auto pool_changer = new pool_changer_t();
    pool_changer->parent = this;
    pool_changer->cfg = cfg.object_items();
    pool_changer->force = cfg["force"].bool_value();
    return [pool_changer](cli_result_t & result)
    {
        pool_changer->loop();
        if (pool_changer->is_done())
        {
            result = pool_changer->result;
            delete pool_changer;
            return true;
        }
        return false;
    };
}
