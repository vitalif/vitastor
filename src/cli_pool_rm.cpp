/*
 =========================================================================
 Copyright (c) 2023 MIND Software LLC. All Rights Reserved.
 This file is part of the Software-Defined Storage MIND UStor Project.
 For more information about this product, please visit https://mindsw.io
 or contact us directly at info@mindsw.io
 =========================================================================
 */

#include <ctype.h>
#include "cli.h"
#include "cluster_client.h"
#include "str_util.h"

struct pool_remover_t
{
    cli_tool_t *parent;

    // Required parameters (id/name)

    pool_id_t pool_id = 0;
    std::string pool_name;

    // Force removal
    bool force;

    int state = 0;
    cli_result_t result;

    // Is pool valid?
    bool pool_valid = false;

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
        else if (state == 3)
            goto resume_3;

        // Pool name (or id) required
        if (!pool_id && pool_name == "")
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Pool name or id must be given\n" };
            state = 100;
            return;
        }

        // Validate pool name/id

        // Get pool id by name (if name given)
        if (pool_name != "")
        {
            for (auto & ic: parent->cli->st_cli.pool_config)
            {
                if (ic.second.name == pool_name)
                {
                    pool_id = ic.first;
                    pool_valid = 1;
                    break;
                }
            }
        }
        // Otherwise, check if given pool id is valid
        else
        {
            // Set pool name from id (for easier logging)
            pool_name = "id " + std::to_string(pool_id);

            // Look-up pool id in pool_config
            if (parent->cli->st_cli.pool_config.find(pool_id) != parent->cli->st_cli.pool_config.end())
            {
                pool_valid = 1;
            }
        }

        // Need a valid pool to proceed
        if (!pool_valid)
        {
            result = (cli_result_t){ .err = ENOENT, .text = "Pool "+pool_name+" does not exist" };
            state = 100;
            return;
        }

        // Unless forced, check if pool has associated Images/Snapshots
        if (!force)
        {
            std::string images;

            for (auto & ic: parent->cli->st_cli.inode_config)
            {
                if (pool_id && INODE_POOL(ic.second.num) != pool_id)
                {
                    continue;
                }
                images += ((images != "") ? ", " : "") + ic.second.name;
            }

            if (images != "")
            {
                result = (cli_result_t){
                    .err = ENOTEMPTY,
                    .text =
                        "Pool "+pool_name+" cannot be removed as it still has the following "
                        "images/snapshots associated with it: "+images
                };
                state = 100;
                return;
            }
        }

        // Proceed to deleting the pool
        state = 1;
        do
        {
resume_1:
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
            {
                // Parse received pools from etcd
                auto kv = parent->cli->st_cli.parse_etcd_kv(parent->etcd_result["responses"][0]["response_range"]["kvs"][0]);

                // Remove pool
                auto p = kv.value.object_items();
                if (p.erase(std::to_string(pool_id)) != 1)
                {
                    result = (cli_result_t){
                            .err = ENOENT,
                            .text = "Failed to erase pool "+pool_name+" from: "+kv.value.string_value()
                    };
                    state = 100;
                    return;
                }

                // Record updated pools
                new_pools = p;

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
            state = 3;
resume_3:
            if (parent->waiting > 0)
                return;
            if (parent->etcd_err.err)
            {
                result = parent->etcd_err;
                state = 100;
                return;
            }
        } while (!parent->etcd_result["succeeded"].bool_value());

        // Successfully deleted pool
        result = (cli_result_t){
            .err = 0,
            .text = "Pool "+pool_name+" deleted",
            .data = new_pools
        };
        state = 100;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_pool_rm(json11::Json cfg)
{
    auto pool_remover = new pool_remover_t();
    pool_remover->parent = this;

    pool_remover->pool_id = cfg["pool"].uint64_value();
    pool_remover->pool_name = pool_remover->pool_id ? "" : cfg["pool"].as_string();

    pool_remover->force = !cfg["force"].is_null();

    return [pool_remover](cli_result_t & result)
    {
        pool_remover->loop();
        if (pool_remover->is_done())
        {
            result = pool_remover->result;
            delete pool_remover;
            return true;
        }
        return false;
    };
}
