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
#include "cli_pool_cfg.h"
#include "cluster_client.h"
#include "str_util.h"

struct pool_changer_t
{
    cli_tool_t *parent;

    // Required parameters (id/name)

    pool_id_t pool_id = 0;
    std::string pool_name;

    // Force removal
    bool force;

    // Pool configurator
    pool_configurator_t *cfg;

    int state = 0;
    cli_result_t result;

    // Config of pool to be modified
    pool_config_t *pool_config = NULL;

    // Tags
    json11::Json osd_tags_json;
    json11::Json primary_affinity_tags_json;

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

        // Validate pool name/id

        // Get pool id by name (if name given)
        if (pool_name != "")
        {
            for (auto & pce: parent->cli->st_cli.pool_config)
            {
                if (pce.second.name == pool_name)
                {
                    pool_id = pce.first;
                    pool_config = &(pce.second);
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
            auto pce = parent->cli->st_cli.pool_config.find(pool_id);
            if (pce != parent->cli->st_cli.pool_config.end())
            {
                pool_config = &(pce->second);
            }
        }

        // Need pool config to proceed
        if (!pool_config)
        {
            result = (cli_result_t){ .err = ENOENT, .text = "Pool "+pool_name+" does not exist" };
            state = 100;
            return;
        }

        // Validate pool parameters
        if (!cfg->validate(parent->cli->st_cli, pool_config, !force))
        {
            result = (cli_result_t){ .err = EINVAL, .text = cfg->get_error_string() + "\n" };
            state = 100;
            return;
        }

        // OSD tags
        if (cfg->osd_tags != "")
        {
            osd_tags_json = parent->parse_tags(cfg->osd_tags);
        }

        // Primary affinity tags
        if (cfg->primary_affinity_tags != "")
        {
            primary_affinity_tags_json = parent->parse_tags(cfg->primary_affinity_tags);
        }

        // Proceed to modifying the pool
        state = 1;
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

            // Update pool

            auto pls = kv.value.object_items();
            auto p = pls[std::to_string(pool_id)].object_items();

            if (cfg->name != "")
                p["name"] = cfg->name;
            if (cfg->pg_size)
                p["pg_size"] = cfg->pg_size;
            if (cfg->pg_minsize)
                p["pg_minsize"] = cfg->pg_minsize;
            if (cfg->pg_count)
                p["pg_count"] = cfg->pg_count;
            if (cfg->failure_domain != "")
                p["failure_domain"] = cfg->failure_domain;
            if (cfg->max_osd_combinations)
                p["max_osd_combinations"] = cfg->max_osd_combinations;
            if (cfg->block_size)
                p["block_size"] = cfg->block_size;
            if (cfg->bitmap_granularity)
                p["bitmap_granularity"] = cfg->bitmap_granularity;
            if (cfg->immediate_commit != "")
                p["immediate_commit"] = cfg->immediate_commit;
            if (cfg->pg_stripe_size)
                p["pg_stripe_size"] = cfg->pg_stripe_size;
            if (cfg->root_node != "")
                p["root_node"] = cfg->root_node;
            if (cfg->scrub_interval != "")
                p["scrub_interval"] = cfg->scrub_interval;
            if (cfg->osd_tags != "")
                p["osd_tags"] = osd_tags_json;
            if (cfg->primary_affinity_tags != "")
                p["primary_affinity_tags"] = primary_affinity_tags_json;

            pls[std::to_string(pool_id)] = p;

            // Record updated pools
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

        // Successfully updated pool
        result = (cli_result_t){
            .err = 0,
            .text = "Pool "+pool_name+" updated",
            .data = new_pools
        };
        state = 100;
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_pool_modify(json11::Json cfg)
{
    auto pool_changer = new pool_changer_t();
    pool_changer->parent = this;

    // Pool name (or id) required
    if (!cfg["pool"].uint64_value() && cfg["pool"].as_string() == "")
    {
        return [](cli_result_t & result)
        {
            result = (cli_result_t){
                .err = EINVAL, .text = "Pool name or id must be given\n"
            };
            return true;
        };
    }

    pool_changer->pool_id = cfg["pool"].uint64_value();
    pool_changer->pool_name = pool_changer->pool_id ? "" : cfg["pool"].as_string();

    pool_changer->cfg = new pool_configurator_t();
    if (!pool_changer->cfg->parse(cfg, false))
    {
        std::string err = pool_changer->cfg->get_error_string();
        return [err](cli_result_t & result)
        {
            result = (cli_result_t){ .err = EINVAL, .text = err + "\n" };
            return true;
        };
    }

    pool_changer->force = !cfg["force"].is_null();

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
