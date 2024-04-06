// Copyright (c) Vitaliy Filippov, 2024
// License: VNPL-1.1 (see README.md for details)

#include "cli_pool_cfg.h"
#include "etcd_state_client.h"
#include "str_util.h"

std::string validate_pool_config(json11::Json::object & new_cfg, json11::Json old_cfg,
    uint64_t global_block_size, uint64_t global_bitmap_granularity, bool force)
{
    // short option names
    if (new_cfg.find("count") != new_cfg.end())
    {
        new_cfg["pg_count"] = new_cfg["count"];
        new_cfg.erase("count");
    }
    if (new_cfg.find("size") != new_cfg.end())
    {
        new_cfg["pg_size"] = new_cfg["size"];
        new_cfg.erase("size");
    }

    // --ec shortcut
    if (new_cfg.find("ec") != new_cfg.end())
    {
        if (new_cfg.find("scheme") != new_cfg.end() ||
            new_cfg.find("pg_size") != new_cfg.end() ||
            new_cfg.find("parity_chunks") != new_cfg.end())
        {
            return "--ec can't be used with --pg_size, --parity_chunks or --scheme";
        }
        // pg_size = N+K
        // parity_chunks = K
        uint64_t data_chunks = 0, parity_chunks = 0;
        char null_byte = 0;
        int ret = sscanf(new_cfg["ec"].string_value().c_str(), "%ju+%ju%c", &data_chunks, &parity_chunks, &null_byte);
        if (ret != 2 || !data_chunks || !parity_chunks)
        {
            return "--ec should be <N>+<K> format (<N>, <K> - numbers)";
        }
        new_cfg.erase("ec");
        new_cfg["scheme"] = "ec";
        new_cfg["pg_size"] = data_chunks+parity_chunks;
        new_cfg["parity_chunks"] = parity_chunks;
    }

    if (old_cfg.is_null() && new_cfg["scheme"].string_value() == "")
    {
        // Default scheme
        new_cfg["scheme"] = "replicated";
    }
    if (new_cfg.find("pg_minsize") == new_cfg.end() && (old_cfg.is_null() || new_cfg.find("pg_size") != new_cfg.end()))
    {
        // Default pg_minsize
        if (new_cfg["scheme"] == "replicated")
        {
            // pg_minsize = (N+K > 2) ? 2 : 1
            new_cfg["pg_minsize"] = new_cfg["pg_size"].uint64_value() > 2 ? 2 : 1;
        }
        else // ec or xor
        {
            // pg_minsize = (K > 1) ? N + 1 : N
            new_cfg["pg_minsize"] = new_cfg["pg_size"].uint64_value() - new_cfg["parity_chunks"].uint64_value() +
                (new_cfg["parity_chunks"].uint64_value() > 1 ? 1 : 0);
        }
    }

    // Check integer values and unknown keys
    for (auto kv_it = new_cfg.begin(); kv_it != new_cfg.end(); )
    {
        auto & key = kv_it->first;
        auto & value = kv_it->second;
        if (key == "pg_size" || key == "parity_chunks" || key == "pg_minsize" ||
            key == "pg_count" || key == "max_osd_combinations" || key == "block_size" ||
            key == "bitmap_granularity" || key == "pg_stripe_size")
        {
            if (value.is_number() && value.uint64_value() != value.number_value() ||
                value.is_string() && !value.uint64_value() && value.string_value() != "0")
            {
                return key+" must be a non-negative integer";
            }
            value = value.uint64_value();
        }
        else if (key == "name" || key == "scheme" || key == "immediate_commit" ||
            key == "failure_domain" || key == "root_node" || key == "scrub_interval" || key == "used_for_fs" ||
            key == "raw_placement")
        {
            if (!value.is_string())
            {
                return key+" must be a string";
            }
        }
        else if (key == "level_placement")
        {
            // level=rule, level=rule, ...
            if (!value.is_object())
            {
                json11::Json::object obj;
                for (auto & item: explode(",", value.string_value(), true))
                {
                    auto pair = explode("=", item, true);
                    if (pair.size() >= 2)
                    {
                        obj[pair[0]] = pair[1];
                    }
                }
                if (obj.size())
                {
                    value = obj;
                }
                else
                {
                    new_cfg.erase(kv_it++);
                    continue;
                }
            }
        }
        else if (key == "osd_tags" || key == "primary_affinity_tags")
        {
            if (value.is_string())
            {
                value = explode(",", value.string_value(), true);
            }
        }
        else
        {
            // Unknown parameter
            new_cfg.erase(kv_it++);
            continue;
        }
        kv_it++;
    }

    // Merge with the old config
    if (!old_cfg.is_null())
    {
        for (auto & kv: old_cfg.object_items())
        {
            if (new_cfg.find(kv.first) == new_cfg.end())
            {
                new_cfg[kv.first] = kv.second;
            }
        }
    }

    // Check after merging
    if (new_cfg["scheme"] != "ec")
    {
        new_cfg.erase("parity_chunks");
    }
    if (new_cfg.find("used_for_fs") != new_cfg.end() && new_cfg["used_for_fs"].string_value() == "")
    {
        new_cfg.erase("used_for_fs");
    }

    // Prevent autovivification of object keys. Now we don't modify the config, we just check it
    json11::Json cfg = new_cfg;

    // Validate changes
    if (!old_cfg.is_null() && !force)
    {
        if (old_cfg["scheme"] != cfg["scheme"])
        {
            return "Changing scheme for an existing pool will lead to data loss. Use --force to proceed";
        }
        if (etcd_state_client_t::parse_scheme(old_cfg["scheme"].string_value()) == POOL_SCHEME_EC)
        {
            uint64_t old_data_chunks = old_cfg["pg_size"].uint64_value() - old_cfg["parity_chunks"].uint64_value();
            uint64_t new_data_chunks = cfg["pg_size"].uint64_value() - cfg["parity_chunks"].uint64_value();
            if (old_data_chunks != new_data_chunks)
            {
                return "Changing EC data chunk count for an existing pool will lead to data loss. Use --force to proceed";
            }
        }
        if (old_cfg["block_size"] != cfg["block_size"] ||
            old_cfg["bitmap_granularity"] != cfg["bitmap_granularity"] ||
            old_cfg["immediate_commit"] != cfg["immediate_commit"])
        {
            return "Changing block_size, bitmap_granularity or immediate_commit"
                " for an existing pool will lead to incomplete PGs. Use --force to proceed";
        }
        if (old_cfg["pg_stripe_size"] != cfg["pg_stripe_size"])
        {
            return "Changing pg_stripe_size for an existing pool will lead to data loss. Use --force to proceed";
        }
    }

    // Validate values
    if (cfg["name"].string_value() == "")
    {
        return "Non-empty pool name is required";
    }

    // scheme
    auto scheme = etcd_state_client_t::parse_scheme(cfg["scheme"].string_value());
    if (!scheme)
    {
        return "Scheme must be one of \"replicated\", \"ec\" or \"xor\"";
    }

    // pg_size
    auto pg_size = cfg["pg_size"].uint64_value();
    if (!pg_size)
    {
        return "Non-zero PG size is required";
    }
    if (scheme != POOL_SCHEME_REPLICATED && pg_size < 3)
    {
        return "PG size can't be smaller than 3 for EC/XOR pools";
    }
    if (pg_size > 256)
    {
        return "PG size can't be greater than 256";
    }

    // PG rules
    if (!cfg["level_placement"].is_null())
    {
        for (auto & lr: cfg["level_placement"].object_items())
        {
            int len = 0;
            if (lr.second.is_array())
            {
                for (auto & lri: lr.second.array_items())
                {
                    if (!lri.is_string() && !lri.is_number())
                    {
                        return "--level_placement contains an array with non-scalar value: "+lri.dump();
                    }
                }
                len = lr.second.array_items().size();
            }
            else if (!lr.second.is_string())
            {
                return "--level_placement contains a non-array and non-string value: "+lr.second.dump();
            }
            else
            {
                len = lr.second.string_value().size();
            }
            if (len != pg_size)
            {
                return "values in --level_placement should be exactly pg_size ("+std::to_string(pg_size)+") long";
            }
        }
    }

    // parity_chunks
    uint64_t parity_chunks = 1;
    if (scheme == POOL_SCHEME_EC)
    {
        parity_chunks = cfg["parity_chunks"].uint64_value();
        if (!parity_chunks)
        {
            return "Non-zero parity_chunks is required";
        }
        if (parity_chunks > pg_size-2)
        {
            return "parity_chunks can't be greater than "+std::to_string(pg_size-2)+" (PG size - 2)";
        }
    }

    // pg_minsize
    auto pg_minsize = cfg["pg_minsize"].uint64_value();
    if (!pg_minsize)
    {
        return "Non-zero pg_minsize is required";
    }
    else if (pg_minsize > pg_size)
    {
        return "pg_minsize can't be greater than "+std::to_string(pg_size)+" (PG size)";
    }
    else if (scheme != POOL_SCHEME_REPLICATED && pg_minsize < pg_size-parity_chunks)
    {
        return "pg_minsize can't be smaller than "+std::to_string(pg_size-parity_chunks)+
            " (pg_size - parity_chunks) for XOR/EC pool";
    }

    // pg_count
    if (!cfg["pg_count"].uint64_value())
    {
        return "Non-zero pg_count is required";
    }

    // max_osd_combinations
    if (!cfg["max_osd_combinations"].is_null() && cfg["max_osd_combinations"].uint64_value() < 100)
    {
        return "max_osd_combinations must be at least 100, but it is "+cfg["max_osd_combinations"].as_string();
    }

    // block_size
    auto block_size = cfg["block_size"].uint64_value();
    if (!cfg["block_size"].is_null() && ((block_size & (block_size-1)) ||
        block_size < MIN_DATA_BLOCK_SIZE || block_size > MAX_DATA_BLOCK_SIZE))
    {
        return "block_size must be a power of two between "+std::to_string(MIN_DATA_BLOCK_SIZE)+
            " and "+std::to_string(MAX_DATA_BLOCK_SIZE)+", but it is "+std::to_string(block_size);
    }
    block_size = (block_size ? block_size : global_block_size);

    // bitmap_granularity
    auto bitmap_granularity = cfg["bitmap_granularity"].uint64_value();
    if (!cfg["bitmap_granularity"].is_null() && (!bitmap_granularity || (bitmap_granularity % 512)))
    {
        return "bitmap_granularity must be a multiple of 512, but it is "+std::to_string(bitmap_granularity);
    }
    bitmap_granularity = (bitmap_granularity ? bitmap_granularity : global_bitmap_granularity);
    if (block_size % bitmap_granularity)
    {
        return "bitmap_granularity must divide data block size ("+std::to_string(block_size)+"), but it is "+std::to_string(bitmap_granularity);
    }

    // immediate_commit
    if (!cfg["immediate_commit"].is_null() && !etcd_state_client_t::parse_immediate_commit(cfg["immediate_commit"].string_value()))
    {
        return "immediate_commit must be one of \"all\", \"small\", or \"none\", but it is "+cfg["immediate_commit"].as_string();
    }

    // scrub_interval
    if (!cfg["scrub_interval"].is_null())
    {
        bool ok;
        parse_time(cfg["scrub_interval"].string_value(), &ok);
        if (!ok)
        {
            return "scrub_interval must be a time interval (number + unit s/m/h/d/M/y), but it is "+cfg["scrub_interval"].as_string();
        }
    }

    return "";
}
