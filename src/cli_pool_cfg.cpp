/*
 =========================================================================
 Copyright (c) 2023 MIND Software LLC. All Rights Reserved.
 This file is part of the Software-Defined Storage MIND UStor Project.
 For more information about this product, please visit https://mindsw.io
 or contact us directly at info@mindsw.io
 =========================================================================
 */

#include "cli_pool_cfg.h"

bool pool_configurator_t::is_valid_scheme_string(std::string scheme_str)
{
    if (scheme_str != "replicated" && scheme_str != "xor" && scheme_str != "ec")
    {
        error = "Coding scheme should be one of \"xor\", \"replicated\", \"ec\" or \"jerasure\"";
        return false;
    }
    return true;
}

bool pool_configurator_t::is_valid_immediate_commit_string(std::string immediate_commit_str)
{
    if (immediate_commit != "" && immediate_commit != "all" && immediate_commit != "small" && immediate_commit != "none")
    {
        error = "Immediate Commit should be one of \"all\", \"small\", or \"none\"";
        return false;
    }
    return true;
}

std::string pool_configurator_t::get_error_string()
{
    return error;
}

bool pool_configurator_t::parse(json11::Json cfg, bool new_pool)
{
    if (new_pool) // New pool configuration
    {
        // Pool name (req)
        name = cfg["name"].string_value();
        if (name == "")
        {
            error = "Pool name must be given";
            return false;
        }

        // Exclusive ec shortcut check
        if (!cfg["ec"].is_null() &&
            (!cfg["scheme"].is_null() || !cfg["size"].is_null() || !cfg["pg_size"].is_null() || !cfg["parity_chunks"].is_null()))
        {
            error = "You cannot use 'ec' shortcut together with PG size, parity chunks and scheme arguments";
            return false;
        }

        // ec = N+K (opt)
        if (cfg["ec"].is_string())
        {
            scheme = "ec";

            // pg_size = N+K
            // parity_chunks = K

            int ret = sscanf(cfg["ec"].string_value().c_str(), "%lu+%lu", &pg_size, &parity_chunks);
            if (ret != 2)
            {
                error = "Shortcut for 'ec' scheme has an invalid value. Format: --ec <N>+<K>";
                return false;
            }
            if (!pg_size || !parity_chunks)
            {
                error = "<N>+<K> values for 'ec' scheme cannot be 0";
                return false;
            }
            pg_size += parity_chunks;
        }
        // scheme (opt) + pg_size (req) + parity_chunks (req)
        else
        {
            scheme = cfg["scheme"].is_string() ?
                (cfg["scheme"].string_value() == "jerasure" ? "ec" : cfg["scheme"].string_value()) : "replicated";

            if (!is_valid_scheme_string(scheme))
            {
                return false;
            }

            if (!cfg["size"].is_null() && !cfg["pg_size"].is_null() ||
                !cfg["size"].is_null() && !cfg["size"].uint64_value() ||
                !cfg["pg_size"].is_null() && !cfg["pg_size"].uint64_value())
            {
                error = "PG size has an invalid value";
                return false;
            }

            pg_size = !cfg["size"].is_null() ? cfg["size"].uint64_value() : cfg["pg_size"].uint64_value();
            if (!pg_size)
            {
                error = "PG size must be given with value >= 1";
                return false;
            }

            if (!cfg["parity_chunks"].is_null() && !cfg["parity_chunks"].uint64_value())
            {
                error = "Parity chunks has an invalid value";
                return false;
            }

            parity_chunks = cfg["parity_chunks"].uint64_value();
            if (scheme == "xor" && !parity_chunks)
            {
                parity_chunks = 1;
            }
            if (!parity_chunks)
            {
                error = "Parity Chunks must be given with value >= 1";
                return false;
            }
        }

        // pg_minsize (opt)
        if (cfg["pg_minsize"].uint64_value())
        {
            pg_minsize = cfg["pg_minsize"].uint64_value();
        }
        else
        {
            if (!cfg["pg_minsize"].is_null())
            {
                error = "PG minsize has an invalid value";
                return false;
            }
            if (scheme == "replicated")
            {
                // pg_minsize = (N+K > 2) ? 2 : 1
                pg_minsize = pg_size > 2 ? 2 : 1;
            }
            else // ec or xor
            {
                // pg_minsize = (K > 1) ? N + 1 : N
                pg_minsize = pg_size - parity_chunks + (parity_chunks > 1 ? 1 : 0);
            }
        }
        if (!pg_minsize)
        {
            error = "PG minsize must be given with value >= 1";
            return false;
        }

        // pg_count (req)
        if (!cfg["count"].is_null() && !cfg["pg_count"].is_null() ||
            !cfg["count"].is_null() && !cfg["count"].uint64_value() ||
            !cfg["pg_count"].is_null() && !cfg["pg_count"].uint64_value())
        {
            error = "PG count has an invalid value";
            return false;
        }

        pg_count = !cfg["count"].is_null() ? cfg["count"].uint64_value() : cfg["pg_count"].uint64_value();
        if (!pg_count)
        {
            error = "PG count must be given with value >= 1";
            return false;
        }

        // Optional params
        failure_domain = cfg["failure_domain"].string_value();

        if (!cfg["max_osd_combinations"].is_null() && !cfg["max_osd_combinations"].uint64_value())
        {
            error = "Max OSD combinations has an invalid value";
            return false;
        }
        max_osd_combinations = cfg["max_osd_combinations"].uint64_value();

        if (!cfg["block_size"].is_null() && !cfg["block_size"].uint64_value())
        {
            error = "Block size has an invalid value";
            return false;
        }
        block_size = cfg["block_size"].uint64_value();

        if (!cfg["bitmap_granularity"].is_null() && !cfg["bitmap_granularity"].uint64_value())
        {
            error = "Bitmap granularity has an invalid value";
            return false;
        }
        bitmap_granularity = cfg["bitmap_granularity"].uint64_value();

        if (!is_valid_immediate_commit_string(cfg["immediate_commit"].string_value()))
        {
            return false;
        }
        immediate_commit = cfg["immediate_commit"].string_value();

        if (!cfg["pg_stripe_size"].is_null() && !cfg["pg_stripe_size"].uint64_value())
        {
            error = "PG stripe size has an invalid value";
            return false;
        }

        pg_stripe_size = cfg["pg_stripe_size"].uint64_value();
        root_node = cfg["root_node"].string_value();
        osd_tags = cfg["osd_tags"].string_value();
        primary_affinity_tags = cfg["primary_affinity_tags"].string_value();
        scrub_interval = cfg["scrub_interval"].string_value();
    }
    else // Modified pool configuration
    {
        bool has_changes = false;

        // Unsupported parameters

        if (!cfg["scheme"].is_null() || !cfg["parity_chunks"].is_null() || !cfg["ec"].is_null() || !cfg["bitmap_granularity"].is_null())
        {
            error = "Scheme, parity_chunks and bitmap_granularity parameters cannot be modified";
            return false;
        }

        // Supported parameters

        if (!cfg["name"].is_null())
        {
            name = cfg["name"].string_value();
            has_changes = true;
        }

        if (!cfg["size"].is_null() || !cfg["pg_size"].is_null())
        {
            if (!cfg["size"].is_null() && !cfg["pg_size"].is_null())
            {
                error = "Cannot use both size and pg_size parameters at the same time.";
                return false;
            }
            else if (!cfg["size"].is_null() && !cfg["size"].uint64_value() ||
                    !cfg["pg_size"].is_null() && !cfg["pg_size"].uint64_value())
            {
                error = "PG size has an invalid value";
                return false;
            }

            pg_size = !cfg["size"].is_null() ? cfg["size"].uint64_value() : cfg["pg_size"].uint64_value();
            has_changes = true;
        }

        if (!cfg["pg_minsize"].is_null())
        {
            if (!cfg["pg_minsize"].uint64_value())
            {
                error = "PG minsize has an invalid value";
                return false;
            }

            pg_minsize = cfg["pg_minsize"].uint64_value();
            has_changes = true;
        }

        if (!cfg["count"].is_null() || !cfg["pg_count"].is_null())
        {
            if (!cfg["count"].is_null() && !cfg["pg_count"].is_null())
            {
                error = "Cannot use both count and pg_count parameters at the same time.";
                return false;
            }
            else if (!cfg["count"].is_null() && !cfg["count"].uint64_value() ||
                    !cfg["pg_count"].is_null() && !cfg["pg_count"].uint64_value())
            {
                error = "PG count has an invalid value";
                return false;
            }

            pg_count = !cfg["count"].is_null() ? cfg["count"].uint64_value() : cfg["pg_count"].uint64_value();
            has_changes = true;
        }

        if (!cfg["failure_domain"].is_null())
        {
            failure_domain = cfg["failure_domain"].string_value();
            has_changes = true;
        }

        if (!cfg["max_osd_combinations"].is_null())
        {
            if (!cfg["max_osd_combinations"].uint64_value())
            {
                error = "Max OSD combinations has an invalid value";
                return false;
            }

            max_osd_combinations = cfg["max_osd_combinations"].uint64_value();
            has_changes = true;
        }

        if (!cfg["block_size"].is_null())
        {
            if (!cfg["block_size"].uint64_value())
            {
                error = "Block size has an invalid value";
                return false;
            }

            block_size = cfg["block_size"].uint64_value();
            has_changes = true;
        }

        if (!cfg["immediate_commit"].is_null())
        {
            if (!is_valid_immediate_commit_string(cfg["immediate_commit"].string_value()))
            {
                return false;
            }

            immediate_commit = cfg["immediate_commit"].string_value();
            has_changes = true;
        }

        if (!cfg["pg_stripe_size"].is_null())
        {
            if (!cfg["pg_stripe_size"].uint64_value())
            {
                error = "PG stripe size has an invalid value";
                return false;
            }

            pg_stripe_size = cfg["pg_stripe_size"].uint64_value();
            has_changes = true;
        }

        if (!cfg["root_node"].is_null())
        {
            root_node = cfg["root_node"].string_value();
            has_changes = true;
        }

        if (!cfg["osd_tags"].is_null())
        {
            osd_tags = cfg["osd_tags"].string_value();
            has_changes = true;
        }

        if (!cfg["primary_affinity_tags"].is_null())
        {
            primary_affinity_tags = cfg["primary_affinity_tags"].string_value();
            has_changes = true;
        }

        if (!cfg["scrub_interval"].is_null())
        {
            scrub_interval = cfg["scrub_interval"].string_value();
            has_changes = true;
        }

        if (!has_changes)
        {
            error = "No changes were provided to modify pool";
            return false;
        }
    }

    return true;
}

bool pool_configurator_t::validate(etcd_state_client_t &st_cli, pool_config_t *pool_config, bool strict)
{
    // Validate pool parameters

    // Scheme
    uint64_t p_scheme = (scheme != "" ?
        (scheme == "xor" ? POOL_SCHEME_XOR : (scheme == "ec" ? POOL_SCHEME_EC : POOL_SCHEME_REPLICATED)) :
        (pool_config ? pool_config->scheme : 0));

    // PG size
    uint64_t p_pg_size = (pg_size ? pg_size : (pool_config ? pool_config->pg_size : 0));
    if (p_pg_size)
    {
        // Min PG size
        if ((p_scheme == POOL_SCHEME_XOR || p_scheme == POOL_SCHEME_EC) && p_pg_size < 3)
        {
            error = "PG size cannot be less than 3 for XOR/EC pool";
            return false;
        }
        // Max PG size
        else if (p_pg_size > 256)
        {
            error = "PG size cannot be greater than 256";
            return false;
        }
    }

    // Parity Chunks
    uint64_t p_parity_chunks = (parity_chunks ? parity_chunks : (pool_config ? pool_config->parity_chunks : 0));
    if (p_parity_chunks)
    {
        if (p_scheme == POOL_SCHEME_XOR && p_parity_chunks > 1)
        {
            error = "Parity Chunks must be 1 for XOR pool";
            return false;
        }
        if (p_scheme == POOL_SCHEME_EC && (p_parity_chunks < 1 || p_parity_chunks > p_pg_size-2))
        {
            error = "Parity Chunks must be between 1 and pg_size-2 for EC pool";
            return false;
        }
    }

    // PG minsize
    uint64_t p_pg_minsize = (pg_minsize ? pg_minsize : (pool_config ? pool_config->pg_minsize : 0));
    if (p_pg_minsize)
    {
        // Max PG minsize relative to PG size
        if (p_pg_minsize > p_pg_size)
        {
            error = "PG minsize cannot be greater than "+std::to_string(p_pg_size)+" (PG size)";
            return false;
        }
        // PG minsize relative to PG size and Parity Chunks
        else if ((p_scheme == POOL_SCHEME_XOR || p_scheme == POOL_SCHEME_EC) && p_pg_minsize < (p_pg_size - p_parity_chunks))
        {
            error =
                "PG minsize cannot be less than "+std::to_string(p_pg_size - p_parity_chunks)+" "
                "(PG size - Parity Chunks) for XOR/EC pool";
            return false;
        }
    }

    // Max OSD Combinations (optional)
    if (max_osd_combinations > 0 && max_osd_combinations < 100)
    {
        error = "Max OSD Combinations must be at least 100";
        return false;
    }

    // Scrub interval (optional)
    if (scrub_interval != "")
    {
        bool ok;
        parse_time(scrub_interval, &ok);
        if (!ok)
        {
            error = "Failed to parse scrub interval. Format: number + unit s/m/h/d/M/y";
            return false;
        }
    }

    // Additional checks (only if strict)
    if (strict)
    {
        uint64_t p_block_size = block_size ? block_size :
            (pool_config ? pool_config->data_block_size : st_cli.global_block_size);

        uint64_t p_bitmap_granularity = bitmap_granularity ? bitmap_granularity :
            (pool_config ? pool_config->bitmap_granularity : st_cli.global_bitmap_granularity);

        // Block size value and range
        if ((p_block_size & (p_block_size-1)) || p_block_size < MIN_DATA_BLOCK_SIZE || p_block_size > MAX_DATA_BLOCK_SIZE)
        {
            error =
                "Data block size must be a power of two between "+std::to_string(MIN_DATA_BLOCK_SIZE)+" "
                "and "+std::to_string(MAX_DATA_BLOCK_SIZE);
            return false;
        }

        // Block size relative to bitmap granularity
        if (p_block_size % p_bitmap_granularity)
        {
            error = "Data block size must be devisible by "+std::to_string(p_bitmap_granularity)+" (Bitmap Granularity)";
            return false;
        }
    }

    return true;
}
