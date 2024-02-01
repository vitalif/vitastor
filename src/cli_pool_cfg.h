/*
 =========================================================================
 Copyright (c) 2023 MIND Software LLC. All Rights Reserved.
 This file is part of the Software-Defined Storage MIND UStor Project.
 For more information about this product, please visit https://mindsw.io
 or contact us directly at info@mindsw.io
 =========================================================================
 */

#pragma once

#include "json11/json11.hpp"

#include "etcd_state_client.h"
#include "str_util.h"

struct pool_configurator_t
{
protected:
    std::string error;
    bool is_valid_scheme_string(std::string scheme_str);
    bool is_valid_immediate_commit_string(std::string immediate_commit_str);

public:
    std::string name;

    std::string scheme;
    uint64_t pg_size, pg_minsize, pg_count;
    uint64_t parity_chunks;

    std::string immediate_commit;
    std::string failure_domain;
    std::string root_node;

    uint64_t max_osd_combinations;
    uint64_t block_size, bitmap_granularity;
    uint64_t pg_stripe_size;

    std::string osd_tags;
    std::string primary_affinity_tags;

    std::string scrub_interval;

    std::string get_error_string();

    bool parse(json11::Json cfg, bool new_pool);
    bool validate(etcd_state_client_t &st_cli, pool_config_t *pool_config, bool strict);
};
