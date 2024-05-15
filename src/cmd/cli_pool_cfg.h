// Copyright (c) Vitaliy Filippov, 2024
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include "json11/json11.hpp"
#include <stdint.h>

std::string validate_pool_config(json11::Json::object & new_cfg, json11::Json old_cfg,
    uint64_t global_block_size, uint64_t global_bitmap_granularity, bool force);
