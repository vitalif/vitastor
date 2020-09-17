// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

#pragma once

#include <stdint.h>

// Hierarchical bitmap allocator
class allocator
{
    uint64_t size;
    uint64_t free;
    uint64_t last_one_mask;
    uint64_t *mask;
public:
    allocator(uint64_t blocks);
    ~allocator();
    void set(uint64_t addr, bool value);
    uint64_t find_free();
    uint64_t get_free_count();
};
