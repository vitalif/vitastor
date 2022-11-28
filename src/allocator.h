// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include <stdint.h>

// Hierarchical bitmap allocator
class allocator
{
    uint64_t total;
    uint64_t size;
    uint64_t free;
    uint64_t last_one_mask;
    uint64_t *mask;
public:
    allocator(uint64_t blocks);
    ~allocator();
    bool get(uint64_t addr);
    void set(uint64_t addr, bool value);
    uint64_t find_free();
    uint64_t get_free_count();
};

void bitmap_set(void *bitmap, uint64_t start, uint64_t len, uint64_t bitmap_granularity);
void bitmap_clear(void *bitmap, uint64_t start, uint64_t len, uint64_t bitmap_granularity);
bool bitmap_check(void *bitmap, uint64_t start, uint64_t len, uint64_t bitmap_granularity);
