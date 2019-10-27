#pragma once

#include <stdint.h>

// Hierarchical bitmap allocator
struct allocator
{
    uint64_t size;
    uint64_t last_one_mask;
    uint64_t mask[];
};

allocator *allocator_create(uint64_t blocks);
void allocator_destroy(allocator *alloc);
void allocator_set(allocator *alloc, uint64_t addr, bool value);
uint64_t allocator_find_free(allocator *alloc);
