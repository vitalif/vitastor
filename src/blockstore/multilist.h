// Variable-length O(1) disk space allocator
// Copyright (c) Vitaliy Filippov, 2025+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include <stdint.h>
#include <vector>

struct multilist_alloc_t
{
    const uint32_t count, maxn;
    std::vector<int32_t> sizes;
    std::vector<uint32_t> nexts, prevs, heads;

    multilist_alloc_t(uint32_t count, uint32_t maxn);
    bool is_free(uint32_t pos);
    uint32_t find(uint32_t size);
    void use_full(uint32_t pos);
    void use(uint32_t pos, uint32_t size);
    void do_free(uint32_t pos);
    void free(uint32_t pos);
    void verify();
    void print();
};

struct multilist_index_t
{
    const uint32_t count, max_used;
    std::vector<uint32_t> nexts, prevs, heads;

    // used should be always < max_used
    multilist_index_t(uint32_t count, uint32_t max_used, uint32_t init_used);
    uint32_t find(uint32_t wanted_used);
    uint32_t next(uint32_t pos);
    void change(uint32_t pos, uint32_t old_used, uint32_t new_used);
    void print();
};
