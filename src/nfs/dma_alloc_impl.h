// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Simple & stupid freelist memory allocator (allocates buffers within regions)

#pragma once

#include <stdint.h>
#include <map>
#include <set>
#include "dma_alloc.h"

struct dma_region_t
{
    void *buf = NULL;
    size_t len = 0;
    bool mapped = false;
    void *handle = NULL;
};

struct dma_frag_t
{
    dma_region_t *rgn = NULL;
    size_t len = 0;
    bool is_free = false;
};

struct dma_free_t
{
    size_t len = 0;
    void *buf = NULL;
};

inline bool operator < (const dma_free_t &a, const dma_free_t &b)
{
    return a.len < b.len || a.len == b.len && a.buf < b.buf;
}

class dma_allocator_t: public dma_allocator_i
{
protected:
    size_t page_size = 4096;
    size_t region_size = 1048576;
    size_t max_unused = 500*1048576;
    size_t alignment = 1;
    bool try_hugepages = false;

    std::set<dma_region_t*> regions;
    std::map<void*, dma_frag_t> frags;
    std::set<dma_free_t> freelist;
    size_t freebuffers = 0;

    virtual void* reg_mr(void* buf, size_t len) = 0;
    virtual void dereg_mr(void* buf, size_t len, void* handle) = 0;
    void destroy_region(dma_region_t *rgn);
    void free_unused_buffers(size_t max_unused, bool force);
public:
    dma_allocator_t(size_t region_size, size_t max_unused, size_t alignment);
    virtual ~dma_allocator_t();
    void *alloc(size_t size, void **handle = NULL, size_t *offset = NULL) override;
    void free(void *buf) override;
    void *get_handle(void *buf, size_t *offset = NULL) override;
};
