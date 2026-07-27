// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Simple & stupid RDMA-enabled memory allocator (allocates buffers within ibv_mr's)

#pragma once

#include <stdint.h>

struct ibv_pd;

class dma_allocator_i
{
public:
    virtual ~dma_allocator_i() = default;
    virtual void *alloc(size_t size) = 0;
    virtual void free(void *buf) = 0;
    virtual void *get_handle(void *buf) = 0;
};

dma_allocator_i *rdma_malloc_create(ibv_pd *pd, size_t rdma_alloc_size, size_t rdma_max_unused, int rdma_access);
