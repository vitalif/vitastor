// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Simple & stupid RDMA-enabled memory allocator (allocates buffers within ibv_mr's)

#include <stdio.h>
#include <stdlib.h>
#include <infiniband/verbs.h>
#include "dma_alloc_impl.h"
#include "rdma_alloc.h"

class rdma_allocator_t: public dma_allocator_t
{
protected:
    int ibv_access = IBV_ACCESS_LOCAL_WRITE;
    ibv_pd *pd = NULL;

    void* reg_mr(void* buf, size_t len) override
    {
        ibv_mr *mr = ibv_reg_mr(pd, buf, len, ibv_access);
        if (!mr)
        {
            fprintf(stderr, "Failed to register RDMA memory region: %s\n", strerror(errno));
            exit(1);
        }
        return mr;
    }

    void dereg_mr(void* buf, size_t len, void* handle) override
    {
        ibv_dereg_mr((ibv_mr*)handle);
    }

public:
    rdma_allocator_t(ibv_pd *pd, size_t region_size, size_t max_unused, int ibv_access):
        dma_allocator_t(region_size, max_unused, 1)
    {
        this->pd = pd;
        this->ibv_access = ibv_access;
    }

    ~rdma_allocator_t()
    {
        free_unused_buffers(0, true);
    }
};

dma_allocator_i *rdma_malloc_create(ibv_pd *pd, size_t region_size, size_t max_unused, int rdma_access)
{
    return new rdma_allocator_t(pd, region_size, max_unused, rdma_access);
}
