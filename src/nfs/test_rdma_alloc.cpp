// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <set>
#include "rdma_alloc.h"

std::set<void*> mrs;

extern "C" {

ibv_mr *ibv_reg_mr_iova2(ibv_pd *pd, void *addr, size_t length, uint64_t, unsigned)
{
    bool added = mrs.insert(addr).second;
    assert(added);
    return (ibv_mr*)addr;
}

int ibv_dereg_mr(ibv_mr *mr)
{
    auto erased = mrs.erase((void*)mr);
    assert(erased);
    return 0;
}

}

void test_split()
{
    rdma_allocator_t *alloc = rdma_malloc_create(NULL, 1048576, 64*1048576, IBV_ACCESS_LOCAL_WRITE);
    auto buf0 = rdma_malloc_alloc(alloc, 1024*1024);
    rdma_malloc_free(alloc, buf0);
    auto buf1 = rdma_malloc_alloc(alloc, 224*1024);
    auto buf2 = rdma_malloc_alloc(alloc, 500*1024);
    auto buf3 = rdma_malloc_alloc(alloc, 300*1024);
    rdma_malloc_free(alloc, buf2);
    auto buf4 = rdma_malloc_alloc(alloc, 300*1024);
    auto buf5 = rdma_malloc_alloc(alloc, 200*1024);
    assert((buf5 == buf2 || buf4 == buf2) && buf5 != buf4 && buf5 != buf3 && buf5 != buf1 &&
        buf4 != buf3 && buf4 != buf1 && buf3 != buf1);
    rdma_malloc_free(alloc, buf5);
    rdma_malloc_free(alloc, buf4);
    rdma_malloc_free(alloc, buf3);
    rdma_malloc_free(alloc, buf1);
    // now check merge-both
    buf1 = rdma_malloc_alloc(alloc, 224*1024);
    buf2 = rdma_malloc_alloc(alloc, 500*1024);
    buf3 = rdma_malloc_alloc(alloc, 300*1024);
    rdma_malloc_free(alloc, buf3);
    rdma_malloc_free(alloc, buf1);
    rdma_malloc_free(alloc, buf2);
    rdma_malloc_destroy(alloc);
    printf("ok test_split\n");
}

int main(int narg, char *args[])
{
    test_split();
    return 0;
}
