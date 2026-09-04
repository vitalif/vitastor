// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#undef NDEBUG
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <set>
#include "dma_alloc_impl.h"

class test_dma_allocator_t: public dma_allocator_t
{
public:
    std::set<void*> mrs;

    test_dma_allocator_t(size_t region_size, size_t max_unused, size_t alignment):
        dma_allocator_t(region_size, max_unused, alignment)
    {
    }

    ~test_dma_allocator_t()
    {
        free_unused_buffers(0, true);
    }

    void* reg_mr(void *buf, size_t len) override
    {
        bool added = mrs.insert(buf).second;
        assert(added);
        return buf;
    }

    void dereg_mr(void *buf, size_t len, void *handle) override
    {
        auto erased = mrs.erase(handle);
        assert(erased);
    }
};

void test_split()
{
    dma_allocator_i *a = new test_dma_allocator_t(1048576, 64*1048576, 1);
    auto buf0 = a->alloc(1024*1024);
    a->free(buf0);
    auto buf1 = a->alloc(224*1024);
    auto buf2 = a->alloc(500*1024);
    auto buf3 = a->alloc(300*1024);
    a->free(buf2);
    auto buf4 = a->alloc(300*1024);
    auto buf5 = a->alloc(200*1024);
    assert((buf5 == buf2 || buf4 == buf2) && buf5 != buf4 && buf5 != buf3 && buf5 != buf1 &&
        buf4 != buf3 && buf4 != buf1 && buf3 != buf1);
    a->free(buf5);
    a->free(buf4);
    a->free(buf3);
    a->free(buf1);
    // now check merge-both
    buf1 = a->alloc(224*1024);
    buf2 = a->alloc(500*1024);
    buf3 = a->alloc(300*1024);
    a->free(buf3);
    a->free(buf1);
    a->free(buf2);
    delete a;
    printf("ok test_split\n");
}

int main(int narg, char *args[])
{
    test_split();
    return 0;
}
