// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <stdio.h>
#include <stdlib.h>
#include "allocator.h"

void alloc_all(int size)
{
    allocator *a = new allocator(size);
    for (int i = 0; i < size; i++)
    {
        uint64_t x = a->find_free();
        if (x == UINT64_MAX)
        {
            printf("ran out of space %d allocated=%d\n", size, i);
            exit(1);
        }
        if (x != i)
        {
            printf("incorrect block allocated: expected %d, got %lu\n", i, x);
        }
        a->set(x, true);
    }
    uint64_t x = a->find_free();
    if (x != UINT64_MAX)
    {
        printf("extra free space found: %lx (%d)\n", x, size);
        exit(1);
    }
    delete a;
}

int main(int narg, char *args[])
{
    alloc_all(8192);
    alloc_all(8062);
    alloc_all(4096);
    return 0;
}
