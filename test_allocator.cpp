// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

#include <stdio.h>
#include "allocator.h"

int main(int narg, char *args[])
{
    allocator a(8192);
    for (int i = 0; i < 8192; i++)
    {
        uint64_t x = a.find_free();
        if (x == UINT64_MAX)
        {
            printf("ran out of space %d\n", i);
            return 1;
        }
        a.set(x, true);
    }
    return 0;
}
