// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <malloc.h>
#include <stdlib.h>

#pragma GCC visibility push(default)

inline void* memalign_or_die(size_t alignment, size_t size)
{
    void *buf = memalign(alignment, size);
    if (!buf)
    {
        printf("Failed to allocate %zu bytes\n", size);
        exit(1);
    }
    return buf;
}

inline void* malloc_or_die(size_t size)
{
    void *buf = malloc(size);
    if (!buf)
    {
        printf("Failed to allocate %zu bytes\n", size);
        exit(1);
    }
    return buf;
}

inline void* realloc_or_die(void *ptr, size_t size)
{
    void *buf = realloc(ptr, size);
    if (!buf)
    {
        printf("Failed to allocate %zu bytes\n", size);
        exit(1);
    }
    return buf;
}

inline void* calloc_or_die(size_t nmemb, size_t size)
{
    void *buf = calloc(nmemb, size);
    if (!buf)
    {
        printf("Failed to allocate %zu bytes\n", size * nmemb);
        exit(1);
    }
    return buf;
}

#pragma GCC visibility pop
