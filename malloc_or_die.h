#pragma once

#include <malloc.h>

inline void* memalign_or_die(size_t alignment, size_t size)
{
    void *buf = memalign(alignment, size);
    if (!buf)
    {
        printf("Failed to allocate %lu bytes\n", size);
        exit(1);
    }
    return buf;
}

inline void* malloc_or_die(size_t size)
{
    void *buf = malloc(size);
    if (!buf)
    {
        printf("Failed to allocate %lu bytes\n", size);
        exit(1);
    }
    return buf;
}

inline void* realloc_or_die(void *ptr, size_t size)
{
    void *buf = realloc(ptr, size);
    if (!buf)
    {
        printf("Failed to allocate %lu bytes\n", size);
        exit(1);
    }
    return buf;
}

inline void* calloc_or_die(size_t nmemb, size_t size)
{
    void *buf = calloc(nmemb, size);
    if (!buf)
    {
        printf("Failed to allocate %lu bytes\n", size * nmemb);
        exit(1);
    }
    return buf;
}
