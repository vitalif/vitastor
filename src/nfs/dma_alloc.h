// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Simple & stupid freelist memory allocator (allocates buffers within regions)

#pragma once

#include <stdint.h>

class dma_allocator_i
{
public:
    virtual ~dma_allocator_i() = default;
    virtual void *alloc(size_t size, void **handle = NULL, size_t *offset = NULL) = 0;
    virtual void free(void *buf) = 0;
    virtual void *get_handle(void *buf, size_t *offset = NULL) = 0;
};
