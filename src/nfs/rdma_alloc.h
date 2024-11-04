// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// Simple & stupid RDMA-enabled memory allocator (allocates buffers within ibv_mr's)

#pragma once

#include <infiniband/verbs.h>
#include <stdint.h>

struct rdma_allocator_t;

rdma_allocator_t *rdma_malloc_create(ibv_pd *pd, size_t rdma_alloc_size, size_t rdma_max_unused, int rdma_access);
void rdma_malloc_destroy(rdma_allocator_t *self);
void *rdma_malloc_alloc(rdma_allocator_t *self, size_t size);
void rdma_malloc_free(rdma_allocator_t *self, void *buf);
uint32_t rdma_malloc_get_lkey(rdma_allocator_t *self, void *buf);
