// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)
//
// RDMA variant of our stupid memory allocator (allocates buffers within ibv_mr's)

#pragma once

#include "dma_alloc.h"

struct ibv_pd;

dma_allocator_i *rdma_malloc_create(ibv_pd *pd, size_t rdma_alloc_size, size_t rdma_max_unused, int rdma_access);
