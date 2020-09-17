// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

#pragma once

#include <stdint.h>
#include "object_id.h"
#include "osd_id.h"

#ifndef MEM_ALIGNMENT
#define MEM_ALIGNMENT 512
#endif

struct buf_len_t
{
    void *buf;
    uint64_t len;
};

struct osd_rmw_stripe_t
{
    void *read_buf, *write_buf;
    uint32_t req_start, req_end;
    uint32_t read_start, read_end;
    uint32_t write_start, write_end;
    bool missing;
};

void split_stripes(uint64_t pg_minsize, uint32_t bs_block_size, uint32_t start, uint32_t len, osd_rmw_stripe_t *stripes);

void reconstruct_stripe_xor(osd_rmw_stripe_t *stripes, int pg_size, int role);

int extend_missing_stripes(osd_rmw_stripe_t *stripes, osd_num_t *osd_set, int minsize, int size);

void* alloc_read_buffer(osd_rmw_stripe_t *stripes, int read_pg_size, uint64_t add_size);

void* calc_rmw(void *request_buf, osd_rmw_stripe_t *stripes, uint64_t *read_osd_set,
    uint64_t pg_size, uint64_t pg_minsize, uint64_t pg_cursize, uint64_t *write_osd_set, uint64_t chunk_size);

void calc_rmw_parity_xor(osd_rmw_stripe_t *stripes, int pg_size, uint64_t *read_osd_set, uint64_t *write_osd_set, uint32_t chunk_size);
