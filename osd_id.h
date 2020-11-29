// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#define POOL_SCHEME_REPLICATED 1
#define POOL_SCHEME_XOR 2
#define POOL_SCHEME_JERASURE 3
#define POOL_ID_MAX 0x10000
#define POOL_ID_BITS 16
#define INODE_POOL(inode) (pool_id_t)((inode) >> (64 - POOL_ID_BITS))

// Pool ID is 16 bits long
typedef uint32_t pool_id_t;

typedef uint64_t osd_num_t;
typedef uint32_t pg_num_t;

struct pool_pg_num_t
{
    pool_id_t pool_id;
    pg_num_t pg_num;
};

inline bool operator < (const pool_pg_num_t & a, const pool_pg_num_t & b)
{
    return a.pool_id < b.pool_id || a.pool_id == b.pool_id && a.pg_num < b.pg_num;
}
