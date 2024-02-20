// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include "object_id.h"

#define POOL_SCHEME_REPLICATED 1
#define POOL_SCHEME_XOR 2
#define POOL_SCHEME_EC 3
#define POOL_ID_MAX 0x10000
#define POOL_ID_BITS 16
#define INODE_POOL(inode) (pool_id_t)((inode) >> (64 - POOL_ID_BITS))
#define INODE_NO_POOL(inode) (inode_t)(inode & (((uint64_t)1 << (64-POOL_ID_BITS)) - 1))
#define INODE_WITH_POOL(pool_id, inode) (((inode_t)(pool_id) << (64-POOL_ID_BITS)) | INODE_NO_POOL(inode))

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

inline bool operator == (const pool_pg_num_t & a, const pool_pg_num_t & b)
{
    return a.pool_id == b.pool_id && a.pg_num == b.pg_num;
}

inline bool operator != (const pool_pg_num_t & a, const pool_pg_num_t & b)
{
    return a.pool_id != b.pool_id || a.pg_num != b.pg_num;
}
