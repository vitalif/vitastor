// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include "object_id.h"

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
