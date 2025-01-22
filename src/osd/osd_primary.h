// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include "osd.h"
#include "osd_rmw.h"

#define SUBMIT_READ 0
#define SUBMIT_RMW_READ 1
#define SUBMIT_WRITE 2
#define SUBMIT_SCRUB_READ 3

struct unstable_osd_num_t
{
    osd_num_t osd_num;
    int start, len;
};

struct osd_primary_op_data_t
{
    int st = 0;
    pg_num_t pg_num = 0;
    object_id oid = {};
    uint64_t target_ver = 0;
    uint64_t orig_ver = 0, fact_ver = 0;
    int n_subops = 0, done = 0, errors = 0, drops = 0, errcode = 0;
    int degraded = 0;
    int stripe_count = 0;
    osd_rmw_stripe_t *stripes = NULL;
    pg_t *pg = NULL;
    osd_op_t *subops = NULL;
    uint64_t *prev_set = NULL;
    pg_osd_set_state_t *object_state = NULL;

    union
    {
        struct
        {
            // for sync. oops, requires freeing
            std::vector<unstable_osd_num_t> *unstable_write_osds;
            pool_pg_num_t *dirty_pgs;
            int dirty_pg_count;
            osd_num_t *dirty_osds;
            int dirty_osd_count;
            obj_ver_id *unstable_writes;
            obj_ver_osd_t *copies_to_delete;
            int copies_to_delete_count;
        };
        struct
        {
            // for read_bitmaps
            void *snapshot_bitmaps;
            inode_t *read_chain;
            pg_osd_set_state_t **chain_states;
            uint8_t *missing_flags;
            int chain_size;
            osd_chain_read_t *chain_reads;
            int chain_read_count;
        };
    };
};

bool contains_osd(osd_num_t *osd_set, uint64_t size, osd_num_t osd_num);
