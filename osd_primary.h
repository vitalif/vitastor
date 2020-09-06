#pragma once

#include "osd.h"
#include "osd_rmw.h"

#define SUBMIT_READ 0
#define SUBMIT_RMW_READ 1
#define SUBMIT_WRITE 2

struct unstable_osd_num_t
{
    osd_num_t osd_num;
    int start, len;
};

struct osd_primary_op_data_t
{
    int st = 0;
    pg_num_t pg_num;
    object_id oid;
    uint64_t target_ver;
    uint64_t fact_ver = 0;
    uint64_t scheme = 0;
    int n_subops = 0, done = 0, errors = 0, epipe = 0;
    int degraded = 0, pg_size, pg_minsize;
    osd_rmw_stripe_t *stripes;
    osd_op_t *subops = NULL;
    uint64_t *prev_set = NULL;
    pg_osd_set_state_t *object_state = NULL;

    // for sync. oops, requires freeing
    std::vector<unstable_osd_num_t> *unstable_write_osds = NULL;
    pool_pg_num_t *dirty_pgs = NULL;
    int dirty_pg_count = 0;
    osd_num_t *dirty_osds = NULL;
    int dirty_osd_count = 0;
    obj_ver_id *unstable_writes = NULL;
};
