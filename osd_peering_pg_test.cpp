// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

#define _LARGEFILE64_SOURCE

#include "osd_peering_pg.h"
#define STRIPE_SHIFT 12

/**
 * TODO tests for object & pg state calculation.
 *
 * 1) pg=1,2,3. objects:
 *    v1=1s,2s,3s -> clean
 *    v1=1s,2s,3 v2=1s,2s,_ -> degraded + needs_rollback
 *    v1=1s,2s,_ -> degraded
 *    v1=1s,2s,3s v2=1,6,_ -> degraded + needs_stabilize
 *    v1=2s,1s,3s -> misplaced
 *    v1=4,5,6 -> misplaced + needs_stabilize
 *    v1=1s,2s,6s -> misplaced
 * 2) ...
 */
int main(int argc, char *argv[])
{
    pg_t pg = {
        .state = PG_PEERING,
        .pg_num = 1,
        .target_set = { 1, 2, 3 },
        .cur_set = { 1, 2, 3 },
        .peering_state = new pg_peering_state_t(),
    };
    for (uint64_t osd_num = 1; osd_num <= 3; osd_num++)
    {
        pg_list_result_t r = {
            .buf = (obj_ver_id*)malloc_or_die(sizeof(obj_ver_id) * 1024*1024*8),
            .total_count = 1024*1024*8,
            .stable_count = (uint64_t)(1024*1024*8 - (osd_num == 1 ? 10 : 0)),
        };
        for (uint64_t i = 0; i < r.total_count; i++)
        {
            r.buf[i] = {
                .oid = {
                    .inode = 1,
                    .stripe = (i << STRIPE_SHIFT) | (osd_num-1),
                },
                .version = (uint64_t)(osd_num == 1 && i >= r.total_count - 10 ? 2 : 1),
            };
        }
        pg.peering_state->list_results[osd_num] = r;
    }
    pg.calc_object_states(0);
    printf("deviation variants=%ld clean=%lu\n", pg.state_dict.size(), pg.clean_count);
    for (auto it: pg.state_dict)
    {
        printf("dev: state=%lx\n", it.second.state);
    }
    return 0;
}
