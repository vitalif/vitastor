// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <queue>
#include "osd_primary.h"

struct unclean_list_t
{
    btree::btree_map<object_id, pg_osd_set_state_t*>::iterator it, end;
    uint64_t state_mask, state;
};

struct desc_item_list_t
{
    int alloc, size;
    osd_reply_describe_item_t *items;
};

static void include_list(std::vector<unclean_list_t> & lists,
    btree::btree_map<object_id, pg_osd_set_state_t*> & from,
    osd_op_describe_t & desc, uint64_t state_mask, uint64_t state)
{
    auto it = desc.min_inode || desc.min_offset ? from.lower_bound((object_id){
        .inode = desc.min_inode,
        .stripe = desc.min_offset,
    }) : from.begin();
    auto end_it = desc.max_inode || desc.max_offset ? from.upper_bound((object_id){
        .inode = desc.max_inode,
        .stripe = desc.max_offset,
    }) : from.end();
    lists.push_back((unclean_list_t){
        .it = it,
        .end = end_it,
        .state_mask = state_mask,
        .state = state,
    });
}

struct obj_list_t
{
    object_id oid;
    int list_id;
};

static inline bool operator < (const obj_list_t & a, const obj_list_t & b)
{
    return b.oid < a.oid;
}

static void scan_lists(std::vector<unclean_list_t> & lists, uint64_t limit, desc_item_list_t & res)
{
    if (limit > 1048576)
    {
        limit = 1048576;
    }
    std::priority_queue<obj_list_t> min;
    for (int i = 0; i < lists.size(); i++)
    {
        if (lists[i].it != lists[i].end)
        {
            min.push((obj_list_t){ .oid = lists[i].it->first, .list_id = i });
        }
    }
    while (min.size() && (!limit || res.size < limit))
    {
        auto i = min.top().list_id;
        min.pop();
        for (auto & chunk: lists[i].it->second->osd_set)
        {
            if (res.size >= res.alloc)
            {
                res.alloc = !res.alloc ? 128 : (res.alloc*2);
                res.items = (osd_reply_describe_item_t*)realloc_or_die(res.items, res.alloc * sizeof(osd_reply_describe_item_t));
            }
            res.items[res.size++] = (osd_reply_describe_item_t){
                .inode   = lists[i].it->first.inode,
                .stripe  = lists[i].it->first.stripe,
                .role    = (uint32_t)chunk.role,
                .loc_bad = chunk.loc_bad,
                .osd_num = chunk.osd_num,
            };
        }
        lists[i].it++;
        if (lists[i].it != lists[i].end)
        {
            min.push((obj_list_t){ .oid = lists[i].it->first, .list_id = i });
        }
    }
}

// Describe unclean objects
void osd_t::continue_primary_describe(osd_op_t *cur_op)
{
    auto & desc = cur_op->req.describe;
    if (!desc.object_state)
        desc.object_state = ~desc.object_state;
    std::vector<unclean_list_t> lists;
    auto pg_first = pgs.begin();
    auto pg_last = pgs.end();
    if (desc.pool_id && desc.pg_num)
    {
        pg_first = pgs.find((pool_pg_num_t){ .pool_id = desc.pool_id, .pg_num = desc.pg_num });
        pg_last = pg_first != pgs.end() ? std::next(pg_first) : pgs.end();
    }
    else if (desc.pool_id)
    {
        pg_first = pgs.lower_bound((pool_pg_num_t){ .pool_id = desc.pool_id });
        pg_last = pgs.lower_bound((pool_pg_num_t){ .pool_id = desc.pool_id+1 });
    }
    for (auto pg_it = pg_first; pg_it != pg_last; pg_it++)
    {
        auto & pg = pg_it->second;
        if (desc.object_state & OBJ_INCONSISTENT)
            include_list(lists, pg.inconsistent_objects, desc, 0, 0);
        if (desc.object_state & OBJ_CORRUPTED)
        {
            if (!(desc.object_state & OBJ_INCOMPLETE))
                include_list(lists, pg.incomplete_objects, desc, OBJ_CORRUPTED, OBJ_CORRUPTED);
            if (!(desc.object_state & OBJ_DEGRADED))
                include_list(lists, pg.degraded_objects, desc, OBJ_CORRUPTED, OBJ_CORRUPTED);
            if (!(desc.object_state & OBJ_MISPLACED))
                include_list(lists, pg.misplaced_objects, desc, OBJ_CORRUPTED, OBJ_CORRUPTED);
        }
        uint64_t skip_corrupted = !(desc.object_state & OBJ_CORRUPTED) ? OBJ_CORRUPTED : 0;
        if (desc.object_state & OBJ_INCOMPLETE)
            include_list(lists, pg.incomplete_objects, desc, skip_corrupted, 0);
        if (desc.object_state & OBJ_DEGRADED)
            include_list(lists, pg.degraded_objects, desc, skip_corrupted, 0);
        if (desc.object_state & OBJ_MISPLACED)
            include_list(lists, pg.misplaced_objects, desc, skip_corrupted, 0);
    }
    desc_item_list_t res = {};
    scan_lists(lists, desc.limit, res);
    assert(!cur_op->buf);
    cur_op->buf = res.items;
    cur_op->reply.describe.result_bytes = res.size * sizeof(osd_reply_describe_item_t);
    if (res.items)
        cur_op->iov.push_back(res.items, res.size * sizeof(osd_reply_describe_item_t));
    finish_op(cur_op, res.size);
}
