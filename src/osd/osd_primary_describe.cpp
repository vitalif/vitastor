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

static void add_primary_list(btree::btree_map<object_id, pg_osd_set_state_t*> & list, osd_op_sec_list_t & req, std::set<object_id> & oids)
{
    auto begin_it = list.begin();
    auto end_it = list.end();
    if (req.min_inode)
        begin_it = list.lower_bound((object_id){ .inode = req.min_inode, .stripe = req.min_stripe });
    if (req.max_inode)
        end_it = list.upper_bound((object_id){ .inode = req.max_inode, .stripe = (req.max_stripe ? req.max_stripe : UINT64_MAX) });
    for (auto list_it = begin_it; list_it != end_it; list_it++)
        oids.insert(list_it->first);
}

void osd_t::continue_primary_list(osd_op_t *cur_op)
{
    auto pool_cfg_it = st_cli.pool_config.find(INODE_POOL(cur_op->req.sec_list.min_inode));
    // Validate the request
    if (!cur_op->req.sec_list.list_pg ||
        !INODE_POOL(cur_op->req.sec_list.min_inode) ||
        INODE_NO_POOL(cur_op->req.sec_list.min_inode) != INODE_NO_POOL(cur_op->req.sec_list.max_inode) ||
        INODE_POOL(cur_op->req.sec_list.max_inode) != INODE_POOL(cur_op->req.sec_list.min_inode) ||
        cur_op->req.sec_list.stable_limit ||
        pool_cfg_it == st_cli.pool_config.end() ||
        (cur_op->req.sec_list.pg_stripe_size != 0 && cur_op->req.sec_list.pg_stripe_size != pool_cfg_it->second.pg_stripe_size) ||
        (cur_op->req.sec_list.pg_count != 0 && cur_op->req.sec_list.pg_count != pool_cfg_it->second.real_pg_count))
    {
        finish_op(cur_op, -EINVAL);
        return;
    }
    auto pg_it = pgs.find({ .pool_id = INODE_POOL(cur_op->req.sec_list.min_inode), .pg_num = cur_op->req.sec_list.list_pg });
    if (pg_it == pgs.end())
    {
        // Not primary
        finish_op(cur_op, -EPIPE);
        return;
    }
    cur_op->bs_op = new blockstore_op_t();
    cur_op->bs_op->opcode = BS_OP_LIST;
    cur_op->bs_op->pg_alignment = pool_cfg_it->second.pg_stripe_size;
    cur_op->bs_op->pg_count = pool_cfg_it->second.real_pg_count;
    cur_op->bs_op->pg_number = cur_op->req.sec_list.list_pg - 1;
    cur_op->bs_op->min_oid.inode = cur_op->req.sec_list.min_inode;
    cur_op->bs_op->min_oid.stripe = cur_op->req.sec_list.min_stripe;
    cur_op->bs_op->max_oid.inode = cur_op->req.sec_list.max_inode;
    if (cur_op->req.sec_list.max_inode && cur_op->req.sec_list.max_stripe != UINT64_MAX)
    {
        cur_op->bs_op->max_oid.stripe = cur_op->req.sec_list.max_stripe
            ? cur_op->req.sec_list.max_stripe : UINT64_MAX;
    }
    cur_op->bs_op->callback = [this, cur_op](blockstore_op_t* bs_op)
    {
        if (bs_op->retval < 0)
        {
            auto retval = bs_op->retval;
            delete bs_op;
            cur_op->bs_op = NULL;
            finish_op(cur_op, retval);
            return;
        }
        // Move into a set
        std::set<object_id> oids;
        obj_ver_id *rbuf = (obj_ver_id*)bs_op->buf;
        uint64_t total_count = bs_op->retval;
        for (uint64_t i = 0; i < total_count; i++)
        {
            oids.insert(rbuf[i].oid);
        }
        if (bs_op->buf)
        {
            free(bs_op->buf);
            bs_op->buf = NULL;
        }
        delete bs_op;
        cur_op->bs_op = NULL;
        // Check if still primary
        auto pg_it = pgs.find({ .pool_id = INODE_POOL(cur_op->req.sec_list.min_inode), .pg_num = cur_op->req.sec_list.list_pg });
        if (pg_it == pgs.end())
        {
            finish_op(cur_op, -EPIPE);
            return;
        }
        // Add unclean objects which may be not present on the primary OSD
        auto & pg = pg_it->second;
        add_primary_list(pg.inconsistent_objects, cur_op->req.sec_list, oids);
        add_primary_list(pg.incomplete_objects, cur_op->req.sec_list, oids);
        add_primary_list(pg.degraded_objects, cur_op->req.sec_list, oids);
        add_primary_list(pg.misplaced_objects, cur_op->req.sec_list, oids);
        // Generate the result
        if (oids.size())
        {
            rbuf = (obj_ver_id*)malloc_or_die(sizeof(obj_ver_id) * oids.size());
            uint64_t i = 0;
            for (auto oid: oids)
            {
                rbuf[i++] = (obj_ver_id){ .oid = oid };
            }
            cur_op->buf = rbuf;
            cur_op->iov.push_back(rbuf, sizeof(obj_ver_id) * oids.size());
        }
        cur_op->reply.sec_list.stable_count = oids.size();
        cur_op->reply.sec_list.flags = OSD_LIST_PRIMARY;
        finish_op(cur_op, oids.size());
    };
    bs->enqueue_op(cur_op->bs_op);
}
