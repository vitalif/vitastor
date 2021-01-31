// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "osd_primary.h"
#include "allocator.h"

// read: read directly or read paired stripe(s), reconstruct, return
// write: read paired stripe(s), reconstruct, modify, calculate parity, write
//
// nuance: take care to read the same version from paired stripes!
// to do so, we remember "last readable" version until a write request completes
// and we postpone other write requests to the same stripe until completion of previous ones
//
// sync: sync peers, get unstable versions, stabilize them

bool osd_t::prepare_primary_rw(osd_op_t *cur_op)
{
    // PG number is calculated from the offset
    // Our EC scheme stores data in fixed chunks equal to (K*block size)
    // K = (pg_size-parity_chunks) in case of EC/XOR, or 1 for replicated pools
    pool_id_t pool_id = INODE_POOL(cur_op->req.rw.inode);
    // Note: We read pool config here, so we must NOT change it when PGs are active
    auto pool_cfg_it = st_cli.pool_config.find(pool_id);
    if (pool_cfg_it == st_cli.pool_config.end())
    {
        // Pool config is not loaded yet
        finish_op(cur_op, -EPIPE);
        return false;
    }
    auto & pool_cfg = pool_cfg_it->second;
    uint64_t pg_data_size = (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks);
    uint64_t pg_block_size = bs_block_size * pg_data_size;
    object_id oid = {
        .inode = cur_op->req.rw.inode,
        // oid.stripe = starting offset of the parity stripe
        .stripe = (cur_op->req.rw.offset/pg_block_size)*pg_block_size,
    };
    pg_num_t pg_num = (cur_op->req.rw.inode + oid.stripe/pool_cfg.pg_stripe_size) % pg_counts[pool_id] + 1;
    auto pg_it = pgs.find({ .pool_id = pool_id, .pg_num = pg_num });
    if (pg_it == pgs.end() || !(pg_it->second.state & PG_ACTIVE))
    {
        // This OSD is not primary for this PG or the PG is inactive
        // FIXME: Allow reads from PGs degraded under pg_minsize, but don't allow writes
        finish_op(cur_op, -EPIPE);
        return false;
    }
    if ((cur_op->req.rw.offset + cur_op->req.rw.len) > (oid.stripe + pg_block_size) ||
        (cur_op->req.rw.offset % bs_bitmap_granularity) != 0 ||
        (cur_op->req.rw.len % bs_bitmap_granularity) != 0)
    {
        finish_op(cur_op, -EINVAL);
        return false;
    }
    int stripe_count = (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pg_it->second.pg_size);
    osd_primary_op_data_t *op_data = (osd_primary_op_data_t*)calloc_or_die(
        1, sizeof(osd_primary_op_data_t) + (entry_attr_size + sizeof(osd_rmw_stripe_t)) * stripe_count
    );
    op_data->pg_num = pg_num;
    op_data->oid = oid;
    op_data->stripes = ((osd_rmw_stripe_t*)(op_data+1));
    op_data->scheme = pool_cfg.scheme;
    op_data->pg_data_size = pg_data_size;
    cur_op->op_data = op_data;
    split_stripes(pg_data_size, bs_block_size, (uint32_t)(cur_op->req.rw.offset - oid.stripe), cur_op->req.rw.len, op_data->stripes);
    // Allocate bitmaps along with stripes to avoid extra allocations and fragmentation
    for (int i = 0; i < stripe_count; i++)
    {
        op_data->stripes[i].bmp_buf = (void*)(op_data->stripes+stripe_count) + entry_attr_size*i;
    }
    pg_it->second.inflight++;
    return true;
}

uint64_t* osd_t::get_object_osd_set(pg_t &pg, object_id &oid, uint64_t *def, pg_osd_set_state_t **object_state)
{
    if (!(pg.state & (PG_HAS_INCOMPLETE | PG_HAS_DEGRADED | PG_HAS_MISPLACED)))
    {
        *object_state = NULL;
        return def;
    }
    auto st_it = pg.incomplete_objects.find(oid);
    if (st_it != pg.incomplete_objects.end())
    {
        *object_state = st_it->second;
        return st_it->second->read_target.data();
    }
    st_it = pg.degraded_objects.find(oid);
    if (st_it != pg.degraded_objects.end())
    {
        *object_state = st_it->second;
        return st_it->second->read_target.data();
    }
    st_it = pg.misplaced_objects.find(oid);
    if (st_it != pg.misplaced_objects.end())
    {
        *object_state = st_it->second;
        return st_it->second->read_target.data();
    }
    *object_state = NULL;
    return def;
}

void osd_t::continue_primary_read(osd_op_t *cur_op)
{
    if (!cur_op->op_data && !prepare_primary_rw(cur_op))
    {
        return;
    }
    cur_op->reply.rw.bitmap_len = 0;
    osd_primary_op_data_t *op_data = cur_op->op_data;
    if (op_data->st == 1)      goto resume_1;
    else if (op_data->st == 2) goto resume_2;
    {
        auto & pg = pgs.at({ .pool_id = INODE_POOL(op_data->oid.inode), .pg_num = op_data->pg_num });
        for (int role = 0; role < op_data->pg_data_size; role++)
        {
            op_data->stripes[role].read_start = op_data->stripes[role].req_start;
            op_data->stripes[role].read_end = op_data->stripes[role].req_end;
        }
        // Determine version
        auto vo_it = pg.ver_override.find(op_data->oid);
        op_data->target_ver = vo_it != pg.ver_override.end() ? vo_it->second : UINT64_MAX;
        if (pg.state == PG_ACTIVE || op_data->scheme == POOL_SCHEME_REPLICATED)
        {
            // Fast happy-path
            cur_op->buf = alloc_read_buffer(op_data->stripes, op_data->pg_data_size, 0);
            submit_primary_subops(SUBMIT_READ, op_data->target_ver,
                (op_data->scheme == POOL_SCHEME_REPLICATED ? pg.pg_size : op_data->pg_data_size), pg.cur_set.data(), cur_op);
            op_data->st = 1;
        }
        else
        {
            // PG may be degraded or have misplaced objects
            uint64_t* cur_set = get_object_osd_set(pg, op_data->oid, pg.cur_set.data(), &op_data->object_state);
            if (extend_missing_stripes(op_data->stripes, cur_set, op_data->pg_data_size, pg.pg_size) < 0)
            {
                finish_op(cur_op, -EIO);
                return;
            }
            // Submit reads
            op_data->pg_size = pg.pg_size;
            op_data->scheme = pg.scheme;
            op_data->degraded = 1;
            cur_op->buf = alloc_read_buffer(op_data->stripes, pg.pg_size, 0);
            submit_primary_subops(SUBMIT_READ, op_data->target_ver, pg.pg_size, cur_set, cur_op);
            op_data->st = 1;
        }
    }
resume_1:
    return;
resume_2:
    if (op_data->errors > 0)
    {
        finish_op(cur_op, op_data->epipe > 0 ? -EPIPE : -EIO);
        return;
    }
    if (op_data->degraded)
    {
        // Reconstruct missing stripes
        osd_rmw_stripe_t *stripes = op_data->stripes;
        if (op_data->scheme == POOL_SCHEME_XOR)
        {
            reconstruct_stripes_xor(stripes, op_data->pg_size, entry_attr_size);
        }
        else if (op_data->scheme == POOL_SCHEME_JERASURE)
        {
            reconstruct_stripes_jerasure(stripes, op_data->pg_size, op_data->pg_data_size, entry_attr_size);
        }
        for (int role = 0; role < op_data->pg_size; role++)
        {
            if (stripes[role].req_end != 0)
            {
                // Send buffer in parts to avoid copying
                cur_op->iov.push_back(
                    stripes[role].read_buf + (stripes[role].req_start - stripes[role].read_start),
                    stripes[role].req_end - stripes[role].req_start
                );
            }
        }
    }
    else
    {
        cur_op->iov.push_back(cur_op->buf, cur_op->req.rw.len);
    }
    cur_op->reply.rw.bitmap_len = op_data->pg_data_size * entry_attr_size;
    cur_op->iov.push_back(op_data->stripes[0].bmp_buf, cur_op->reply.rw.bitmap_len);
    finish_op(cur_op, cur_op->req.rw.len);
}

// Decrement pg_osd_set_state_t's object_count and change PG state accordingly
void osd_t::remove_object_from_state(object_id & oid, pg_osd_set_state_t *object_state, pg_t & pg)
{
    if (object_state->state & OBJ_INCOMPLETE)
    {
        // Successful write means that object is not incomplete anymore
        this->incomplete_objects--;
        pg.incomplete_objects.erase(oid);
        if (!pg.incomplete_objects.size())
        {
            pg.state = pg.state & ~PG_HAS_INCOMPLETE;
            report_pg_state(pg);
        }
    }
    else if (object_state->state & OBJ_DEGRADED)
    {
        this->degraded_objects--;
        pg.degraded_objects.erase(oid);
        if (!pg.degraded_objects.size())
        {
            pg.state = pg.state & ~PG_HAS_DEGRADED;
            report_pg_state(pg);
        }
    }
    else if (object_state->state & OBJ_MISPLACED)
    {
        this->misplaced_objects--;
        pg.misplaced_objects.erase(oid);
        if (!pg.misplaced_objects.size())
        {
            pg.state = pg.state & ~PG_HAS_MISPLACED;
            report_pg_state(pg);
        }
    }
    else
    {
        throw std::runtime_error("BUG: Invalid object state: "+std::to_string(object_state->state));
    }
}

void osd_t::free_object_state(pg_t & pg, pg_osd_set_state_t **object_state)
{
    if (*object_state && !(--(*object_state)->object_count))
    {
        pg.state_dict.erase((*object_state)->osd_set);
        *object_state = NULL;
    }
}

void osd_t::continue_primary_del(osd_op_t *cur_op)
{
    if (!cur_op->op_data && !prepare_primary_rw(cur_op))
    {
        return;
    }
    osd_primary_op_data_t *op_data = cur_op->op_data;
    auto & pg = pgs.at({ .pool_id = INODE_POOL(op_data->oid.inode), .pg_num = op_data->pg_num });
    if (op_data->st == 1)      goto resume_1;
    else if (op_data->st == 2) goto resume_2;
    else if (op_data->st == 3) goto resume_3;
    else if (op_data->st == 4) goto resume_4;
    else if (op_data->st == 5) goto resume_5;
    assert(op_data->st == 0);
    // Delete is forbidden even in active PGs if they're also degraded or have previous dead OSDs
    if (pg.state & (PG_DEGRADED | PG_LEFT_ON_DEAD))
    {
        finish_op(cur_op, -EBUSY);
        return;
    }
    if (!check_write_queue(cur_op, pg))
    {
        return;
    }
resume_1:
    // Determine which OSDs contain this object and delete it
    op_data->prev_set = get_object_osd_set(pg, op_data->oid, pg.cur_set.data(), &op_data->object_state);
    // Submit 1 read to determine the actual version number
    submit_primary_subops(SUBMIT_RMW_READ, UINT64_MAX, pg.pg_size, op_data->prev_set, cur_op);
resume_2:
    op_data->st = 2;
    return;
resume_3:
    if (op_data->errors > 0)
    {
        pg_cancel_write_queue(pg, cur_op, op_data->oid, op_data->epipe > 0 ? -EPIPE : -EIO);
        return;
    }
    // Save version override for parallel reads
    pg.ver_override[op_data->oid] = op_data->fact_ver;
    // Submit deletes
    op_data->fact_ver++;
    submit_primary_del_subops(cur_op, NULL, 0, op_data->object_state ? op_data->object_state->osd_set : pg.cur_loc_set);
resume_4:
    op_data->st = 4;
    return;
resume_5:
    if (op_data->errors > 0)
    {
        pg_cancel_write_queue(pg, cur_op, op_data->oid, op_data->epipe > 0 ? -EPIPE : -EIO);
        return;
    }
    // Remove version override
    pg.ver_override.erase(op_data->oid);
    // Adjust PG stats after "instant stabilize", because we need object_state above
    if (!op_data->object_state)
    {
        pg.clean_count--;
    }
    else
    {
        remove_object_from_state(op_data->oid, op_data->object_state, pg);
        free_object_state(pg, &op_data->object_state);
    }
    pg.total_count--;
    osd_op_t *next_op = NULL;
    auto next_it = pg.write_queue.find(op_data->oid);
    if (next_it != pg.write_queue.end() && next_it->second == cur_op)
    {
        pg.write_queue.erase(next_it++);
        if (next_it != pg.write_queue.end() && next_it->first == op_data->oid)
            next_op = next_it->second;
    }
    finish_op(cur_op, cur_op->req.rw.len);
    if (next_op)
    {
        // Continue next write to the same object
        continue_primary_write(next_op);
    }
}
