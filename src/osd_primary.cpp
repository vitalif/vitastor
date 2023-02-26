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
    // FIXME: op_data->pg_data_size can probably be removed (there's pg.pg_data_size)
    uint64_t pg_data_size = (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks);
    uint64_t pg_block_size = bs_block_size * pg_data_size;
    object_id oid = {
        .inode = cur_op->req.rw.inode,
        // oid.stripe = starting offset of the parity stripe
        .stripe = (cur_op->req.rw.offset/pg_block_size)*pg_block_size,
    };
    pg_num_t pg_num = (oid.stripe/pool_cfg.pg_stripe_size) % pg_counts[pool_id] + 1; // like map_to_pg()
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
    int chain_size = 0;
    if (cur_op->req.hdr.opcode == OSD_OP_READ && cur_op->req.rw.meta_revision > 0)
    {
        // Chained read
        auto inode_it = st_cli.inode_config.find(cur_op->req.rw.inode);
        if (inode_it->second.mod_revision != cur_op->req.rw.meta_revision)
        {
            // Client view of the metadata differs from OSD's view
            // Operation can't be completed correctly, client should retry later
            finish_op(cur_op, -EPIPE);
            return false;
        }
        // Find parents from the same pool. Optimized reads only work within pools
        while (inode_it != st_cli.inode_config.end() && inode_it->second.parent_id &&
            INODE_POOL(inode_it->second.parent_id) == pg_it->second.pool_id &&
            // Check for loops
            inode_it->second.parent_id != cur_op->req.rw.inode)
        {
            chain_size++;
            inode_it = st_cli.inode_config.find(inode_it->second.parent_id);
        }
        if (chain_size)
        {
            // Add the original inode
            chain_size++;
        }
    }
    osd_primary_op_data_t *op_data = (osd_primary_op_data_t*)calloc_or_die(
        // Allocate:
        // - op_data
        1, sizeof(osd_primary_op_data_t) +
        // - stripes
        // - resulting bitmap buffers
        stripe_count * (clean_entry_bitmap_size + sizeof(osd_rmw_stripe_t)) +
        chain_size * (
            // - copy of the chain
            sizeof(inode_t) +
            // - object states for every chain item
            sizeof(void*) +
            // - bitmap buffers for chained read
            stripe_count * clean_entry_bitmap_size +
            // - 'missing' flags for chained reads
            (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 0 : pg_it->second.pg_size)
        )
    );
    void *data_buf = (uint8_t*)op_data + sizeof(osd_primary_op_data_t);
    op_data->pg_num = pg_num;
    op_data->oid = oid;
    op_data->stripes = (osd_rmw_stripe_t*)data_buf;
    data_buf = (uint8_t*)data_buf + sizeof(osd_rmw_stripe_t) * stripe_count;
    op_data->scheme = pool_cfg.scheme;
    op_data->pg_data_size = pg_data_size;
    op_data->pg_size = pg_it->second.pg_size;
    cur_op->op_data = op_data;
    split_stripes(pg_data_size, bs_block_size, (uint32_t)(cur_op->req.rw.offset - oid.stripe), cur_op->req.rw.len, op_data->stripes);
    // Allocate bitmaps along with stripes to avoid extra allocations and fragmentation
    for (int i = 0; i < stripe_count; i++)
    {
        op_data->stripes[i].bmp_buf = data_buf;
        data_buf = (uint8_t*)data_buf + clean_entry_bitmap_size;
    }
    op_data->chain_size = chain_size;
    if (chain_size > 0)
    {
        op_data->read_chain = (inode_t*)data_buf;
        data_buf = (uint8_t*)data_buf + sizeof(inode_t) * chain_size;
        op_data->chain_states = (pg_osd_set_state_t**)data_buf;
        data_buf = (uint8_t*)data_buf + sizeof(pg_osd_set_state_t*) * chain_size;
        op_data->snapshot_bitmaps = data_buf;
        data_buf = (uint8_t*)data_buf + chain_size * stripe_count * clean_entry_bitmap_size;
        op_data->missing_flags = (uint8_t*)data_buf;
        data_buf = (uint8_t*)data_buf + chain_size * (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 0 : pg_it->second.pg_size);
        // Copy chain
        int chain_num = 0;
        op_data->read_chain[chain_num++] = cur_op->req.rw.inode;
        auto inode_it = st_cli.inode_config.find(cur_op->req.rw.inode);
        while (inode_it != st_cli.inode_config.end() && inode_it->second.parent_id &&
            INODE_POOL(inode_it->second.parent_id) == pg_it->second.pool_id &&
            // Check for loops
            inode_it->second.parent_id != cur_op->req.rw.inode)
        {
            op_data->read_chain[chain_num++] = inode_it->second.parent_id;
            op_data->chain_states[chain_num++] = NULL;
            inode_it = st_cli.inode_config.find(inode_it->second.parent_id);
        }
    }
    pg_it->second.inflight++;
    return true;
}

uint64_t* osd_t::get_object_osd_set(pg_t &pg, object_id &oid, pg_osd_set_state_t **object_state)
{
    if (!(pg.state & (PG_HAS_INCOMPLETE | PG_HAS_DEGRADED | PG_HAS_MISPLACED)))
    {
        *object_state = NULL;
        return pg.cur_set.data();
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
    return pg.cur_set.data();
}

void osd_t::continue_primary_read(osd_op_t *cur_op)
{
    if (!cur_op->op_data && !prepare_primary_rw(cur_op))
    {
        return;
    }
    osd_primary_op_data_t *op_data = cur_op->op_data;
    if (op_data->chain_size)
    {
        continue_chained_read(cur_op);
        return;
    }
    if (op_data->st == 1)
        goto resume_1;
    else if (op_data->st == 2)
        goto resume_2;
resume_0:
    cur_op->reply.rw.bitmap_len = 0;
    {
        auto & pg = pgs.at({ .pool_id = INODE_POOL(op_data->oid.inode), .pg_num = op_data->pg_num });
        if (cur_op->req.rw.len == 0)
        {
            // len=0 => bitmap read
            for (int role = 0; role < op_data->pg_data_size; role++)
            {
                op_data->stripes[role].read_start = 0;
                op_data->stripes[role].read_end = UINT32_MAX;
            }
        }
        else
        {
            for (int role = 0; role < op_data->pg_data_size; role++)
            {
                op_data->stripes[role].read_start = op_data->stripes[role].req_start;
                op_data->stripes[role].read_end = op_data->stripes[role].req_end;
            }
        }
        // Determine version
        auto vo_it = pg.ver_override.find(op_data->oid);
        op_data->target_ver = vo_it != pg.ver_override.end() ? vo_it->second : UINT64_MAX;
        // PG may have degraded or misplaced objects
        op_data->prev_set = get_object_osd_set(pg, op_data->oid, &op_data->object_state);
        if (pg.state == PG_ACTIVE || op_data->scheme == POOL_SCHEME_REPLICATED)
        {
            // Fast happy-path
            if (op_data->scheme == POOL_SCHEME_REPLICATED &&
                op_data->object_state && (op_data->object_state->state & OBJ_INCOMPLETE))
            {
                finish_op(cur_op, -EIO);
                return;
            }
            cur_op->buf = alloc_read_buffer(op_data->stripes, op_data->pg_data_size, 0);
            submit_primary_subops(SUBMIT_RMW_READ, op_data->target_ver, op_data->prev_set, cur_op);
            op_data->st = 1;
        }
        else
        {
            if (extend_missing_stripes(op_data->stripes, op_data->prev_set, op_data->pg_data_size, pg.pg_size) < 0)
            {
                finish_op(cur_op, -EIO);
                return;
            }
            // Submit reads
            op_data->pg_size = pg.pg_size;
            op_data->scheme = pg.scheme;
            op_data->degraded = 1;
            cur_op->buf = alloc_read_buffer(op_data->stripes, pg.pg_size, 0);
            submit_primary_subops(SUBMIT_RMW_READ, op_data->target_ver, op_data->prev_set, cur_op);
            op_data->st = 1;
        }
    }
resume_1:
    return;
resume_2:
    if (op_data->errors > 0)
    {
        if (op_data->errcode == -EIO || op_data->errcode == -EDOM)
        {
            // I/O or checksum error
            auto & pg = pgs.at({ .pool_id = INODE_POOL(op_data->oid.inode), .pg_num = op_data->pg_num });
            // FIXME: ref = true ideally... because new_state != state is not necessarily true if it's freed and recreated
            op_data->object_state = mark_object_corrupted(pg, op_data->oid, op_data->object_state, op_data->stripes, false);
            goto resume_0;
        }
        finish_op(cur_op, op_data->errcode);
        return;
    }
    cur_op->reply.rw.version = op_data->fact_ver;
    cur_op->reply.rw.bitmap_len = op_data->pg_data_size * clean_entry_bitmap_size;
    if (op_data->degraded)
    {
        // Reconstruct missing stripes
        osd_rmw_stripe_t *stripes = op_data->stripes;
        if (op_data->scheme == POOL_SCHEME_XOR)
        {
            reconstruct_stripes_xor(stripes, op_data->pg_size, clean_entry_bitmap_size);
        }
        else if (op_data->scheme == POOL_SCHEME_EC)
        {
            reconstruct_stripes_ec(stripes, op_data->pg_size, op_data->pg_data_size, clean_entry_bitmap_size);
        }
        cur_op->iov.push_back(op_data->stripes[0].bmp_buf, cur_op->reply.rw.bitmap_len);
        for (int role = 0; role < op_data->pg_size; role++)
        {
            if (stripes[role].req_end != 0)
            {
                // Send buffer in parts to avoid copying
                cur_op->iov.push_back(
                    (uint8_t*)stripes[role].read_buf + (stripes[role].req_start - stripes[role].read_start),
                    stripes[role].req_end - stripes[role].req_start
                );
            }
        }
    }
    else
    {
        cur_op->iov.push_back(op_data->stripes[0].bmp_buf, cur_op->reply.rw.bitmap_len);
        cur_op->iov.push_back(cur_op->buf, cur_op->req.rw.len);
    }
    finish_op(cur_op, cur_op->req.rw.len);
}

pg_osd_set_state_t *osd_t::mark_object_corrupted(pg_t & pg, object_id oid, pg_osd_set_state_t *prev_object_state, osd_rmw_stripe_t *stripes, bool ref)
{
    pg_osd_set_state_t *object_state = NULL;
    get_object_osd_set(pg, oid, &object_state);
    if (prev_object_state != object_state)
    {
        // Object state changed in between by a parallel I/O operation, skip marking as failed
        if (ref)
        {
            deref_object_state(pg, &prev_object_state, ref);
            if (object_state)
                object_state->ref_count++;
        }
        return object_state;
    }
    pg_osd_set_t corrupted_set;
    if (object_state)
    {
        corrupted_set = object_state->osd_set;
    }
    else
    {
        for (int i = 0; i < pg.cur_set.size(); i++)
        {
            corrupted_set.push_back((pg_obj_loc_t){
                .role = (pg.scheme == POOL_SCHEME_REPLICATED ? 0 : (uint64_t)i),
                .osd_num = pg.cur_set[i],
            });
        }
    }
    // Mark object chunk(s) as corrupted
    uint64_t has_roles = 0, n_roles = 0, n_copies = 0, n_corrupted = 0;
    for (auto & chunk: corrupted_set)
    {
        bool corrupted = stripes[chunk.role].osd_num == chunk.osd_num && stripes[chunk.role].read_error;
        if (corrupted && !(chunk.loc_bad & LOC_CORRUPTED))
            n_corrupted++;
        chunk.loc_bad = chunk.loc_bad | (corrupted ? LOC_CORRUPTED : 0);
        if (!chunk.loc_bad)
        {
            if (pg.scheme == POOL_SCHEME_REPLICATED)
                n_roles = 1;
            else if (!(has_roles & (1 << chunk.role)))
            {
                n_roles++;
                has_roles |= (1 << chunk.role);
            }
            n_copies++;
        }
    }
    if (!n_corrupted)
    {
        // No chunks newly marked as corrupted - object is already marked or moved
        return object_state;
    }
    int old_pg_state = pg.state;
    if (object_state)
    {
        remove_object_from_state(oid, &object_state, pg, false);
        deref_object_state(pg, &object_state, ref);
    }
    // Calculate object state
    uint64_t obj_state = OBJ_CORRUPTED;
    int pg_state_bits = PG_HAS_CORRUPTED;
    this->corrupted_objects++;
    pg.corrupted_count++;
    if (log_level > 1)
    {
        printf("Marking object %lx:%lx corrupted: %lu chunks / %lu copies available, %lu corrupted\n",
            oid.inode, oid.stripe, n_roles, n_copies, n_corrupted);
    }
    if (n_roles < pg.pg_data_size)
    {
        this->incomplete_objects++;
        obj_state |= OBJ_INCOMPLETE;
        pg_state_bits = PG_HAS_INCOMPLETE;
    }
    else if (n_roles < pg.pg_cursize)
    {
        this->degraded_objects++;
        obj_state |= OBJ_DEGRADED;
        pg_state_bits = PG_HAS_DEGRADED;
    }
    else
    {
        this->misplaced_objects++;
        obj_state |= OBJ_MISPLACED;
        pg_state_bits = PG_HAS_MISPLACED;
    }
    pg.state |= pg_state_bits;
    if (pg.state != old_pg_state)
    {
        report_pg_state(pg);
        if ((pg.state & (PG_HAS_DEGRADED | PG_HAS_MISPLACED)) !=
            (old_pg_state & (PG_HAS_DEGRADED | PG_HAS_MISPLACED)))
        {
            peering_state = peering_state | OSD_RECOVERING;
            if ((pg.state & PG_HAS_DEGRADED) != (old_pg_state & PG_HAS_DEGRADED))
            {
                // Restart recovery from degraded objects
                recovery_last_degraded = true;
                recovery_last_pg = {};
                recovery_last_oid = {};
            }
            ringloop->wakeup();
        }
    }
    // Insert object into the new state and retry
    object_state = pg.add_object_to_state(oid, obj_state, corrupted_set);
    if (ref)
        object_state->ref_count++;
    return object_state;
}

// Decrement pg_osd_set_state_t's object_count and change PG state accordingly
void osd_t::remove_object_from_state(object_id & oid, pg_osd_set_state_t **object_state, pg_t & pg, bool report)
{
    if (!*object_state)
    {
        return;
    }
    pg_osd_set_state_t *recheck_state = NULL;
    get_object_osd_set(pg, oid, &recheck_state);
    if (recheck_state != *object_state)
    {
        recheck_state->ref_count++;
        (*object_state)->ref_count--;
        *object_state = recheck_state;
        return;
    }
    (*object_state)->object_count--;
    if ((*object_state)->state & OBJ_CORRUPTED)
    {
        this->corrupted_objects--;
        pg.corrupted_count--;
    }
    bool changed = false;
    if ((*object_state)->state & OBJ_INCOMPLETE)
    {
        // Successful write means that object is not incomplete anymore
        this->incomplete_objects--;
        pg.incomplete_objects.erase(oid);
        if (!pg.incomplete_objects.size())
        {
            pg.state = pg.state & ~PG_HAS_INCOMPLETE;
            changed = true;
        }
    }
    else if ((*object_state)->state & OBJ_DEGRADED)
    {
        this->degraded_objects--;
        pg.degraded_objects.erase(oid);
        if (!pg.degraded_objects.size())
        {
            pg.state = pg.state & ~PG_HAS_DEGRADED;
            changed = true;
        }
    }
    else if ((*object_state)->state & OBJ_MISPLACED)
    {
        this->misplaced_objects--;
        pg.misplaced_objects.erase(oid);
        if (!pg.misplaced_objects.size())
        {
            pg.state = pg.state & ~PG_HAS_MISPLACED;
            changed = true;
        }
    }
    else
    {
        throw std::runtime_error("BUG: Invalid object state: "+std::to_string((*object_state)->state));
    }
    if (changed && report)
    {
        report_pg_state(pg);
    }
}

void osd_t::deref_object_state(pg_t & pg, pg_osd_set_state_t **object_state, bool deref)
{
    if (*object_state)
    {
        if (deref)
        {
            (*object_state)->ref_count--;
        }
        if (!(*object_state)->object_count && !(*object_state)->ref_count)
        {
            pg.state_dict.erase((*object_state)->osd_set);
            *object_state = NULL;
        }
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
    op_data->prev_set = get_object_osd_set(pg, op_data->oid, &op_data->object_state);
    if (op_data->object_state)
    {
        op_data->object_state->ref_count++;
    }
    // Submit 1 read to determine the actual version number
    submit_primary_subops(SUBMIT_RMW_READ, UINT64_MAX, op_data->prev_set, cur_op);
    op_data->prev_set = NULL;
resume_2:
    op_data->st = 2;
    return;
resume_3:
    if (op_data->errors > 0)
    {
        deref_object_state(pg, &op_data->object_state, true);
        pg_cancel_write_queue(pg, cur_op, op_data->oid, op_data->errcode);
        return;
    }
    // Check CAS version
    if (cur_op->req.rw.version && op_data->fact_ver != (cur_op->req.rw.version-1))
    {
        deref_object_state(pg, &op_data->object_state, true);
        cur_op->reply.hdr.retval = -EINTR;
        cur_op->reply.rw.version = op_data->fact_ver;
        goto continue_others;
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
        deref_object_state(pg, &op_data->object_state, true);
        pg_cancel_write_queue(pg, cur_op, op_data->oid, op_data->errcode);
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
        remove_object_from_state(op_data->oid, &op_data->object_state, pg);
        deref_object_state(pg, &op_data->object_state, true);
    }
    pg.total_count--;
    cur_op->reply.hdr.retval = 0;
continue_others:
    osd_op_t *next_op = NULL;
    auto next_it = pg.write_queue.find(op_data->oid);
    if (next_it != pg.write_queue.end() && next_it->second == cur_op)
    {
        pg.write_queue.erase(next_it++);
        if (next_it != pg.write_queue.end() && next_it->first == op_data->oid)
            next_op = next_it->second;
    }
    finish_op(cur_op, cur_op->reply.hdr.retval);
    if (next_op)
    {
        // Continue next write to the same object
        continue_primary_write(next_op);
    }
}
