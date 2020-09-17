// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

#include "osd_primary.h"

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
    // K = pg_minsize in case of EC/XOR, or 1 for replicated pools
    pool_id_t pool_id = INODE_POOL(cur_op->req.rw.inode);
    auto & pool_cfg = st_cli.pool_config[pool_id];
    uint64_t pg_block_size = bs_block_size * (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_minsize);
    object_id oid = {
        .inode = cur_op->req.rw.inode,
        // oid.stripe = starting offset of the parity stripe
        .stripe = (cur_op->req.rw.offset/pg_block_size)*pg_block_size,
    };
    // FIXME: pg_stripe_size may be a per-pool config
    pg_num_t pg_num = (cur_op->req.rw.inode + oid.stripe/pg_stripe_size) % pg_counts[pool_id] + 1;
    auto pg_it = pgs.find({ .pool_id = pool_id, .pg_num = pg_num });
    if (pg_it == pgs.end() || !(pg_it->second.state & PG_ACTIVE))
    {
        // This OSD is not primary for this PG or the PG is inactive
        finish_op(cur_op, -EPIPE);
        return false;
    }
    if ((cur_op->req.rw.offset + cur_op->req.rw.len) > (oid.stripe + pg_block_size) ||
        (cur_op->req.rw.offset % bs_disk_alignment) != 0 ||
        (cur_op->req.rw.len % bs_disk_alignment) != 0)
    {
        finish_op(cur_op, -EINVAL);
        return false;
    }
    osd_primary_op_data_t *op_data = (osd_primary_op_data_t*)calloc_or_die(
        1, sizeof(osd_primary_op_data_t) + sizeof(osd_rmw_stripe_t) * (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pg_it->second.pg_size)
    );
    op_data->pg_num = pg_num;
    op_data->oid = oid;
    op_data->stripes = ((osd_rmw_stripe_t*)(op_data+1));
    op_data->scheme = pool_cfg.scheme;
    cur_op->op_data = op_data;
    split_stripes((pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pg_it->second.pg_minsize),
        bs_block_size, (uint32_t)(cur_op->req.rw.offset - oid.stripe), cur_op->req.rw.len, op_data->stripes);
    pg_it->second.inflight++;
    return true;
}

static uint64_t* get_object_osd_set(pg_t &pg, object_id &oid, uint64_t *def, pg_osd_set_state_t **object_state)
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
    osd_primary_op_data_t *op_data = cur_op->op_data;
    if (op_data->st == 1)      goto resume_1;
    else if (op_data->st == 2) goto resume_2;
    {
        auto & pg = pgs[{ .pool_id = INODE_POOL(op_data->oid.inode), .pg_num = op_data->pg_num }];
        for (int role = 0; role < (op_data->scheme == POOL_SCHEME_REPLICATED ? 1 : pg.pg_minsize); role++)
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
            cur_op->buf = alloc_read_buffer(op_data->stripes,
                (op_data->scheme == POOL_SCHEME_REPLICATED ? 1 : pg.pg_minsize), 0);
            submit_primary_subops(SUBMIT_READ, op_data->target_ver,
                (op_data->scheme == POOL_SCHEME_REPLICATED ? pg.pg_size : pg.pg_minsize), pg.cur_set.data(), cur_op);
            op_data->st = 1;
        }
        else
        {
            // PG may be degraded or have misplaced objects
            uint64_t* cur_set = get_object_osd_set(pg, op_data->oid, pg.cur_set.data(), &op_data->object_state);
            if (extend_missing_stripes(op_data->stripes, cur_set, pg.pg_minsize, pg.pg_size) < 0)
            {
                finish_op(cur_op, -EIO);
                return;
            }
            // Submit reads
            op_data->pg_minsize = pg.pg_minsize;
            op_data->pg_size = pg.pg_size;
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
        // FIXME: Always EC(k+1) by now. Add different coding schemes
        osd_rmw_stripe_t *stripes = op_data->stripes;
        for (int role = 0; role < op_data->pg_minsize; role++)
        {
            if (stripes[role].read_end != 0 && stripes[role].missing)
            {
                reconstruct_stripe_xor(stripes, op_data->pg_size, role);
            }
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
    finish_op(cur_op, cur_op->req.rw.len);
}

bool osd_t::check_write_queue(osd_op_t *cur_op, pg_t & pg)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    // Check if actions are pending for this object
    auto act_it = pg.flush_actions.lower_bound((obj_piece_id_t){
        .oid = op_data->oid,
        .osd_num = 0,
    });
    if (act_it != pg.flush_actions.end() &&
        act_it->first.oid.inode == op_data->oid.inode &&
        (act_it->first.oid.stripe & ~STRIPE_MASK) == op_data->oid.stripe)
    {
        pg.write_queue.emplace(op_data->oid, cur_op);
        return false;
    }
    // Check if there are other write requests to the same object
    auto vo_it = pg.write_queue.find(op_data->oid);
    if (vo_it != pg.write_queue.end())
    {
        op_data->st = 1;
        pg.write_queue.emplace(op_data->oid, cur_op);
        return false;
    }
    pg.write_queue.emplace(op_data->oid, cur_op);
    return true;
}

void osd_t::continue_primary_write(osd_op_t *cur_op)
{
    if (!cur_op->op_data && !prepare_primary_rw(cur_op))
    {
        return;
    }
    osd_primary_op_data_t *op_data = cur_op->op_data;
    auto & pg = pgs[{ .pool_id = INODE_POOL(op_data->oid.inode), .pg_num = op_data->pg_num }];
    if (op_data->st == 1)      goto resume_1;
    else if (op_data->st == 2) goto resume_2;
    else if (op_data->st == 3) goto resume_3;
    else if (op_data->st == 4) goto resume_4;
    else if (op_data->st == 5) goto resume_5;
    else if (op_data->st == 6) goto resume_6;
    else if (op_data->st == 7) goto resume_7;
    else if (op_data->st == 8) goto resume_8;
    else if (op_data->st == 9) goto resume_9;
    else if (op_data->st == 10) goto resume_10;
    assert(op_data->st == 0);
    if (!check_write_queue(cur_op, pg))
    {
        return;
    }
resume_1:
    // Determine blocks to read and write
    // Missing chunks are allowed to be overwritten even in incomplete objects
    // FIXME: Allow to do small writes to the old (degraded/misplaced) OSD set for lower performance impact
    op_data->prev_set = get_object_osd_set(pg, op_data->oid, pg.cur_set.data(), &op_data->object_state);
    if (op_data->scheme == POOL_SCHEME_REPLICATED)
    {
        // Simplified algorithm
        op_data->stripes[0].write_start = op_data->stripes[0].req_start;
        op_data->stripes[0].write_end = op_data->stripes[0].req_end;
        op_data->stripes[0].write_buf = cur_op->buf;
        if (pg.cur_set.data() != op_data->prev_set && (op_data->stripes[0].write_start != 0 ||
            op_data->stripes[0].write_end != bs_block_size))
        {
            // Object is degraded/misplaced and will be moved to <write_osd_set>
            op_data->stripes[0].read_start = 0;
            op_data->stripes[0].read_end = bs_block_size;
            cur_op->rmw_buf = op_data->stripes[0].read_buf = memalign_or_die(MEM_ALIGNMENT, bs_block_size);
        }
    }
    else
    {
        cur_op->rmw_buf = calc_rmw(cur_op->buf, op_data->stripes, op_data->prev_set,
            pg.pg_size, pg.pg_minsize, pg.pg_cursize, pg.cur_set.data(), bs_block_size);
    }
    // Read required blocks
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
    if (op_data->scheme == POOL_SCHEME_REPLICATED)
    {
        // Only (possibly) copy new data from the request into the recovery buffer
        if (pg.cur_set.data() != op_data->prev_set && (op_data->stripes[0].write_start != 0 ||
            op_data->stripes[0].write_end != bs_block_size))
        {
            memcpy(
                op_data->stripes[0].read_buf + op_data->stripes[0].req_start,
                op_data->stripes[0].write_buf,
                op_data->stripes[0].req_end - op_data->stripes[0].req_start
            );
            op_data->stripes[0].write_buf = op_data->stripes[0].read_buf;
            op_data->stripes[0].write_start = 0;
            op_data->stripes[0].write_end = bs_block_size;
        }
    }
    else
    {
        // Recover missing stripes, calculate parity
        calc_rmw_parity_xor(op_data->stripes, pg.pg_size, op_data->prev_set, pg.cur_set.data(), bs_block_size);
    }
    // Send writes
    if ((op_data->fact_ver >> (64-PG_EPOCH_BITS)) < pg.epoch)
    {
        op_data->target_ver = ((uint64_t)pg.epoch << (64-PG_EPOCH_BITS)) | 1;
    }
    else
    {
        if ((op_data->fact_ver & (1ul<<(64-PG_EPOCH_BITS) - 1)) == (1ul<<(64-PG_EPOCH_BITS) - 1))
        {
            assert(pg.epoch != ((1ul << PG_EPOCH_BITS)-1));
            pg.epoch++;
        }
        op_data->target_ver = op_data->fact_ver + 1;
    }
    if (pg.epoch > pg.reported_epoch)
    {
        // Report newer epoch before writing
        // FIXME: We may report only one PG state here...
        this->pg_state_dirty.insert({ .pool_id = pg.pool_id, .pg_num = pg.pg_num });
        pg.history_changed = true;
        report_pg_states();
resume_10:
        if (pg.epoch > pg.reported_epoch)
        {
            op_data->st = 10;
            return;
        }
    }
    submit_primary_subops(SUBMIT_WRITE, op_data->target_ver, pg.pg_size, pg.cur_set.data(), cur_op);
resume_4:
    op_data->st = 4;
    return;
resume_5:
    if (op_data->errors > 0)
    {
        pg_cancel_write_queue(pg, cur_op, op_data->oid, op_data->epipe > 0 ? -EPIPE : -EIO);
        return;
    }
    if (op_data->fact_ver == 1)
    {
        // Object is created
        pg.clean_count++;
        pg.total_count++;
    }
    if (op_data->object_state)
    {
        {
            int recovery_type = op_data->object_state->state & (OBJ_DEGRADED|OBJ_INCOMPLETE) ? 0 : 1;
            recovery_stat_count[0][recovery_type]++;
            if (!recovery_stat_count[0][recovery_type])
            {
                recovery_stat_count[0][recovery_type]++;
                recovery_stat_bytes[0][recovery_type] = 0;
            }
            for (int role = 0; role < (op_data->scheme == POOL_SCHEME_REPLICATED ? 1 : pg.pg_size); role++)
            {
                recovery_stat_bytes[0][recovery_type] += op_data->stripes[role].write_end - op_data->stripes[role].write_start;
            }
        }
        if (op_data->object_state->state & OBJ_MISPLACED)
        {
            // Remove extra chunks
            submit_primary_del_subops(cur_op, pg.cur_set.data(), pg.pg_size, op_data->object_state->osd_set);
            if (op_data->n_subops > 0)
            {
resume_8:
                op_data->st = 8;
                return;
resume_9:
                if (op_data->errors > 0)
                {
                    pg_cancel_write_queue(pg, cur_op, op_data->oid, op_data->epipe > 0 ? -EPIPE : -EIO);
                    return;
                }
            }
        }
        // Clear object state
        remove_object_from_state(op_data->oid, op_data->object_state, pg);
        pg.clean_count++;
    }
    // Remove version override
    pg.ver_override.erase(op_data->oid);
    // FIXME: Check for immediate_commit == IMMEDIATE_SMALL
resume_6:
resume_7:
    if (!remember_unstable_write(cur_op, pg, pg.cur_loc_set, 6))
    {
        return;
    }
    object_id oid = op_data->oid;
    finish_op(cur_op, cur_op->req.rw.len);
    // Continue other write operations to the same object
    auto next_it = pg.write_queue.find(oid);
    auto this_it = next_it;
    if (this_it != pg.write_queue.end() && this_it->second == cur_op)
    {
        next_it++;
        pg.write_queue.erase(this_it);
        if (next_it != pg.write_queue.end() && next_it->first == oid)
        {
            osd_op_t *next_op = next_it->second;
            continue_primary_write(next_op);
        }
    }
}

bool osd_t::remember_unstable_write(osd_op_t *cur_op, pg_t & pg, pg_osd_set_t & loc_set, int base_state)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    if (op_data->st == base_state)
    {
        goto resume_6;
    }
    else if (op_data->st == base_state+1)
    {
        goto resume_7;
    }
    if (immediate_commit == IMMEDIATE_ALL)
    {
        if (op_data->scheme != POOL_SCHEME_REPLICATED)
        {
            // Send STABILIZE ops immediately
            op_data->unstable_write_osds = new std::vector<unstable_osd_num_t>();
            op_data->unstable_writes = new obj_ver_id[loc_set.size()];
            {
                int last_start = 0;
                for (auto & chunk: loc_set)
                {
                    op_data->unstable_writes[last_start] = (obj_ver_id){
                        .oid = {
                            .inode = op_data->oid.inode,
                            .stripe = op_data->oid.stripe | chunk.role,
                        },
                        .version = op_data->fact_ver,
                    };
                    op_data->unstable_write_osds->push_back((unstable_osd_num_t){
                        .osd_num = chunk.osd_num,
                        .start = last_start,
                        .len = 1,
                    });
                    last_start++;
                }
            }
            submit_primary_stab_subops(cur_op);
resume_6:
            op_data->st = 6;
            return false;
resume_7:
            // FIXME: Free those in the destructor?
            delete op_data->unstable_write_osds;
            delete[] op_data->unstable_writes;
            op_data->unstable_writes = NULL;
            op_data->unstable_write_osds = NULL;
            if (op_data->errors > 0)
            {
                pg_cancel_write_queue(pg, cur_op, op_data->oid, op_data->epipe > 0 ? -EPIPE : -EIO);
                return false;
            }
        }
    }
    else
    {
        if (op_data->scheme != POOL_SCHEME_REPLICATED)
        {
            // Remember version as unstable for EC/XOR
            for (auto & chunk: loc_set)
            {
                this->dirty_osds.insert(chunk.osd_num);
                this->unstable_writes[(osd_object_id_t){
                    .osd_num = chunk.osd_num,
                    .oid = {
                        .inode = op_data->oid.inode,
                        .stripe = op_data->oid.stripe | chunk.role,
                    },
                }] = op_data->fact_ver;
            }
        }
        else
        {
            // Only remember to sync OSDs for replicated pools
            for (auto & chunk: loc_set)
            {
                this->dirty_osds.insert(chunk.osd_num);
            }
        }
        // Remember PG as dirty to drop the connection when PG goes offline
        // (this is required because of the "lazy sync")
        c_cli.clients[cur_op->peer_fd].dirty_pgs.insert({ .pool_id = pg.pool_id, .pg_num = pg.pg_num });
        dirty_pgs.insert({ .pool_id = pg.pool_id, .pg_num = pg.pg_num });
    }
    return true;
}

// Save and clear unstable_writes -> SYNC all -> STABLE all
void osd_t::continue_primary_sync(osd_op_t *cur_op)
{
    if (!cur_op->op_data)
    {
        cur_op->op_data = (osd_primary_op_data_t*)calloc_or_die(1, sizeof(osd_primary_op_data_t));
    }
    osd_primary_op_data_t *op_data = cur_op->op_data;
    if (op_data->st == 1)      goto resume_1;
    else if (op_data->st == 2) goto resume_2;
    else if (op_data->st == 3) goto resume_3;
    else if (op_data->st == 4) goto resume_4;
    else if (op_data->st == 5) goto resume_5;
    else if (op_data->st == 6) goto resume_6;
    assert(op_data->st == 0);
    if (syncs_in_progress.size() > 0)
    {
        // Wait for previous syncs, if any
        // FIXME: We may try to execute the current one in parallel, like in Blockstore, but I'm not sure if it matters at all
        syncs_in_progress.push_back(cur_op);
        op_data->st = 1;
resume_1:
        return;
    }
    else
    {
        syncs_in_progress.push_back(cur_op);
    }
resume_2:
    if (dirty_osds.size() == 0)
    {
        // Nothing to sync
        goto finish;
    }
    // Save and clear unstable_writes
    // In theory it is possible to do in on a per-client basis, but this seems to be an unnecessary complication
    // It would be cool not to copy these here at all, but someone has to deduplicate them by object IDs anyway
    if (unstable_writes.size() > 0)
    {
        op_data->unstable_write_osds = new std::vector<unstable_osd_num_t>();
        op_data->unstable_writes = new obj_ver_id[this->unstable_writes.size()];
        osd_num_t last_osd = 0;
        int last_start = 0, last_end = 0;
        for (auto it = this->unstable_writes.begin(); it != this->unstable_writes.end(); it++)
        {
            if (last_osd != it->first.osd_num)
            {
                if (last_osd != 0)
                {
                    op_data->unstable_write_osds->push_back((unstable_osd_num_t){
                        .osd_num = last_osd,
                        .start = last_start,
                        .len = last_end - last_start,
                    });
                }
                last_osd = it->first.osd_num;
                last_start = last_end;
            }
            op_data->unstable_writes[last_end] = (obj_ver_id){
                .oid = it->first.oid,
                .version = it->second,
            };
            last_end++;
        }
        if (last_osd != 0)
        {
            op_data->unstable_write_osds->push_back((unstable_osd_num_t){
                .osd_num = last_osd,
                .start = last_start,
                .len = last_end - last_start,
            });
        }
        this->unstable_writes.clear();
    }
    {
        void *dirty_buf = malloc_or_die(sizeof(pool_pg_num_t)*dirty_pgs.size() + sizeof(osd_num_t)*dirty_osds.size());
        op_data->dirty_pgs = (pool_pg_num_t*)dirty_buf;
        op_data->dirty_osds = (osd_num_t*)(dirty_buf + sizeof(pool_pg_num_t)*dirty_pgs.size());
        op_data->dirty_pg_count = dirty_pgs.size();
        op_data->dirty_osd_count = dirty_osds.size();
        int dpg = 0;
        for (auto dirty_pg_num: dirty_pgs)
        {
            pgs[dirty_pg_num].inflight++;
            op_data->dirty_pgs[dpg++] = dirty_pg_num;
        }
        dirty_pgs.clear();
        dpg = 0;
        for (auto osd_num: dirty_osds)
        {
            op_data->dirty_osds[dpg++] = osd_num;
        }
        dirty_osds.clear();
    }
    if (immediate_commit != IMMEDIATE_ALL)
    {
        // SYNC
        submit_primary_sync_subops(cur_op);
resume_3:
        op_data->st = 3;
        return;
resume_4:
        if (op_data->errors > 0)
        {
            goto resume_6;
        }
    }
    if (op_data->unstable_writes)
    {
        // Stabilize version sets, if any
        submit_primary_stab_subops(cur_op);
resume_5:
        op_data->st = 5;
        return;
    }
resume_6:
    if (op_data->errors > 0)
    {
        // Return PGs and OSDs back into their dirty sets
        for (int i = 0; i < op_data->dirty_pg_count; i++)
        {
            dirty_pgs.insert(op_data->dirty_pgs[i]);
        }
        for (int i = 0; i < op_data->dirty_osd_count; i++)
        {
            dirty_osds.insert(op_data->dirty_osds[i]);
        }
        if (op_data->unstable_writes)
        {
            // Return objects back into the unstable write set
            for (auto unstable_osd: *(op_data->unstable_write_osds))
            {
                for (int i = 0; i < unstable_osd.len; i++)
                {
                    // Except those from peered PGs
                    auto & w = op_data->unstable_writes[i];
                    pool_pg_num_t wpg = { .pool_id = INODE_POOL(w.oid.inode), .pg_num = map_to_pg(w.oid) };
                    if (pgs[wpg].state & PG_ACTIVE)
                    {
                        uint64_t & dest = this->unstable_writes[(osd_object_id_t){
                            .osd_num = unstable_osd.osd_num,
                            .oid = w.oid,
                        }];
                        dest = dest < w.version ? w.version : dest;
                        dirty_pgs.insert(wpg);
                    }
                }
            }
        }
    }
    for (int i = 0; i < op_data->dirty_pg_count; i++)
    {
        auto & pg = pgs.at(op_data->dirty_pgs[i]);
        pg.inflight--;
        if ((pg.state & PG_STOPPING) && pg.inflight == 0 && !pg.flush_batch)
        {
            finish_stop_pg(pg);
        }
    }
    // FIXME: Free those in the destructor?
    free(op_data->dirty_pgs);
    op_data->dirty_pgs = NULL;
    op_data->dirty_osds = NULL;
    if (op_data->unstable_writes)
    {
        delete op_data->unstable_write_osds;
        delete[] op_data->unstable_writes;
        op_data->unstable_writes = NULL;
        op_data->unstable_write_osds = NULL;
    }
    if (op_data->errors > 0)
    {
        finish_op(cur_op, op_data->epipe > 0 ? -EPIPE : -EIO);
    }
    else
    {
finish:
        if (cur_op->peer_fd)
        {
            auto it = c_cli.clients.find(cur_op->peer_fd);
            if (it != c_cli.clients.end())
                it->second.dirty_pgs.clear();
        }
        finish_op(cur_op, 0);
    }
    assert(syncs_in_progress.front() == cur_op);
    syncs_in_progress.pop_front();
    if (syncs_in_progress.size() > 0)
    {
        cur_op = syncs_in_progress.front();
        op_data = cur_op->op_data;
        op_data->st++;
        goto resume_2;
    }
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
    object_state->object_count--;
    if (!object_state->object_count)
    {
        pg.state_dict.erase(object_state->osd_set);
    }
}

void osd_t::continue_primary_del(osd_op_t *cur_op)
{
    if (!cur_op->op_data && !prepare_primary_rw(cur_op))
    {
        return;
    }
    osd_primary_op_data_t *op_data = cur_op->op_data;
    auto & pg = pgs[{ .pool_id = INODE_POOL(op_data->oid.inode), .pg_num = op_data->pg_num }];
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
    }
    pg.total_count--;
    object_id oid = op_data->oid;
    finish_op(cur_op, cur_op->req.rw.len);
    // Continue other write operations to the same object
    auto next_it = pg.write_queue.find(oid);
    auto this_it = next_it;
    if (this_it != pg.write_queue.end() && this_it->second == cur_op)
    {
        next_it++;
        pg.write_queue.erase(this_it);
        if (next_it != pg.write_queue.end() &&
            next_it->first == oid)
        {
            osd_op_t *next_op = next_it->second;
            continue_primary_write(next_op);
        }
    }
}
