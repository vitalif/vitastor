// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "osd_primary.h"
#include "allocator.h"

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
    auto & pg = pgs.at({ .pool_id = INODE_POOL(op_data->oid.inode), .pg_num = op_data->pg_num });
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
        op_data->stripes[0].bmp_buf = (void*)(op_data->stripes+1);
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
            pg.pg_size, op_data->pg_data_size, pg.pg_cursize, pg.cur_set.data(), bs_block_size, clean_entry_bitmap_size);
        if (!cur_op->rmw_buf)
        {
            // Refuse partial overwrite of an incomplete object
            cur_op->reply.hdr.retval = -EINVAL;
            goto continue_others;
        }
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
    if (op_data->scheme == POOL_SCHEME_REPLICATED)
    {
        // Set bitmap bits
        bitmap_set(op_data->stripes[0].bmp_buf, op_data->stripes[0].write_start, op_data->stripes[0].write_end, bs_bitmap_granularity);
        // Possibly copy new data from the request into the recovery buffer
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
        // For EC/XOR pools, save version override to make it impossible
        // for parallel reads to read different versions of data and parity
        pg.ver_override[op_data->oid] = op_data->fact_ver;
        // Recover missing stripes, calculate parity
        if (pg.scheme == POOL_SCHEME_XOR)
        {
            calc_rmw_parity_xor(op_data->stripes, pg.pg_size, op_data->prev_set, pg.cur_set.data(), bs_block_size, clean_entry_bitmap_size);
        }
        else if (pg.scheme == POOL_SCHEME_JERASURE)
        {
            calc_rmw_parity_jerasure(op_data->stripes, pg.pg_size, op_data->pg_data_size, op_data->prev_set, pg.cur_set.data(), bs_block_size, clean_entry_bitmap_size);
        }
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
    if (op_data->scheme != POOL_SCHEME_REPLICATED)
    {
        // Remove version override just after the write, but before stabilizing
        pg.ver_override.erase(op_data->oid);
    }
    if (op_data->errors > 0)
    {
        pg_cancel_write_queue(pg, cur_op, op_data->oid, op_data->epipe > 0 ? -EPIPE : -EIO);
        return;
    }
    if (op_data->object_state)
    {
        // We must forget the unclean state of the object before deleting it
        // so the next reads don't accidentally read a deleted version
        // And it should be done at the same time as the removal of the version override
        remove_object_from_state(op_data->oid, op_data->object_state, pg);
        pg.clean_count++;
    }
resume_6:
resume_7:
    if (!remember_unstable_write(cur_op, pg, pg.cur_loc_set, 6))
    {
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
        // Any kind of a non-clean object can have extra chunks, because we don't record objects
        // as degraded & misplaced or incomplete & misplaced at the same time. So try to remove extra chunks
        if (immediate_commit != IMMEDIATE_ALL)
        {
            // We can't remove extra chunks yet if fsyncs are explicit, because
            // new copies may not be committed to stable storage yet
            // We can only remove extra chunks after a successful SYNC for this PG
            for (auto & chunk: op_data->object_state->osd_set)
            {
                // Check is the same as in submit_primary_del_subops()
                if (op_data->scheme == POOL_SCHEME_REPLICATED
                    ? !contains_osd(pg.cur_set.data(), pg.pg_size, chunk.osd_num)
                    : (chunk.osd_num != pg.cur_set[chunk.role]))
                {
                    pg.copies_to_delete_after_sync.push_back((obj_ver_osd_t){
                        .osd_num = chunk.osd_num,
                        .oid = {
                            .inode = op_data->oid.inode,
                            .stripe = op_data->oid.stripe | (op_data->scheme == POOL_SCHEME_REPLICATED ? 0 : chunk.role),
                        },
                        .version = op_data->fact_ver,
                    });
                    copies_to_delete_after_sync_count++;
                }
            }
            free_object_state(pg, &op_data->object_state);
        }
        else
        {
            submit_primary_del_subops(cur_op, pg.cur_set.data(), pg.pg_size, op_data->object_state->osd_set);
            free_object_state(pg, &op_data->object_state);
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
    }
    cur_op->reply.hdr.retval = cur_op->req.rw.len;
continue_others:
    osd_op_t *next_op = NULL;
    auto next_it = pg.write_queue.find(op_data->oid);
    // Remove the operation from queue before calling finish_op so it doesn't see the completed operation in queue
    if (next_it != pg.write_queue.end() && next_it->second == cur_op)
    {
        pg.write_queue.erase(next_it++);
        if (next_it != pg.write_queue.end() && next_it->first == op_data->oid)
            next_op = next_it->second;
    }
    // finish_op would invalidate next_it if it cleared pg.write_queue, but it doesn't do that :)
    finish_op(cur_op, cur_op->req.rw.len);
    if (next_op)
    {
        // Continue next write to the same object
        continue_primary_write(next_op);
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
immediate:
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
    else if (immediate_commit == IMMEDIATE_SMALL)
    {
        int stripe_count = (op_data->scheme == POOL_SCHEME_REPLICATED ? 1 : op_data->pg_size);
        for (int role = 0; role < stripe_count; role++)
        {
            if (op_data->stripes[role].write_start == 0 &&
                op_data->stripes[role].write_end == bs_block_size)
            {
                // Big write. Treat write as unsynced
                goto lazy;
            }
        }
        goto immediate;
    }
    else
    {
lazy:
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
        auto cl_it = c_cli.clients.find(cur_op->peer_fd);
        if (cl_it != c_cli.clients.end())
        {
            cl_it->second->dirty_pgs.insert({ .pool_id = pg.pool_id, .pg_num = pg.pg_num });
        }
        dirty_pgs.insert({ .pool_id = pg.pool_id, .pg_num = pg.pg_num });
    }
    return true;
}
