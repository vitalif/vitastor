// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "osd_primary.h"

#define SELF_FD -1

void osd_t::scrub_list(pool_pg_num_t pg_id, osd_num_t role_osd, object_id min_oid)
{
    pool_id_t pool_id = pg_id.pool_id;
    pg_num_t pg_num = pg_id.pg_num;
    assert(!scrub_list_op);
    if (role_osd == this->osd_num)
    {
        // Self
        osd_op_t *op = new osd_op_t();
        op->op_type = 0;
        op->peer_fd = SELF_FD;
        clock_gettime(CLOCK_REALTIME, &op->tv_begin);
        op->bs_op = new blockstore_op_t();
        op->bs_op->opcode = BS_OP_LIST;
        op->bs_op->pg_alignment = st_cli.pool_config[pool_id].pg_stripe_size;
        if (min_oid.inode != 0 || min_oid.stripe != 0)
            op->bs_op->min_oid = min_oid;
        else
        {
            op->bs_op->min_oid.inode = ((uint64_t)pool_id << (64 - POOL_ID_BITS));
            op->bs_op->min_oid.stripe = 0;
        }
        op->bs_op->max_oid.inode = ((uint64_t)(pool_id+1) << (64 - POOL_ID_BITS)) - 1;
        op->bs_op->max_oid.stripe = UINT64_MAX;
        op->bs_op->list_stable_limit = scrub_list_limit;
        op->bs_op->pg_count = pg_counts[pool_id];
        op->bs_op->pg_number = pg_num-1;
        op->bs_op->callback = [this, op](blockstore_op_t *bs_op)
        {
            scrub_list_op = NULL;
            if (op->bs_op->retval < 0)
            {
                printf("Local OP_LIST failed: retval=%d\n", op->bs_op->retval);
                force_stop(1);
                return;
            }
            add_bs_subop_stats(op);
            scrub_cur_list = {
                .buf = (obj_ver_id*)op->bs_op->buf,
                .total_count = (uint64_t)op->bs_op->retval,
                .stable_count = op->bs_op->version,
            };
            delete op->bs_op;
            op->bs_op = NULL;
            delete op;
            continue_scrub();
        };
        scrub_list_op = op;
        bs->enqueue_op(op->bs_op);
    }
    else
    {
        // Peer
        osd_op_t *op = new osd_op_t();
        op->op_type = OSD_OP_OUT;
        op->peer_fd = msgr.osd_peer_fds.at(role_osd);
        op->req = (osd_any_op_t){
            .sec_list = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = msgr.next_subop_id++,
                    .opcode = OSD_OP_SEC_LIST,
                },
                .list_pg = pg_num,
                .pg_count = pg_counts[pool_id],
                .pg_stripe_size = st_cli.pool_config[pool_id].pg_stripe_size,
                .min_inode = min_oid.inode ? min_oid.inode : ((uint64_t)(pool_id) << (64 - POOL_ID_BITS)),
                .max_inode = ((uint64_t)(pool_id+1) << (64 - POOL_ID_BITS)) - 1,
                .min_stripe = min_oid.stripe,
                .stable_limit = scrub_list_limit,
            },
        };
        op->callback = [this, role_osd](osd_op_t *op)
        {
            scrub_list_op = NULL;
            if (op->reply.hdr.retval < 0)
            {
                printf("Failed to get object list from OSD %ju (retval=%jd), disconnecting peer\n", role_osd, op->reply.hdr.retval);
                int fail_fd = op->peer_fd;
                delete op;
                msgr.stop_client(fail_fd);
                return;
            }
            scrub_cur_list = {
                .buf = (obj_ver_id*)op->buf,
                .total_count = (uint64_t)op->reply.hdr.retval,
                .stable_count = op->reply.sec_list.stable_count,
            };
            // set op->buf to NULL so it doesn't get freed
            op->buf = NULL;
            delete op;
            continue_scrub();
        };
        scrub_list_op = op;
        msgr.outbox_push(op);
    }
}

int osd_t::pick_next_scrub(object_id & next_oid)
{
    if (!pgs.size())
    {
        if (scrub_cur_list.buf)
        {
            free(scrub_cur_list.buf);
            scrub_cur_list = {};
            scrub_last_pg = {};
        }
        return 0;
    }
    if (scrub_list_op)
    {
        return 1;
    }
    timespec tv_now;
    clock_gettime(CLOCK_REALTIME, &tv_now);
    bool rescan = scrub_last_pg.pool_id != 0 || scrub_last_pg.pg_num != 0;
    // Restart scanning from the same PG as the last time
    auto pg_it = pgs.lower_bound(scrub_last_pg);
    if (pg_it == pgs.end() && rescan)
    {
        pg_it = pgs.begin();
        rescan = false;
    }
    while (pg_it != pgs.end())
    {
        if ((pg_it->second.state & PG_ACTIVE) && pg_it->second.next_scrub && pg_it->second.next_scrub <= tv_now.tv_sec)
        {
            // Continue scrubbing from the next object
            if (scrub_last_pg == pg_it->first)
            {
                while (scrub_list_pos < scrub_cur_list.total_count)
                {
                    auto oid = scrub_cur_list.buf[scrub_list_pos].oid;
                    oid.stripe &= ~STRIPE_MASK;
                    scrub_list_pos++;
                    if (recovery_ops.find(oid) == recovery_ops.end() &&
                        scrub_ops.find(oid) == scrub_ops.end() &&
                        pg_it->second.write_queue.find(oid) == pg_it->second.write_queue.end())
                    {
                        next_oid = oid;
                        if (!(pg_it->second.state & PG_SCRUBBING))
                        {
                            // Currently scrubbing this PG
                            pg_it->second.state = pg_it->second.state | PG_SCRUBBING;
                            report_pg_state(pg_it->second);
                        }
                        return 2;
                    }
                }
            }
            if (scrub_last_pg == pg_it->first &&
                scrub_list_pos >= scrub_cur_list.total_count &&
                scrub_cur_list.stable_count < scrub_list_limit)
            {
                // End of the list, mark this PG as scrubbed and go to the next PG
            }
            else
            {
                // Continue listing
                object_id scrub_last_oid = {};
                if (scrub_last_pg == pg_it->first && scrub_cur_list.stable_count > 0)
                {
                    scrub_last_oid = scrub_cur_list.buf[scrub_cur_list.stable_count-1].oid;
                    scrub_last_oid.stripe++;
                }
                osd_num_t scrub_osd = 0;
                for (osd_num_t pg_osd: pg_it->second.cur_set)
                {
                    if (pg_osd == this->osd_num || scrub_osd == 0)
                        scrub_osd = pg_osd;
                }
                if (!(pg_it->second.state & PG_SCRUBBING))
                {
                    // Currently scrubbing this PG
                    pg_it->second.state = pg_it->second.state | PG_SCRUBBING;
                    report_pg_state(pg_it->second);
                }
                if (scrub_cur_list.buf)
                {
                    free(scrub_cur_list.buf);
                    scrub_cur_list = {};
                    scrub_list_pos = 0;
                }
                scrub_last_pg = pg_it->first;
                scrub_list(pg_it->first, scrub_osd, scrub_last_oid);
                return 1;
            }
            if (pg_it->second.state & PG_SCRUBBING)
            {
                scrub_last_pg = {};
                pg_it->second.state = pg_it->second.state & ~PG_SCRUBBING;
                pg_it->second.next_scrub = 0;
                pg_it->second.history_changed = true;
                report_pg_state(pg_it->second);
            }
            // The list is definitely not needed anymore
            if (scrub_cur_list.buf)
            {
                free(scrub_cur_list.buf);
                scrub_cur_list = {};
            }
        }
        pg_it++;
        if (pg_it == pgs.end() && rescan)
        {
            // Scan one more time to guarantee that there are no PGs to scrub
            pg_it = pgs.begin();
            rescan = false;
        }
    }
    // Scanned all PGs - no more scrubs to do
    return 0;
}

void osd_t::submit_scrub_op(object_id oid)
{
    auto osd_op = new osd_op_t();
    osd_op->op_type = OSD_OP_OUT;
    osd_op->peer_fd = -1;
    osd_op->req = (osd_any_op_t){
        .rw = {
            .header = {
                .magic = SECONDARY_OSD_OP_MAGIC,
                .id = 1,
                .opcode = OSD_OP_SCRUB,
            },
            .inode = oid.inode,
            .offset = oid.stripe,
            .len = 0,
        },
    };
    if (log_level > 2)
    {
        printf("Submitting scrub for %jx:%jx\n", oid.inode, oid.stripe);
    }
    osd_op->callback = [this](osd_op_t *osd_op)
    {
        object_id oid = { .inode = osd_op->req.rw.inode, .stripe = osd_op->req.rw.offset };
        if (osd_op->reply.hdr.retval < 0 && osd_op->reply.hdr.retval != -ENOENT)
        {
            // Scrub error
            printf(
                "Scrub failed with object %jx:%jx (PG %u/%u): error %jd\n",
                oid.inode, oid.stripe, INODE_POOL(oid.inode),
                map_to_pg(oid, st_cli.pool_config.at(INODE_POOL(oid.inode)).pg_stripe_size),
                osd_op->reply.hdr.retval
            );
        }
        else if (log_level > 2)
        {
            printf("Scrubbed %jx:%jx\n", oid.inode, oid.stripe);
        }
        delete osd_op;
        if (scrub_sleep_ms)
        {
            this->tfd->set_timer(scrub_sleep_ms, false, [this, oid](int timer_id)
            {
                scrub_ops.erase(oid);
                continue_scrub();
            });
        }
        else
        {
            scrub_ops.erase(oid);
            continue_scrub();
        }
    };
    scrub_ops[oid] = osd_op;
    exec_op(osd_op);
}

// Triggers scrub requests
// Scrub reads data from all replicas and compares it
// To scrub first we need to read objects listings
bool osd_t::continue_scrub()
{
    if (scrub_list_op)
    {
        return true;
    }
    if (no_scrub)
    {
        // Return false = no more scrub work to do
        scrub_cur_list = {};
        scrub_last_pg = {};
        scrub_nearest_ts = 0;
        if (scrub_timer_id >= 0)
        {
            tfd->clear_timer(scrub_timer_id);
            scrub_timer_id = -1;
        }
        for (auto pg_it = pgs.begin(); pg_it != pgs.end(); pg_it++)
        {
            if (pg_it->second.state & PG_SCRUBBING)
            {
                pg_it->second.state = pg_it->second.state & ~PG_SCRUBBING;
                report_pg_state(pg_it->second);
            }
        }
        return false;
    }
    while (scrub_ops.size() < scrub_queue_depth)
    {
        object_id oid;
        int r = pick_next_scrub(oid);
        if (r == 2)
            submit_scrub_op(oid);
        else
            return r;
    }
    return true;
}

void osd_t::plan_scrub(pg_t & pg, bool report_state)
{
    if ((pg.state & PG_ACTIVE) && !pg.next_scrub && auto_scrub)
    {
        timespec tv_now;
        clock_gettime(CLOCK_REALTIME, &tv_now);
        auto & pool_cfg = st_cli.pool_config.at(pg.pool_id);
        auto interval = pool_cfg.scrub_interval ? pool_cfg.scrub_interval : global_scrub_interval;
        if (pg.next_scrub != tv_now.tv_sec + interval)
        {
            pool_cfg.pg_config[pg.pg_num].next_scrub = pg.next_scrub = tv_now.tv_sec + interval;
            pg.history_changed = true;
            if (report_state)
                report_pg_state(pg);
        }
        schedule_scrub(pg);
    }
}

void osd_t::schedule_scrub(pg_t & pg)
{
    if (!no_scrub && pg.next_scrub && (!scrub_nearest_ts || scrub_nearest_ts > pg.next_scrub))
    {
        scrub_nearest_ts = pg.next_scrub;
        timespec tv_now;
        clock_gettime(CLOCK_REALTIME, &tv_now);
        if (scrub_timer_id >= 0)
        {
            tfd->clear_timer(scrub_timer_id);
            scrub_timer_id = -1;
        }
        if (tv_now.tv_sec >= scrub_nearest_ts)
        {
            scrub_nearest_ts = 0;
            peering_state = peering_state | OSD_SCRUBBING;
            ringloop->wakeup();
        }
        else
        {
            scrub_timer_id = tfd->set_timer((scrub_nearest_ts-tv_now.tv_sec)*1000, false, [this](int timer_id)
            {
                scrub_timer_id = -1;
                scrub_nearest_ts = 0;
                peering_state = peering_state | OSD_SCRUBBING;
                ringloop->wakeup();
            });
        }
    }
}

void osd_t::submit_scrub_subops(osd_op_t *cur_op)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    assert(!op_data->stripe_count);
    cur_op->req.rw.len = bs_block_size * op_data->pg->pg_data_size;
    // Determine version
    auto vo_it = op_data->pg->ver_override.find(op_data->oid);
    op_data->target_ver = vo_it != op_data->pg->ver_override.end() ? vo_it->second : UINT64_MAX;
    // Find object state
    op_data->prev_set = get_object_osd_set(*op_data->pg, op_data->oid, &op_data->object_state);
    if (!op_data->object_state)
    {
        op_data->stripe_count = op_data->pg->pg_size;
        op_data->stripes = (osd_rmw_stripe_t*)calloc_or_die(op_data->stripe_count, sizeof(osd_rmw_stripe_t));
        for (int i = 0; i < op_data->pg->pg_size; i++)
        {
            op_data->stripes[i].osd_num = op_data->prev_set[i];
            op_data->stripes[i].role = (op_data->pg->scheme == POOL_SCHEME_REPLICATED ? 0 : i);
            op_data->stripes[i].read_end = bs_block_size;
        }
    }
    else
    {
        op_data->stripe_count = 0;
        for (auto & chunk: op_data->object_state->osd_set)
        {
            // Read all chunks except outdated
            if (!(chunk.loc_bad & LOC_OUTDATED))
                op_data->stripe_count++;
        }
        op_data->stripes = (osd_rmw_stripe_t*)calloc_or_die(op_data->stripe_count, sizeof(osd_rmw_stripe_t));
        int i = 0;
        for (auto & chunk: op_data->object_state->osd_set)
        {
            if (!(chunk.loc_bad & LOC_OUTDATED))
            {
                op_data->stripes[i].osd_num = chunk.osd_num;
                op_data->stripes[i].role = chunk.role;
                op_data->stripes[i].read_end = bs_block_size;
                i++;
            }
        }
    }
    assert(!cur_op->bitmap_buf);
    cur_op->bitmap_buf = calloc_or_die(1, clean_entry_bitmap_size * op_data->stripe_count);
    for (int i = 0; i < op_data->stripe_count; i++)
    {
        op_data->stripes[i].bmp_buf = (uint8_t*)cur_op->bitmap_buf + clean_entry_bitmap_size * i;
    }
    cur_op->buf = alloc_read_buffer(op_data->stripes, op_data->stripe_count, 0);
    op_data->fact_ver = 0;
    op_data->done = op_data->errors = op_data->errcode = 0;
    op_data->n_subops = op_data->stripe_count;
    op_data->subops = new osd_op_t[op_data->stripe_count];
    op_data->st = 1;
    for (int i = 0; i < op_data->stripe_count; i++)
    {
        submit_primary_subop(cur_op, &op_data->subops[i], &op_data->stripes[i],
            false, op_data->oid.inode, op_data->target_ver);
    }
}

// The idea is that scrub should not only find out if the object
// is corrupted, but it should also verify availability of all copies
void osd_t::scrub_check_results(osd_op_t *cur_op)
{
    osd_primary_op_data_t *op_data = cur_op->op_data;
    bool inconsistent = false;
    int total = 0;
    for (int role = 0; role < op_data->stripe_count; role++)
    {
        if (!op_data->stripes[role].not_exists)
            total++;
    }
    if (!total)
    {
        // Object is deleted manually from all OSDs, forget it
        printf(
            "[PG %u/%u] Scrub detected a deleted object %jx:%jx\n",
            INODE_POOL(op_data->oid.inode), op_data->pg_num,
            op_data->oid.inode, op_data->oid.stripe
        );
        remove_object_from_state(op_data->oid, &op_data->object_state, *op_data->pg, false);
        deref_object_state(*op_data->pg, &op_data->object_state, true);
        return;
    }
    if (op_data->pg->scheme == POOL_SCHEME_REPLICATED)
    {
        // Check that all chunks have returned the same data
        int total = 0;
        int eq_to[op_data->stripe_count];
        for (int role = 0; role < op_data->stripe_count; role++)
        {
            eq_to[role] = -1;
            if (op_data->stripes[role].read_end != 0 &&
                !op_data->stripes[role].read_error &&
                !op_data->stripes[role].not_exists)
            {
                total++;
                eq_to[role] = role;
                for (int other = 0; other < role; other++)
                {
                    // Only compare with unique chunks (eq_to[other] == other)
                    if (eq_to[other] == other && memcmp(op_data->stripes[role].read_buf, op_data->stripes[other].read_buf, bs_block_size) == 0)
                    {
                        eq_to[role] = eq_to[other];
                        break;
                    }
                }
            }
        }
        int votes[op_data->stripe_count];
        for (int role = 0; role < op_data->stripe_count; role++)
            votes[role] = 0;
        for (int role = 0; role < op_data->stripe_count; role++)
        {
            if (eq_to[role] != -1)
                votes[eq_to[role]]++;
        }
        int best = -1;
        for (int role = 0; role < op_data->stripe_count; role++)
        {
            if (votes[role] > (best >= 0 ? votes[best] : 0))
                best = role;
        }
        if (best >= 0 && votes[best] < total)
        {
            bool unknown = false;
            for (int role = 0; role < op_data->stripe_count; role++)
            {
                if (role != best && votes[role] == votes[best])
                {
                    unknown = true;
                }
                if (votes[role] > 0 && votes[role] < votes[best])
                {
                    printf(
                        "[PG %u/%u] Object %jx:%jx v%ju copy on OSD %ju doesn't match %d other copies%s\n",
                        INODE_POOL(op_data->oid.inode), op_data->pg_num,
                        op_data->oid.inode, op_data->oid.stripe, op_data->fact_ver,
                        op_data->stripes[role].osd_num, votes[best],
                        scrub_find_best ? ", marking it as corrupted" : ""
                    );
                    if (scrub_find_best)
                    {
                        op_data->stripes[role].read_error = true;
                    }
                }
            }
            if (!scrub_find_best)
            {
                unknown = true;
            }
            if (unknown)
            {
                // It's unknown which replica is good. There are multiple versions with no majority
                // Mark all good replicas as ambiguous
                best = -1;
                inconsistent = true;
                printf(
                    "[PG %u/%u] Object %jx:%jx v%ju is inconsistent: copies don't match. Use vitastor-cli fix to fix it\n",
                    INODE_POOL(op_data->oid.inode), op_data->pg_num,
                    op_data->oid.inode, op_data->oid.stripe, op_data->fact_ver
                );
            }
        }
    }
    else
    {
        assert(op_data->pg->scheme == POOL_SCHEME_EC || op_data->pg->scheme == POOL_SCHEME_XOR);
        auto good_subset = ec_find_good(
            op_data->stripes, op_data->stripe_count,
            op_data->pg->pg_size, op_data->pg->pg_data_size, op_data->pg->scheme == POOL_SCHEME_XOR,
            bs_block_size, clean_entry_bitmap_size, scrub_ec_max_bruteforce, scrub_find_best
        );
        if (!good_subset.size())
        {
            inconsistent = true;
            printf(
                "[PG %u/%u] Object %jx:%jx v%ju is inconsistent: parity chunks don't match data. Use vitastor-cli fix to fix it\n",
                INODE_POOL(op_data->oid.inode), op_data->pg_num,
                op_data->oid.inode, op_data->oid.stripe, op_data->fact_ver
            );
        }
        else
        {
            int total = 0;
            for (int i = 0; i < op_data->stripe_count; i++)
            {
                if (!op_data->stripes[i].not_exists)
                {
                    // use "missing" flag to distinguish actual read errors and inconsistent chunks
                    total++;
                    op_data->stripes[i].missing = true;
                }
            }
            for (int i: good_subset)
            {
                op_data->stripes[i].missing = false;
            }
            for (int i = 0; i < op_data->stripe_count; i++)
            {
                if (op_data->stripes[i].missing)
                {
                    op_data->stripes[i].read_error = true;
                    printf(
                        "[PG %u/%u] Object %jx:%jx v%ju chunk %d on OSD %ju doesn't match other chunks%s\n",
                        INODE_POOL(op_data->oid.inode), op_data->pg_num,
                        op_data->oid.inode, op_data->oid.stripe, op_data->fact_ver,
                        op_data->stripes[i].role, op_data->stripes[i].osd_num,
                        scrub_find_best ? ", marking it as corrupted" : ""
                    );
                }
            }
        }
    }
    bool mark = inconsistent;
    for (int role = 0; !mark && role < op_data->stripe_count; role++)
    {
        if (op_data->stripes[role].read_error || op_data->stripes[role].not_exists)
            mark = true;
    }
    if (!mark)
    {
        return;
    }
    // FIXME: ref = true ideally... because new_state != state is not necessarily true if it's freed and recreated
    op_data->object_state = mark_object(*op_data->pg, op_data->oid, op_data->object_state, false /*ref*/, [op_data, inconsistent](pg_osd_set_t & new_set)
    {
        // Mark object chunk(s) as corrupted and/or missing and/or inconsistent
        int changes = 0;
        for (int i = 0; i < op_data->stripe_count; i++)
        {
            // Find the same stripe in new_set
            int set_pos = 0;
            while (set_pos < new_set.size() && (op_data->stripes[i].osd_num != new_set[set_pos].osd_num ||
                op_data->stripes[i].role != new_set[set_pos].role))
            {
                set_pos++;
            }
            if (set_pos >= new_set.size())
            {
                continue;
            }
            if (op_data->stripes[i].not_exists)
            {
                changes++;
                new_set.erase(new_set.begin()+set_pos, new_set.begin()+set_pos+1);
                continue;
            }
            auto & chunk = new_set[set_pos];
            if (op_data->stripes[i].read_error && chunk.loc_bad != LOC_CORRUPTED)
            {
                changes++;
                chunk.loc_bad = LOC_CORRUPTED;
            }
            else if (op_data->stripes[i].read_end > 0 && !op_data->stripes[chunk.role].missing &&
                (chunk.loc_bad & LOC_CORRUPTED))
            {
                changes++;
                chunk.loc_bad &= ~LOC_CORRUPTED;
            }
            if (inconsistent && !(chunk.loc_bad & LOC_INCONSISTENT))
            {
                changes++;
                chunk.loc_bad |= LOC_INCONSISTENT;
            }
            else if (!inconsistent && (chunk.loc_bad & LOC_INCONSISTENT))
            {
                changes++;
                chunk.loc_bad &= ~LOC_INCONSISTENT;
            }
        }
        return changes;
    });
}

void osd_t::continue_primary_scrub(osd_op_t *cur_op)
{
    if (!cur_op->op_data && !prepare_primary_rw(cur_op))
        return;
    if (cur_op->op_data->st == 1)
        goto resume_1;
    else if (cur_op->op_data->st == 2)
        goto resume_2;
    submit_scrub_subops(cur_op);
resume_1:
    return;
resume_2:
    if (cur_op->op_data->errors > 0 &&
        // I/O and checksum errors (represented by stripes[i].read_error) are OK
        (cur_op->op_data->errcode != -EIO && cur_op->op_data->errcode != -EDOM))
    {
        finish_op(cur_op, cur_op->op_data->errcode);
        return;
    }
    scrub_check_results(cur_op);
    finish_op(cur_op, 0);
}
