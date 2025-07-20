// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "osd.h"

#define FLUSH_BATCH 512

void osd_t::submit_pg_flush_ops(pg_t & pg)
{
    pg_flush_batch_t *fb = new pg_flush_batch_t();
    pg.flush_batch = fb;
    auto it = pg.flush_actions.begin(), prev_it = pg.flush_actions.begin();
    bool first = true;
    while (it != pg.flush_actions.end())
    {
        if (!first &&
            (it->first.oid.inode != prev_it->first.oid.inode ||
                (it->first.oid.stripe & ~STRIPE_MASK) != (prev_it->first.oid.stripe & ~STRIPE_MASK)) &&
            (fb->rollback_lists[it->first.osd_num].size() >= FLUSH_BATCH ||
                fb->stable_lists[it->first.osd_num].size() >= FLUSH_BATCH))
        {
            // Stop only at the object boundary
            break;
        }
        it->second.submitted = true;
        if (it->second.rollback)
        {
            fb->flush_objects++;
            fb->rollback_lists[it->first.osd_num].push_back((obj_ver_id){
                .oid = it->first.oid,
                .version = it->second.rollback_to,
            });
        }
        if (it->second.make_stable)
        {
            fb->flush_objects++;
            fb->stable_lists[it->first.osd_num].push_back((obj_ver_id){
                .oid = it->first.oid,
                .version = it->second.stable_to,
            });
        }
        prev_it = it;
        first = false;
        it++;
    }
    for (auto & l: fb->rollback_lists)
    {
        if (l.second.size() > 0)
        {
            fb->flush_ops++;
            if (!submit_flush_op(pg.pool_id, pg.pg_num, fb, true, l.first, l.second.size(), l.second.data()))
                return;
        }
    }
    for (auto & l: fb->stable_lists)
    {
        if (l.second.size() > 0)
        {
            fb->flush_ops++;
            if (!submit_flush_op(pg.pool_id, pg.pg_num, fb, false, l.first, l.second.size(), l.second.data()))
                return;
        }
    }
}

void osd_t::handle_flush_op(bool rollback, pool_id_t pool_id, pg_num_t pg_num, pg_flush_batch_t *fb, osd_num_t peer_osd, int retval)
{
    if (log_level > 2)
    {
        printf("[PG %u/%u] flush batch %jx completed on OSD %ju with result %d\n",
            pool_id, pg_num, (uint64_t)fb, peer_osd, retval);
    }
    pool_pg_num_t pg_id = { .pool_id = pool_id, .pg_num = pg_num };
    if (pgs.find(pg_id) == pgs.end() || pgs[pg_id].flush_batch != fb)
    {
        // Throw the result away
        return;
    }
    fb->flush_done++;
    if (retval != 0)
    {
        if (peer_osd == this->osd_num)
        {
            throw std::runtime_error(
                std::string(rollback
                    ? "Error while doing local rollback operation: "
                    : "Error while doing local stabilize operation: "
                ) + strerror(-retval)
            );
        }
        else
        {
            printf("Error while doing flush on OSD %ju: %d (%s)\n", osd_num, retval, strerror(-retval));
            auto fd_it = msgr.osd_peer_fds.find(peer_osd);
            if (fd_it != msgr.osd_peer_fds.end())
            {
                // Will repeer/stop this PG
                msgr.stop_client(fd_it->second);
            }
        }
    }
    if (fb->flush_done == fb->flush_ops)
    {
        // This flush batch is done
        std::vector<osd_op_t*> continue_ops;
        auto & pg = pgs.at(pg_id);
        auto it = pg.flush_actions.begin(), prev_it = it;
        while (1)
        {
            if (it == pg.flush_actions.end() || !it->second.submitted ||
                it->first.oid.inode != prev_it->first.oid.inode ||
                (it->first.oid.stripe & ~STRIPE_MASK) != (prev_it->first.oid.stripe & ~STRIPE_MASK))
            {
                pg.ver_override.erase((object_id){
                    .inode = prev_it->first.oid.inode,
                    .stripe = (prev_it->first.oid.stripe & ~STRIPE_MASK),
                });
                auto wr_it = pg.write_queue.find((object_id){
                    .inode = prev_it->first.oid.inode,
                    .stripe = (prev_it->first.oid.stripe & ~STRIPE_MASK),
                });
                if (wr_it != pg.write_queue.end())
                {
                    if (log_level > 2)
                    {
                        printf("[PG %u/%u] continuing write %jx to object %jx:%jx after flush\n",
                            pool_id, pg_num, (uint64_t)wr_it->second, wr_it->first.inode, wr_it->first.stripe);
                    }
                    continue_ops.push_back(wr_it->second);
                }
            }
            if (it == pg.flush_actions.end() || !it->second.submitted)
            {
                if (it != pg.flush_actions.begin())
                {
                    pg.flush_actions.erase(pg.flush_actions.begin(), it);
                }
                break;
            }
            prev_it = it++;
        }
        delete fb;
        pg.flush_batch = NULL;
        if (!pg.flush_actions.size())
        {
            pg.state = pg.state & ~PG_HAS_UNCLEAN;
            report_pg_state(pg);
        }
        for (osd_op_t *op: continue_ops)
        {
            continue_primary_write(op);
        }
        continue_pg(pg);
    }
}

bool osd_t::submit_flush_op(pool_id_t pool_id, pg_num_t pg_num, pg_flush_batch_t *fb, bool rollback, osd_num_t peer_osd, int count, obj_ver_id *data)
{
    osd_op_t *op = new osd_op_t();
    // Copy buffer so it gets freed along with the operation
    op->buf = malloc_or_die(sizeof(obj_ver_id) * count);
    memcpy(op->buf, data, sizeof(obj_ver_id) * count);
    if (log_level > 2)
    {
        printf(
            "[PG %u/%u] flush batch %jx on OSD %ju: %s objects: ",
            pool_id, pg_num, (uint64_t)fb, peer_osd, rollback ? "rollback" : "stabilize"
        );
        for (int i = 0; i < count; i++)
        {
            printf(i > 0 ? ", %jx:%jx v%ju" : "%jx:%jx v%ju", data[i].oid.inode, data[i].oid.stripe, data[i].version);
        }
        printf("\n");
    }
    if (peer_osd == this->osd_num)
    {
        // local
        clock_gettime(CLOCK_REALTIME, &op->tv_begin);
        op->bs_op = new blockstore_op_t((blockstore_op_t){
            .opcode = (uint64_t)(rollback ? BS_OP_ROLLBACK : BS_OP_STABLE),
            .callback = [this, op, pool_id, pg_num, fb](blockstore_op_t *bs_op)
            {
                add_bs_subop_stats(op);
                handle_flush_op(bs_op->opcode == BS_OP_ROLLBACK, pool_id, pg_num, fb, this->osd_num, bs_op->retval);
                delete op->bs_op;
                op->bs_op = NULL;
                delete op;
            },
            {
                .len = (uint32_t)count,
            },
            .buf = (uint8_t*)op->buf,
        });
        bs->enqueue_op(op->bs_op);
    }
    else
    {
        // Peer
        op->op_type = OSD_OP_OUT;
        op->iov.push_back(op->buf, count * sizeof(obj_ver_id));
        op->req = (osd_any_op_t){
            .sec_stab = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .opcode = (uint64_t)(rollback ? OSD_OP_SEC_ROLLBACK : OSD_OP_SEC_STABILIZE),
                },
                .len = count * sizeof(obj_ver_id),
            },
        };
        op->callback = [this, pool_id, pg_num, fb, peer_osd](osd_op_t *op)
        {
            handle_flush_op(op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK, pool_id, pg_num, fb, peer_osd, op->reply.hdr.retval);
            delete op;
        };
        auto peer_fd_it = msgr.osd_peer_fds.find(peer_osd);
        if (peer_fd_it != msgr.osd_peer_fds.end())
        {
            op->peer_fd = peer_fd_it->second;
            msgr.outbox_push(op);
        }
        else
        {
            // Fail it immediately
            op->reply.hdr.retval = -EPIPE;
            op->callback(op);
            return false;
        }
    }
    return true;
}

bool osd_t::pick_next_recovery(osd_recovery_op_t &op)
{
    if (!pgs.size())
    {
        return false;
    }
    // Restart scanning from the same degraded/misplaced status as the last time
    for (int tried_degraded = 0; tried_degraded < 2; tried_degraded++)
    {
        if (recovery_last_degraded ? !no_recovery : !no_rebalance)
        {
            // Don't try to "recover" misplaced objects if "recovery" would make them degraded
            auto mask = recovery_last_degraded ? (PG_ACTIVE | PG_HAS_DEGRADED) : (PG_ACTIVE | PG_DEGRADED | PG_HAS_MISPLACED);
            auto check = recovery_last_degraded ? (PG_ACTIVE | PG_HAS_DEGRADED) : (PG_ACTIVE | PG_HAS_MISPLACED);
            // Restart scanning from the same PG as the last time
        restart:
            for (auto pg_it = pgs.lower_bound(recovery_last_pg); pg_it != pgs.end(); pg_it++)
            {
                auto & src = recovery_last_degraded ? pg_it->second.degraded_objects : pg_it->second.misplaced_objects;
                if ((pg_it->second.state & mask) == check && src.size() > 0)
                {
                    auto pool_it = st_cli.pool_config.find(pg_it->first.pool_id);
                    if (pool_it != st_cli.pool_config.end() && pool_it->second.backfillfull)
                    {
                        // Skip the pool
                        recovery_last_pg.pool_id++;
                        goto restart;
                    }
                    // Restart scanning from the next object
                    for (auto obj_it = src.upper_bound(recovery_last_oid); obj_it != src.end(); obj_it++)
                    {
                        if (recovery_ops.find(obj_it->first) == recovery_ops.end())
                        {
                            op.degraded = recovery_last_degraded;
                            recovery_last_oid = op.oid = obj_it->first;
                            recovery_pg_done++;
                            // Switch to another PG after recovery_pg_switch operations
                            // to always mix all PGs during recovery but still benefit
                            // from recovery queue depth greater than 1
                            if (recovery_pg_done >= recovery_pg_switch)
                            {
                                recovery_pg_done = 0;
                                recovery_last_pg.pg_num++;
                                recovery_last_oid = {};
                            }
                            return true;
                        }
                    }
                }
            }
        }
        recovery_last_degraded = !recovery_last_degraded;
        recovery_last_pg = {};
        recovery_last_oid = {};
    }
    return false;
}

void osd_t::submit_recovery_op(osd_recovery_op_t *op)
{
    op->osd_op = new osd_op_t();
    op->osd_op->op_type = OSD_OP_OUT;
    op->osd_op->req = (osd_any_op_t){
        .rw = {
            .header = {
                .magic = SECONDARY_OSD_OP_MAGIC,
                .id = 1,
                .opcode = OSD_OP_WRITE,
            },
            .inode = op->oid.inode,
            .offset = op->oid.stripe,
            .len = 0,
        },
    };
    if (log_level > 2)
    {
        printf("Submitting recovery operation for %jx:%jx (%s)\n", op->oid.inode, op->oid.stripe, op->degraded ? "degraded" : "misplaced");
    }
    op->osd_op->peer_fd = -1;
    op->osd_op->callback = [this, op](osd_op_t *osd_op)
    {
        if (osd_op->reply.hdr.retval < 0)
        {
            // Error recovering object
            // EPIPE is totally harmless (peer is gone), others like EIO/EDOM may be not
            printf(
                "[PG %u/%u] Recovery operation failed with object %jx:%jx: error %jd\n",
                INODE_POOL(op->oid.inode),
                map_to_pg(op->oid, st_cli.pool_config.at(INODE_POOL(op->oid.inode)).pg_stripe_size),
                op->oid.inode, op->oid.stripe, osd_op->reply.hdr.retval
            );
        }
        else if (log_level > 2)
        {
            printf("Recovery operation done for %jx:%jx\n", op->oid.inode, op->oid.stripe);
        }
        finish_recovery_op(op);
    };
    exec_op(op->osd_op);
}

void osd_t::apply_recovery_tune_interval()
{
    if (rtune_timer_id >= 0)
    {
        tfd->clear_timer(rtune_timer_id);
        rtune_timer_id = -1;
    }
    if (recovery_tune_interval != 0)
    {
        rtune_timer_id = this->tfd->set_timer(recovery_tune_interval*1000, true, [this](int timer_id)
        {
            tune_recovery();
        });
    }
    else
    {
        recovery_target_sleep_us = recovery_sleep_us;
    }
}

void osd_t::finish_recovery_op(osd_recovery_op_t *op)
{
    // CAREFUL! op = &recovery_ops[op->oid]. Don't access op->* after recovery_ops.erase()
    delete op->osd_op;
    op->osd_op = NULL;
    recovery_ops.erase(op->oid);
    if (immediate_commit != IMMEDIATE_ALL)
    {
        recovery_done++;
        if (recovery_done >= recovery_sync_batch)
        {
            // Force sync every <recovery_sync_batch> operations
            // This is required not to pile up an excessive amount of delete operations
            autosync();
            recovery_done = 0;
        }
    }
    continue_recovery();
}

void osd_t::tune_recovery()
{
    static int accounted_ops[] = {
        OSD_OP_SEC_READ, OSD_OP_SEC_WRITE, OSD_OP_SEC_WRITE_STABLE,
        OSD_OP_SEC_STABILIZE, OSD_OP_SEC_SYNC, OSD_OP_SEC_DELETE
    };
    uint64_t total_client_usec = 0, total_recovery_usec = 0, recovery_count = 0;
    for (int i = 0; i < sizeof(accounted_ops)/sizeof(accounted_ops[0]); i++)
    {
        total_client_usec += (msgr.stats.op_stat_sum[accounted_ops[i]]
            - rtune_prev_stats.op_stat_sum[accounted_ops[i]]);
        total_recovery_usec += (msgr.recovery_stats.op_stat_sum[accounted_ops[i]]
            - rtune_prev_recovery_stats.op_stat_sum[accounted_ops[i]]);
        recovery_count += (msgr.recovery_stats.op_stat_count[accounted_ops[i]]
            - rtune_prev_recovery_stats.op_stat_count[accounted_ops[i]]);
        rtune_prev_stats.op_stat_sum[accounted_ops[i]] = msgr.stats.op_stat_sum[accounted_ops[i]];
        rtune_prev_recovery_stats.op_stat_sum[accounted_ops[i]] = msgr.recovery_stats.op_stat_sum[accounted_ops[i]];
        rtune_prev_recovery_stats.op_stat_count[accounted_ops[i]] = msgr.recovery_stats.op_stat_count[accounted_ops[i]];
    }
    total_client_usec -= total_recovery_usec;
    if (recovery_count == 0)
    {
        return;
    }
    // example:
    // total 3 GB/s
    // recovery queue 1
    // 120 OSDs
    // EC 5+3
    // 128kb block_size => 640kb object
    // 3000*1024/640/120 = 40 MB/s per OSD = 64 recovered objects per OSD
    //   = 64*8*2 subops = 1024 recovery subop iops
    // 8 recovery subop queue
    // => subop avg latency = 0.0078125 sec
    // utilisation = 8
    // target util 1
    // intuitively target latency should be 8x of real
    // target_lat = rtune_avg_lat * utilisation / target_util
    //            = rtune_avg_lat * rtune_avg_lat * rtune_avg_iops / target_util
    //            = 0.0625
    // recovery utilisation will be 1
    rtune_client_util = total_client_usec/1000000.0/recovery_tune_interval;
    rtune_target_util = (rtune_client_util < recovery_tune_client_util_low
        ? recovery_tune_util_high
        : recovery_tune_util_low + (rtune_client_util >= recovery_tune_client_util_high
            ? 0 : (recovery_tune_util_high-recovery_tune_util_low)*
                (recovery_tune_client_util_high-rtune_client_util)/(recovery_tune_client_util_high-recovery_tune_client_util_low)
        )
    );
    rtune_avg_lat = total_recovery_usec/recovery_count;
    uint64_t target_lat = rtune_avg_lat * rtune_avg_lat/1000000.0 * recovery_count/recovery_tune_interval / rtune_target_util;
    auto sleep_us = target_lat > rtune_avg_lat+recovery_tune_sleep_min_us ? target_lat-rtune_avg_lat : 0;
    if (sleep_us > recovery_tune_sleep_cutoff_us)
    {
        return;
    }
    if (recovery_target_sleep_items.size() != recovery_tune_agg_interval)
    {
        recovery_target_sleep_items.resize(recovery_tune_agg_interval);
        for (int i = 0; i < recovery_tune_agg_interval; i++)
            recovery_target_sleep_items[i] = 0;
        recovery_target_sleep_total = 0;
        recovery_target_sleep_cur = 0;
        recovery_target_sleep_count = 0;
    }
    recovery_target_sleep_total -= recovery_target_sleep_items[recovery_target_sleep_cur];
    recovery_target_sleep_items[recovery_target_sleep_cur] = sleep_us;
    recovery_target_sleep_cur = (recovery_target_sleep_cur+1) % recovery_tune_agg_interval;
    recovery_target_sleep_total += sleep_us;
    if (recovery_target_sleep_count < recovery_tune_agg_interval)
        recovery_target_sleep_count++;
    recovery_target_sleep_us = recovery_target_sleep_total / recovery_target_sleep_count;
    if (log_level > 1)
    {
        printf(
            "[OSD %ju] auto-tune: client util: %.2f, recovery util: %.2f, lat: %ju us -> target util %.2f, delay %ju us\n",
            osd_num, rtune_client_util, total_recovery_usec/1000000.0/recovery_tune_interval,
            rtune_avg_lat, rtune_target_util, recovery_target_sleep_us
        );
    }
}

// Just trigger write requests for degraded objects. They'll be recovered during writing
bool osd_t::continue_recovery()
{
    while (recovery_ops.size() < recovery_queue_depth)
    {
        osd_recovery_op_t op;
        if (pick_next_recovery(op))
        {
            recovery_ops[op.oid] = op;
            submit_recovery_op(&recovery_ops[op.oid]);
        }
        else
            return false;
    }
    return true;
}
