// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <netinet/tcp.h>
#include <sys/epoll.h>

#include <algorithm>

#include "str_util.h"
#include "osd.h"

#define SELF_FD -1

// Peering loop
void osd_t::handle_peers()
{
    if (peering_state & OSD_PEERING_PGS)
    {
        bool still = false;
        for (auto & p: pgs)
        {
            if (p.second.state == PG_PEERING)
            {
                if (continue_pg_peering(p.second))
                {
                    ringloop->wakeup();
                    return;
                }
                else
                {
                    still = true;
                }
            }
        }
        if (!still)
        {
            // Done all PGs
            peering_state = peering_state & ~OSD_PEERING_PGS;
        }
    }
    if ((peering_state & OSD_FLUSHING_PGS) && !readonly)
    {
        bool still = false;
        for (auto & p: pgs)
        {
            if ((p.second.state & (PG_ACTIVE | PG_HAS_UNCLEAN)) == (PG_ACTIVE | PG_HAS_UNCLEAN))
            {
                if (!p.second.flush_batch)
                {
                    submit_pg_flush_ops(p.second);
                }
                still = true;
            }
        }
        if (!still)
        {
            peering_state = peering_state & ~OSD_FLUSHING_PGS | OSD_RECOVERING;
        }
    }
    if (!(peering_state & OSD_FLUSHING_PGS) && (peering_state & OSD_RECOVERING) && !readonly)
    {
        if (!continue_recovery())
        {
            peering_state = peering_state & ~OSD_RECOVERING;
        }
    }
    if (peering_state & OSD_SCRUBBING)
    {
        if (!continue_scrub())
        {
            peering_state = peering_state & ~OSD_SCRUBBING;
        }
    }
}

void osd_t::break_pg_locks(osd_num_t peer_osd)
{
    for (auto lock_it = pg_locks.begin(); lock_it != pg_locks.end(); )
    {
        if (lock_it->second.primary_osd == peer_osd)
        {
            if (log_level > 3)
            {
                printf("Break PG %u/%u lock on disconnection of OSD %ju\n", lock_it->first.pool_id, lock_it->first.pg_num, peer_osd);
            }
            pg_locks.erase(lock_it++);
        }
        else
            lock_it++;
    }
}

void osd_t::repeer_pgs(osd_num_t peer_osd)
{
    // Re-peer affected PGs
    for (auto & p: pgs)
    {
        auto & pg = p.second;
        bool repeer = false;
        if (pg.state & (PG_PEERING | PG_ACTIVE | PG_INCOMPLETE))
        {
            for (osd_num_t pg_osd: pg.all_peers)
            {
                if (pg_osd == peer_osd)
                {
                    repeer = true;
                    break;
                }
            }
            if (repeer)
            {
                // Repeer this pg
                printf("[PG %u/%u] Repeer because of OSD %ju\n", pg.pool_id, pg.pg_num, peer_osd);
                repeer_pg(pg);
            }
        }
    }
}

void osd_t::repeer_pg(pg_t & pg)
{
    if (!(pg.state & (PG_ACTIVE | PG_REPEERING)) || pg.can_repeer())
    {
        start_pg_peering(pg);
    }
    else
    {
        // Stop accepting new operations, wait for current ones to finish or fail
        pg.state = pg.state & ~PG_ACTIVE | PG_REPEERING;
        report_pg_state(pg);
    }
}

// Reset PG state (when peering or stopping)
void osd_t::reset_pg(pg_t & pg)
{
    pg.cur_peers.clear();
    pg.dead_peers.clear();
    pg.state_dict.clear();
    copies_to_delete_after_sync_count -= pg.copies_to_delete_after_sync.size();
    pg.copies_to_delete_after_sync.clear();
    corrupted_objects -= pg.corrupted_count;
    incomplete_objects -= pg.incomplete_objects.size();
    misplaced_objects -= pg.misplaced_objects.size();
    degraded_objects -= pg.degraded_objects.size();
    pg.corrupted_count = 0;
    pg.incomplete_objects.clear();
    pg.misplaced_objects.clear();
    pg.degraded_objects.clear();
    pg.flush_actions.clear();
    pg.ver_override.clear();
    if (pg.flush_batch)
    {
        delete pg.flush_batch;
    }
    pg.flush_batch = NULL;
    for (auto p: pg.write_queue)
    {
        cancel_primary_write(p.second);
    }
    pg.write_queue.clear();
    uint64_t pg_stripe_size = st_cli.pool_config[pg.pool_id].pg_stripe_size;
    for (auto it = unstable_writes.begin(); it != unstable_writes.end(); )
    {
        // Forget this PG's unstable writes
        if (INODE_POOL(it->first.oid.inode) == pg.pool_id && map_to_pg(it->first.oid, pg_stripe_size) == pg.pg_num)
            unstable_writes.erase(it++);
        else
            it++;
    }
    dirty_pgs.erase({ .pool_id = pg.pool_id, .pg_num = pg.pg_num });
}

// Drop connections of clients who have this PG in dirty_pgs
void osd_t::drop_dirty_pg_connections(pool_pg_num_t pg)
{
    if (immediate_commit != IMMEDIATE_ALL)
    {
        std::vector<int> to_stop;
        for (auto & cp: msgr.clients)
        {
            if (cp.second->dirty_pgs.find(pg) != cp.second->dirty_pgs.end())
            {
                to_stop.push_back(cp.first);
            }
        }
        for (auto peer_fd: to_stop)
        {
            msgr.stop_client(peer_fd);
        }
    }
}

// Repeer on each connect/disconnect peer event
void osd_t::start_pg_peering(pg_t & pg)
{
    pg.state = PG_PEERING;
    this->peering_state |= OSD_PEERING_PGS;
    reset_pg(pg);
    drop_dirty_pg_connections({ .pool_id = pg.pool_id, .pg_num = pg.pg_num });
    // Try to connect with current peers if they're up, but we don't have connections to them
    // Otherwise we may erroneously decide that the pg is incomplete :-)
    bool all_connected = true;
    for (auto pg_osd: pg.all_peers)
    {
        if (pg_osd != this->osd_num &&
            msgr.osd_peer_fds.find(pg_osd) == msgr.osd_peer_fds.end() &&
            msgr.wanted_peers.find(pg_osd) == msgr.wanted_peers.end())
        {
            msgr.connect_peer(pg_osd, st_cli.peer_states[pg_osd]);
            if (!st_cli.peer_states[pg_osd].is_null())
                all_connected = false;
        }
    }
    if (!all_connected && !allow_net_split)
    {
        // Wait until all OSDs are either connected or their /osd/state disappears from etcd
        pg.state = PG_INCOMPLETE;
        // Fall through to cleanup list results
    }
    // Calculate current write OSD set
    pg.pg_cursize = 0;
    pg.cur_set.resize(pg.target_set.size());
    pg.cur_loc_set.clear();
    for (int role = 0; role < pg.target_set.size(); role++)
    {
        pg.cur_set[role] = pg.target_set[role] == this->osd_num ||
            msgr.osd_peer_fds.find(pg.target_set[role]) != msgr.osd_peer_fds.end() ? pg.target_set[role] : 0;
        if (pg.cur_set[role] != 0)
        {
            pg.pg_cursize++;
            pg.cur_loc_set.push_back({
                .role = (uint64_t)role,
                .osd_num = pg.cur_set[role],
                .loc_bad = 0,
            });
        }
    }
    if (pg.pg_cursize < pg.pg_minsize)
    {
        // FIXME: Incomplete EC PGs may currently easily lead to write hangs ("slow ops" in OSD logs)
        // because such PGs don't flush unstable entries on secondary OSDs so they can't remove these
        // entries from their journals...
        pg.state = PG_INCOMPLETE;
    }
    std::set<osd_num_t> cur_peers;
    std::set<osd_num_t> dead_peers;
    for (auto pg_osd: pg.all_peers)
    {
        if (pg_osd == this->osd_num || msgr.osd_peer_fds.find(pg_osd) != msgr.osd_peer_fds.end())
            cur_peers.insert(pg_osd);
        else
            dead_peers.insert(pg_osd);
    }
    pg.cur_peers.insert(pg.cur_peers.begin(), cur_peers.begin(), cur_peers.end());
    pg.dead_peers.insert(pg.dead_peers.begin(), dead_peers.begin(), dead_peers.end());
    if (pg.target_history.size())
    {
        // Refuse to start PG if no peers are available from any of the historical OSD sets
        // (PG history is kept up to the latest active+clean state)
        for (auto & history_set: pg.target_history)
        {
            int nonzero = 0, found = 0;
            for (auto history_osd: history_set)
            {
                if (history_osd != 0)
                {
                    nonzero++;
                    if (history_osd == this->osd_num ||
                        msgr.osd_peer_fds.find(history_osd) != msgr.osd_peer_fds.end())
                    {
                        found++;
                    }
                }
            }
            if (nonzero >= pg.pg_data_size && found < pg.pg_data_size)
            {
                pg.state = PG_INCOMPLETE;
            }
        }
    }
    if (pg.peering_state)
    {
        // Adjust the peering operation that's still in progress - discard unneeded results
        for (auto it = pg.peering_state->list_ops.begin(); it != pg.peering_state->list_ops.end();)
        {
            if (pg.state == PG_INCOMPLETE || cur_peers.find(it->first) == cur_peers.end())
            {
                // Discard the result after completion, which, chances are, will be unsuccessful
                discard_list_subop(it->second);
                pg.peering_state->list_ops.erase(it++);
            }
            else
                it++;
        }
        for (auto it = pg.peering_state->list_results.begin(); it != pg.peering_state->list_results.end();)
        {
            if (pg.state == PG_INCOMPLETE || cur_peers.find(it->first) == cur_peers.end())
            {
                if (it->second.buf)
                {
                    free(it->second.buf);
                }
                pg.peering_state->list_results.erase(it++);
            }
            else
                it++;
        }
    }
    if (pg.state == PG_INCOMPLETE)
    {
        if (pg.peering_state)
        {
            delete pg.peering_state;
            pg.peering_state = NULL;
        }
        report_pg_state(pg);
        return;
    }
    if (!pg.peering_state)
    {
        pg.peering_state = new pg_peering_state_t();
        pg.peering_state->pool_id = pg.pool_id;
        pg.peering_state->pg_num = pg.pg_num;
    }
    pg.peering_state->locked = false;
    pg.peering_state->lists_done = false;
    report_pg_state(pg);
}

bool osd_t::continue_pg_peering(pg_t & pg)
{
    if (pg.peering_state->locked)
    {
        pg.peering_state->lists_done = true;
        for (osd_num_t peer_osd: pg.cur_peers)
        {
            if (pg.peering_state->list_results.find(peer_osd) == pg.peering_state->list_results.end())
            {
                pg.peering_state->lists_done = false;
            }
            if (pg.peering_state->list_ops.find(peer_osd) != pg.peering_state->list_ops.end() ||
                pg.peering_state->list_results.find(peer_osd) != pg.peering_state->list_results.end())
            {
                continue;
            }
            submit_list_subop(peer_osd, pg.peering_state);
        }
    }
    if (pg.peering_state->lists_done)
    {
        pg.calc_object_states(log_level);
        report_pg_state(pg);
        schedule_scrub(pg);
        incomplete_objects += pg.incomplete_objects.size();
        misplaced_objects += pg.misplaced_objects.size();
        // FIXME: degraded objects may currently include misplaced, too! Report them separately?
        degraded_objects += pg.degraded_objects.size();
        if (pg.state & PG_HAS_UNCLEAN)
            this->peering_state = peering_state | OSD_FLUSHING_PGS;
        else if (pg.state & (PG_HAS_DEGRADED | PG_HAS_MISPLACED))
        {
            this->peering_state = peering_state | OSD_RECOVERING;
            if (pg.state & PG_HAS_DEGRADED)
            {
                // Restart recovery from degraded objects
                this->recovery_last_degraded = true;
                this->recovery_last_pg = {};
                this->recovery_last_oid = {};
            }
        }
        return true;
    }
    return false;
}

void osd_t::record_pg_lock(pg_t & pg, osd_num_t peer_osd, uint64_t pg_state)
{
    if (!pg_state)
        pg.lock_peers.erase(peer_osd);
    else
        pg.lock_peers[peer_osd] = pg_state;
}

void osd_t::relock_pg(pg_t & pg)
{
    if (!enable_pg_locks || pg.disable_pg_locks && !pg.lock_peers.size())
    {
        if (pg.state & PG_PEERING)
            pg.peering_state->locked = true;
        continue_pg(pg);
        return;
    }
    if (pg.inflight_locks > 0 || pg.lock_waiting)
    {
        return;
    }
    // Check that lock_peers are equal to cur_peers and correct the difference, if any
    uint64_t wanted_state = pg.state;
    std::vector<osd_num_t> diff_osds;
    if (!(pg.state & (PG_STOPPING | PG_OFFLINE | PG_INCOMPLETE)) && !pg.disable_pg_locks)
    {
        for (osd_num_t peer_osd: pg.cur_peers)
        {
            if (peer_osd != this->osd_num)
            {
                auto lock_it = pg.lock_peers.find(peer_osd);
                if (lock_it == pg.lock_peers.end())
                    diff_osds.push_back(peer_osd);
                else
                {
                    if (lock_it->second != wanted_state)
                        diff_osds.push_back(peer_osd);
                    lock_it->second |= ((uint64_t)1 << 63);
                }
            }
        }
    }
    int relock_osd_count = diff_osds.size();
    for (auto & lp: pg.lock_peers)
    {
        if (!(lp.second & ((uint64_t)1 << 63)))
            diff_osds.push_back(lp.first);
        lp.second &= ~((uint64_t)1 << 63);
    }
    if (!diff_osds.size())
    {
        if (pg.state & PG_PEERING)
            pg.peering_state->locked = true;
        continue_pg(pg);
        return;
    }
    pg.inflight_locks++;
    for (int i = 0; i < diff_osds.size(); i++)
    {
        bool unlock_peer = (i >= relock_osd_count);
        uint64_t new_state = unlock_peer ? 0 : pg.state;
        auto peer_osd = diff_osds[i];
        auto peer_fd_it = msgr.osd_peer_fds.find(peer_osd);
        if (peer_fd_it == msgr.osd_peer_fds.end())
        {
            if (unlock_peer)
            {
                // Peer is dead - unlocked automatically
                record_pg_lock(pg, peer_osd, new_state);
                diff_osds.erase(diff_osds.begin()+(i--));
            }
            continue;
        }
        int peer_fd = peer_fd_it->second;
        auto cl = msgr.clients.at(peer_fd);
        if (!cl->enable_pg_locks)
        {
            // Peer does not support locking - just instantly remember the lock as successful
            record_pg_lock(pg, peer_osd, new_state);
            diff_osds.erase(diff_osds.begin()+(i--));
            continue;
        }
        pg.inflight_locks++;
        osd_op_t *op = new osd_op_t();
        op->op_type = OSD_OP_OUT;
        op->peer_fd = peer_fd;
        op->req = (osd_any_op_t){
            .sec_lock = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .opcode = OSD_OP_SEC_LOCK,
                },
                .flags = (uint64_t)(unlock_peer ? OSD_SEC_UNLOCK_PG : OSD_SEC_LOCK_PG),
                .pool_id = pg.pool_id,
                .pg_num = pg.pg_num,
                .pg_state = new_state,
            },
        };
        op->callback = [this, peer_osd](osd_op_t *op)
        {
            pool_pg_num_t pg_id = { .pool_id = (pool_id_t)op->req.sec_lock.pool_id, .pg_num = (pg_num_t)op->req.sec_lock.pg_num };
            auto pg_it = pgs.find(pg_id);
            if (pg_it == pgs.end())
            {
                printf("Warning: PG %u/%u is gone during lock attempt\n", pg_id.pool_id, pg_id.pg_num);
                return;
            }
            auto & pg = pg_it->second;
            if (op->reply.hdr.retval == 0)
            {
                record_pg_lock(pg_it->second, peer_osd, op->req.sec_lock.pg_state);
            }
            else if (op->reply.hdr.retval != -EPIPE)
            {
                printf(
                    (op->reply.hdr.retval == -ENOENT
                        ? "Failed to %1$s PG %2$u/%3$u on OSD %4$ju - peer didn't load PG info yet\n"
                        : (op->reply.sec_lock.cur_primary
                            ? "Failed to %1$s PG %2$u/%3$u on OSD %4$ju - taken by OSD %6$ju (retval=%5$jd)\n"
                            : "Failed to %1$s PG %2$u/%3$u on OSD %4$ju - retval=%5$jd\n")),
                    op->req.sec_lock.flags == OSD_SEC_UNLOCK_PG ? "unlock" : "lock",
                    pg_id.pool_id, pg_id.pg_num, peer_osd, op->reply.hdr.retval, op->reply.sec_lock.cur_primary
                );
                // Retry relocking/unlocking PG after a short time
                pg.lock_waiting = true;
                tfd->set_timer(pg_lock_retry_interval_ms, false, [this, pg_id](int)
                {
                    auto pg_it = pgs.find(pg_id);
                    if (pg_it != pgs.end())
                    {
                        pg_it->second.lock_waiting = false;
                        relock_pg(pg_it->second);
                    }
                });
            }
            pg.inflight_locks--;
            relock_pg(pg);
            delete op;
        };
        msgr.outbox_push(op);
    }
    if (pg.state & PG_PEERING)
    {
        pg.peering_state->locked = !diff_osds.size();
    }
    pg.inflight_locks--;
    continue_pg(pg);
}

void osd_t::submit_list_subop(osd_num_t role_osd, pg_peering_state_t *ps)
{
    if (role_osd == this->osd_num)
    {
        // Self
        osd_op_t *op = new osd_op_t();
        op->op_type = 0;
        op->peer_fd = SELF_FD;
        clock_gettime(CLOCK_REALTIME, &op->tv_begin);
        op->bs_op = new blockstore_op_t();
        op->bs_op->opcode = BS_OP_LIST;
        op->bs_op->pg_alignment = st_cli.pool_config[ps->pool_id].pg_stripe_size;
        op->bs_op->min_oid.inode = ((uint64_t)ps->pool_id << (64 - POOL_ID_BITS));
        op->bs_op->max_oid.inode = ((uint64_t)(ps->pool_id+1) << (64 - POOL_ID_BITS)) - 1;
        op->bs_op->max_oid.stripe = UINT64_MAX;
        op->bs_op->pg_count = pg_counts[ps->pool_id];
        op->bs_op->pg_number = ps->pg_num-1;
        op->bs_op->callback = [this, ps, op, role_osd](blockstore_op_t *bs_op)
        {
            if (op->bs_op->retval < 0)
            {
                printf("Local OP_LIST failed: retval=%d\n", op->bs_op->retval);
                force_stop(1);
                return;
            }
            add_bs_subop_stats(op);
            printf(
                "[PG %u/%u] Got object list from OSD %ju (local): %d object versions (%ju of them stable)\n",
                ps->pool_id, ps->pg_num, role_osd, bs_op->retval, bs_op->version
            );
            ps->list_results[role_osd] = {
                .buf = (obj_ver_id*)op->bs_op->buf,
                .total_count = (uint64_t)op->bs_op->retval,
                .stable_count = op->bs_op->version,
            };
            ps->list_ops.erase(role_osd);
            delete op->bs_op;
            op->bs_op = NULL;
            delete op;
        };
        ps->list_ops[role_osd] = op;
        bs->enqueue_op(op->bs_op);
    }
    else
    {
        auto role_fd_it = msgr.osd_peer_fds.find(role_osd);
        if (role_fd_it == msgr.osd_peer_fds.end())
        {
            printf("Failed to get object list from OSD %ju because it is disconnected\n", role_osd);
            return;
        }
        // Peer
        osd_op_t *op = new osd_op_t();
        op->op_type = OSD_OP_OUT;
        op->peer_fd = role_fd_it->second;
        op->req = (osd_any_op_t){
            .sec_list = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .opcode = OSD_OP_SEC_LIST,
                },
                .list_pg = ps->pg_num,
                .pg_count = pg_counts[ps->pool_id],
                .pg_stripe_size = st_cli.pool_config[ps->pool_id].pg_stripe_size,
                .min_inode = ((uint64_t)(ps->pool_id) << (64 - POOL_ID_BITS)),
                .max_inode = ((uint64_t)(ps->pool_id+1) << (64 - POOL_ID_BITS)) - 1,
            },
        };
        op->callback = [this, ps, role_osd](osd_op_t *op)
        {
            if (op->reply.hdr.retval < 0)
            {
                printf("Failed to get object list from OSD %ju (retval=%jd), disconnecting peer\n", role_osd, op->reply.hdr.retval);
                int fail_fd = op->peer_fd;
                ps->list_ops.erase(role_osd);
                delete op;
                msgr.stop_client(fail_fd);
                return;
            }
            printf(
                "[PG %u/%u] Got object list from OSD %ju: %jd object versions (%ju of them stable)\n",
                ps->pool_id, ps->pg_num, role_osd, op->reply.hdr.retval, op->reply.sec_list.stable_count
            );
            ps->list_results[role_osd] = {
                .buf = (obj_ver_id*)op->buf,
                .total_count = (uint64_t)op->reply.hdr.retval,
                .stable_count = op->reply.sec_list.stable_count,
            };
            // set op->buf to NULL so it doesn't get freed
            op->buf = NULL;
            ps->list_ops.erase(role_osd);
            delete op;
        };
        ps->list_ops[role_osd] = op;
        msgr.outbox_push(op);
    }
}

void osd_t::discard_list_subop(osd_op_t *list_op)
{
    if (list_op->peer_fd == SELF_FD)
    {
        // Self
        list_op->bs_op->callback = [list_op](blockstore_op_t *bs_op)
        {
            if (list_op->bs_op->buf)
                free(list_op->bs_op->buf);
            delete list_op->bs_op;
            list_op->bs_op = NULL;
            delete list_op;
        };
    }
    else
    {
        // Peer
        list_op->callback = [](osd_op_t *list_op)
        {
            delete list_op;
        };
    }
}

bool osd_t::stop_pg(pg_t & pg)
{
    if (pg.peering_state)
    {
        // Stop peering
        for (auto it = pg.peering_state->list_ops.begin(); it != pg.peering_state->list_ops.end(); it++)
        {
            discard_list_subop(it->second);
        }
        for (auto it = pg.peering_state->list_results.begin(); it != pg.peering_state->list_results.end(); it++)
        {
            if (it->second.buf)
            {
                free(it->second.buf);
            }
        }
        delete pg.peering_state;
        pg.peering_state = NULL;
    }
    if (pg.state & (PG_STOPPING | PG_OFFLINE))
    {
        return false;
    }
    drop_dirty_pg_connections({ .pool_id = pg.pool_id, .pg_num = pg.pg_num });
    pg.state = pg.state & ~PG_STARTING & ~PG_PEERING & ~PG_INCOMPLETE & ~PG_ACTIVE & ~PG_REPEERING & ~PG_OFFLINE | PG_STOPPING;
    if (pg.can_stop())
    {
        finish_stop_pg(pg);
    }
    else
    {
        report_pg_state(pg);
    }
    return true;
}

void osd_t::finish_stop_pg(pg_t & pg)
{
    pg.state = PG_OFFLINE;
    reset_pg(pg);
    report_pg_state(pg);
}

void osd_t::report_pg_state(pg_t & pg)
{
    pg.print_state();
    this->pg_state_dirty.insert({ .pool_id = pg.pool_id, .pg_num = pg.pg_num });
    if (pg.state & PG_ACTIVE)
    {
        plan_scrub(pg, false);
    }
    if (pg.state == PG_ACTIVE && (pg.target_history.size() > 0 || pg.all_peers.size() > pg.target_set.size()))
    {
        // Clear history of active+clean PGs
        pg.history_changed = true;
        pg.target_history.clear();
        pg.all_peers = pg.target_set;
        std::sort(pg.all_peers.begin(), pg.all_peers.end());
        pg.cur_peers = pg.target_set;
        // Change pg_config at the same time, otherwise our PG reconciling loop may try to apply the old metadata
        auto & pg_cfg = st_cli.pool_config[pg.pool_id].pg_config[pg.pg_num];
        pg_cfg.target_history = pg.target_history;
        pg_cfg.all_peers = pg.all_peers;
    }
    else if (pg.state == (PG_ACTIVE|PG_LEFT_ON_DEAD))
    {
        // Clear history of active+left_on_dead PGs, but leave dead OSDs in all_peers
        if (pg.target_history.size())
        {
            pg.history_changed = true;
            pg.target_history.clear();
        }
        std::set<osd_num_t> dead_peers;
        for (auto pg_osd: pg.all_peers)
        {
            dead_peers.insert(pg_osd);
        }
        for (auto pg_osd: pg.cur_peers)
        {
            dead_peers.erase(pg_osd);
        }
        for (auto pg_osd: pg.target_set)
        {
            if (pg_osd)
            {
                dead_peers.insert(pg_osd);
            }
        }
        auto new_all_peers = std::vector<osd_num_t>(dead_peers.begin(), dead_peers.end());
        if (pg.all_peers != new_all_peers)
        {
            pg.history_changed = true;
            pg.all_peers = new_all_peers;
        }
        pg.cur_peers.clear();
        for (auto pg_osd: pg.target_set)
        {
            if (pg_osd)
            {
                pg.cur_peers.push_back(pg_osd);
            }
        }
        auto & pg_cfg = st_cli.pool_config[pg.pool_id].pg_config[pg.pg_num];
        pg_cfg.target_history = pg.target_history;
        pg_cfg.all_peers = pg.all_peers;
    }
    relock_pg(pg);
    if (pg.state == PG_OFFLINE && !this->pg_config_applied)
    {
        apply_pg_config();
    }
    report_pg_states();
}

void osd_t::rm_inflight(pg_t & pg)
{
    pg.inflight--;
    assert(pg.inflight >= 0);
    continue_pg(pg);
}

void osd_t::continue_pg(pg_t & pg)
{
    if ((pg.state & PG_STOPPING) && pg.can_stop())
    {
        finish_stop_pg(pg);
    }
    else if ((pg.state & PG_REPEERING) && pg.can_repeer())
    {
        start_pg_peering(pg);
    }
    else if ((pg.state & PG_PEERING) && pg.peering_state->locked)
    {
        continue_pg_peering(pg);
    }
}
