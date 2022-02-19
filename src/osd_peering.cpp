// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include <netinet/tcp.h>
#include <sys/epoll.h>

#include <algorithm>

#include "base64.h"
#include "osd.h"

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
                if (!p.second.peering_state->list_ops.size())
                {
                    p.second.calc_object_states(log_level);
                    report_pg_state(p.second);
                    incomplete_objects += p.second.incomplete_objects.size();
                    misplaced_objects += p.second.misplaced_objects.size();
                    // FIXME: degraded objects may currently include misplaced, too! Report them separately?
                    degraded_objects += p.second.degraded_objects.size();
                    if (p.second.state & PG_HAS_UNCLEAN)
                        peering_state = peering_state | OSD_FLUSHING_PGS;
                    else if (p.second.state & (PG_HAS_DEGRADED | PG_HAS_MISPLACED))
                        peering_state = peering_state | OSD_RECOVERING;
                    ringloop->wakeup();
                    return;
                }
                else
                {
                    still = true;
                }
            }
            else if (p.second.state & PG_PEERED)
            {
                still = true;
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
            else if (p.second.state & PG_PEERED)
            {
                still = true;
            }
        }
        if (!still)
        {
            peering_state = peering_state & ~OSD_FLUSHING_PGS | OSD_RECOVERING;
        }
    }
    if ((peering_state & OSD_RECOVERING) && !readonly)
    {
        if (!continue_recovery())
        {
            peering_state = peering_state & ~OSD_RECOVERING;
        }
    }
}

void osd_t::repeer_pgs(osd_num_t peer_osd)
{
    // Re-peer affected PGs
    for (auto & p: pgs)
    {
        auto & pg = p.second;
        bool repeer = false;
        if (pg.state & (PG_PEERING | PG_PEERED | PG_ACTIVE | PG_INCOMPLETE))
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
                printf("[PG %u/%u] Repeer because of OSD %lu\n", pg.pool_id, pg.pg_num, peer_osd);
                if (!(pg.state & (PG_ACTIVE | PG_REPEERING)) || pg.inflight == 0 && !pg.flush_batch)
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
        }
    }
}

// Reset PG state (when peering or stopping)
void osd_t::reset_pg(pg_t & pg)
{
    pg.cur_peers.clear();
    pg.state_dict.clear();
    copies_to_delete_after_sync_count -= pg.copies_to_delete_after_sync.size();
    pg.copies_to_delete_after_sync.clear();
    incomplete_objects -= pg.incomplete_objects.size();
    misplaced_objects -= pg.misplaced_objects.size();
    degraded_objects -= pg.degraded_objects.size();
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

// Repeer on each connect/disconnect peer event
void osd_t::start_pg_peering(pg_t & pg)
{
    pg.state = PG_PEERING;
    this->peering_state |= OSD_PEERING_PGS;
    reset_pg(pg);
    report_pg_state(pg);
    // Drop connections of clients who have this PG in dirty_pgs
    if (immediate_commit != IMMEDIATE_ALL)
    {
        std::vector<int> to_stop;
        for (auto & cp: msgr.clients)
        {
            if (cp.second->dirty_pgs.find({ .pool_id = pg.pool_id, .pg_num = pg.pg_num }) != cp.second->dirty_pgs.end())
            {
                to_stop.push_back(cp.first);
            }
        }
        for (auto peer_fd: to_stop)
        {
            msgr.stop_client(peer_fd);
        }
    }
    // Try to connect with current peers if they're up, but we don't have connections to them
    // Otherwise we may erroneously decide that the pg is incomplete :-)
    for (auto pg_osd: pg.all_peers)
    {
        if (pg_osd != this->osd_num &&
            msgr.osd_peer_fds.find(pg_osd) == msgr.osd_peer_fds.end() &&
            msgr.wanted_peers.find(pg_osd) == msgr.wanted_peers.end())
        {
            msgr.connect_peer(pg_osd, st_cli.peer_states[pg_osd]);
        }
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
                .outdated = false,
            });
        }
    }
    if (pg.pg_cursize < pg.pg_minsize)
    {
        pg.state = PG_INCOMPLETE;
        report_pg_state(pg);
        return;
    }
    std::set<osd_num_t> cur_peers;
    for (auto pg_osd: pg.all_peers)
    {
        if (pg_osd == this->osd_num || msgr.osd_peer_fds.find(pg_osd) != msgr.osd_peer_fds.end())
        {
            cur_peers.insert(pg_osd);
        }
    }
    if (pg.target_history.size())
    {
        // Refuse to start PG if no peers are available from any of the historical OSD sets
        // (PG history is kept up to the latest active+clean state)
        for (auto & history_set: pg.target_history)
        {
            bool found = true;
            for (auto history_osd: history_set)
            {
                if (history_osd != 0)
                {
                    found = false;
                    if (history_osd == this->osd_num ||
                        msgr.osd_peer_fds.find(history_osd) != msgr.osd_peer_fds.end())
                    {
                        found = true;
                        break;
                    }
                }
            }
            if (!found)
            {
                pg.state = PG_INCOMPLETE;
                report_pg_state(pg);
                return;
            }
        }
    }
    pg.cur_peers.insert(pg.cur_peers.begin(), cur_peers.begin(), cur_peers.end());
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
        return;
    }
    if (!pg.peering_state)
    {
        pg.peering_state = new pg_peering_state_t();
        pg.peering_state->pool_id = pg.pool_id;
        pg.peering_state->pg_num = pg.pg_num;
    }
    for (osd_num_t peer_osd: cur_peers)
    {
        if (pg.peering_state->list_ops.find(peer_osd) != pg.peering_state->list_ops.end() ||
            pg.peering_state->list_results.find(peer_osd) != pg.peering_state->list_results.end())
        {
            continue;
        }
        submit_sync_and_list_subop(peer_osd, pg.peering_state);
    }
    ringloop->wakeup();
}

void osd_t::submit_sync_and_list_subop(osd_num_t role_osd, pg_peering_state_t *ps)
{
    // Sync before listing, if not readonly
    if (readonly)
    {
        submit_list_subop(role_osd, ps);
    }
    else if (role_osd == this->osd_num)
    {
        // Self
        osd_op_t *op = new osd_op_t();
        op->op_type = 0;
        op->peer_fd = 0;
        clock_gettime(CLOCK_REALTIME, &op->tv_begin);
        op->bs_op = new blockstore_op_t();
        op->bs_op->opcode = BS_OP_SYNC;
        op->bs_op->callback = [this, ps, op, role_osd](blockstore_op_t *bs_op)
        {
            if (bs_op->retval < 0)
            {
                printf("Local OP_SYNC failed: %d (%s)\n", bs_op->retval, strerror(-bs_op->retval));
                force_stop(1);
                return;
            }
            add_bs_subop_stats(op);
            delete op->bs_op;
            op->bs_op = NULL;
            delete op;
            ps->list_ops.erase(role_osd);
            submit_list_subop(role_osd, ps);
        };
        bs->enqueue_op(op->bs_op);
        ps->list_ops[role_osd] = op;
    }
    else
    {
        // Peer
        auto & cl = msgr.clients.at(msgr.osd_peer_fds.at(role_osd));
        osd_op_t *op = new osd_op_t();
        op->op_type = OSD_OP_OUT;
        op->peer_fd = cl->peer_fd;
        op->req = (osd_any_op_t){
            .sec_sync = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = msgr.next_subop_id++,
                    .opcode = OSD_OP_SEC_SYNC,
                },
            },
        };
        op->callback = [this, ps, role_osd](osd_op_t *op)
        {
            if (op->reply.hdr.retval < 0)
            {
                // FIXME: Mark peer as failed and don't reconnect immediately after dropping the connection
                printf("Failed to sync OSD %lu: %ld (%s), disconnecting peer\n", role_osd, op->reply.hdr.retval, strerror(-op->reply.hdr.retval));
                int fail_fd = op->peer_fd;
                ps->list_ops.erase(role_osd);
                delete op;
                msgr.stop_client(fail_fd);
                return;
            }
            delete op;
            ps->list_ops.erase(role_osd);
            submit_list_subop(role_osd, ps);
        };
        msgr.outbox_push(op);
        ps->list_ops[role_osd] = op;
    }
}

void osd_t::submit_list_subop(osd_num_t role_osd, pg_peering_state_t *ps)
{
    if (role_osd == this->osd_num)
    {
        // Self
        osd_op_t *op = new osd_op_t();
        op->op_type = 0;
        op->peer_fd = 0;
        clock_gettime(CLOCK_REALTIME, &op->tv_begin);
        op->bs_op = new blockstore_op_t();
        op->bs_op->opcode = BS_OP_LIST;
        op->bs_op->oid.stripe = st_cli.pool_config[ps->pool_id].pg_stripe_size;
        op->bs_op->oid.inode = ((uint64_t)ps->pool_id << (64 - POOL_ID_BITS));
        op->bs_op->version = ((uint64_t)(ps->pool_id+1) << (64 - POOL_ID_BITS)) - 1;
        op->bs_op->len = pg_counts[ps->pool_id];
        op->bs_op->offset = ps->pg_num-1;
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
                "[PG %u/%u] Got object list from OSD %lu (local): %d object versions (%lu of them stable)\n",
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
        bs->enqueue_op(op->bs_op);
        ps->list_ops[role_osd] = op;
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
                printf("Failed to get object list from OSD %lu (retval=%ld), disconnecting peer\n", role_osd, op->reply.hdr.retval);
                int fail_fd = op->peer_fd;
                ps->list_ops.erase(role_osd);
                delete op;
                msgr.stop_client(fail_fd);
                return;
            }
            printf(
                "[PG %u/%u] Got object list from OSD %lu: %ld object versions (%lu of them stable)\n",
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
        msgr.outbox_push(op);
        ps->list_ops[role_osd] = op;
    }
}

void osd_t::discard_list_subop(osd_op_t *list_op)
{
    if (list_op->peer_fd == 0)
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
    if (!(pg.state & (PG_ACTIVE | PG_REPEERING)))
    {
        finish_stop_pg(pg);
        return true;
    }
    pg.state = pg.state & ~PG_ACTIVE & ~PG_REPEERING | PG_STOPPING;
    if (pg.inflight == 0 && !pg.flush_batch)
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
    if (pg.state == PG_ACTIVE && (pg.target_history.size() > 0 || pg.all_peers.size() > pg.target_set.size()))
    {
        // Clear history of active+clean PGs
        pg.history_changed = true;
        pg.target_history.clear();
        pg.all_peers = pg.target_set;
        pg.cur_peers = pg.target_set;
    }
    else if (pg.state == (PG_ACTIVE|PG_LEFT_ON_DEAD))
    {
        // Clear history of active+left_on_dead PGs, but leave dead OSDs in all_peers
        pg.history_changed = true;
        pg.target_history.clear();
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
        pg.all_peers.clear();
        pg.all_peers.insert(pg.all_peers.begin(), dead_peers.begin(), dead_peers.end());
        pg.cur_peers.clear();
        for (auto pg_osd: pg.target_set)
        {
            if (pg_osd)
            {
                pg.cur_peers.push_back(pg_osd);
            }
        }
    }
    if (pg.state == PG_OFFLINE && !this->pg_config_applied)
    {
        apply_pg_config();
    }
    report_pg_states();
}
