// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <algorithm>
#include "assert.h"
#include "pg_states.h"
#include "cluster_client.h"

#define LIST_PG_INIT 0
#define LIST_PG_WAIT_ACTIVE 1
#define LIST_PG_WAIT_CONNECT 2
#define LIST_PG_WAIT_RETRY 3
#define LIST_PG_SENT 4
#define LIST_PG_DONE 5

struct inode_list_t;

struct inode_list_pg_t;

struct inode_list_osd_t
{
    inode_list_pg_t *pg = NULL;
    osd_num_t osd_num = 0;
};

struct inode_list_pg_t
{
    inode_list_t *lst = NULL;
    int errcode = 0;
    pg_num_t pg_num = 0;
    osd_num_t cur_primary = 0;
    int state = 0;
    int inflight_ops = 0;
    timespec wait_until;
    std::vector<inode_list_osd_t> list_osds;

    bool has_unstable = false;
    std::set<object_id> objects;
    std::vector<osd_num_t> inactive_osds;
};

struct inode_list_t
{
    cluster_client_t *cli = NULL;
    pool_id_t pool_id = 0;
    inode_t inode = 0;
    uint64_t min_offset = 0;
    uint64_t max_offset = 0;
    int max_parallel_pgs = 16;

    bool fallback = false;
    int inflight_pgs = 0;
    std::map<osd_num_t, int> inflight_per_osd;
    int done_pgs = 0;
    int onstack = 0;
    std::vector<inode_list_pg_t*> pgs;
    pg_num_t real_pg_count = 0;
    std::function<void(int status, int pgs_left, pg_num_t pg_num, std::set<object_id>&& objects)> callback;
};

void cluster_client_t::list_inode(inode_t inode, uint64_t min_offset, uint64_t max_offset, int max_parallel_pgs, std::function<void(
    int status, int pgs_left, pg_num_t pg_num, std::set<object_id>&& objects)> pg_callback)
{
    init_msgr();
    pool_id_t pool_id = INODE_POOL(inode);
    if (!pool_id || st_cli.pool_config.find(pool_id) == st_cli.pool_config.end())
    {
        if (log_level > 0)
            fprintf(stderr, "Pool %u does not exist\n", pool_id);
        pg_callback(-EINVAL, 0, 0, std::set<object_id>());
        return;
    }
    auto pg_stripe_size = st_cli.pool_config.at(pool_id).pg_stripe_size;
    if (min_offset)
        min_offset = (min_offset/pg_stripe_size) * pg_stripe_size;
    inode_list_t *lst = new inode_list_t();
    lst->cli = this;
    lst->pool_id = pool_id;
    lst->inode = inode;
    lst->min_offset = min_offset;
    lst->max_offset = max_offset;
    lst->callback = pg_callback;
    lst->max_parallel_pgs = max_parallel_pgs <= 0 ? 16 : max_parallel_pgs;
    lists.push_back(lst);
    continue_listing(lst);
}

bool cluster_client_t::continue_listing(inode_list_t *lst)
{
    if (lst->onstack > 0)
    {
        return true;
    }
    lst->onstack++;
    if (restart_listing(lst))
    {
        for (int i = 0; i < lst->pgs.size() && lst->inflight_pgs < lst->max_parallel_pgs; i++)
        {
            retry_start_pg_listing(lst->pgs[i]);
        }
    }
    if (check_finish_listing(lst))
    {
        // Do not change lst->onstack because it's already freed
        return false;
    }
    lst->onstack--;
    return true;
}

bool cluster_client_t::restart_listing(inode_list_t* lst)
{
    auto pool_it = st_cli.pool_config.find(lst->pool_id);
    // We want listing to be consistent. To achieve it we should:
    // 1) retry listing of each PG if its state changes
    // 2) abort listing if PG count changes during listing
    // 3) ideally, only talk to the primary OSD - this will be done separately
    // So first we add all PGs without checking their state
    if (pool_it == st_cli.pool_config.end() ||
        lst->real_pg_count != pool_it->second.real_pg_count)
    {
        for (auto pg: lst->pgs)
        {
            if (pg->inflight_ops > 0)
            {
                // Wait until all in-progress listings complete or fail
                return false;
            }
        }
        for (auto pg: lst->pgs)
        {
            delete pg;
        }
        if (log_level > 0 && lst->real_pg_count)
        {
            fprintf(stderr, "PG count in pool %u changed during listing\n", lst->pool_id);
        }
        lst->pgs.clear();
        if (pool_it == st_cli.pool_config.end())
        {
            // Unknown pool
            lst->callback(-EINVAL, 0, 0, std::set<object_id>());
            return false;
        }
        else if (lst->done_pgs)
        {
            // PG count changed during listing, it should fail
            lst->callback(-EAGAIN, 0, 0, std::set<object_id>());
            return false;
        }
        else
        {
            lst->real_pg_count = pool_it->second.real_pg_count;
            for (pg_num_t pg_num = 1; pg_num <= lst->real_pg_count; pg_num++)
            {
                inode_list_pg_t *pg = new inode_list_pg_t();
                pg->lst = lst;
                pg->pg_num = pg_num;
                lst->pgs.push_back(pg);
            }
        }
    }
    return true;
}

void cluster_client_t::retry_start_pg_listing(inode_list_pg_t *pg)
{
    if (pg->state == LIST_PG_SENT || pg->state == LIST_PG_DONE)
    {
        return;
    }
    if (pg->state == LIST_PG_WAIT_RETRY)
    {
        // Check if the timeout expired
        timespec tv;
        clock_gettime(CLOCK_REALTIME, &tv);
        if (tv.tv_sec < pg->wait_until.tv_sec ||
            tv.tv_sec == pg->wait_until.tv_sec && tv.tv_nsec < pg->wait_until.tv_nsec)
        {
            return;
        }
    }
    int new_st = start_pg_listing(pg);
    if (new_st == LIST_PG_SENT || new_st == LIST_PG_WAIT_CONNECT)
    {
        // sent => wait for completion
        // not connected, but OSD state exists => wait for PG or OSD state change infinitely
        pg->state = new_st;
        return;
    }
    if (new_st == LIST_PG_WAIT_ACTIVE && pg->state != LIST_PG_WAIT_ACTIVE)
    {
        if (!client_wait_up_timeout)
        {
            fprintf(stderr, "PG %u/%u is inactive, skipping listing\n", pg->lst->pool_id, pg->pg_num);
            pg->errcode = -EPIPE;
            pg->list_osds.clear();
            pg->objects.clear();
            finish_list_pg(pg, false);
            return;
        }
        pg->state = new_st;
        clock_gettime(CLOCK_REALTIME, &pg->wait_until);
        pg->wait_until.tv_sec += client_wait_up_timeout;
        if (log_level > 1)
        {
            fprintf(stderr, "Waiting for PG %u/%u to become active for %d seconds\n", pg->lst->pool_id, pg->pg_num, client_wait_up_timeout);
        }
        set_list_retry_timeout(client_wait_up_timeout*1000, pg->wait_until);
        return;
    }
    assert(pg->state == LIST_PG_WAIT_ACTIVE);
    // Check if the timeout expired
    timespec tv;
    clock_gettime(CLOCK_REALTIME, &tv);
    if (tv.tv_sec > pg->wait_until.tv_sec ||
        tv.tv_sec == pg->wait_until.tv_sec && tv.tv_nsec >= pg->wait_until.tv_nsec)
    {
        fprintf(stderr, "Failed to wait for PG %u/%u to become active, skipping listing\n", pg->lst->pool_id, pg->pg_num);
        pg->errcode = -EPIPE;
        pg->list_osds.clear();
        pg->objects.clear();
        finish_list_pg(pg, false);
    }
}

void cluster_client_t::set_list_retry_timeout(int ms, timespec new_time)
{
    if (!list_retry_time.tv_sec || list_retry_time.tv_sec < new_time.tv_sec ||
        list_retry_time.tv_sec == new_time.tv_sec && list_retry_time.tv_nsec < new_time.tv_nsec)
    {
        list_retry_time = new_time;
        if (list_retry_timeout_id >= 0)
        {
            tfd->clear_timer(list_retry_timeout_id);
        }
        list_retry_timeout_id = tfd->set_timer(ms, false, [this](int timer_id)
        {
            list_retry_timeout_id = -1;
            list_retry_time = {};
            continue_lists();
        });
    }
}

int cluster_client_t::start_pg_listing(inode_list_pg_t *pg)
{
    auto & pool_cfg = st_cli.pool_config.at(pg->lst->pool_id);
    auto pg_it = pool_cfg.pg_config.find(pg->pg_num);
    assert(pg->lst->real_pg_count == pool_cfg.real_pg_count);
    if (pg_it == pool_cfg.pg_config.end() ||
        pg_it->second.pause ||
        !pg_it->second.cur_primary ||
        !(pg_it->second.cur_state & PG_ACTIVE))
    {
        // PG is (temporarily?) unavailable
        return LIST_PG_WAIT_ACTIVE;
    }
    pg->inactive_osds.clear();
    std::set<osd_num_t> all_peers;
    if (pg_it->second.cur_state != PG_ACTIVE && pg->lst->fallback)
    {
        // Not clean and OSDs don't support listing from primary
        for (osd_num_t pg_osd: pg_it->second.target_set)
            all_peers.insert(pg_osd);
        for (osd_num_t pg_osd: pg_it->second.all_peers)
            all_peers.insert(pg_osd);
        for (auto & hist_item: pg_it->second.target_history)
            for (auto pg_osd: hist_item)
                all_peers.insert(pg_osd);
        // Remove zero OSD number
        all_peers.erase(0);
        // Remove unconnectable peers except cur_primary
        for (auto peer_it = all_peers.begin(); peer_it != all_peers.end(); )
        {
            if (*peer_it != pg_it->second.cur_primary &&
                st_cli.peer_states[*peer_it].is_null())
            {
                pg->inactive_osds.push_back(*peer_it);
                all_peers.erase(peer_it++);
            }
            else
                peer_it++;
        }
    }
    else
    {
        // Clean
        all_peers.insert(pg_it->second.cur_primary);
    }
    // Check that we're connected to all PG OSDs
    bool conn = true;
    for (osd_num_t peer_osd: all_peers)
    {
        if (msgr.osd_peer_fds.find(peer_osd) == msgr.osd_peer_fds.end())
        {
            // Initiate connection
            if (st_cli.peer_states[peer_osd].is_null())
            {
                return LIST_PG_WAIT_ACTIVE;
            }
            msgr.connect_peer(peer_osd, st_cli.peer_states[peer_osd]);
            conn = false;
        }
    }
    if (!conn)
    {
        return LIST_PG_WAIT_CONNECT;
    }
    // Send all listings at once as the simplest way to guarantee that we connect
    // to the exact same OSDs that are listed in PG state
    pg->errcode = 0;
    pg->list_osds.clear();
    pg->has_unstable = false;
    pg->objects.clear();
    pg->cur_primary = pg_it->second.cur_primary;
    for (osd_num_t peer_osd: all_peers)
    {
        pg->list_osds.push_back((inode_list_osd_t){
            .pg = pg,
            .osd_num = peer_osd,
        });
    }
    for (auto & list_osd: pg->list_osds)
    {
        send_list(&list_osd);
    }
    return LIST_PG_SENT;
}

void cluster_client_t::send_list(inode_list_osd_t *cur_list)
{
    if (!cur_list->pg->inflight_ops)
        cur_list->pg->lst->inflight_pgs++;
    cur_list->pg->inflight_ops++;
    auto & pool_cfg = st_cli.pool_config[cur_list->pg->lst->pool_id];
    osd_op_t *op = new osd_op_t();
    op->op_type = OSD_OP_OUT;
    // Already checked that it exists above, but anyway
    op->peer_fd = msgr.osd_peer_fds.at(cur_list->osd_num);
    op->req = (osd_any_op_t){
        .sec_list = {
            .header = {
                .magic = SECONDARY_OSD_OP_MAGIC,
                .opcode = OSD_OP_SEC_LIST,
            },
            .list_pg = cur_list->pg->pg_num,
            .pg_count = (pg_num_t)pool_cfg.real_pg_count,
            .pg_stripe_size = pool_cfg.pg_stripe_size,
            .min_inode = cur_list->pg->lst->inode,
            .max_inode = cur_list->pg->lst->inode,
            .min_stripe = cur_list->pg->lst->min_offset,
            .max_stripe = cur_list->pg->lst->max_offset,
            .flags = (uint64_t)(cur_list->pg->lst->fallback ? 0 : OSD_LIST_PRIMARY),
        },
    };
    op->callback = [this, cur_list](osd_op_t *op)
    {
        if (op->reply.hdr.retval < 0)
        {
            fprintf(stderr, "Failed to get PG %u/%u object list from OSD %ju (retval=%jd), skipping\n",
                cur_list->pg->lst->pool_id, cur_list->pg->pg_num, cur_list->osd_num, op->reply.hdr.retval);
            if (!cur_list->pg->errcode ||
                cur_list->pg->errcode == -EPIPE ||
                op->reply.hdr.retval != -EPIPE)
            {
                cur_list->pg->errcode = op->reply.hdr.retval;
            }
        }
        else if ((op->req.sec_list.flags & OSD_LIST_PRIMARY) &&
            !(op->reply.sec_list.flags & OSD_LIST_PRIMARY))
        {
            // OSD is old and doesn't support listing from primary
            if (log_level > 0)
            {
                fprintf(
                    stderr, "[PG %u/%u] Primary OSD doesn't support consistent listings, falling back to listings from all peers\n",
                    cur_list->pg->lst->pool_id, cur_list->pg->pg_num
                );
            }
            cur_list->pg->lst->fallback = true;
            if (!cur_list->pg->errcode)
            {
                cur_list->pg->errcode = -EPIPE;
            }
        }
        else
        {
            if (op->reply.sec_list.stable_count < op->reply.hdr.retval)
            {
                // Unstable objects, if present, mean that someone still writes into the inode. Warn the user about it.
                cur_list->pg->has_unstable = true;
                fprintf(
                    stderr, "[PG %u/%u] Inode still has %ju unstable object versions out of total %ju - is it still open?\n",
                    cur_list->pg->lst->pool_id, cur_list->pg->pg_num, op->reply.hdr.retval - op->reply.sec_list.stable_count,
                    op->reply.hdr.retval
                );
            }
            if (log_level > 0)
            {
                fprintf(
                    stderr, "[PG %u/%u] Got inode object list from OSD %ju: %jd object versions\n",
                    cur_list->pg->lst->pool_id, cur_list->pg->pg_num, cur_list->osd_num, op->reply.hdr.retval
                );
            }
            for (uint64_t i = 0; i < op->reply.hdr.retval; i++)
            {
                object_id oid = ((obj_ver_id*)op->buf)[i].oid;
                oid.stripe = oid.stripe & ~STRIPE_MASK;
                cur_list->pg->objects.insert(oid);
            }
        }
        delete op;
        cur_list->pg->inflight_ops--;
        if (!cur_list->pg->inflight_ops)
            cur_list->pg->lst->inflight_pgs--;
        finish_list_pg(cur_list->pg, true);
        continue_listing(cur_list->pg->lst);
    };
    msgr.outbox_push(op);
}

void cluster_client_t::finish_list_pg(inode_list_pg_t *pg, bool retry_epipe)
{
    auto lst = pg->lst;
    if (pg->inflight_ops == 0)
    {
        if (pg->errcode == -EPIPE && retry_epipe)
        {
            // Retry listing after <client_retry_interval> ms on EPIPE
            pg->state = LIST_PG_WAIT_RETRY;
            clock_gettime(CLOCK_REALTIME, &pg->wait_until);
            pg->wait_until.tv_nsec += client_retry_interval*1000000;
            pg->wait_until.tv_sec += (pg->wait_until.tv_nsec / 1000000000);
            pg->wait_until.tv_nsec = (pg->wait_until.tv_nsec % 1000000000);
            set_list_retry_timeout(client_retry_interval, pg->wait_until);
            return;
        }
        lst->done_pgs++;
        pg->state = LIST_PG_DONE;
        lst->callback(pg->errcode, lst->pgs.size()-lst->done_pgs, pg->pg_num, std::move(pg->objects));
        pg->objects.clear();
        pg->inactive_osds.clear();
    }
}

void cluster_client_t::continue_lists()
{
    for (int i = lists.size()-1; i >= 0; i--)
    {
        continue_listing(lists[i]);
    }
}

bool cluster_client_t::check_finish_listing(inode_list_t *lst)
{
    if (lst->done_pgs >= lst->pgs.size())
    {
        for (auto pg: lst->pgs)
        {
            delete pg;
        }
        lst->pgs.clear();
        for (int i = 0; i < lists.size(); i++)
        {
            if (lists[i] == lst)
            {
                lists.erase(lists.begin()+i, lists.begin()+i+1);
                break;
            }
        }
        delete lst;
        return true;
    }
    return false;
}
