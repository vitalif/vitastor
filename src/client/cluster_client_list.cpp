// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <algorithm>
#include "pg_states.h"
#include "cluster_client.h"

struct inode_list_t;

struct inode_list_pg_t;

struct inode_list_osd_t
{
    inode_list_pg_t *pg = NULL;
    osd_num_t osd_num = 0;
    bool sent = false;
};

struct inode_list_pg_t
{
    inode_list_t *lst = NULL;
    int pos = 0;
    pg_num_t pg_num;
    osd_num_t cur_primary;
    bool has_unstable = false;
    int sent = 0;
    int done = 0;
    std::vector<inode_list_osd_t> list_osds;
    std::set<object_id> objects;
};

struct inode_list_t
{
    cluster_client_t *cli = NULL;
    pool_id_t pool_id = 0;
    inode_t inode = 0;
    int done_pgs = 0;
    int want = 0;
    int onstack = 0;
    std::vector<osd_num_t> inactive_osds;
    std::vector<pg_num_t> inactive_pgs;
    std::vector<inode_list_pg_t*> pgs;
    std::function<void(inode_list_t* lst, std::set<object_id>&& objects, pg_num_t pg_num, osd_num_t primary_osd, int status)> callback;
};

inode_list_t* cluster_client_t::list_inode_start(inode_t inode,
    std::function<void(inode_list_t* lst, std::set<object_id>&& objects, pg_num_t pg_num, osd_num_t primary_osd, int status)> callback)
{
    init_msgr();
    pool_id_t pool_id = INODE_POOL(inode);
    if (!pool_id || st_cli.pool_config.find(pool_id) == st_cli.pool_config.end())
    {
        if (log_level > 0)
        {
            fprintf(stderr, "Pool %u does not exist\n", pool_id);
        }
        return NULL;
    }
    inode_list_t *lst = new inode_list_t();
    lst->cli = this;
    lst->pool_id = pool_id;
    lst->inode = inode;
    lst->callback = callback;
    auto pool_cfg = st_cli.pool_config[pool_id];
    std::set<osd_num_t> inactive_osd_set;
    for (auto & pg_item: pool_cfg.pg_config)
    {
        auto & pg = pg_item.second;
        if (pg.pause || !pg.cur_primary || !(pg.cur_state & PG_ACTIVE))
        {
            lst->inactive_pgs.push_back(pg_item.first);
            if (log_level > 0)
            {
                fprintf(stderr, "PG %u is inactive, skipping\n", pg_item.first);
            }
            continue;
        }
        inode_list_pg_t *r = new inode_list_pg_t();
        r->lst = lst;
        r->pg_num = pg_item.first;
        r->cur_primary = pg.cur_primary;
        if (pg.cur_state != PG_ACTIVE)
        {
            // Not clean
            std::set<osd_num_t> all_peers;
            for (osd_num_t pg_osd: pg.target_set)
            {
                if (pg_osd != 0)
                {
                    all_peers.insert(pg_osd);
                }
            }
            for (osd_num_t pg_osd: pg.all_peers)
            {
                if (pg_osd != 0)
                {
                    all_peers.insert(pg_osd);
                }
            }
            for (auto & hist_item: pg.target_history)
            {
                for (auto pg_osd: hist_item)
                {
                    if (pg_osd != 0)
                    {
                        all_peers.insert(pg_osd);
                    }
                }
            }
            for (osd_num_t peer_osd: all_peers)
            {
                if (st_cli.peer_states.find(peer_osd) != st_cli.peer_states.end())
                {
                    r->list_osds.push_back((inode_list_osd_t){
                        .pg = r,
                        .osd_num = peer_osd,
                        .sent = false,
                    });
                }
                else
                {
                    inactive_osd_set.insert(peer_osd);
                }
            }
        }
        // FIXME: We should retry (or at least OPTIONALLY retry) connecting during delete, but only _for_some_time_
        // FIXME: Also we should handle the case when the OSD is disconnected during delete
        else if (st_cli.peer_states.find(pg.cur_primary) != st_cli.peer_states.end())
        {
            // Clean
            r->list_osds.push_back((inode_list_osd_t){
                .pg = r,
                .osd_num = pg.cur_primary,
                .sent = false,
            });
        }
        else
        {
            inactive_osd_set.insert(pg.cur_primary);
        }
        lst->pgs.push_back(r);
    }
    std::sort(lst->pgs.begin(), lst->pgs.end(), [](inode_list_pg_t *a, inode_list_pg_t *b)
    {
        return a->cur_primary < b->cur_primary ? true : false;
    });
    for (int i = 0; i < lst->pgs.size(); i++)
    {
        lst->pgs[i]->pos = i;
    }
    lst->inactive_osds.insert(lst->inactive_osds.end(), inactive_osd_set.begin(), inactive_osd_set.end());
    lists.push_back(lst);
    return lst;
}

int cluster_client_t::list_pg_count(inode_list_t *lst)
{
    return lst->pgs.size();
}

const std::vector<osd_num_t> & cluster_client_t::list_inode_get_inactive_osds(inode_list_t *lst)
{
    return lst->inactive_osds;
}

const std::vector<pg_num_t> & cluster_client_t::list_inode_get_inactive_pgs(inode_list_t *lst)
{
    return lst->inactive_pgs;
}

void cluster_client_t::list_inode_next(inode_list_t *lst, int next_pgs)
{
    if (next_pgs >= 0)
    {
        lst->want += next_pgs;
    }
    continue_listing(lst);
}

void cluster_client_t::continue_listing(inode_list_t *lst)
{
    if (lst->done_pgs >= lst->pgs.size())
    {
        return;
    }
    if (lst->want <= 0)
    {
        return;
    }
    if (lst->onstack > 0)
    {
        return;
    }
    lst->onstack++;
    for (int i = 0; i < lst->pgs.size(); i++)
    {
        if (!lst->pgs[i])
        {
        }
        else if (lst->pgs[i]->sent < lst->pgs[i]->list_osds.size())
        {
            for (int j = 0; j < lst->pgs[i]->list_osds.size(); j++)
            {
                send_list(&lst->pgs[i]->list_osds[j]);
                if (lst->want <= 0)
                {
                    lst->onstack--;
                    return;
                }
            }
        }
        else if (!lst->pgs[i]->list_osds.size())
        {
            finish_list_pg(lst->pgs[i]);
            if (check_finish_listing(lst))
            {
                // Do not change lst->onstack because it's already freed
                return;
            }
        }
    }
    lst->onstack--;
}

void cluster_client_t::send_list(inode_list_osd_t *cur_list)
{
    if (cur_list->sent)
    {
        return;
    }
    if (msgr.osd_peer_fds.find(cur_list->osd_num) == msgr.osd_peer_fds.end())
    {
        // Initiate connection
        msgr.connect_peer(cur_list->osd_num, st_cli.peer_states[cur_list->osd_num]);
        return;
    }
    auto & pool_cfg = st_cli.pool_config[cur_list->pg->lst->pool_id];
    osd_op_t *op = new osd_op_t();
    op->op_type = OSD_OP_OUT;
    // Already checked that it exists above, but anyway
    op->peer_fd = msgr.osd_peer_fds.at(cur_list->osd_num);
    op->req = (osd_any_op_t){
        .sec_list = {
            .header = {
                .magic = SECONDARY_OSD_OP_MAGIC,
                .id = next_op_id(),
                .opcode = OSD_OP_SEC_LIST,
            },
            .list_pg = cur_list->pg->pg_num,
            .pg_count = (pg_num_t)pool_cfg.real_pg_count,
            .pg_stripe_size = pool_cfg.pg_stripe_size,
            .min_inode = cur_list->pg->lst->inode,
            .max_inode = cur_list->pg->lst->inode,
        },
    };
    op->callback = [this, cur_list](osd_op_t *op)
    {
        if (op->reply.hdr.retval < 0)
        {
            fprintf(stderr, "Failed to get PG %u/%u object list from OSD %ju (retval=%jd), skipping\n",
                cur_list->pg->lst->pool_id, cur_list->pg->pg_num, cur_list->osd_num, op->reply.hdr.retval);
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
        auto lst = cur_list->pg->lst;
        cur_list->pg->done++;
        finish_list_pg(cur_list->pg);
        if (!check_finish_listing(lst))
        {
            continue_listing(lst);
        }
    };
    msgr.outbox_push(op);
    cur_list->sent = true;
    cur_list->pg->sent++;
    cur_list->pg->lst->want--;
}

void cluster_client_t::finish_list_pg(inode_list_pg_t *pg)
{
    auto lst = pg->lst;
    if (pg->done >= pg->list_osds.size())
    {
        int status = 0;
        lst->done_pgs++;
        if (lst->done_pgs >= lst->pgs.size())
        {
            status |= INODE_LIST_DONE;
        }
        if (pg->has_unstable)
        {
            status |= INODE_LIST_HAS_UNSTABLE;
        }
        lst->pgs[pg->pos] = NULL;
        lst->callback(lst, std::move(pg->objects), pg->pg_num, pg->cur_primary, status);
        delete pg;
    }
    else
    {
        lst->want++;
    }
}

bool cluster_client_t::check_finish_listing(inode_list_t *lst)
{
    if (lst->done_pgs >= lst->pgs.size())
    {
        // All done
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

void cluster_client_t::continue_lists()
{
    for (auto lst: lists)
    {
        continue_listing(lst);
    }
}
