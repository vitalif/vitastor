// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

/**
 * Inode removal tool
 * May be included into a bigger "command-line management interface" in the future
 */

#include <vector>
#include <algorithm>

#include "epoll_manager.h"
#include "cluster_client.h"
#include "pg_states.h"

#define RM_LISTING 1
#define RM_REMOVING 2
#define RM_END 3

const char *exe_name = NULL;

struct rm_pg_t;

struct rm_pg_osd_t
{
    rm_pg_t *pg = NULL;
    osd_num_t osd_num;
    bool sent = false;
};

struct rm_pg_t
{
    pg_num_t pg_num;
    osd_num_t rm_osd_num;
    std::vector<rm_pg_osd_t> list_osds;
    int state = 0;
    int to_list;
    std::set<object_id> objects;
    std::set<object_id>::iterator obj_pos;
    uint64_t obj_count = 0, obj_done = 0, obj_prev_done = 0;
    int in_flight = 0;
};

class rm_inode_t
{
protected:
    uint64_t inode = 0;
    pool_id_t pool_id = 0;
    uint64_t iodepth = 0, parallel_osds = 0;

    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;
    ring_consumer_t consumer;

    std::vector<rm_pg_t*> lists;
    uint64_t total_count = 0, total_done = 0, total_prev_pct = 0;
    uint64_t pgs_to_list = 0;
    bool started = false;
    bool progress = true;
    bool list_first = false;
    int log_level = 0;

public:
    static json11::Json::object parse_args(int narg, const char *args[])
    {
        json11::Json::object cfg;
        cfg["progress"] = "1";
        for (int i = 1; i < narg; i++)
        {
            if (!strcmp(args[i], "-h") || !strcmp(args[i], "--help"))
            {
                help();
            }
            else if (args[i][0] == '-' && args[i][1] == '-')
            {
                const char *opt = args[i]+2;
                cfg[opt] = !strcmp(opt, "json") || !strcmp(opt, "wait-list") || i == narg-1 ? "1" : args[++i];
            }
        }
        return cfg;
    }

    static void help()
    {
        printf(
            "Vitastor inode removal tool\n"
            "(c) Vitaliy Filippov, 2020 (VNPL-1.1)\n\n"
            "USAGE:\n"
            "  %s --etcd_address <etcd_address> --pool <pool> --inode <inode> [--wait-list]\n",
            exe_name
        );
        exit(0);
    }

    void run(json11::Json cfg)
    {
        if (cfg["etcd_address"].string_value() == "")
        {
            fprintf(stderr, "etcd_address is missing\n");
            exit(1);
        }
        inode = cfg["inode"].uint64_value();
        pool_id = cfg["pool"].uint64_value();
        if (pool_id)
            inode = (inode & ((1l << (64-POOL_ID_BITS)) - 1)) | (((uint64_t)pool_id) << (64-POOL_ID_BITS));
        pool_id = INODE_POOL(inode);
        if (!pool_id)
        {
            fprintf(stderr, "pool is missing");
            exit(1);
        }
        iodepth = cfg["iodepth"].uint64_value();
        if (!iodepth)
            iodepth = 32;
        parallel_osds = cfg["parallel_osds"].uint64_value();
        if (!parallel_osds)
            parallel_osds = 4;
        log_level = cfg["log_level"].int64_value();
        progress = cfg["progress"].uint64_value() ? true : false;
        list_first = cfg["wait-list"].uint64_value() ? true : false;
        // Create client
        ringloop = new ring_loop_t(512);
        epmgr = new epoll_manager_t(ringloop);
        cli = new cluster_client_t(ringloop, epmgr->tfd, cfg);
        cli->on_ready([this]() { start_delete(); });
        // Initialize job
        consumer.loop = [this]()
        {
            if (started)
                continue_delete();
            ringloop->submit();
        };
        ringloop->register_consumer(&consumer);
        // Loop until it completes
        while (1)
        {
            ringloop->loop();
            ringloop->wait();
        }
    }

    void start_delete()
    {
        if (cli->st_cli.pool_config.find(pool_id) == cli->st_cli.pool_config.end())
        {
            fprintf(stderr, "Pool %u does not exist\n", pool_id);
            exit(1);
        }
        auto pool_cfg = cli->st_cli.pool_config[pool_id];
        for (auto & pg_item: pool_cfg.pg_config)
        {
            auto & pg = pg_item.second;
            if (pg.pause || !pg.cur_primary || !(pg.cur_state & PG_ACTIVE))
            {
                fprintf(stderr, "PG %u is inactive, skipping\n", pg_item.first);
                continue;
            }
            rm_pg_t *r = new rm_pg_t();
            r->pg_num = pg_item.first;
            r->rm_osd_num = pg.cur_primary;
            r->state = RM_LISTING;
            if (pg.cur_state != PG_ACTIVE)
            {
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
                    r->list_osds.push_back((rm_pg_osd_t){ .pg = r, .osd_num = peer_osd, .sent = false });
                }
            }
            else
            {
                r->list_osds.push_back((rm_pg_osd_t){ .pg = r, .osd_num = pg.cur_primary, .sent = false });
            }
            r->to_list = r->list_osds.size();
            lists.push_back(r);
        }
        std::sort(lists.begin(), lists.end(), [](rm_pg_t *a, rm_pg_t *b)
        {
            return a->rm_osd_num < b->rm_osd_num ? true : false;
        });
        pgs_to_list = lists.size();
        started = true;
        continue_delete();
    }

    void send_list(rm_pg_osd_t *cur_list)
    {
        if (cur_list->sent)
        {
            return;
        }
        if (cli->msgr.osd_peer_fds.find(cur_list->osd_num) ==
            cli->msgr.osd_peer_fds.end())
        {
            // Initiate connection
            cli->msgr.connect_peer(cur_list->osd_num, cli->st_cli.peer_states[cur_list->osd_num]);
            return;
        }
        osd_op_t *op = new osd_op_t();
        op->op_type = OSD_OP_OUT;
        op->peer_fd = cli->msgr.osd_peer_fds[cur_list->osd_num];
        op->req = (osd_any_op_t){
            .sec_list = {
                .header = {
                    .magic = SECONDARY_OSD_OP_MAGIC,
                    .id = cli->msgr.next_subop_id++,
                    .opcode = OSD_OP_SEC_LIST,
                },
                .list_pg = cur_list->pg->pg_num,
                .pg_count = (pg_num_t)cli->st_cli.pool_config[pool_id].real_pg_count,
                .pg_stripe_size = cli->st_cli.pool_config[pool_id].pg_stripe_size,
                .min_inode = inode,
                .max_inode = inode,
            },
        };
        op->callback = [this, cur_list](osd_op_t *op)
        {
            cur_list->pg->to_list--;
            if (op->reply.hdr.retval < 0)
            {
                fprintf(stderr, "Failed to get PG %u/%u object list from OSD %lu (retval=%ld), skipping\n",
                    pool_id, cur_list->pg->pg_num, cur_list->osd_num, op->reply.hdr.retval);
            }
            else
            {
                if (op->reply.sec_list.stable_count < op->reply.hdr.retval)
                {
                    // Unstable objects, if present, mean that someone still writes into the inode. Warn the user about it.
                    printf(
                        "[PG %u/%u] Inode still has %lu unstable object versions - is it still open? Not a good idea to delete it.\n",
                        pool_id, cur_list->pg->pg_num, op->reply.hdr.retval - op->reply.sec_list.stable_count
                    );
                }
                if (log_level > 0)
                {
                    printf(
                        "[PG %u/%u] Got inode object list from OSD %lu: %ld object versions\n",
                        pool_id, cur_list->pg->pg_num, cur_list->osd_num, op->reply.hdr.retval
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
            if (cur_list->pg->to_list <= 0)
            {
                cur_list->pg->obj_done = cur_list->pg->obj_prev_done = 0;
                cur_list->pg->obj_pos = cur_list->pg->objects.begin();
                cur_list->pg->obj_count = cur_list->pg->objects.size();
                total_count += cur_list->pg->obj_count;
                total_prev_pct = 0;
                cur_list->pg->state = RM_REMOVING;
                pgs_to_list--;
            }
            continue_delete();
        };
        cli->msgr.outbox_push(op);
        cur_list->sent = true;
    }

    void send_ops(rm_pg_t *cur_list)
    {
        if (cli->msgr.osd_peer_fds.find(cur_list->rm_osd_num) ==
            cli->msgr.osd_peer_fds.end())
        {
            // Initiate connection
            cli->msgr.connect_peer(cur_list->rm_osd_num, cli->st_cli.peer_states[cur_list->rm_osd_num]);
            return;
        }
        while (cur_list->in_flight < iodepth && cur_list->obj_pos != cur_list->objects.end())
        {
            osd_op_t *op = new osd_op_t();
            op->op_type = OSD_OP_OUT;
            op->peer_fd = cli->msgr.osd_peer_fds[cur_list->rm_osd_num];
            op->req = (osd_any_op_t){
                .rw = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = cli->msgr.next_subop_id++,
                        .opcode = OSD_OP_DELETE,
                    },
                    .inode = cur_list->obj_pos->inode,
                    .offset = (cur_list->obj_pos->stripe & ~STRIPE_MASK),
                    .len = 0,
                },
            };
            op->callback = [this, cur_list](osd_op_t *op)
            {
                cur_list->in_flight--;
                if (op->reply.hdr.retval < 0)
                {
                    fprintf(stderr, "Failed to remove object from PG %u (OSD %lu) (retval=%ld)\n",
                        cur_list->pg_num, cur_list->rm_osd_num, op->reply.hdr.retval);
                }
                delete op;
                cur_list->obj_done++;
                total_done++;
                continue_delete();
            };
            cli->msgr.outbox_push(op);
            cur_list->obj_pos++;
            cur_list->in_flight++;
        }
        if (!cur_list->in_flight && cur_list->obj_pos == cur_list->objects.end())
        {
            cur_list->obj_count = 0;
            cur_list->obj_done = cur_list->obj_prev_done = 0;
            cur_list->state = RM_END;
        }
    }

    void continue_delete()
    {
        int par_osd = 0;
        osd_num_t max_seen_osd = 0;
        bool no_del = false;
        if (list_first)
        {
            int i, n = 0;
            for (i = 0; i < lists.size(); i++)
            {
                if (lists[i]->state == RM_LISTING)
                {
                    n++;
                }
            }
            if (n > 0)
            {
                no_del = true;
            }
        }
        for (int i = 0; i < lists.size(); i++)
        {
            if (lists[i]->state == RM_END)
            {
                delete lists[i];
                lists.erase(lists.begin()+i, lists.begin()+i+1);
                i--;
            }
            else if (lists[i]->rm_osd_num > max_seen_osd)
            {
                if (lists[i]->state == RM_LISTING)
                {
                    for (int j = 0; j < lists[i]->list_osds.size(); j++)
                    {
                        send_list(&lists[i]->list_osds[j]);
                    }
                }
                else if (lists[i]->state == RM_REMOVING)
                {
                    if (no_del)
                    {
                        continue;
                    }
                    send_ops(lists[i]);
                }
                par_osd++;
                max_seen_osd = lists[i]->rm_osd_num;
                if (par_osd >= parallel_osds)
                {
                    break;
                }
            }
        }
        if (progress && total_count > 0 && total_done*1000/total_count != total_prev_pct)
        {
            printf("\rRemoved %lu/%lu objects, %lu more PGs to list...", total_done, total_count, pgs_to_list);
            total_prev_pct = total_done*1000/total_count;
        }
        if (!lists.size())
        {
            printf("Done, inode %lu in pool %u removed\n", (inode & ((1l << (64-POOL_ID_BITS)) - 1)), pool_id);
            exit(0);
        }
    }
};

int main(int narg, const char *args[])
{
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
    exe_name = args[0];
    rm_inode_t *p = new rm_inode_t();
    p->run(rm_inode_t::parse_args(narg, args));
    return 0;
}
