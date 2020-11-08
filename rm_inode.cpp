// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 (see README.md for details)

/**
 * Inode removal tool
 * May be included into a bigger "command-line management interface" in the future
 */

#include <algorithm>

#include "epoll_manager.h"
#include "cluster_client.h"
#include "pg_states.h"

#define RM_NO_LIST 1
#define RM_LIST_SENT 2
#define RM_REMOVING 3
#define RM_END 4

const char *exe_name = NULL;

struct rm_pg_osd_t
{
    pg_num_t pg_num;
    osd_num_t osd_num;
    int state = 0;
    obj_ver_id *obj_list = NULL;
    uint64_t obj_count = 0, obj_pos = 0, obj_done = 0, obj_prev_done = 0;
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

    std::vector<rm_pg_osd_t*> lists;
    uint64_t total_count = 0, total_done = 0, total_prev_pct = 0;
    uint64_t pgs_to_list = 0;
    bool started = false;
    bool progress = true;
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
                cfg[opt] = !strcmp(opt, "json") || i == narg-1 ? "1" : args[++i];
            }
        }
        return cfg;
    }

    static void help()
    {
        printf(
            "Vitastor inode removal tool\n"
            "(c) Vitaliy Filippov, 2020 (VNPL-1.0)\n\n"
            "USAGE:\n"
            "  %s --etcd_address <etcd_address> --pool <pool> --inode <inode>\n",
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
            if (pg.pause || !pg.cur_primary || pg.cur_state != PG_ACTIVE)
            {
                // FIXME Support deletion in non-clean active PGs by introducing a "primary-list" command
                fprintf(stderr, "PG %u is not active+clean, skipping\n", pg_item.first);
                continue;
            }
            rm_pg_osd_t *r = new rm_pg_osd_t();
            r->pg_num = pg_item.first;
            r->osd_num = pg.cur_primary;
            r->state = RM_NO_LIST;
            lists.push_back(r);
        }
        std::sort(lists.begin(), lists.end(), [](rm_pg_osd_t *a, rm_pg_osd_t *b)
        {
            return a->osd_num < b->osd_num ? true : false;
        });
        pgs_to_list = lists.size();
        started = true;
        continue_delete();
    }

    void send_list(rm_pg_osd_t *cur_list)
    {
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
                .list_pg = cur_list->pg_num,
                .pg_count = (pg_num_t)cli->st_cli.pool_config[pool_id].real_pg_count,
                .pg_stripe_size = cli->st_cli.pool_config[pool_id].pg_stripe_size,
                .min_inode = inode,
                .max_inode = inode,
            },
        };
        op->callback = [this, cur_list](osd_op_t *op)
        {
            pgs_to_list--;
            if (op->reply.hdr.retval < 0)
            {
                fprintf(stderr, "Failed to get object list from OSD %lu (retval=%ld), skipping the PG\n",
                    cur_list->osd_num, op->reply.hdr.retval);
                cli->msgr.stop_client(cur_list->osd_num);
                delete op;
                cur_list->state = RM_END;
                continue_delete();
                return;
            }
            if (log_level > 0)
            {
                printf(
                    "[PG %u/%u] Got inode object list from OSD %lu: %ld object versions\n",
                    pool_id, cur_list->pg_num, cur_list->osd_num, op->reply.hdr.retval
                );
            }
            cur_list->obj_list = (obj_ver_id*)op->buf;
            cur_list->obj_count = (uint64_t)op->reply.hdr.retval;
            cur_list->obj_done = cur_list->obj_prev_done = cur_list->obj_pos = 0;
            total_count += cur_list->obj_count;
            total_prev_pct = 0;
            // set op->buf to NULL so it doesn't get freed
            op->buf = NULL;
            delete op;
            cur_list->state = RM_REMOVING;
            continue_delete();
        };
        cur_list->state = RM_LIST_SENT;
        cli->msgr.outbox_push(op);
    }

    void send_ops(rm_pg_osd_t *cur_list)
    {
        if (cli->msgr.osd_peer_fds.find(cur_list->osd_num) ==
            cli->msgr.osd_peer_fds.end())
        {
            // Initiate connection
            cli->msgr.connect_peer(cur_list->osd_num, cli->st_cli.peer_states[cur_list->osd_num]);
            return;
        }
        while (cur_list->in_flight < iodepth && cur_list->obj_pos < cur_list->obj_count)
        {
            osd_op_t *op = new osd_op_t();
            op->op_type = OSD_OP_OUT;
            op->peer_fd = cli->msgr.osd_peer_fds[cur_list->osd_num];
            op->req = (osd_any_op_t){
                .rw = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = cli->msgr.next_subop_id++,
                        .opcode = OSD_OP_DELETE,
                    },
                    .inode = cur_list->obj_list[cur_list->obj_pos].oid.inode,
                    .offset = (cur_list->obj_list[cur_list->obj_pos].oid.stripe & ~STRIPE_MASK),
                    .len = 0,
                },
            };
            op->callback = [this, cur_list](osd_op_t *op)
            {
                cur_list->in_flight--;
                if (op->reply.hdr.retval < 0)
                {
                    fprintf(stderr, "Failed to remove object from PG %u (OSD %lu) (retval=%ld)\n",
                        cur_list->pg_num, cur_list->osd_num, op->reply.hdr.retval);
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
        if (!cur_list->in_flight && cur_list->obj_pos >= cur_list->obj_count)
        {
            free(cur_list->obj_list);
            cur_list->obj_list = NULL;
            cur_list->obj_count = 0;
            cur_list->obj_done = cur_list->obj_prev_done = cur_list->obj_pos = 0;
            cur_list->state = RM_END;
        }
    }

    void continue_delete()
    {
        int par_osd = 0;
        osd_num_t max_seen_osd = 0;
        for (int i = 0; i < lists.size(); i++)
        {
            if (lists[i]->state == RM_END)
            {
                delete lists[i];
                lists.erase(lists.begin()+i, lists.begin()+i+1);
                i--;
            }
            else if (lists[i]->osd_num > max_seen_osd)
            {
                if (lists[i]->state == RM_NO_LIST)
                {
                    send_list(lists[i]);
                }
                else if (lists[i]->state == RM_REMOVING)
                {
                    send_ops(lists[i]);
                }
                par_osd++;
                max_seen_osd = lists[i]->osd_num;
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
