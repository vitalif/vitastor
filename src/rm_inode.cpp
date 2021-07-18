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

struct rm_pg_t
{
    pg_num_t pg_num;
    osd_num_t rm_osd_num;
    std::set<object_id> objects;
    std::set<object_id>::iterator obj_pos;
    uint64_t obj_count = 0, obj_done = 0, obj_prev_done = 0;
    int state = 0;
    int in_flight = 0;
};

class rm_inode_t
{
protected:
    uint64_t inode = 0;
    pool_id_t pool_id = 0;
    uint64_t iodepth = 0, parallel_osds = 0;
    inode_list_t *lister = NULL;

    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;
    ring_consumer_t consumer;

    std::vector<rm_pg_t*> lists;
    uint64_t total_count = 0, total_done = 0, total_prev_pct = 0;
    uint64_t pgs_to_list = 0;
    bool started = false;
    bool progress = true;
    bool list_first = false, lists_done = false;
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
            "  %s [--etcd_address <etcd_address>] --pool <pool> --inode <inode> [--wait-list]\n",
            exe_name
        );
        exit(0);
    }

    void run(json11::Json cfg)
    {
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
        lister = cli->list_inode_start(inode, [this](std::set<object_id>&& objects, pg_num_t pg_num, osd_num_t primary_osd, int status)
        {
            rm_pg_t *rm = new rm_pg_t({
                .pg_num = pg_num,
                .rm_osd_num = primary_osd,
                .objects = objects,
                .obj_count = objects.size(),
                .obj_done = 0,
                .obj_prev_done = 0,
            });
            rm->obj_pos = rm->objects.begin();
            lists.push_back(rm);
            if (list_first)
            {
                cli->list_inode_next(lister, 1);
            }
            if (status & INODE_LIST_DONE)
            {
                lists_done = true;
            }
            pgs_to_list--;
            continue_delete();
        });
        if (!lister)
        {
            fprintf(stderr, "Failed to list inode %lx objects\n", inode);
            exit(1);
        }
        pgs_to_list = cli->list_pg_count(lister);
        cli->list_inode_next(lister, parallel_osds);
        started = true;
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
                        .id = cli->next_op_id(),
                        .opcode = OSD_OP_DELETE,
                    },
                    .inode = cur_list->obj_pos->inode,
                    .offset = cur_list->obj_pos->stripe,
                    .len = 0,
                },
            };
            op->callback = [this, cur_list](osd_op_t *op)
            {
                cur_list->in_flight--;
                if (op->reply.hdr.retval < 0)
                {
                    fprintf(stderr, "Failed to remove object %lx:%lx from PG %u (OSD %lu) (retval=%ld)\n",
                        op->req.rw.inode, op->req.rw.offset,
                        cur_list->pg_num, cur_list->rm_osd_num, op->reply.hdr.retval);
                }
                delete op;
                cur_list->obj_done++;
                total_done++;
                continue_delete();
            };
            cur_list->obj_pos++;
            cur_list->in_flight++;
            cli->msgr.outbox_push(op);
        }
    }

    void continue_delete()
    {
        if (list_first && !lists_done)
        {
            return;
        }
        for (int i = 0; i < lists.size(); i++)
        {
            if (!lists[i]->in_flight && lists[i]->obj_pos == lists[i]->objects.end())
            {
                delete lists[i];
                lists.erase(lists.begin()+i, lists.begin()+i+1);
                i--;
                if (!lists_done)
                {
                    cli->list_inode_next(lister, 1);
                }
            }
            else
            {
                send_ops(lists[i]);
            }
        }
        if (progress && total_count > 0 && total_done*1000/total_count != total_prev_pct)
        {
            printf("\rRemoved %lu/%lu objects, %lu more PGs to list...", total_done, total_count, pgs_to_list);
            total_prev_pct = total_done*1000/total_count;
        }
        if (lists_done && !lists.size())
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
