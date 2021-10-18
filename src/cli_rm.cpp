// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"

#define RM_LISTING 1
#define RM_REMOVING 2
#define RM_END 3

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

struct rm_inode_t
{
    uint64_t inode = 0;
    pool_id_t pool_id = 0;

    cli_tool_t *parent = NULL;
    inode_list_t *lister = NULL;
    std::vector<rm_pg_t*> lists;
    uint64_t total_count = 0, total_done = 0, total_prev_pct = 0;
    uint64_t pgs_to_list = 0;
    bool lists_done = false;
    int state = 0;

    void start_delete()
    {
        lister = parent->cli->list_inode_start(inode, [this](inode_list_t *lst,
            std::set<object_id>&& objects, pg_num_t pg_num, osd_num_t primary_osd, int status)
        {
            rm_pg_t *rm = new rm_pg_t((rm_pg_t){
                .pg_num = pg_num,
                .rm_osd_num = primary_osd,
                .objects = objects,
                .obj_count = objects.size(),
                .obj_done = 0,
                .obj_prev_done = 0,
            });
            rm->obj_pos = rm->objects.begin();
            lists.push_back(rm);
            if (parent->list_first)
            {
                parent->cli->list_inode_next(lister, 1);
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
            fprintf(stderr, "Failed to list inode %lu from pool %u objects\n", INODE_NO_POOL(inode), INODE_POOL(inode));
            exit(1);
        }
        pgs_to_list = parent->cli->list_pg_count(lister);
        parent->cli->list_inode_next(lister, parent->parallel_osds);
    }

    void send_ops(rm_pg_t *cur_list)
    {
        if (parent->cli->msgr.osd_peer_fds.find(cur_list->rm_osd_num) ==
            parent->cli->msgr.osd_peer_fds.end())
        {
            // Initiate connection
            parent->cli->msgr.connect_peer(cur_list->rm_osd_num, parent->cli->st_cli.peer_states[cur_list->rm_osd_num]);
            return;
        }
        while (cur_list->in_flight < parent->iodepth && cur_list->obj_pos != cur_list->objects.end())
        {
            osd_op_t *op = new osd_op_t();
            op->op_type = OSD_OP_OUT;
            op->peer_fd = parent->cli->msgr.osd_peer_fds[cur_list->rm_osd_num];
            op->req = (osd_any_op_t){
                .rw = {
                    .header = {
                        .magic = SECONDARY_OSD_OP_MAGIC,
                        .id = parent->cli->next_op_id(),
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
            parent->cli->msgr.outbox_push(op);
        }
    }

    void continue_delete()
    {
        if (parent->list_first && !lists_done)
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
                    parent->cli->list_inode_next(lister, 1);
                }
            }
            else
            {
                send_ops(lists[i]);
            }
        }
        if (parent->progress && total_count > 0 && total_done*1000/total_count != total_prev_pct)
        {
            printf("\rRemoved %lu/%lu objects, %lu more PGs to list...", total_done, total_count, pgs_to_list);
            total_prev_pct = total_done*1000/total_count;
        }
        if (lists_done && !lists.size())
        {
            printf("Done, inode %lu in pool %u data removed\n", INODE_NO_POOL(inode), pool_id);
            state = 2;
        }
    }

    bool loop()
    {
        if (state == 0)
        {
            start_delete();
            state = 1;
        }
        else if (state == 1)
        {
            continue_delete();
        }
        else if (state == 2)
        {
            return true;
        }
        return false;
    }
};

std::function<bool(void)> cli_tool_t::start_rm(json11::Json cfg)
{
    auto remover = new rm_inode_t();
    remover->parent = this;
    remover->inode = cfg["inode"].uint64_value();
    remover->pool_id = cfg["pool"].uint64_value();
    if (remover->pool_id)
    {
        remover->inode = (remover->inode & ((1l << (64-POOL_ID_BITS)) - 1)) | (((uint64_t)remover->pool_id) << (64-POOL_ID_BITS));
    }
    remover->pool_id = INODE_POOL(remover->inode);
    if (!remover->pool_id)
    {
        fprintf(stderr, "pool is missing\n");
        exit(1);
    }
    return [remover]()
    {
        if (remover->loop())
        {
            delete remover;
            return true;
        }
        return false;
    };
}
