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
    std::set<object_id> objects;
    std::set<object_id>::iterator obj_pos;
    uint64_t obj_count = 0, obj_done = 0;
    int state = 0;
    int in_flight = 0;
    bool synced = false;
};

struct rm_inode_t
{
    uint64_t inode = 0;
    pool_id_t pool_id = 0;
    uint64_t min_offset = 0;
    uint64_t max_offset = 0;
    bool down_ok = false;

    cli_tool_t *parent = NULL;
    inode_list_t *lister = NULL;
    std::vector<rm_pg_t*> lists;
    std::set<osd_num_t> inactive_osds;
    std::set<pg_num_t> inactive_pgs;
    uint64_t total_count = 0, total_done = 0, total_prev_pct = 0;
    bool lists_done = false;
    int pgs_to_list = 0;
    int state = 0;
    int error_count = 0;

    cli_result_t result;

    void start_delete()
    {
        auto pool_it = parent->cli->st_cli.pool_config.find(pool_id);
        if (pool_it == parent->cli->st_cli.pool_config.end())
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Pool does not exist" };
            state = 100;
            return;
        }
        pgs_to_list = pool_it->second.real_pg_count;
        parent->cli->list_inode(inode, min_offset, max_offset, parent->parallel_osds, [this](
            int errcode, int pgs_left, pg_num_t pg_num, std::set<object_id>&& objects, std::vector<osd_num_t> && inactive_osds)
        {
            if (errcode)
            {
                inactive_pgs.insert(pg_num);
            }
            else
            {
                for (auto osd_num: inactive_osds)
                {
                    this->inactive_osds.insert(osd_num);
                }
                rm_pg_t *rm = new rm_pg_t((rm_pg_t){
                    .pg_num = pg_num,
                    .objects = std::move(objects),
                    .obj_done = 0,
                    .synced = !objects.size() || parent->cli->get_immediate_commit(inode),
                });
                if (min_offset == 0 && max_offset == 0)
                {
                    total_count += objects.size();
                }
                else
                {
                    for (object_id oid: objects)
                    {
                        if (oid.stripe >= min_offset && (!max_offset || oid.stripe < max_offset))
                        {
                            total_count++;
                        }
                    }
                }
                rm->obj_pos = rm->objects.begin();
                lists.push_back(rm);
            }
            pgs_to_list = pgs_left;
            lists_done = !pgs_to_list;
            continue_delete();
        });
    }

    void send_ops(rm_pg_t *cur_list)
    {
        while (cur_list->in_flight < parent->iodepth && cur_list->obj_pos != cur_list->objects.end())
        {
            if (cur_list->obj_pos->stripe >= min_offset && (!max_offset || cur_list->obj_pos->stripe < max_offset))
            {
                cluster_op_t *op = new cluster_op_t;
                op->opcode = OSD_OP_DELETE;
                op->inode = cur_list->obj_pos->inode;
                op->offset = cur_list->obj_pos->stripe;
                op->len = 0;
                op->flags = OSD_OP_IGNORE_READONLY | OSD_OP_WAIT_UP_TIMEOUT;
                op->callback = [this, cur_list](cluster_op_t *op)
                {
                    cur_list->in_flight--;
                    if (op->retval < 0)
                    {
                        fprintf(stderr, "Failed to remove object %jx:%jx from PG %u (retval=%d)\n",
                            op->inode, op->offset, cur_list->pg_num, op->retval);
                        error_count++;
                    }
                    delete op;
                    cur_list->obj_done++;
                    total_done++;
                    continue_delete();
                };
                cur_list->in_flight++;
                cur_list->obj_pos++;
                parent->cli->execute(op);
            }
            else
            {
                cur_list->obj_pos++;
            }
        }
        if (cur_list->in_flight == 0 && cur_list->obj_pos == cur_list->objects.end() &&
            !cur_list->synced)
        {
            cluster_op_t *op = new cluster_op_t;
            op->opcode = OSD_OP_SYNC;
            op->callback = [this, cur_list](cluster_op_t *op)
            {
                cur_list->in_flight--;
                cur_list->synced = true;
                if (op->retval < 0)
                {
                    fprintf(stderr, "Failed to sync after deletion (retval=%d)\n", op->retval);
                    error_count++;
                }
                delete op;
                continue_delete();
            };
            cur_list->in_flight++;
            parent->cli->execute(op);
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
            if (!lists[i]->in_flight && lists[i]->obj_pos == lists[i]->objects.end() &&
                lists[i]->synced)
            {
                delete lists[i];
                lists.erase(lists.begin()+i, lists.begin()+i+1);
                i--;
            }
            else
            {
                send_ops(lists[i]);
            }
        }
        if (parent->progress && total_count > 0 && total_done*1000/total_count != total_prev_pct)
        {
            fprintf(stderr, parent->color
                ? "\rRemoved %ju/%ju objects, %u more PGs to list..."
                : "Removed %ju/%ju objects, %u more PGs to list...\n", total_done, total_count, pgs_to_list);
            total_prev_pct = total_done*1000/total_count;
        }
        if (lists_done && !lists.size())
        {
            if (parent->progress && total_count > 0)
            {
                fprintf(stderr, "\n");
            }
            // FIXME: for 100% correctness inactive_osds should be taken from OSD_OP_DELETE reply, not from the listing
            if (inactive_osds.size() && !parent->json_output)
            {
                fprintf(stderr, "Some data may remain after delete on OSDs which are currently down: ");
                int i = 0;
                for (auto osd_num: inactive_osds)
                {
                    fprintf(stderr, (i++) ? ", %ju" : "%ju", osd_num);
                }
                fprintf(stderr, "\n");
            }
            if (inactive_pgs.size() && !parent->json_output)
            {
                fprintf(stderr, "Failed to list some PGs, deletion is not complete: PG ");
                int i = 0;
                for (auto pg_num: inactive_pgs)
                {
                    fprintf(stderr, (i++) > 0 ? ", %u" : "%u", pg_num);
                }
                fprintf(stderr, "\n");
            }
            if (error_count > 0)
            {
                fprintf(stderr, "Failed to delete %u objects from active OSD(s).\n", error_count);
            }
            json11::Json::array inactive_pgs_json;
            for (auto pg_num: inactive_pgs)
            {
                inactive_pgs_json.push_back((uint64_t)pg_num);
            }
            json11::Json data = json11::Json::object {
                { "removed_objects", total_done },
                { "total_objects", total_count },
                { "error_count", error_count },
                { "inactive_osds", json11::Json::array(inactive_osds.begin(), inactive_osds.end()) },
                { "inactive_pgs", inactive_pgs_json },
            };
            state = 100;
            if (total_done < total_count || inactive_pgs.size() > 0 || error_count > 0 ||
                inactive_osds.size() > 0 && !down_ok)
            {
                // Error
                result = (cli_result_t){
                    .err = EIO,
                    .text = "Failed: some blocks were not removed",
                    .data = data,
                };
            }
            else
            {
                if (parent->progress && inactive_osds.size() > 0 && down_ok)
                {
                    fprintf(
                        stderr, "Warning: --down-ok is set and some OSDs are down.\n"
                        "Pool:%u,ID:%ju inode data may not have been fully removed.\n"
                        "Use `vitastor-cli rm-data --pool %u --inode %ju` if you encounter it in listings.\n",
                        pool_id, INODE_NO_POOL(inode), pool_id, INODE_NO_POOL(inode)
                    );
                }
                result = (cli_result_t){
                    .text = "Done, inode "+std::to_string(INODE_NO_POOL(inode))+" from pool "+
                        std::to_string(pool_id)+" removed",
                    .data = data,
                };
            }
        }
    }

    bool is_done()
    {
        return state == 100;
    }

    void loop()
    {
        if (state == 1)
            goto resume_1;
        if (state == 100)
            return;
        if (!pool_id)
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Pool is not specified" };
            state = 100;
            return;
        }
        start_delete();
        if (state == 100)
            return;
        state = 1;
    resume_1:
        continue_delete();
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_rm_data(json11::Json cfg)
{
    auto remover = new rm_inode_t();
    remover->parent = this;
    remover->inode = cfg["inode"].uint64_value();
    remover->pool_id = cfg["pool"].uint64_value();
    if (remover->pool_id)
    {
        remover->inode = (remover->inode & (((uint64_t)1 << (64-POOL_ID_BITS)) - 1)) | (((uint64_t)remover->pool_id) << (64-POOL_ID_BITS));
    }
    remover->down_ok = cfg["down_ok"].bool_value();
    remover->pool_id = INODE_POOL(remover->inode);
    remover->min_offset = cfg["min_offset"].uint64_value();
    remover->max_offset = cfg["max_offset"].uint64_value();
    return [remover](cli_result_t & result)
    {
        remover->loop();
        if (remover->is_done())
        {
            result = remover->result;
            delete remover;
            return true;
        }
        return false;
    };
}
