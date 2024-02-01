// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "cli.h"
#include "cluster_client.h"
#include "cpp-btree/safe_btree_set.h"

struct snap_rw_op_t
{
    uint64_t offset = 0;
    void *buf = NULL;
    cluster_op_t op;
    int todo = 0;
    uint32_t start = 0, end = 0;
    int error_code = 0;
    uint64_t error_offset = 0;
    bool error_read = false;
};

// Layer merge is the base for multiple operations:
// 1) Delete snapshot "up" = merge child layer into the parent layer, remove the child
//    and rename the parent to the child
// 2) Delete snapshot "down" = merge parent layer into the child layer and remove the parent
// 3) Flatten image = merge parent layers into the child layer and break the connection
struct snap_merger_t
{
    cli_tool_t *parent;

    // -- CONFIGURATION --
    // merge from..to into target (target may be one of from..to)
    std::string from_name, to_name, target_name;
    // inode=>rank (bigger rank means child layers)
    std::map<inode_t,int> sources;
    // delete merged source inode data during merge
    bool delete_source = false;
    // use CAS writes (0 = never, 1 = auto, 2 = always)
    int use_cas = 1;
    // don't necessarily delete source data, but perform checks as if we were to do it
    bool check_delete_source = false;
    // interval between fsyncs
    int fsync_interval = 128;

    // -- STATE --
    inode_t target, to_num;
    int target_rank;
    bool inside_continue = false;
    int state = 0;
    int lists_todo = 0;
    uint64_t target_block_size = 0;
    uint32_t target_bitmap_granularity = 0;
    btree::safe_btree_set<uint64_t> merge_offsets;
    btree::safe_btree_set<uint64_t>::iterator oit;
    std::map<inode_t, std::vector<uint64_t>> layer_lists;
    std::map<inode_t, uint64_t> layer_block_size;
    std::map<inode_t, uint64_t> layer_list_pos;
    std::vector<snap_rw_op_t*> continue_rwo, continue_rwo2;
    int in_flight = 0;
    uint64_t last_fsync_offset = 0;
    uint64_t last_written_offset = 0;
    int deleted_unsynced = 0;
    uint64_t processed = 0, to_process = 0;
    std::string rwo_error;

    cli_result_t result;

    void start_merge()
    {
        if (from_name == "" || to_name == "")
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Beginning or end of the merge sequence is missing" };
            state = 100;
            return;
        }
        check_delete_source = delete_source || check_delete_source;
        inode_config_t *from_cfg = parent->get_inode_cfg(from_name);
        if (!from_cfg)
        {
            result = (cli_result_t){ .err = ENOENT, .text = "Layer "+from_name+" not found" };
            state = 100;
            return;
        }
        inode_config_t *to_cfg = parent->get_inode_cfg(to_name);
        if (!to_cfg)
        {
            result = (cli_result_t){ .err = ENOENT, .text = "Layer "+to_name+" not found" };
            state = 100;
            return;
        }
        inode_config_t *target_cfg = target_name == "" ? from_cfg : parent->get_inode_cfg(target_name);
        if (!target_cfg)
        {
            result = (cli_result_t){ .err = ENOENT, .text = "Layer "+target_name+" not found" };
            state = 100;
            return;
        }
        if (to_cfg->num == from_cfg->num)
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Only one layer specified, nothing to merge" };
            state = 100;
            return;
        }
        to_num = to_cfg->num;
        // Check that to_cfg is actually a child of from_cfg and target_cfg is somewhere between them
        std::vector<inode_t> chain_list;
        inode_config_t *cur = to_cfg;
        chain_list.push_back(cur->num);
        layer_block_size[cur->num] = get_block_size(cur->num, NULL);
        while (cur->parent_id != from_cfg->num &&
            cur->parent_id != to_cfg->num &&
            cur->parent_id != 0)
        {
            auto it = parent->cli->st_cli.inode_config.find(cur->parent_id);
            if (it == parent->cli->st_cli.inode_config.end())
            {
                result = (cli_result_t){
                    .err = ENOENT,
                    .text = "Parent inode of layer "+cur->name+" (id "+std::to_string(cur->parent_id)+") does not exist",
                    .data = json11::Json::object {
                        { "error", "parent-not-found" },
                        { "inode_id", cur->num },
                        { "inode_name", cur->name },
                        { "parent_id", cur->parent_id },
                    },
                };
                state = 100;
                return;
            }
            cur = &it->second;
            chain_list.push_back(cur->num);
            layer_block_size[cur->num] = get_block_size(cur->num, NULL);
        }
        if (cur->parent_id != from_cfg->num)
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Layer "+to_name+" is not a child of "+from_name };
            state = 100;
            return;
        }
        chain_list.push_back(from_cfg->num);
        layer_block_size[from_cfg->num] = get_block_size(from_cfg->num, NULL);
        int i = chain_list.size()-1;
        for (inode_t item: chain_list)
        {
            sources[item] = i--;
        }
        if (sources.find(target_cfg->num) == sources.end())
        {
            result = (cli_result_t){ .err = EINVAL, .text = "Layer "+target_name+" is not between "+to_name+" and "+from_name };
            state = 100;
            return;
        }
        target = target_cfg->num;
        target_rank = sources.at(target);
        int to_rank = sources.at(to_cfg->num);
        bool to_has_children = false;
        // Check that there are no other inodes dependent on altered layers
        //
        // 1) everything between <target> and <to> except <to> is not allowed
        //    to have children other than <to> if <to> is a child of <target>:
        //
        //    <target> - <layer 3> - <to>
        //            \- <layer 4> <--------X--------- NOT ALLOWED
        //
        // 2) everything between <from> and <target>, except <target>, is not allowed
        //    to have children other than <target> if sources are to be deleted after merging:
        //
        //    <from> - <layer 1> - <target> - <to>
        //          \- <layer 2> <---------X-------- NOT ALLOWED
        for (auto & ic: parent->cli->st_cli.inode_config)
        {
            auto it = sources.find(ic.second.num);
            if (it == sources.end() && ic.second.parent_id != 0)
            {
                it = sources.find(ic.second.parent_id);
                if (it != sources.end())
                {
                    int parent_rank = it->second;
                    if (parent_rank < to_rank && (parent_rank >= target_rank || check_delete_source))
                    {
                        result = (cli_result_t){
                            .err = EINVAL,
                            .text = "Layers at or above "+(check_delete_source ? from_name : target_name)+
                                ", but below "+to_name+" are not allowed to have other children, but "+
                                ic.second.name+" is a child of "+
                                parent->cli->st_cli.inode_config.at(ic.second.parent_id).name,
                        };
                        state = 100;
                        return;
                    }
                    if (parent_rank >= to_rank)
                    {
                        to_has_children = true;
                    }
                }
            }
        }
        if ((target_rank < to_rank || to_has_children) && use_cas == 1)
        {
            // <to> has children itself, no need for CAS
            use_cas = 0;
        }
        sources.erase(target);
        if (parent->progress)
        {
            printf(
                "Merging %ld layer(s) into target %s%s (inode %lu in pool %u)\n",
                sources.size(), target_cfg->name.c_str(),
                use_cas ? " online (with CAS)" : "", INODE_NO_POOL(target), INODE_POOL(target)
            );
        }
        target_block_size = get_block_size(target, &target_bitmap_granularity);
    }

    uint64_t get_block_size(inode_t inode, uint32_t *bitmap_granularity)
    {
        auto & pool_cfg = parent->cli->st_cli.pool_config.at(INODE_POOL(inode));
        uint64_t pg_data_size = (pool_cfg.scheme == POOL_SCHEME_REPLICATED ? 1 : pool_cfg.pg_size-pool_cfg.parity_chunks);
        if (bitmap_granularity)
            *bitmap_granularity = pool_cfg.bitmap_granularity;
        return pool_cfg.data_block_size * pg_data_size;
    }

    void continue_merge_reent()
    {
        if (!inside_continue)
        {
            inside_continue = true;
            continue_merge();
            inside_continue = false;
        }
    }

    bool is_done()
    {
        return state == 100;
    }

    void continue_merge()
    {
        if (state == 1)
            goto resume_1;
        else if (state == 2)
            goto resume_2;
        else if (state == 3)
            goto resume_3;
        else if (state == 4)
            goto resume_4;
        else if (state == 5)
            goto resume_5;
        else if (state == 100)
            goto resume_100;
        // Get parents and so on
        start_merge();
        // First list lower layers
        list_layers(true);
        state = 1;
    resume_1:
        while (lists_todo > 0)
        {
            // Wait for lists
            return;
        }
        if (merge_offsets.size() > 0)
        {
            state = 2;
            oit = merge_offsets.begin();
            processed = 0;
            to_process = merge_offsets.size();
    resume_2:
            // Then remove blocks already filled in target by issuing zero-length reads and checking bitmaps
            while (in_flight < parent->iodepth*parent->parallel_osds && oit != merge_offsets.end())
            {
                in_flight++;
                check_if_full(*oit);
                oit++;
                processed++;
                if (parent->progress && !(processed % 128))
                {
                    printf("\rFiltering target blocks: %lu/%lu", processed, to_process);
                }
            }
            if (in_flight > 0 || oit != merge_offsets.end())
            {
                // Wait until reads finish
                return;
            }
            if (parent->progress)
            {
                printf("\r%lu full blocks of target filtered out\n", to_process-merge_offsets.size());
            }
        }
        state = 3;
    resume_3:
        // Then list upper layers
        list_layers(false);
        state = 4;
    resume_4:
        while (lists_todo > 0)
        {
            // Wait for lists
            return;
        }
        state = 5;
        processed = 0;
        to_process = merge_offsets.size();
        oit = merge_offsets.begin();
    resume_5:
        // Now read, overwrite and optionally delete offsets one by one
        continue_rwo2.swap(continue_rwo);
        for (auto rwo: continue_rwo2)
        {
            next_write(rwo);
        }
        continue_rwo2.clear();
        while (in_flight < parent->iodepth*parent->parallel_osds &&
            oit != merge_offsets.end() && !rwo_error.size())
        {
            in_flight++;
            read_and_write(*oit);
            oit++;
            processed++;
            if (parent->progress && !(processed % 128))
            {
                printf("\rOverwriting blocks: %lu/%lu", processed, to_process);
            }
        }
        if (in_flight == 0 && rwo_error.size())
        {
            result = (cli_result_t){
                .err = EIO,
                .text = rwo_error,
            };
            state = 100;
            return;
        }
        if (in_flight > 0 || oit != merge_offsets.end())
        {
            // Wait until overwrites finish
            return;
        }
        if (parent->progress)
        {
            printf("\rOverwriting blocks: %lu/%lu\n", to_process, to_process);
        }
        // Done
        result = (cli_result_t){ .text = "Done, layers from "+from_name+" to "+to_name+" merged into "+target_name, .data = json11::Json::object {
            { "from", from_name },
            { "to", to_name },
            { "into", target_name },
        }};
        state = 100;
    resume_100:
        return;
    }

    void list_layers(bool lower)
    {
        for (auto & sp: sources)
        {
            inode_t src = sp.first;
            if (lower ? (sp.second < target_rank) : (sp.second > target_rank))
            {
                lists_todo++;
                inode_list_t* lst = parent->cli->list_inode_start(src, [this, src](
                    inode_list_t *lst, std::set<object_id>&& objects, pg_num_t pg_num, osd_num_t primary_osd, int status)
                {
                    uint64_t layer_block = layer_block_size.at(src);
                    for (object_id obj: objects)
                    {
                        merge_offsets.insert(obj.stripe - obj.stripe % target_block_size);
                        for (int i = target_block_size; i < layer_block; i += target_block_size)
                        {
                            merge_offsets.insert(obj.stripe - obj.stripe % target_block_size + i);
                        }
                    }
                    if (delete_source)
                    {
                        // Also store individual lists
                        auto & layer_list = layer_lists[src];
                        int pos = layer_list.size();
                        layer_list.resize(pos + objects.size());
                        for (object_id obj: objects)
                        {
                            layer_list[pos++] = obj.stripe;
                        }
                    }
                    if (status & INODE_LIST_DONE)
                    {
                        auto & name = parent->cli->st_cli.inode_config.at(src).name;
                        if (parent->progress)
                        {
                            printf("Got listing of layer %s (inode %lu in pool %u)\n", name.c_str(), INODE_NO_POOL(src), INODE_POOL(src));
                        }
                        if (delete_source)
                        {
                            // Sort the inode listing
                            std::sort(layer_lists[src].begin(), layer_lists[src].end());
                        }
                        lists_todo--;
                        continue_merge_reent();
                    }
                    else
                    {
                        parent->cli->list_inode_next(lst, 1);
                    }
                });
                parent->cli->list_inode_next(lst, parent->parallel_osds);
            }
        }
    }

    // Check if <offset> is fully written in <target> and remove it from merge_offsets if so
    void check_if_full(uint64_t offset)
    {
        cluster_op_t *op = new cluster_op_t;
        op->opcode = OSD_OP_READ_BITMAP;
        op->inode = target;
        op->offset = offset;
        op->len = target_block_size;
        op->callback = [this](cluster_op_t *op)
        {
            if (op->retval < 0)
            {
                fprintf(stderr, "error reading target bitmap at offset %lx: %s\n", op->offset, strerror(-op->retval));
            }
            else
            {
                uint64_t bitmap_bytes = target_block_size/target_bitmap_granularity/8;
                int i;
                for (i = 0; i < bitmap_bytes; i++)
                {
                    if (((uint8_t*)op->bitmap_buf)[i] != 0xff)
                    {
                        break;
                    }
                }
                if (i == bitmap_bytes)
                {
                    // full
                    merge_offsets.erase(op->offset);
                }
            }
            delete op;
            in_flight--;
            continue_merge_reent();
        };
        parent->cli->execute(op);
    }

    // Read <offset> from <to>, write it to <target> and optionally delete it
    // from all layers except <target> after fsync'ing
    void read_and_write(uint64_t offset)
    {
        snap_rw_op_t *rwo = new snap_rw_op_t;
        // Initialize counter to 1 to later allow write_subop() to return immediately
        // (even though it shouldn't really do that)
        rwo->todo = 1;
        rwo->buf = malloc(target_block_size);
        rwo->offset = offset;
        rwo_read(rwo);
    }

    void rwo_read(snap_rw_op_t *rwo)
    {
        cluster_op_t *op = &rwo->op;
        op->opcode = OSD_OP_READ;
        op->inode = to_num;
        op->offset = rwo->offset;
        op->len = target_block_size;
        op->iov.push_back(rwo->buf, target_block_size);
        op->callback = [this, rwo](cluster_op_t *op)
        {
            if (op->retval != op->len)
            {
                rwo->error_code = -op->retval;
                rwo->error_offset = op->offset;
                rwo->error_read = true;
            }
            continue_rwo.push_back(rwo);
            parent->ringloop->wakeup();
        };
        parent->cli->execute(op);
    }

    void next_write(snap_rw_op_t *rwo)
    {
        // Write each non-empty range using an individual operation
        // FIXME: Allow to use single write with "holes" (OSDs don't allow it yet)
        uint32_t gran = target_bitmap_granularity;
        uint64_t bitmap_size = target_block_size / gran;
        while (rwo->end < bitmap_size && !rwo->error_code)
        {
            auto bit = ((*((uint8_t*)rwo->op.bitmap_buf + (rwo->end >> 3))) & (1 << (rwo->end & 0x7)));
            if (!bit)
            {
                if (rwo->end > rwo->start)
                {
                    // write start->end
                    rwo->todo++;
                    write_subop(rwo, rwo->start*gran, rwo->end*gran, use_cas && to_num == target ? 1+rwo->op.version : 0);
                    rwo->start = rwo->end;
                    if (use_cas)
                    {
                        // Submit one by one if using CAS writes
                        return;
                    }
                }
                rwo->start = rwo->end = rwo->end+1;
            }
            else
            {
                rwo->end++;
            }
        }
        if (rwo->end > rwo->start && !rwo->error_code)
        {
            // write start->end
            rwo->todo++;
            write_subop(rwo, rwo->start*gran, rwo->end*gran, use_cas && to_num == target ? 1+rwo->op.version : 0);
            rwo->start = rwo->end;
            if (use_cas)
            {
                return;
            }
        }
        rwo->todo--;
        // Just in case, if everything is done
        autofree_op(rwo);
    }

    void write_subop(snap_rw_op_t *rwo, uint32_t start, uint32_t end, uint64_t version)
    {
        cluster_op_t *subop = new cluster_op_t;
        subop->opcode = OSD_OP_WRITE;
        subop->inode = target;
        subop->offset = rwo->offset+start;
        subop->len = end-start;
        subop->version = version;
        subop->flags = OSD_OP_IGNORE_READONLY;
        subop->iov.push_back((uint8_t*)rwo->buf+start, end-start);
        subop->callback = [this, rwo](cluster_op_t *subop)
        {
            rwo->todo--;
            if (subop->retval != subop->len)
            {
                if (use_cas && subop->retval == -EINTR)
                {
                    // CAS failure - reread and repeat optimistically
                    rwo->start = rwo->end = 0;
                    rwo_read(rwo);
                    delete subop;
                    return;
                }
                rwo->error_code = -subop->retval;
                rwo->error_offset = subop->offset;
                rwo->error_read = false;
            }
            // Increment CAS version
            rwo->op.version = subop->version;
            delete subop;
            continue_rwo.push_back(rwo);
            parent->ringloop->wakeup();
        };
        parent->cli->execute(subop);
    }

    void delete_offset(inode_t inode_num, uint64_t offset)
    {
        cluster_op_t *subop = new cluster_op_t;
        subop->opcode = OSD_OP_DELETE;
        subop->inode = inode_num;
        subop->offset = offset;
        subop->len = 0;
        subop->flags = OSD_OP_IGNORE_READONLY;
        subop->callback = [](cluster_op_t *subop)
        {
            if (subop->retval != 0)
            {
                fprintf(stderr, "error deleting from layer 0x%lx at offset %lx: %s", subop->inode, subop->offset, strerror(-subop->retval));
            }
            delete subop;
        };
        parent->cli->execute(subop);
    }

    void autofree_op(snap_rw_op_t *rwo)
    {
        if (!rwo->todo)
        {
            if (!rwo->error_code &&
                last_written_offset < rwo->op.offset+target_block_size)
            {
                last_written_offset = rwo->op.offset+target_block_size;
            }
            if (!rwo->error_code && delete_source)
            {
                deleted_unsynced++;
                if (deleted_unsynced >= fsync_interval)
                {
                    uint64_t to = last_written_offset;
                    cluster_op_t *subop = new cluster_op_t;
                    subop->opcode = OSD_OP_SYNC;
                    subop->callback = [this, to](cluster_op_t *subop)
                    {
                        delete subop;
                        // We can now delete source data between <from> and <to>
                        // But to do this we have to keep all object lists in memory :-(
                        for (auto & lp: layer_list_pos)
                        {
                            auto & layer_list = layer_lists.at(lp.first);
                            uint64_t layer_block = layer_block_size.at(lp.first);
                            int cur_pos = lp.second;
                            while (cur_pos < layer_list.size() && layer_list[cur_pos]+layer_block < to)
                            {
                                delete_offset(lp.first, layer_list[cur_pos]);
                                cur_pos++;
                            }
                            lp.second = cur_pos;
                        }
                    };
                    parent->cli->execute(subop);
                }
            }
            free(rwo->buf);
            if (rwo->error_code)
            {
                char buf[1024];
                snprintf(buf, 1024, "Error %s target at offset %lx: %s",
                    rwo->error_read ? "reading" : "writing", rwo->error_offset, strerror(rwo->error_code));
                rwo_error = std::string(buf);
            }
            delete rwo;
            in_flight--;
            continue_merge_reent();
        }
    }
};

std::function<bool(cli_result_t &)> cli_tool_t::start_merge(json11::Json cfg)
{
    auto merger = new snap_merger_t();
    merger->parent = this;
    merger->from_name = cfg["from"].string_value();
    merger->to_name = cfg["to"].string_value();
    merger->target_name = cfg["target"].string_value();
    merger->delete_source = cfg["delete_source"].string_value() != "";
    merger->fsync_interval = cfg["fsync_interval"].uint64_value();
    if (!merger->fsync_interval)
        merger->fsync_interval = 128;
    if (!cfg["cas"].is_null())
        merger->use_cas = cfg["cas"].uint64_value() ? 2 : 0;
    return [merger](cli_result_t & result)
    {
        merger->continue_merge_reent();
        if (merger->is_done())
        {
            result = merger->result;
            delete merger;
            return true;
        }
        return false;
    };
}
