// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "osd_primary.h"

// Save and clear unstable_writes -> SYNC all -> STABLE all
void osd_t::continue_primary_sync(osd_op_t *cur_op)
{
    if (!cur_op->op_data)
    {
        cur_op->op_data = (osd_primary_op_data_t*)calloc_or_die(1, sizeof(osd_primary_op_data_t));
    }
    osd_primary_op_data_t *op_data = cur_op->op_data;
    if (op_data->st == 1)      goto resume_1;
    else if (op_data->st == 2) goto resume_2;
    else if (op_data->st == 3) goto resume_3;
    else if (op_data->st == 4) goto resume_4;
    else if (op_data->st == 5) goto resume_5;
    else if (op_data->st == 6) goto resume_6;
    else if (op_data->st == 7) goto resume_7;
    else if (op_data->st == 8) goto resume_8;
    assert(op_data->st == 0);
    if (syncs_in_progress.size() > 0)
    {
        // Wait for previous syncs, if any
        // FIXME: We may try to execute the current one in parallel, like in Blockstore, but I'm not sure if it matters at all
        syncs_in_progress.push_back(cur_op);
        op_data->st = 1;
resume_1:
        return;
    }
    else
    {
        syncs_in_progress.push_back(cur_op);
    }
resume_2:
    if (dirty_osds.size() == 0)
    {
        // Nothing to sync
        goto finish;
    }
    // Save and clear unstable_writes
    // In theory it is possible to do in on a per-client basis, but this seems to be an unnecessary complication
    // It would be cool not to copy these here at all, but someone has to deduplicate them by object IDs anyway
    if (unstable_writes.size() > 0)
    {
        op_data->unstable_write_osds = new std::vector<unstable_osd_num_t>();
        op_data->unstable_writes = new obj_ver_id[this->unstable_writes.size()];
        osd_num_t last_osd = 0;
        int last_start = 0, last_end = 0;
        for (auto it = this->unstable_writes.begin(); it != this->unstable_writes.end(); it++)
        {
            if (last_osd != it->first.osd_num)
            {
                if (last_osd != 0)
                {
                    op_data->unstable_write_osds->push_back((unstable_osd_num_t){
                        .osd_num = last_osd,
                        .start = last_start,
                        .len = last_end - last_start,
                    });
                }
                last_osd = it->first.osd_num;
                last_start = last_end;
            }
            op_data->unstable_writes[last_end] = (obj_ver_id){
                .oid = it->first.oid,
                .version = it->second,
            };
            last_end++;
        }
        if (last_osd != 0)
        {
            op_data->unstable_write_osds->push_back((unstable_osd_num_t){
                .osd_num = last_osd,
                .start = last_start,
                .len = last_end - last_start,
            });
        }
        this->unstable_writes.clear();
    }
    {
        op_data->dirty_pg_count = dirty_pgs.size();
        op_data->dirty_osd_count = dirty_osds.size();
        void *dirty_buf = malloc_or_die(
            sizeof(pool_pg_num_t)*dirty_pgs.size() +
            sizeof(uint64_t)*dirty_pgs.size() +
            sizeof(osd_num_t)*dirty_osds.size() +
            sizeof(obj_ver_osd_t)*this->copies_to_delete_after_sync_count
        );
        op_data->dirty_pgs = (pool_pg_num_t*)dirty_buf;
        uint64_t *pg_del_counts = (uint64_t*)((uint8_t*)op_data->dirty_pgs + (sizeof(pool_pg_num_t))*op_data->dirty_pg_count);
        op_data->dirty_osds = (osd_num_t*)((uint8_t*)pg_del_counts + 8*op_data->dirty_pg_count);
        if (this->copies_to_delete_after_sync_count)
        {
            op_data->copies_to_delete_count = 0;
            op_data->copies_to_delete = (obj_ver_osd_t*)(op_data->dirty_osds + op_data->dirty_osd_count);
            for (auto dirty_pg_num: dirty_pgs)
            {
                auto & pg = pgs.at(dirty_pg_num);
                assert(pg.copies_to_delete_after_sync.size() <= this->copies_to_delete_after_sync_count);
                memcpy(
                    op_data->copies_to_delete + op_data->copies_to_delete_count,
                    pg.copies_to_delete_after_sync.data(),
                    sizeof(obj_ver_osd_t)*pg.copies_to_delete_after_sync.size()
                );
                op_data->copies_to_delete_count += pg.copies_to_delete_after_sync.size();
            }
        }
        int dpg = 0;
        for (auto dirty_pg_num: dirty_pgs)
        {
            auto & pg = pgs.at(dirty_pg_num);
            pg.inflight++;
            op_data->dirty_pgs[dpg] = dirty_pg_num;
            pg_del_counts[dpg] = pg.copies_to_delete_after_sync.size();
            dpg++;
        }
        dirty_pgs.clear();
        dpg = 0;
        for (auto osd_num: dirty_osds)
        {
            op_data->dirty_osds[dpg++] = osd_num;
        }
        dirty_osds.clear();
    }
    if (immediate_commit != IMMEDIATE_ALL)
    {
        // SYNC
        if (!submit_primary_sync_subops(cur_op))
        {
            goto resume_4;
        }
resume_3:
        op_data->st = 3;
        return;
resume_4:
        if (op_data->errors > 0)
        {
            goto resume_6;
        }
    }
    if (op_data->unstable_writes)
    {
        // Stabilize version sets, if any
        submit_primary_stab_subops(cur_op);
resume_5:
        op_data->st = 5;
        return;
    }
resume_6:
    if (op_data->errors > 0)
    {
        // Return PGs and OSDs back into their dirty sets
        for (int i = 0; i < op_data->dirty_pg_count; i++)
        {
            dirty_pgs.insert(op_data->dirty_pgs[i]);
        }
        for (int i = 0; i < op_data->dirty_osd_count; i++)
        {
            dirty_osds.insert(op_data->dirty_osds[i]);
        }
        if (op_data->unstable_writes)
        {
            // Return objects back into the unstable write set
            for (auto unstable_osd: *(op_data->unstable_write_osds))
            {
                for (int i = 0; i < unstable_osd.len; i++)
                {
                    // Except those from peered PGs
                    auto & w = op_data->unstable_writes[unstable_osd.start + i];
                    pool_pg_num_t wpg = {
                        .pool_id = INODE_POOL(w.oid.inode),
                        .pg_num = map_to_pg(w.oid),
                    };
                    if (pgs.at(wpg).state & PG_ACTIVE)
                    {
                        uint64_t & dest = this->unstable_writes[(osd_object_id_t){
                            .osd_num = unstable_osd.osd_num,
                            .oid = w.oid,
                        }];
                        dest = dest < w.version ? w.version : dest;
                        dirty_pgs.insert(wpg);
                    }
                }
            }
        }
    }
    else if (op_data->copies_to_delete)
    {
        // Actually delete copies which we wanted to delete
        submit_primary_del_batch(cur_op, op_data->copies_to_delete, op_data->copies_to_delete_count);
resume_7:
        op_data->st = 7;
        return;
resume_8:
        if (op_data->errors > 0)
        {
            goto resume_6;
        }
        {
            uint64_t *pg_del_counts = (uint64_t*)((uint8_t*)op_data->dirty_pgs + (sizeof(pool_pg_num_t))*op_data->dirty_pg_count);
            for (int i = 0; i < op_data->dirty_pg_count; i++)
            {
                auto & pg = pgs.at(op_data->dirty_pgs[i]);
                auto n = pg_del_counts[i];
                assert(copies_to_delete_after_sync_count >= n);
                copies_to_delete_after_sync_count -= n;
                pg.copies_to_delete_after_sync.erase(pg.copies_to_delete_after_sync.begin(), pg.copies_to_delete_after_sync.begin()+n);
                if (!pg.misplaced_objects.size() && !pg.copies_to_delete_after_sync.size() && (pg.state & PG_HAS_MISPLACED))
                {
                    pg.state = pg.state & ~PG_HAS_MISPLACED;
                    report_pg_state(pg);
                }
            }
        }
        if (immediate_commit == IMMEDIATE_NONE)
        {
            // Mark OSDs as dirty because deletions have to be synced too!
            for (int i = 0; i < op_data->copies_to_delete_count; i++)
            {
                auto & chunk = op_data->copies_to_delete[i];
                this->dirty_osds.insert(chunk.osd_num);
            }
        }
    }
    for (int i = 0; i < op_data->dirty_pg_count; i++)
    {
        auto & pg = pgs.at(op_data->dirty_pgs[i]);
        rm_inflight(pg);
    }
    // FIXME: Free those in the destructor (not here)?
    free(op_data->dirty_pgs);
    op_data->dirty_pgs = NULL;
    op_data->dirty_osds = NULL;
    if (op_data->unstable_writes)
    {
        delete op_data->unstable_write_osds;
        delete[] op_data->unstable_writes;
        op_data->unstable_writes = NULL;
        op_data->unstable_write_osds = NULL;
    }
    if (op_data->errors > 0)
    {
        finish_op(cur_op, op_data->errcode);
    }
    else
    {
finish:
        if (cur_op->peer_fd)
        {
            auto it = msgr.clients.find(cur_op->peer_fd);
            if (it != msgr.clients.end())
                it->second->dirty_pgs.clear();
        }
        finish_op(cur_op, 0);
    }
    assert(syncs_in_progress.front() == cur_op);
    syncs_in_progress.pop_front();
    if (syncs_in_progress.size() > 0)
    {
        cur_op = syncs_in_progress.front();
        op_data = cur_op->op_data;
        op_data->st++;
        goto resume_2;
    }
}
