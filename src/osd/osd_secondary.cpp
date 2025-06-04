// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "osd.h"
#ifdef WITH_RDMA
#include "msgr_rdma.h"
#endif

#include "json11/json11.hpp"

void osd_t::secondary_op_callback(osd_op_t *op)
{
    if (op->req.hdr.opcode == OSD_OP_SEC_READ ||
        op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
        op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE)
    {
        op->reply.sec_rw.version = op->bs_op->version;
    }
    else if (op->req.hdr.opcode == OSD_OP_SEC_DELETE)
    {
        op->reply.sec_del.version = op->bs_op->version;
    }
    if (op->req.hdr.opcode == OSD_OP_SEC_READ)
    {
        if (op->bs_op->retval >= 0)
            op->reply.sec_rw.attr_len = clean_entry_bitmap_size;
        else
            op->reply.sec_rw.attr_len = 0;
        if (op->bs_op->retval > 0)
            op->iov.push_back(op->buf, op->bs_op->retval);
    }
    else if (op->req.hdr.opcode == OSD_OP_SEC_LIST)
    {
        // allocated by blockstore
        op->buf = op->bs_op->buf;
        if (op->bs_op->retval > 0)
        {
            op->iov.push_back(op->buf, op->bs_op->retval * sizeof(obj_ver_id));
        }
        op->reply.sec_list.stable_count = op->bs_op->version;
    }
    int retval = op->bs_op->retval;
    delete op->bs_op;
    op->bs_op = NULL;
    if (op->is_recovery_related() && recovery_target_sleep_us &&
        op->req.hdr.opcode == OSD_OP_SEC_STABILIZE)
    {
        // Apply pause AFTER commit. Do not apply pause to SYNC at all
        if (!op->tv_end.tv_sec)
        {
            clock_gettime(CLOCK_REALTIME, &op->tv_end);
        }
        tfd->set_timer_us(recovery_target_sleep_us, false, [this, op, retval](int timer_id)
        {
            finish_op(op, retval);
        });
    }
    else
    {
        finish_op(op, retval);
    }
}

void osd_t::exec_secondary(osd_op_t *op)
{
    if (op->is_recovery_related() && recovery_target_sleep_us &&
        op->req.hdr.opcode != OSD_OP_SEC_STABILIZE && op->req.hdr.opcode != OSD_OP_SEC_SYNC)
    {
        // Apply pause BEFORE write/delete
        tfd->set_timer_us(recovery_target_sleep_us, false, [this, op](int timer_id)
        {
            clock_gettime(CLOCK_REALTIME, &op->tv_begin);
            exec_secondary_real(op);
        });
    }
    else
    {
        exec_secondary_real(op);
    }
}

bool osd_t::sec_check_pg_lock(osd_num_t primary_osd, const object_id &oid)
{
    if (!enable_pg_locks)
    {
        return true;
    }
    pool_id_t pool_id = INODE_POOL(oid.inode);
    auto pool_cfg_it = st_cli.pool_config.find(pool_id);
    if (pool_cfg_it == st_cli.pool_config.end())
    {
        return false;
    }
    auto & pool_cfg = pool_cfg_it->second;
    if (pg_locks_localize_only && (pool_cfg.scheme != POOL_SCHEME_REPLICATED || pool_cfg.local_reads == POOL_LOCAL_READ_PRIMARY))
    {
        return true;
    }
    auto ppg = (pool_pg_num_t){ .pool_id = pool_id, .pg_num = map_to_pg(oid, pool_cfg_it->second.pg_stripe_size) };
    auto pg_it = pgs.find(ppg);
    if (pg_it != pgs.end() && pg_it->second.state != PG_OFFLINE)
    {
        return false;
    }
    auto lock_it = pg_locks.find(ppg);
    return lock_it != pg_locks.end() && lock_it->second.primary_osd == primary_osd;
}

void osd_t::exec_secondary_real(osd_op_t *cur_op)
{
    if (cur_op->req.hdr.opcode == OSD_OP_SEC_LIST &&
        (cur_op->req.sec_list.flags & OSD_LIST_PRIMARY))
    {
        continue_primary_list(cur_op);
        return;
    }
    if (cur_op->req.hdr.opcode == OSD_OP_SEC_READ_BMP)
    {
        exec_sec_read_bmp(cur_op);
        return;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SEC_LOCK)
    {
        exec_sec_lock(cur_op);
        return;
    }
    auto cl = msgr.clients.at(cur_op->peer_fd);
    cur_op->bs_op = new blockstore_op_t();
    cur_op->bs_op->callback = [this, cur_op](blockstore_op_t* bs_op) { secondary_op_callback(cur_op); };
    cur_op->bs_op->opcode = (cur_op->req.hdr.opcode == OSD_OP_SEC_READ ? BS_OP_READ
        : (cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE ? BS_OP_WRITE
        : (cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE ? BS_OP_WRITE_STABLE
        : (cur_op->req.hdr.opcode == OSD_OP_SEC_SYNC ? BS_OP_SYNC
        : (cur_op->req.hdr.opcode == OSD_OP_SEC_STABILIZE ? BS_OP_STABLE
        : (cur_op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK ? BS_OP_ROLLBACK
        : (cur_op->req.hdr.opcode == OSD_OP_SEC_DELETE ? BS_OP_DELETE
        : (cur_op->req.hdr.opcode == OSD_OP_SEC_LIST ? BS_OP_LIST
        : -1))))))));
    if (cur_op->req.hdr.opcode == OSD_OP_SEC_READ ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE)
    {
        if (!(cur_op->req.sec_rw.flags & OSD_OP_IGNORE_PG_LOCK) &&
            !sec_check_pg_lock(cl->in_osd_num, cur_op->req.sec_rw.oid))
        {
            cur_op->bs_op->retval = -EPIPE;
            secondary_op_callback(cur_op);
            return;
        }
        if (cur_op->req.hdr.opcode == OSD_OP_SEC_READ)
        {
            // Allocate memory for the read operation
            if (clean_entry_bitmap_size > sizeof(unsigned))
                cur_op->bitmap = cur_op->rmw_buf = malloc_or_die(clean_entry_bitmap_size);
            else
                cur_op->bitmap = &cur_op->bmp_data;
            if (cur_op->req.sec_rw.len > 0)
                cur_op->buf = memalign_or_die(MEM_ALIGNMENT, cur_op->req.sec_rw.len);
        }
        cur_op->bs_op->oid = cur_op->req.sec_rw.oid;
        cur_op->bs_op->version = cur_op->req.sec_rw.version;
        cur_op->bs_op->offset = cur_op->req.sec_rw.offset;
        cur_op->bs_op->len = cur_op->req.sec_rw.len;
        cur_op->bs_op->buf = cur_op->buf;
        cur_op->bs_op->bitmap = cur_op->bitmap;
#ifdef OSD_STUB
        cur_op->bs_op->retval = cur_op->bs_op->len;
#endif
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SEC_DELETE)
    {
        if (!(cur_op->req.sec_del.flags & OSD_OP_IGNORE_PG_LOCK) &&
            !sec_check_pg_lock(cl->in_osd_num, cur_op->req.sec_del.oid))
        {
            cur_op->bs_op->retval = -EPIPE;
            secondary_op_callback(cur_op);
            return;
        }
        cur_op->bs_op->oid = cur_op->req.sec_del.oid;
        cur_op->bs_op->version = cur_op->req.sec_del.version;
#ifdef OSD_STUB
        cur_op->bs_op->retval = 0;
#endif
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SEC_STABILIZE ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK)
    {
        cur_op->bs_op->len = cur_op->req.sec_stab.len/sizeof(obj_ver_id);
        cur_op->bs_op->buf = cur_op->buf;
#ifdef OSD_STUB
        cur_op->bs_op->retval = 0;
#endif
        if (enable_pg_locks && !(cur_op->req.sec_stab.flags & OSD_OP_IGNORE_PG_LOCK))
        {
            for (int i = 0; i < cur_op->bs_op->len; i++)
            {
                if (!sec_check_pg_lock(cl->in_osd_num, ((obj_ver_id*)cur_op->buf)[i].oid))
                {
                    cur_op->bs_op->retval = -EPIPE;
                    secondary_op_callback(cur_op);
                    return;
                }
            }
        }
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SEC_LIST)
    {
        if (cur_op->req.sec_list.pg_count < cur_op->req.sec_list.list_pg)
        {
            // requested pg number is greater than total pg count
            printf("Invalid LIST request: pg count %u < pg number %u\n", cur_op->req.sec_list.pg_count, cur_op->req.sec_list.list_pg);
            cur_op->bs_op->retval = -EINVAL;
            secondary_op_callback(cur_op);
            return;
        }
        cur_op->bs_op->pg_alignment = cur_op->req.sec_list.pg_stripe_size;
        cur_op->bs_op->pg_count = cur_op->req.sec_list.pg_count;
        cur_op->bs_op->pg_number = cur_op->req.sec_list.list_pg - 1;
        cur_op->bs_op->min_oid.inode = cur_op->req.sec_list.min_inode;
        cur_op->bs_op->min_oid.stripe = cur_op->req.sec_list.min_stripe;
        cur_op->bs_op->max_oid.inode = cur_op->req.sec_list.max_inode;
        if (cur_op->req.sec_list.max_inode && cur_op->req.sec_list.max_stripe != UINT64_MAX)
        {
            cur_op->bs_op->max_oid.stripe = cur_op->req.sec_list.max_stripe
                ? cur_op->req.sec_list.max_stripe : UINT64_MAX;
        }
        cur_op->bs_op->list_stable_limit = cur_op->req.sec_list.stable_limit;
#ifdef OSD_STUB
        cur_op->bs_op->retval = 0;
        cur_op->bs_op->buf = NULL;
#endif
    }
#ifdef OSD_STUB
    secondary_op_callback(cur_op);
#else
    bs->enqueue_op(cur_op->bs_op);
#endif
}

void osd_t::exec_sec_read_bmp(osd_op_t *cur_op)
{
    auto cl = msgr.clients.at(cur_op->peer_fd);
    int n = cur_op->req.sec_read_bmp.len / sizeof(obj_ver_id);
    if (n > 0)
    {
        obj_ver_id *ov = (obj_ver_id*)cur_op->buf;
        void *reply_buf = malloc_or_die(n * (8 + clean_entry_bitmap_size));
        void *cur_buf = reply_buf;
        for (int i = 0; i < n; i++)
        {
            if (!sec_check_pg_lock(cl->in_osd_num, ov[i].oid) &&
                !(cur_op->req.sec_read_bmp.flags & OSD_OP_IGNORE_PG_LOCK))
            {
                free(reply_buf);
                cur_op->bs_op->retval = -EPIPE;
                secondary_op_callback(cur_op);
                return;
            }
            bs->read_bitmap(ov[i].oid, ov[i].version, (uint8_t*)cur_buf + sizeof(uint64_t), (uint64_t*)cur_buf);
            cur_buf = (uint8_t*)cur_buf + (8 + clean_entry_bitmap_size);
        }
        free(cur_op->buf);
        cur_op->buf = reply_buf;
    }
    finish_op(cur_op, n * (8 + clean_entry_bitmap_size));
}

// Lock/Unlock PG
void osd_t::exec_sec_lock(osd_op_t *cur_op)
{
    cur_op->reply.sec_lock.cur_primary = 0;
    auto cl = msgr.clients.at(cur_op->peer_fd);
    if (!cl->in_osd_num ||
        cur_op->req.sec_lock.flags != OSD_SEC_LOCK_PG &&
        cur_op->req.sec_lock.flags != OSD_SEC_UNLOCK_PG ||
        cur_op->req.sec_lock.pool_id > ((uint64_t)1<<POOL_ID_BITS) ||
        !cur_op->req.sec_lock.pg_num ||
        cur_op->req.sec_lock.pg_num > UINT32_MAX)
    {
        finish_op(cur_op, -EINVAL);
        return;
    }
    auto ppg = (pool_pg_num_t){ .pool_id = (pool_id_t)cur_op->req.sec_lock.pool_id, .pg_num = (pg_num_t)cur_op->req.sec_lock.pg_num };
    auto pool_cfg_it = st_cli.pool_config.find(ppg.pool_id);
    if (pool_cfg_it == st_cli.pool_config.end() ||
        pool_cfg_it->second.real_pg_count < cur_op->req.sec_lock.pg_num)
    {
        finish_op(cur_op, -ENOENT);
        return;
    }
    auto lock_it = pg_locks.find(ppg);
    if (cur_op->req.sec_lock.flags == OSD_SEC_LOCK_PG)
    {
        if (lock_it != pg_locks.end() && lock_it->second.primary_osd != cl->in_osd_num)
        {
            cur_op->reply.sec_lock.cur_primary = lock_it->second.primary_osd;
            finish_op(cur_op, -EBUSY);
            return;
        }
        auto primary_pg_it = pgs.find(ppg);
        if (primary_pg_it != pgs.end() && primary_pg_it->second.state != PG_OFFLINE)
        {
            cur_op->reply.sec_lock.cur_primary = this->osd_num;
            finish_op(cur_op, -EBUSY);
            return;
        }
        if (log_level > 3)
        {
            printf("Lock PG %u/%u for OSD %ju\n", ppg.pool_id, ppg.pg_num, cl->in_osd_num);
        }
        pg_locks[ppg] = (osd_pg_lock_t){
            .primary_osd = cl->in_osd_num,
            .state = cur_op->req.sec_lock.pg_state,
        };
    }
    else if (lock_it != pg_locks.end() && lock_it->second.primary_osd == cl->in_osd_num)
    {
        if (log_level > 3)
        {
            printf("Unlock PG %u/%u by OSD %ju\n", ppg.pool_id, ppg.pg_num, cl->in_osd_num);
        }
        pg_locks.erase(lock_it);
    }
    finish_op(cur_op, 0);
}

void osd_t::exec_show_config(osd_op_t *cur_op)
{
    std::string json_err;
    json11::Json req_json = cur_op->req.show_conf.json_len > 0
        ? json11::Json::parse(std::string((char *)cur_op->buf), json_err)
        : json11::Json();
    auto peer_osd_num = req_json["osd_num"].uint64_value();
    auto cl = msgr.clients.at(cur_op->peer_fd);
    cl->in_osd_num = peer_osd_num;
    if (req_json["features"]["check_sequencing"].bool_value())
    {
        cl->check_sequencing = true;
        cl->read_op_id = cur_op->req.hdr.id + 1;
    }
    // Expose sensitive configuration values so peers can check them
    json11::Json::object wire_config = json11::Json::object {
        { "osd_num", osd_num },
        { "protocol_version", OSD_PROTOCOL_VERSION },
        { "block_size", (uint64_t)bs_block_size },
        { "bitmap_granularity", (uint64_t)bs_bitmap_granularity },
        { "primary_enabled", run_primary },
        { "blockstore_enabled", bs ? true : false },
        { "readonly", readonly },
        { "immediate_commit", (immediate_commit == IMMEDIATE_ALL ? "all" :
            (immediate_commit == IMMEDIATE_SMALL ? "small" : "none")) },
        { "lease_timeout", etcd_report_interval+(st_cli.max_etcd_attempts*(2*st_cli.etcd_quick_timeout)+999)/1000 },
        { "features", json11::Json::object{ { "pg_locks", true } } },
    };
#ifdef WITH_RDMA
    if (msgr.is_rdma_enabled())
    {
        // Indicate that RDMA is enabled
        wire_config["rdma_enabled"] = true;
        if (req_json["connect_rdma"].is_string())
        {
            // Peer is trying to connect using RDMA, try to satisfy him
            bool ok = msgr.connect_rdma(cur_op->peer_fd, req_json["connect_rdma"].string_value(), req_json["rdma_max_msg"].uint64_value());
            if (ok)
            {
                auto rc = cl->rdma_conn;
                wire_config["rdma_address"] = rc->addr.to_string();
                wire_config["rdma_max_msg"] = rc->max_msg;
            }
        }
    }
#endif
    if (cur_op->buf)
        free(cur_op->buf);
    std::string cfg_str = json11::Json(wire_config).dump();
    cur_op->buf = malloc_or_die(cfg_str.size()+1);
    memcpy(cur_op->buf, cfg_str.c_str(), cfg_str.size()+1);
    cur_op->iov.push_back(cur_op->buf, cfg_str.size()+1);
    finish_op(cur_op, cfg_str.size()+1);
}

void osd_t::exec_sync_stab_all(osd_op_t *cur_op)
{
    // Sync and stabilize all objects
    // This command is only valid for tests
    cur_op->bs_op = new blockstore_op_t();
    if (!allow_test_ops)
    {
        cur_op->bs_op->retval = -EINVAL;
        secondary_op_callback(cur_op);
        return;
    }
    cur_op->bs_op->opcode = BS_OP_SYNC_STAB_ALL;
    cur_op->bs_op->callback = [this, cur_op](blockstore_op_t *bs_op)
    {
        secondary_op_callback(cur_op);
    };
#ifdef OSD_STUB
    cur_op->bs_op->retval = 0;
    secondary_op_callback(cur_op);
#else
    bs->enqueue_op(cur_op->bs_op);
#endif
}
