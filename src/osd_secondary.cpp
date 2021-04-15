// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#include "osd.h"

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
    finish_op(op, retval);
}

void osd_t::exec_secondary(osd_op_t *cur_op)
{
    if (cur_op->req.hdr.opcode == OSD_OP_SEC_READ_BMP)
    {
        int n = cur_op->req.sec_read_bmp.len / sizeof(obj_ver_id);
        if (n > 0)
        {
            obj_ver_id *ov = (obj_ver_id*)cur_op->buf;
            void *reply_buf = malloc_or_die(n * (8 + clean_entry_bitmap_size));
            void *cur_buf = reply_buf;
            for (int i = 0; i < n; i++)
            {
                bs->read_bitmap(ov[i].oid, ov[i].version, cur_buf + sizeof(uint64_t), (uint64_t*)cur_buf);
                cur_buf += (8 + clean_entry_bitmap_size);
            }
            free(cur_op->buf);
            cur_op->buf = reply_buf;
        }
        finish_op(cur_op, n * (8 + clean_entry_bitmap_size));
        return;
    }
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
        cur_op->bs_op->oid.stripe = cur_op->req.sec_list.pg_stripe_size;
        cur_op->bs_op->len = cur_op->req.sec_list.pg_count;
        cur_op->bs_op->offset = cur_op->req.sec_list.list_pg - 1;
        cur_op->bs_op->oid.inode = cur_op->req.sec_list.min_inode;
        cur_op->bs_op->version = cur_op->req.sec_list.max_inode;
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

void osd_t::exec_show_config(osd_op_t *cur_op)
{
    std::string json_err;
    json11::Json req_json = cur_op->req.show_conf.json_len > 0
        ? json11::Json::parse(std::string((char *)cur_op->buf), json_err)
        : json11::Json();
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
        { "lease_timeout", etcd_report_interval+(MAX_ETCD_ATTEMPTS*(2*ETCD_QUICK_TIMEOUT)+999)/1000 },
    };
#ifdef WITH_RDMA
    if (msgr.is_rdma_enabled())
    {
        // Indicate that RDMA is enabled
        wire_config["rdma_enabled"] = true;
        if (req_json["connect_rdma"].is_string())
        {
            // Peer is trying to connect using RDMA, try to satisfy him
            bool ok = msgr.connect_rdma(cur_op->peer_fd, req_json["connect_rdma"].string_value());
            if (ok)
            {
                wire_config["rdma_connected"] = true;
                wire_config["rdma_address"] = msgr.clients.at(cur_op->peer_fd)->rdma_conn->addr.to_string();
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
