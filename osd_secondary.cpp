#include "osd.h"

#include "json11/json11.hpp"

void osd_t::secondary_op_callback(osd_op_t *op)
{
    if (op->req.hdr.opcode == OSD_OP_SECONDARY_READ ||
        op->req.hdr.opcode == OSD_OP_SECONDARY_WRITE)
    {
        op->reply.sec_rw.version = op->bs_op->version;
    }
    else if (op->req.hdr.opcode == OSD_OP_SECONDARY_DELETE)
    {
        op->reply.sec_del.version = op->bs_op->version;
    }
    if (op->req.hdr.opcode == OSD_OP_SECONDARY_READ &&
        op->bs_op->retval > 0)
    {
        op->send_list.push_back(op->buf, op->bs_op->retval);
    }
    else if (op->req.hdr.opcode == OSD_OP_SECONDARY_LIST)
    {
        // allocated by blockstore
        op->buf = op->bs_op->buf;
        if (op->bs_op->retval > 0)
        {
            op->send_list.push_back(op->buf, op->bs_op->retval * sizeof(obj_ver_id));
        }
        op->reply.sec_list.stable_count = op->bs_op->version;
    }
    finish_op(op, op->bs_op->retval);
}

void osd_t::exec_secondary(osd_op_t *cur_op)
{
    cur_op->bs_op = new blockstore_op_t();
    cur_op->bs_op->callback = [this, cur_op](blockstore_op_t* bs_op) { secondary_op_callback(cur_op); };
    cur_op->bs_op->opcode = (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_READ ? BS_OP_READ
        : (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_WRITE ? BS_OP_WRITE
        : (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_SYNC ? BS_OP_SYNC
        : (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_STABILIZE ? BS_OP_STABLE
        : (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_ROLLBACK ? BS_OP_ROLLBACK
        : (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_DELETE ? BS_OP_DELETE
        : (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_LIST ? BS_OP_LIST
        : -1)))))));
    if (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_READ ||
        cur_op->req.hdr.opcode == OSD_OP_SECONDARY_WRITE)
    {
        cur_op->bs_op->oid = cur_op->req.sec_rw.oid;
        cur_op->bs_op->version = cur_op->req.sec_rw.version;
        cur_op->bs_op->offset = cur_op->req.sec_rw.offset;
        cur_op->bs_op->len = cur_op->req.sec_rw.len;
        cur_op->bs_op->buf = cur_op->buf;
#ifdef OSD_STUB
        cur_op->bs_op->retval = cur_op->bs_op->len;
#endif
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_DELETE)
    {
        cur_op->bs_op->oid = cur_op->req.sec_del.oid;
        cur_op->bs_op->version = cur_op->req.sec_del.version;
#ifdef OSD_STUB
        cur_op->bs_op->retval = 0;
#endif
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_STABILIZE ||
        cur_op->req.hdr.opcode == OSD_OP_SECONDARY_ROLLBACK)
    {
        cur_op->bs_op->len = cur_op->req.sec_stab.len/sizeof(obj_ver_id);
        cur_op->bs_op->buf = cur_op->buf;
#ifdef OSD_STUB
        cur_op->bs_op->retval = 0;
#endif
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_LIST)
    {
        if (cur_op->req.sec_list.pg_count < cur_op->req.sec_list.list_pg)
        {
            // requested pg number is greater than total pg count
            cur_op->bs_op->retval = -EINVAL;
            secondary_op_callback(cur_op);
            return;
        }
        cur_op->bs_op->oid.stripe = cur_op->req.sec_list.pg_stripe_size;
        cur_op->bs_op->len = cur_op->req.sec_list.pg_count;
        cur_op->bs_op->offset = cur_op->req.sec_list.list_pg - 1;
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
    // FIXME: Send the real config, not its source
    std::string cfg_str = json11::Json(config).dump();
    cur_op->buf = malloc(cfg_str.size()+1);
    memcpy(cur_op->buf, cfg_str.c_str(), cfg_str.size()+1);
    cur_op->send_list.push_back(cur_op->buf, cur_op->reply.hdr.retval);
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
