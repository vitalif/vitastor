#include "osd.h"

#include "json11/json11.hpp"

void osd_t::secondary_op_callback(osd_op_t *cur_op)
{
    inflight_ops--;
    auto cl_it = clients.find(cur_op->peer_fd);
    if (cl_it != clients.end())
    {
        auto & cl = cl_it->second;
        make_reply(cur_op);
        outbox_push(cl, cur_op);
    }
    else
    {
        delete cur_op;
    }
}

void osd_t::exec_secondary(osd_op_t *cur_op)
{
    cur_op->bs_op.callback = [this, cur_op](blockstore_op_t* bs_op) { secondary_op_callback(cur_op); };
    cur_op->bs_op.opcode = (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_READ ? BS_OP_READ
        : (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_WRITE ? BS_OP_WRITE
        : (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_SYNC ? BS_OP_SYNC
        : (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_STABILIZE ? BS_OP_STABLE
        : (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_ROLLBACK ? BS_OP_ROLLBACK
        : (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_DELETE ? BS_OP_DELETE
        : (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_LIST ? BS_OP_LIST
        : -1)))))));
    if (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_READ ||
        cur_op->op.hdr.opcode == OSD_OP_SECONDARY_WRITE)
    {
        cur_op->bs_op.oid = cur_op->op.sec_rw.oid;
        cur_op->bs_op.version = cur_op->op.sec_rw.version;
        cur_op->bs_op.offset = cur_op->op.sec_rw.offset;
        cur_op->bs_op.len = cur_op->op.sec_rw.len;
        cur_op->bs_op.buf = cur_op->buf;
    }
    else if (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_DELETE)
    {
        cur_op->bs_op.oid = cur_op->op.sec_del.oid;
        cur_op->bs_op.version = cur_op->op.sec_del.version;
    }
    else if (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_STABILIZE ||
        cur_op->op.hdr.opcode == OSD_OP_SECONDARY_ROLLBACK)
    {
        cur_op->bs_op.len = cur_op->op.sec_stab.len/sizeof(obj_ver_id);
        cur_op->bs_op.buf = cur_op->buf;
    }
    else if (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_LIST)
    {
        if (cur_op->op.sec_list.pgtotal < cur_op->op.sec_list.pgnum)
        {
            // requested pg number is greater than total pg count
            cur_op->bs_op.retval = -EINVAL;
            secondary_op_callback(cur_op);
            return;
        }
        cur_op->bs_op.len = cur_op->op.sec_list.pgtotal;
        cur_op->bs_op.offset = cur_op->op.sec_list.pgnum - 1;
    }
#ifdef OSD_STUB
    cur_op->bs_op.retval = cur_op->bs_op.len;
    secondary_op_callback(cur_op);
#else
    bs->enqueue_op(&cur_op->bs_op);
#endif
}

void osd_t::exec_show_config(osd_op_t *cur_op)
{
    // FIXME: Send the real config, not its source
    std::string *cfg_str = new std::string(std::move(json11::Json(config).dump()));
    cur_op->buf = cfg_str;
    auto & cl = clients[cur_op->peer_fd];
    make_reply(cur_op);
    outbox_push(cl, cur_op);
}

void osd_t::exec_sync_stab_all(osd_op_t *cur_op)
{
    // Sync and stabilize all objects
    // This command is only valid for tests
    // FIXME: Dedup between here & fio_engine
    if (!allow_test_ops)
    {
        cur_op->bs_op.retval = -EINVAL;
        secondary_op_callback(cur_op);
        return;
    }
    cur_op->bs_op.opcode = BS_OP_SYNC;
    cur_op->bs_op.callback = [this, cur_op](blockstore_op_t *op)
    {
        auto & unstable_writes = bs->get_unstable_writes();
        if (op->retval >= 0 && unstable_writes.size() > 0)
        {
            op->opcode = BS_OP_STABLE;
            op->len = unstable_writes.size();
            obj_ver_id *vers = new obj_ver_id[op->len];
            op->buf = vers;
            int i = 0;
            for (auto it = unstable_writes.begin(); it != unstable_writes.end(); it++, i++)
            {
                vers[i] = {
                    .oid = it->first,
                    .version = it->second,
                };
            }
            unstable_writes.clear();
            op->callback = [this, cur_op](blockstore_op_t *op)
            {
                secondary_op_callback(cur_op);
                obj_ver_id *vers = (obj_ver_id*)op->buf;
                delete[] vers;
            };
            bs->enqueue_op(op);
        }
        else
        {
            secondary_op_callback(cur_op);
        }
    };
#ifdef OSD_STUB
    cur_op->bs_op.retval = 0;
    secondary_op_callback(cur_op);
#else
    bs->enqueue_op(&cur_op->bs_op);
#endif
}

void osd_t::make_reply(osd_op_t *op)
{
    op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
    op->reply.hdr.id = op->op.hdr.id;
    op->reply.hdr.opcode = op->op.hdr.opcode;
    if (op->op.hdr.opcode == OSD_OP_SHOW_CONFIG)
    {
        std::string *str = (std::string*)op->buf;
        op->reply.hdr.retval = str->size()+1;
        op->send_list.push_back((void*)str->c_str(), op->reply.hdr.retval);
    }
    else
    {
        op->reply.hdr.retval = op->bs_op.retval;
        if (op->op.hdr.opcode == OSD_OP_SECONDARY_LIST)
            op->reply.sec_list.stable_count = op->bs_op.version;
        else if (op->op.hdr.opcode == OSD_OP_SECONDARY_WRITE)
            op->reply.sec_rw.version = op->bs_op.version;
        else if (op->op.hdr.opcode == OSD_OP_SECONDARY_DELETE)
            op->reply.sec_del.version = op->bs_op.version;
    }
    if (op->op.hdr.opcode == OSD_OP_SECONDARY_READ &&
        op->reply.hdr.retval > 0)
    {
        op->send_list.push_back(op->buf, op->reply.hdr.retval);
    }
    else if (op->op.hdr.opcode == OSD_OP_SECONDARY_LIST &&
        op->reply.hdr.retval > 0)
    {
        op->buf = op->bs_op.buf; // allocated by blockstore
        op->send_list.push_back(op->buf, op->reply.hdr.retval * sizeof(obj_ver_id));
    }
}
