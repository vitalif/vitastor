// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <unistd.h>
#include <assert.h>

#include "messenger.h"

void osd_messenger_t::cancel_osd_ops(osd_client_t *cl)
{
    for (auto p: cl->sent_ops)
    {
        cancel_op(p.second);
    }
    cl->sent_ops.clear();
    cl->outbox.clear();
}

void osd_messenger_t::cancel_op(osd_op_t *op)
{
    if (op->op_type == OSD_OP_OUT)
    {
        op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
        op->reply.hdr.id = op->req.hdr.id;
        op->reply.hdr.opcode = op->req.hdr.opcode;
        op->reply.hdr.retval = -EPIPE;
        // Copy lambda to be unaffected by `delete op`
        std::function<void(osd_op_t*)>(op->callback)(op);
    }
    else
    {
        // This function is only called in stop_client(), so it's fine to destroy the operation
        delete op;
    }
}

void osd_messenger_t::stop_client(int peer_fd, bool force)
{
    assert(peer_fd != 0);
    auto it = clients.find(peer_fd);
    if (it == clients.end())
    {
        return;
    }
    uint64_t repeer_osd = 0;
    osd_client_t *cl = it->second;
    if (cl->peer_state == PEER_CONNECTED)
    {
        if (cl->osd_num)
        {
            // Reload configuration from etcd when the connection is dropped
            if (log_level > 0)
                printf("[OSD %lu] Stopping client %d (OSD peer %lu)\n", osd_num, peer_fd, cl->osd_num);
            repeer_osd = cl->osd_num;
        }
        else
        {
            if (log_level > 0)
                printf("[OSD %lu] Stopping client %d (regular client)\n", osd_num, peer_fd);
        }
    }
    else if (!force)
    {
        return;
    }
    cl->peer_state = PEER_STOPPED;
    clients.erase(it);
#ifndef __MOCK__
    tfd->set_fd_handler(peer_fd, false, NULL);
    if (cl->connect_timeout_id >= 0)
    {
        tfd->clear_timer(cl->connect_timeout_id);
        cl->connect_timeout_id = -1;
    }
#endif
    if (cl->osd_num)
    {
        osd_peer_fds.erase(cl->osd_num);
    }
    if (cl->read_op)
    {
        if (cl->read_op->callback)
        {
            cancel_op(cl->read_op);
        }
        else
        {
            delete cl->read_op;
        }
        cl->read_op = NULL;
    }
    for (auto rit = read_ready_clients.begin(); rit != read_ready_clients.end(); rit++)
    {
        if (*rit == peer_fd)
        {
            read_ready_clients.erase(rit);
            break;
        }
    }
    for (auto wit = write_ready_clients.begin(); wit != write_ready_clients.end(); wit++)
    {
        if (*wit == peer_fd)
        {
            write_ready_clients.erase(wit);
            break;
        }
    }
    free(cl->in_buf);
    cl->in_buf = NULL;
#ifndef __MOCK__
    close(peer_fd);
#endif
    if (repeer_osd)
    {
        // First repeer PGs as canceling OSD ops may push new operations
        // and we need correct PG states when we do that
        repeer_pgs(repeer_osd);
    }
    if (cl->osd_num)
    {
        // Cancel outbound operations
        cancel_osd_ops(cl);
    }
    if (cl->refs <= 0)
    {
        delete cl;
    }
}
