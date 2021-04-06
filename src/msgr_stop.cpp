// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <unistd.h>
#include <assert.h>

#include "messenger.h"

void osd_messenger_t::cancel_osd_ops(osd_client_t *cl)
{
    std::vector<osd_op_t*> cancel_ops;
    cancel_ops.resize(cl->sent_ops.size());
    int i = 0;
    for (auto p: cl->sent_ops)
    {
        cancel_ops[i++] = p.second;
    }
    cl->sent_ops.clear();
    cl->outbox.clear();
    for (auto op: cancel_ops)
    {
        cancel_op(op);
    }
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
    osd_client_t *cl = it->second;
    if (cl->peer_state == PEER_CONNECTING && !force || cl->peer_state == PEER_STOPPED)
    {
        return;
    }
    if (log_level > 0)
    {
        if (cl->osd_num)
        {
            printf("[OSD %lu] Stopping client %d (OSD peer %lu)\n", osd_num, peer_fd, cl->osd_num);
        }
        else
        {
            printf("[OSD %lu] Stopping client %d (regular client)\n", osd_num, peer_fd);
        }
    }
    // First set state to STOPPED so another stop_client() call doesn't try to free it again
    cl->refs++;
    cl->peer_state = PEER_STOPPED;
    if (cl->osd_num)
    {
        // ...and forget OSD peer
        osd_peer_fds.erase(cl->osd_num);
    }
#ifndef __MOCK__
    // Then remove FD from the eventloop so we don't accidentally read something
    tfd->set_fd_handler(peer_fd, false, NULL);
    if (cl->connect_timeout_id >= 0)
    {
        tfd->clear_timer(cl->connect_timeout_id);
        cl->connect_timeout_id = -1;
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
#endif
    if (cl->osd_num)
    {
        // Then repeer PGs because cancel_op() callbacks can try to perform
        // some actions and we need correct PG states to not do something silly
        repeer_pgs(cl->osd_num);
    }
    // Then cancel all operations
    if (cl->read_op)
    {
        if (!cl->read_op->callback)
        {
            delete cl->read_op;
        }
        cl->read_op = NULL;
    }
    if (cl->osd_num)
    {
        // Cancel outbound operations
        cancel_osd_ops(cl);
    }
#ifndef __MOCK__
    // And close the FD only when everything is done
    // ...because peer_fd number can get reused after close()
    close(peer_fd);
#endif
    // Find the item again because it can be invalidated at this point
    it = clients.find(peer_fd);
    if (it != clients.end())
    {
        clients.erase(it);
    }
    cl->refs--;
    if (cl->refs <= 0)
    {
        delete cl;
    }
}
