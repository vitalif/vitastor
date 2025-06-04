// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#include <unistd.h>
#include <assert.h>

#include "messenger.h"
#ifdef WITH_RDMA
#include "msgr_rdma.h"
#endif

void osd_client_t::cancel_ops()
{
    std::vector<osd_op_t*> cancel_ops;
    cancel_ops.resize(sent_ops.size());
    int i = 0;
    for (auto p: sent_ops)
    {
        cancel_ops[i++] = p.second;
    }
    sent_ops.clear();
    for (auto op: cancel_ops)
    {
        op->cancel();
    }
}

void osd_op_t::cancel()
{
    if (op_type == OSD_OP_OUT && callback)
    {
        reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
        reply.hdr.id = req.hdr.id;
        reply.hdr.opcode = req.hdr.opcode;
        reply.hdr.retval = -EPIPE;
        // Copy lambda to be unaffected by `delete this`
        (std::function<void(osd_op_t*)>(callback))(this);
    }
    else
    {
        // This function is only called in stop_client(), so it's fine to destroy the operation
        delete this;
    }
}

void osd_messenger_t::stop_client(int peer_fd, bool force, bool force_delete)
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
            fprintf(stderr, "[OSD %ju] Stopping client %d (OSD peer %ju)\n", osd_num, peer_fd, cl->osd_num);
        }
        else
        {
            fprintf(stderr, "[OSD %ju] Stopping client %d (regular client)\n", osd_num, peer_fd);
        }
    }
    // First set state to STOPPED so another stop_client() call doesn't try to free it again
    cl->refs++;
    cl->peer_state = PEER_STOPPED;
    if (cl->osd_num)
    {
        auto osd_it = osd_peer_fds.find(cl->osd_num);
        if (osd_it != osd_peer_fds.end() && osd_it->second == cl->peer_fd)
        {
            // ...and forget OSD peer
            osd_peer_fds.erase(osd_it);
        }
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
    // Find the item again because it can be invalidated at this point
    it = clients.find(peer_fd);
    if (it != clients.end())
    {
        clients.erase(it);
    }
    cl->refs--;
    if (cl->refs <= 0 || force_delete)
    {
        delete cl;
    }
}

osd_client_t::~osd_client_t()
{
    free(in_buf);
    in_buf = NULL;
    if (peer_fd >= 0)
    {
        // Close the FD only when the client is actually destroyed
        // Which only happens when all references are cleared
        close(peer_fd);
        peer_fd = -1;
    }
    // Then cancel all operations
    // Operations have to be canceled only after clearing all references to osd_client_t
    // because otherwise their buffers may be still present in io_uring asynchronous requests
    if (read_op)
    {
        // read_op may be an incoming op or a continued response for an outbound op
        read_op->cancel();
        read_op = NULL;
    }
    // Cancel outbound ops
    cancel_ops();
#ifndef __MOCK__
#ifdef WITH_RDMA
    if (rdma_conn)
    {
        delete rdma_conn;
        rdma_conn = NULL;
    }
#endif
#endif
}
