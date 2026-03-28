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

// force_delete means stop the client anyway, even if there are refs to it in the event loop.
// the flag should be used in the destructor.
// why? - because yes, we could close the FD first and let it fail all requests in the event loop,
// but in that case it can be quickly reopened and we can get old failed responses for the new FD.
void osd_messenger_t::stop_client(uint64_t client_id, bool force_delete)
{
    auto it = clients.find(client_id);
    if (!client_id || it == clients.end())
    {
        return;
    }
    osd_client_t *cl = it->second;
    if (cl->peer_state == PEER_STOPPED)
    {
        if (force_delete)
        {
            destroy_client(cl);
        }
        return;
    }
    cl->received_ops.clear();
    if (log_level > 0)
    {
        if (cl->osd_num)
        {
            fprintf(stderr, "[OSD %ju] Stopping client %ju (OSD peer %ju)\n", osd_num, client_id, cl->osd_num);
        }
        else if (cl->in_osd_num)
        {
            fprintf(stderr, "[OSD %ju] Stopping client %ju (incoming OSD peer %ju)\n", osd_num, client_id, cl->in_osd_num);
        }
        else
        {
            fprintf(stderr, "[OSD %ju] Stopping client %ju (regular client)\n", osd_num, client_id);
        }
    }
    // First set state to STOPPED so another stop_client() call doesn't try to free it again
    cl->refs++;
    int prev_state = cl->peer_state;
    cl->peer_state = PEER_STOPPED;
    if (cl->osd_num)
    {
        auto osd_it = osd_peers.find(cl->osd_num);
        if (osd_it != osd_peers.end() && osd_it->second == cl)
        {
            // ...and forget OSD peer
            osd_peers.erase(osd_it);
        }
    }
#ifdef WITH_RDMA
    if (cl->rdma_conn && cl->rdma_conn->cmid)
    {
        auto rdma_it = rdmacm_connections.find(cl->rdma_conn->cmid);
        if (rdma_it != rdmacm_connections.end() && rdma_it->second == cl)
        {
            rdmacm_connections.erase(rdma_it);
        }
    }
#endif
#ifndef __MOCK__
    if (cl->connect_timeout_id >= 0)
    {
        tfd->clear_timer(cl->connect_timeout_id);
        cl->connect_timeout_id = -1;
    }
#endif
    if (cl->in_osd_num && break_pg_locks)
    {
        // Break PG locks
        break_pg_locks(cl->in_osd_num);
    }
    if (cl->osd_num && prev_state != PEER_CONNECTING)
    {
        // Then repeer PGs because cancel_op() callbacks can try to perform
        // some actions and we need correct PG states to not do something silly
        // PEER_CONNECTING has neither 'just dropped the connection' nor 'just connected'
        // so do not repeer on it.
        repeer_pgs(cl->osd_num);
    }
    if (cl->peer_fd >= 0)
    {
        int r = shutdown(cl->peer_fd, SHUT_RDWR);
        if (r != 0 && errno != ENOTCONN)
        {
            fprintf(stderr, "[OSD %ju] failed to shutdown a socket: %s (code %d)\n", osd_num, strerror(errno), errno);
        }
    }
    cl->refs--;
    if (cl->refs <= 0 || force_delete)
    {
        destroy_client(cl);
    }
}

void osd_messenger_t::destroy_client(osd_client_t *cl)
{
    // Find the item again because it can be invalidated at this point
    clients.erase(cl->client_id);
    if (cl->peer_fd >= 0)
    {
#ifndef __MOCK__
        tfd->set_fd_handler(cl->peer_fd, false, NULL);
#endif
        for (auto rit = read_ready_clients.begin(); rit != read_ready_clients.end(); rit++)
        {
            if (*rit == cl->client_id)
            {
                read_ready_clients.erase(rit);
                break;
            }
        }
        for (auto wit = write_ready_clients.begin(); wit != write_ready_clients.end(); wit++)
        {
            if (*wit == cl->client_id)
            {
                write_ready_clients.erase(wit);
                break;
            }
        }
        clients_by_fd.erase(cl->peer_fd);
    }
    delete cl;
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
    for (osd_op_t *op: zc_free_list)
    {
        if (op)
        {
            delete op;
        }
    }
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
