#include "osd.h"

void osd_t::read_requests()
{
    for (int i = 0; i < read_ready_clients.size(); i++)
    {
        int peer_fd = read_ready_clients[i];
        auto & cl = clients[peer_fd];
        io_uring_sqe* sqe = ringloop->get_sqe();
        if (!sqe)
        {
            read_ready_clients.erase(read_ready_clients.begin(), read_ready_clients.begin() + i);
            return;
        }
        ring_data_t* data = ((ring_data_t*)sqe->user_data);
        if (!cl.read_buf)
        {
            // no reads in progress
            // so this is either a new command or a reply to a previously sent command
            if (!cl.read_op)
            {
                cl.read_op = new osd_op_t;
                cl.read_op->peer_fd = peer_fd;
            }
            cl.read_op->op_type = OSD_OP_IN;
            cl.read_buf = &cl.read_op->op_buf;
            cl.read_remaining = OSD_PACKET_SIZE;
            cl.read_state = CL_READ_OP;
        }
        cl.read_iov.iov_base = cl.read_buf;
        cl.read_iov.iov_len = cl.read_remaining;
        cl.read_msg.msg_iov = &cl.read_iov;
        cl.read_msg.msg_iovlen = 1;
        data->callback = [this, peer_fd](ring_data_t *data) { handle_read(data, peer_fd); };
        my_uring_prep_recvmsg(sqe, peer_fd, &cl.read_msg, 0);
        cl.reading = true;
        cl.read_ready = false;
    }
    read_ready_clients.clear();
}

void osd_t::handle_read(ring_data_t *data, int peer_fd)
{
    auto cl_it = clients.find(peer_fd);
    if (cl_it != clients.end())
    {
        auto & cl = cl_it->second;
        if (data->res < 0 && data->res != -EAGAIN)
        {
            // this is a client socket, so don't panic. just disconnect it
            printf("Client %d socket read error: %d (%s). Disconnecting client\n", peer_fd, -data->res, strerror(-data->res));
            stop_client(peer_fd);
            return;
        }
        cl.reading = false;
        if (cl.read_ready)
        {
            read_ready_clients.push_back(peer_fd);
        }
        if (data->res > 0)
        {
            cl.read_remaining -= data->res;
            cl.read_buf += data->res;
            if (cl.read_remaining <= 0)
            {
                cl.read_buf = NULL;
                if (cl.read_state == CL_READ_OP)
                {
                    if (cl.read_op->op.hdr.magic == SECONDARY_OSD_REPLY_MAGIC)
                    {
                        handle_read_reply(&cl);
                    }
                    else
                    {
                        handle_read_op(&cl);
                    }
                }
                else if (cl.read_state == CL_READ_DATA)
                {
                    // Operation is ready
                    exec_op(cl.read_op);
                    cl.read_op = NULL;
                    cl.read_state = 0;
                }
                else if (cl.read_state == CL_READ_REPLY_DATA)
                {
                    // Reply is ready
                    auto req_it = cl.sent_ops.find(cl.read_reply_id);
                    osd_op_t *request = req_it->second;
                    cl.sent_ops.erase(req_it);
                    cl.read_reply_id = 0;
                    cl.read_state = 0;
                    request->callback(request);
                }
            }
        }
    }
}

void osd_t::handle_read_op(osd_client_t *cl)
{
    osd_op_t *cur_op = cl->read_op;
    if (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_READ ||
        cur_op->op.hdr.opcode == OSD_OP_SECONDARY_WRITE ||
        cur_op->op.hdr.opcode == OSD_OP_SECONDARY_STABILIZE ||
        cur_op->op.hdr.opcode == OSD_OP_SECONDARY_ROLLBACK)
    {
        // Allocate a buffer
        cur_op->buf = memalign(512, cur_op->op.sec_rw.len);
    }
    else if (cur_op->op.hdr.opcode == OSD_OP_READ ||
        cur_op->op.hdr.opcode == OSD_OP_WRITE)
    {
        cur_op->buf = memalign(512, cur_op->op.rw.len);
    }
    if (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_WRITE ||
        cur_op->op.hdr.opcode == OSD_OP_SECONDARY_STABILIZE ||
        cur_op->op.hdr.opcode == OSD_OP_SECONDARY_ROLLBACK ||
        cur_op->op.hdr.opcode == OSD_OP_WRITE)
    {
        // Read data
        cl->read_buf = cur_op->buf;
        cl->read_remaining = (cur_op->op.hdr.opcode == OSD_OP_SECONDARY_WRITE
            ? cur_op->op.sec_rw.len
            : cur_op->op.rw.len);
        cl->read_state = CL_READ_DATA;
    }
    else
    {
        // Operation is ready
        cl->read_op = NULL;
        cl->read_state = 0;
        exec_op(cur_op);
    }
}

void osd_t::handle_read_reply(osd_client_t *cl)
{
    osd_op_t *cur_op = cl->read_op;
    auto req_it = cl->sent_ops.find(cur_op->op.hdr.id);
    if (req_it == cl->sent_ops.end())
    {
        // Command out of sync. Drop connection
        // FIXME This is probably a peer, so handle all previously sent operations carefully
        stop_client(cl->peer_fd);
        return;
    }
    osd_op_t *request = req_it->second;
    memcpy(request->reply_buf, cur_op->op_buf, OSD_PACKET_SIZE);
    if (request->reply.hdr.opcode == OSD_OP_SECONDARY_READ &&
        request->reply.hdr.retval > 0)
    {
        // Read data
        // FIXME: request->buf must be allocated
        cl->read_state = CL_READ_REPLY_DATA;
        cl->read_reply_id = request->op.hdr.id;
        cl->read_buf = request->buf;
        cl->read_remaining = request->reply.hdr.retval;
    }
    else if (request->reply.hdr.opcode == OSD_OP_SECONDARY_LIST &&
        request->reply.hdr.retval > 0)
    {
        request->buf = memalign(512, sizeof(obj_ver_id) * request->reply.hdr.retval);
        cl->read_state = CL_READ_REPLY_DATA;
        cl->read_reply_id = request->op.hdr.id;
        cl->read_buf = request->buf;
        cl->read_remaining = sizeof(obj_ver_id) * request->reply.hdr.retval;
    }
    else
    {
        cl->read_state = 0;
        cl->sent_ops.erase(req_it);
        request->callback(request);
    }
}
