#include "cluster_client.h"

void cluster_client_t::read_requests()
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
        if (!cl.read_op || cl.read_remaining < receive_buffer_size)
        {
            cl.read_iov.iov_base = cl.in_buf;
            cl.read_iov.iov_len = receive_buffer_size;
        }
        else
        {
            cl.read_iov.iov_base = cl.read_buf;
            cl.read_iov.iov_len = cl.read_remaining;
        }
        cl.read_msg.msg_iov = &cl.read_iov;
        cl.read_msg.msg_iovlen = 1;
        data->callback = [this, peer_fd](ring_data_t *data) { handle_read(data->res, peer_fd); };
        my_uring_prep_recvmsg(sqe, peer_fd, &cl.read_msg, 0);
    }
    read_ready_clients.clear();
}

bool cluster_client_t::handle_read(int result, int peer_fd)
{
    auto cl_it = clients.find(peer_fd);
    if (cl_it != clients.end())
    {
        auto & cl = cl_it->second;
        if (result < 0 && result != -EAGAIN)
        {
            // this is a client socket, so don't panic. just disconnect it
            printf("Client %d socket read error: %d (%s). Disconnecting client\n", peer_fd, -result, strerror(-result));
            stop_client(peer_fd);
            return false;
        }
        if (result == -EAGAIN || result < cl.read_iov.iov_len)
        {
            cl.read_ready--;
            if (cl.read_ready > 0)
                read_ready_clients.push_back(peer_fd);
        }
        else
        {
            read_ready_clients.push_back(peer_fd);
        }
        if (result > 0)
        {
            if (cl.read_iov.iov_base == cl.in_buf)
            {
                // Compose operation(s) from the buffer
                int remain = result;
                void *curbuf = cl.in_buf;
                while (remain > 0)
                {
                    if (!cl.read_op)
                    {
                        cl.read_op = new osd_op_t;
                        cl.read_op->peer_fd = peer_fd;
                        cl.read_op->op_type = OSD_OP_IN;
                        cl.read_buf = cl.read_op->req.buf;
                        cl.read_remaining = OSD_PACKET_SIZE;
                        cl.read_state = CL_READ_HDR;
                    }
                    if (cl.read_remaining > remain)
                    {
                        memcpy(cl.read_buf, curbuf, remain);
                        cl.read_remaining -= remain;
                        cl.read_buf += remain;
                        remain = 0;
                        if (cl.read_remaining <= 0)
                            handle_finished_read(cl);
                    }
                    else
                    {
                        memcpy(cl.read_buf, curbuf, cl.read_remaining);
                        curbuf += cl.read_remaining;
                        remain -= cl.read_remaining;
                        cl.read_remaining = 0;
                        cl.read_buf = NULL;
                        handle_finished_read(cl);
                    }
                }
            }
            else
            {
                // Long data
                cl.read_remaining -= result;
                cl.read_buf += result;
                if (cl.read_remaining <= 0)
                {
                    handle_finished_read(cl);
                }
            }
            if (result >= cl.read_iov.iov_len)
            {
                return true;
            }
        }
    }
    return false;
}

void cluster_client_t::handle_finished_read(osd_client_t & cl)
{
    if (cl.read_state == CL_READ_HDR)
    {
        if (cl.read_op->req.hdr.magic == SECONDARY_OSD_REPLY_MAGIC)
            handle_reply_hdr(&cl);
        else
            handle_op_hdr(&cl);
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
        delete cl.read_op;
        cl.read_op = NULL;
        cl.read_state = 0;
        // Measure subop latency
        timespec tv_end;
        clock_gettime(CLOCK_REALTIME, &tv_end);
        stats.subop_stat_count[request->req.hdr.opcode]++;
        if (!stats.subop_stat_count[request->req.hdr.opcode])
        {
            stats.subop_stat_count[request->req.hdr.opcode]++;
            stats.subop_stat_sum[request->req.hdr.opcode] = 0;
        }
        stats.subop_stat_sum[request->req.hdr.opcode] += (
            (tv_end.tv_sec - request->tv_begin.tv_sec)*1000000 +
            (tv_end.tv_nsec - request->tv_begin.tv_nsec)/1000
        );
        request->callback(request);
    }
    else
    {
        assert(0);
    }
}

void cluster_client_t::handle_op_hdr(osd_client_t *cl)
{
    osd_op_t *cur_op = cl->read_op;
    if (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_READ)
    {
        if (cur_op->req.sec_rw.len > 0)
            cur_op->buf = memalign(MEM_ALIGNMENT, cur_op->req.sec_rw.len);
        cl->read_remaining = 0;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_WRITE)
    {
        if (cur_op->req.sec_rw.len > 0)
            cur_op->buf = memalign(MEM_ALIGNMENT, cur_op->req.sec_rw.len);
        cl->read_remaining = cur_op->req.sec_rw.len;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SECONDARY_STABILIZE ||
        cur_op->req.hdr.opcode == OSD_OP_SECONDARY_ROLLBACK)
    {
        if (cur_op->req.sec_stab.len > 0)
            cur_op->buf = memalign(MEM_ALIGNMENT, cur_op->req.sec_stab.len);
        cl->read_remaining = cur_op->req.sec_stab.len;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_READ)
    {
        if (cur_op->req.rw.len > 0)
            cur_op->buf = memalign(MEM_ALIGNMENT, cur_op->req.rw.len);
        cl->read_remaining = 0;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_WRITE)
    {
        if (cur_op->req.rw.len > 0)
            cur_op->buf = memalign(MEM_ALIGNMENT, cur_op->req.rw.len);
        cl->read_remaining = cur_op->req.rw.len;
    }
    if (cl->read_remaining > 0)
    {
        // Read data
        cl->read_buf = cur_op->buf;
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

void cluster_client_t::handle_reply_hdr(osd_client_t *cl)
{
    osd_op_t *cur_op = cl->read_op;
    auto req_it = cl->sent_ops.find(cur_op->req.hdr.id);
    if (req_it == cl->sent_ops.end())
    {
        // Command out of sync. Drop connection
        printf("Client %d command out of sync: id %lu\n", cl->peer_fd, cur_op->req.hdr.id);
        stop_client(cl->peer_fd);
        return;
    }
    osd_op_t *op = req_it->second;
    memcpy(op->reply.buf, cur_op->req.buf, OSD_PACKET_SIZE);
    if (op->reply.hdr.opcode == OSD_OP_SECONDARY_READ &&
        op->reply.hdr.retval > 0)
    {
        // Read data. In this case we assume that the buffer is preallocated by the caller (!)
        assert(op->buf);
        cl->read_state = CL_READ_REPLY_DATA;
        cl->read_reply_id = op->req.hdr.id;
        cl->read_buf = op->buf;
        cl->read_remaining = op->reply.hdr.retval;
    }
    else if (op->reply.hdr.opcode == OSD_OP_SECONDARY_LIST &&
        op->reply.hdr.retval > 0)
    {
        op->buf = memalign(MEM_ALIGNMENT, sizeof(obj_ver_id) * op->reply.hdr.retval);
        cl->read_state = CL_READ_REPLY_DATA;
        cl->read_reply_id = op->req.hdr.id;
        cl->read_buf = op->buf;
        cl->read_remaining = sizeof(obj_ver_id) * op->reply.hdr.retval;
    }
    else if (op->reply.hdr.opcode == OSD_OP_SHOW_CONFIG &&
        op->reply.hdr.retval > 0)
    {
        op->buf = malloc(op->reply.hdr.retval);
        cl->read_state = CL_READ_REPLY_DATA;
        cl->read_reply_id = op->req.hdr.id;
        cl->read_buf = op->buf;
        cl->read_remaining = op->reply.hdr.retval;
    }
    else
    {
        delete cl->read_op;
        cl->read_state = 0;
        cl->read_op = NULL;
        cl->sent_ops.erase(req_it);
        // Measure subop latency
        timespec tv_end;
        clock_gettime(CLOCK_REALTIME, &tv_end);
        stats.subop_stat_count[op->req.hdr.opcode]++;
        if (!stats.subop_stat_count[op->req.hdr.opcode])
        {
            stats.subop_stat_count[op->req.hdr.opcode]++;
            stats.subop_stat_sum[op->req.hdr.opcode] = 0;
        }
        stats.subop_stat_sum[op->req.hdr.opcode] += (
            (tv_end.tv_sec - op->tv_begin.tv_sec)*1000000 +
            (tv_end.tv_nsec - op->tv_begin.tv_nsec)/1000
        );
        // Copy lambda to be unaffected by `delete op`
        std::function<void(osd_op_t*)>(op->callback)(op);
    }
}
