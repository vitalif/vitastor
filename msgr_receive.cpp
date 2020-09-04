#include "messenger.h"

void osd_messenger_t::read_requests()
{
    for (int i = 0; i < read_ready_clients.size(); i++)
    {
        int peer_fd = read_ready_clients[i];
        auto & cl = clients[peer_fd];
        if (cl.read_remaining < receive_buffer_size)
        {
            cl.read_iov.iov_base = cl.in_buf;
            cl.read_iov.iov_len = receive_buffer_size;
            cl.read_msg.msg_iov = &cl.read_iov;
            cl.read_msg.msg_iovlen = 1;
        }
        else
        {
            cl.read_iov.iov_base = 0;
            cl.read_iov.iov_len = cl.read_remaining;
            cl.read_msg.msg_iov = cl.recv_list.get_iovec();
            cl.read_msg.msg_iovlen = cl.recv_list.get_size();
        }
        if (ringloop && !use_sync_send_recv)
        {
            io_uring_sqe* sqe = ringloop->get_sqe();
            if (!sqe)
            {
                read_ready_clients.erase(read_ready_clients.begin(), read_ready_clients.begin() + i);
                return;
            }
            ring_data_t* data = ((ring_data_t*)sqe->user_data);
            data->callback = [this, peer_fd](ring_data_t *data) { handle_read(data->res, peer_fd); };
            my_uring_prep_recvmsg(sqe, peer_fd, &cl.read_msg, 0);
        }
        else
        {
            int result = recvmsg(peer_fd, &cl.read_msg, 0);
            if (result < 0)
            {
                result = -errno;
            }
            handle_read(result, peer_fd);
        }
    }
    read_ready_clients.clear();
}

bool osd_messenger_t::handle_read(int result, int peer_fd)
{
    bool ret = false;
    auto cl_it = clients.find(peer_fd);
    if (cl_it != clients.end())
    {
        auto & cl = cl_it->second;
        if (result <= 0 && result != -EAGAIN)
        {
            // this is a client socket, so don't panic on error. just disconnect it
            if (result != 0)
            {
                printf("Client %d socket read error: %d (%s). Disconnecting client\n", peer_fd, -result, strerror(-result));
            }
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
                        cl.recv_list.push_back(cl.read_op->req.buf, OSD_PACKET_SIZE);
                        cl.read_remaining = OSD_PACKET_SIZE;
                        cl.read_state = CL_READ_HDR;
                    }
                    while (cl.recv_list.done < cl.recv_list.count && remain > 0)
                    {
                        iovec* cur = cl.recv_list.get_iovec();
                        if (cur->iov_len > remain)
                        {
                            memcpy(cur->iov_base, curbuf, remain);
                            cl.read_remaining -= remain;
                            cur->iov_len -= remain;
                            cur->iov_base += remain;
                            remain = 0;
                        }
                        else
                        {
                            memcpy(cur->iov_base, curbuf, cur->iov_len);
                            curbuf += cur->iov_len;
                            cl.read_remaining -= cur->iov_len;
                            remain -= cur->iov_len;
                            cur->iov_len = 0;
                            cl.recv_list.done++;
                        }
                    }
                    if (cl.recv_list.done >= cl.recv_list.count)
                    {
                        if (!handle_finished_read(cl))
                        {
                            goto fin;
                        }
                    }
                }
            }
            else
            {
                // Long data
                cl.read_remaining -= result;
                cl.recv_list.eat(result);
                if (cl.recv_list.done >= cl.recv_list.count)
                {
                    handle_finished_read(cl);
                }
            }
            if (result >= cl.read_iov.iov_len)
            {
                ret = true;
            }
        }
    }
fin:
    for (auto cb: set_immediate)
    {
        cb();
    }
    set_immediate.clear();
    return ret;
}

bool osd_messenger_t::handle_finished_read(osd_client_t & cl)
{
    cl.recv_list.reset();
    if (cl.read_state == CL_READ_HDR)
    {
        if (cl.read_op->req.hdr.magic == SECONDARY_OSD_REPLY_MAGIC)
            return handle_reply_hdr(&cl);
        else
            handle_op_hdr(&cl);
    }
    else if (cl.read_state == CL_READ_DATA)
    {
        // Operation is ready
        cl.received_ops.push_back(cl.read_op);
        set_immediate.push_back([this, op = cl.read_op]() { exec_op(op); });
        cl.read_op = NULL;
        cl.read_state = 0;
    }
    else if (cl.read_state == CL_READ_REPLY_DATA)
    {
        // Reply is ready
        handle_reply_ready(cl.read_op);
        cl.read_op = NULL;
        cl.read_state = 0;
    }
    else
    {
        assert(0);
    }
    return true;
}

void osd_messenger_t::handle_op_hdr(osd_client_t *cl)
{
    osd_op_t *cur_op = cl->read_op;
    if (cur_op->req.hdr.opcode == OSD_OP_SEC_READ)
    {
        if (cur_op->req.sec_rw.len > 0)
            cur_op->buf = memalign_or_die(MEM_ALIGNMENT, cur_op->req.sec_rw.len);
        cl->read_remaining = 0;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE)
    {
        if (cur_op->req.sec_rw.len > 0)
            cur_op->buf = memalign_or_die(MEM_ALIGNMENT, cur_op->req.sec_rw.len);
        cl->read_remaining = cur_op->req.sec_rw.len;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SEC_STABILIZE ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK)
    {
        if (cur_op->req.sec_stab.len > 0)
            cur_op->buf = memalign_or_die(MEM_ALIGNMENT, cur_op->req.sec_stab.len);
        cl->read_remaining = cur_op->req.sec_stab.len;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_READ)
    {
        if (cur_op->req.rw.len > 0)
            cur_op->buf = memalign_or_die(MEM_ALIGNMENT, cur_op->req.rw.len);
        cl->read_remaining = 0;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_WRITE)
    {
        if (cur_op->req.rw.len > 0)
            cur_op->buf = memalign_or_die(MEM_ALIGNMENT, cur_op->req.rw.len);
        cl->read_remaining = cur_op->req.rw.len;
    }
    if (cl->read_remaining > 0)
    {
        // Read data
        cl->recv_list.push_back(cur_op->buf, cl->read_remaining);
        cl->read_state = CL_READ_DATA;
    }
    else
    {
        // Operation is ready
        cl->received_ops.push_back(cur_op);
        set_immediate.push_back([this, cur_op]() { exec_op(cur_op); });
        cl->read_op = NULL;
        cl->read_state = 0;
    }
}

bool osd_messenger_t::handle_reply_hdr(osd_client_t *cl)
{
    auto req_it = cl->sent_ops.find(cl->read_op->req.hdr.id);
    if (req_it == cl->sent_ops.end())
    {
        // Command out of sync. Drop connection
        printf("Client %d command out of sync: id %lu\n", cl->peer_fd, cl->read_op->req.hdr.id);
        stop_client(cl->peer_fd);
        return false;
    }
    osd_op_t *op = req_it->second;
    memcpy(op->reply.buf, cl->read_op->req.buf, OSD_PACKET_SIZE);
    cl->sent_ops.erase(req_it);
    if ((op->reply.hdr.opcode == OSD_OP_SEC_READ || op->reply.hdr.opcode == OSD_OP_READ) &&
        op->reply.hdr.retval > 0)
    {
        // Read data. In this case we assume that the buffer is preallocated by the caller (!)
        assert(op->iov.count > 0);
        cl->recv_list.append(op->iov);
        delete cl->read_op;
        cl->read_op = op;
        cl->read_state = CL_READ_REPLY_DATA;
        cl->read_remaining = op->reply.hdr.retval;
    }
    else if (op->reply.hdr.opcode == OSD_OP_SEC_LIST && op->reply.hdr.retval > 0)
    {
        assert(!op->iov.count);
        delete cl->read_op;
        cl->read_op = op;
        cl->read_state = CL_READ_REPLY_DATA;
        cl->read_remaining = sizeof(obj_ver_id) * op->reply.hdr.retval;
        op->buf = memalign_or_die(MEM_ALIGNMENT, cl->read_remaining);
        cl->recv_list.push_back(op->buf, cl->read_remaining);
    }
    else if (op->reply.hdr.opcode == OSD_OP_SHOW_CONFIG && op->reply.hdr.retval > 0)
    {
        assert(!op->iov.count);
        delete cl->read_op;
        cl->read_op = op;
        cl->read_state = CL_READ_REPLY_DATA;
        cl->read_remaining = op->reply.hdr.retval;
        op->buf = malloc_or_die(op->reply.hdr.retval);
        cl->recv_list.push_back(op->buf, op->reply.hdr.retval);
    }
    else
    {
        // It's fine to reuse cl->read_op for the next reply
        handle_reply_ready(op);
        cl->recv_list.push_back(cl->read_op->req.buf, OSD_PACKET_SIZE);
        cl->read_remaining = OSD_PACKET_SIZE;
        cl->read_state = CL_READ_HDR;
    }
    return true;
}

void osd_messenger_t::handle_reply_ready(osd_op_t *op)
{
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
    set_immediate.push_back([this, op]()
    {
        // Copy lambda to be unaffected by `delete op`
        std::function<void(osd_op_t*)>(op->callback)(op);
    });
}
