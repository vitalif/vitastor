#include "osd.h"

void osd_t::outbox_push(osd_client_t & cl, osd_op_t *cur_op)
{
    assert(cur_op->peer_fd);
    if (cur_op->op_type == OSD_OP_OUT)
    {
        gettimeofday(&cur_op->tv_begin, NULL);
    }
    if (cl.write_state == 0)
    {
        cl.write_state = CL_WRITE_READY;
        write_ready_clients.push_back(cur_op->peer_fd);
    }
    cl.outbox.push_back(cur_op);
    ringloop->wakeup();
}

void osd_t::send_replies()
{
    for (int i = 0; i < write_ready_clients.size(); i++)
    {
        int peer_fd = write_ready_clients[i];
        auto & cl = clients[peer_fd];
        io_uring_sqe* sqe = ringloop->get_sqe();
        if (!sqe)
        {
            write_ready_clients.erase(write_ready_clients.begin(), write_ready_clients.begin() + i);
            return;
        }
        ring_data_t* data = ((ring_data_t*)sqe->user_data);
        if (!cl.write_buf)
        {
            // pick next command
            cl.write_op = cl.outbox.front();
            cl.outbox.pop_front();
            if (cl.write_op->op_type == OSD_OP_OUT)
            {
                cl.write_buf = &cl.write_op->req.buf;
                cl.write_remaining = OSD_PACKET_SIZE;
                cl.write_state = CL_WRITE_REPLY;
            }
            else
            {
                // Measure execution latency
                timeval tv_end;
                gettimeofday(&tv_end, NULL);
                op_stat_count[cl.write_op->req.hdr.opcode]++;
                op_stat_sum[cl.write_op->req.hdr.opcode] += (
                    (tv_end.tv_sec - cl.write_op->tv_begin.tv_sec)*1000000 +
                    tv_end.tv_usec - cl.write_op->tv_begin.tv_usec
                );
                cl.write_buf = &cl.write_op->reply.buf;
                cl.write_remaining = OSD_PACKET_SIZE;
                cl.write_state = CL_WRITE_REPLY;
            }
        }
        cl.write_iov.iov_base = cl.write_buf;
        cl.write_iov.iov_len = cl.write_remaining;
        cl.write_msg.msg_iov = &cl.write_iov;
        cl.write_msg.msg_iovlen = 1;
        // FIXME: This is basically a busy-loop. It may be better to add epoll here(?)
        data->callback = [this, peer_fd](ring_data_t *data) { handle_send(data, peer_fd); };
        my_uring_prep_sendmsg(sqe, peer_fd, &cl.write_msg, 0);
        cl.write_state = cl.write_state | SQE_SENT;
    }
    write_ready_clients.clear();
}

void osd_t::handle_send(ring_data_t *data, int peer_fd)
{
    auto cl_it = clients.find(peer_fd);
    if (cl_it != clients.end())
    {
        auto & cl = cl_it->second;
        if (data->res < 0 && data->res != -EAGAIN)
        {
            // this is a client socket, so don't panic. just disconnect it
            printf("Client %d socket write error: %d (%s). Disconnecting client\n", peer_fd, -data->res, strerror(-data->res));
            stop_client(peer_fd);
            return;
        }
        cl.write_state = cl.write_state & ~SQE_SENT;
        if (data->res > 0)
        {
            cl.write_remaining -= data->res;
            cl.write_buf += data->res;
            if (cl.write_remaining <= 0)
            {
                cl.write_buf = NULL;
                osd_op_t *cur_op = cl.write_op;
                if (cur_op->send_list.sent < cur_op->send_list.count)
                {
                    // Send data
                    cl.write_buf = cur_op->send_list[cur_op->send_list.sent].buf;
                    assert(cl.write_buf);
                    cl.write_remaining = cur_op->send_list[cur_op->send_list.sent].len;
                    cur_op->send_list.sent++;
                    cl.write_state = CL_WRITE_DATA;
                }
                else
                {
                    // Done
                    if (cur_op->op_type == OSD_OP_IN)
                    {
                        delete cur_op;
                    }
                    else
                    {
                        cl.sent_ops[cl.write_op->req.hdr.id] = cl.write_op;
                    }
                    cl.write_op = NULL;
                    cl.write_state = cl.outbox.size() > 0 ? CL_WRITE_READY : 0;
                }
            }
        }
        if (cl.write_state != 0)
        {
            write_ready_clients.push_back(peer_fd);
        }
    }
}
