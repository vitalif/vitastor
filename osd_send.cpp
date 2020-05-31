#include "cluster_client.h"

void cluster_client_t::outbox_push(osd_op_t *cur_op)
{
    assert(cur_op->peer_fd);
    auto & cl = clients.at(cur_op->peer_fd);
    if (cur_op->op_type == OSD_OP_OUT)
    {
        clock_gettime(CLOCK_REALTIME, &cur_op->tv_begin);
    }
    else
    {
        // Check that operation actually belongs to this client
        bool found = false;
        for (auto it = cl.received_ops.begin(); it != cl.received_ops.end(); it++)
        {
            if (*it == cur_op)
            {
                found = true;
                cl.received_ops.erase(it, it+1);
                break;
            }
        }
        if (!found)
        {
            delete cur_op;
            return;
        }
    }
    cl.outbox.push_back(cur_op);
    if (cl.write_op || cl.outbox.size() > 1 || !try_send(cl))
    {
        if (cl.write_state == 0)
        {
            cl.write_state = CL_WRITE_READY;
            write_ready_clients.push_back(cur_op->peer_fd);
        }
        ringloop->wakeup();
    }
}

bool cluster_client_t::try_send(osd_client_t & cl)
{
    int peer_fd = cl.peer_fd;
    io_uring_sqe* sqe = ringloop->get_sqe();
    if (!sqe)
    {
        return false;
    }
    ring_data_t* data = ((ring_data_t*)sqe->user_data);
    if (!cl.write_op)
    {
        // pick next command
        cl.write_op = cl.outbox.front();
        cl.outbox.pop_front();
        cl.write_state = CL_WRITE_REPLY;
        if (cl.write_op->op_type == OSD_OP_IN)
        {
            // Measure execution latency
            timespec tv_end;
            clock_gettime(CLOCK_REALTIME, &tv_end);
            stats.op_stat_count[cl.write_op->req.hdr.opcode]++;
            if (!stats.op_stat_count[cl.write_op->req.hdr.opcode])
            {
                stats.op_stat_count[cl.write_op->req.hdr.opcode]++;
                stats.op_stat_sum[cl.write_op->req.hdr.opcode] = 0;
                stats.op_stat_bytes[cl.write_op->req.hdr.opcode] = 0;
            }
            stats.op_stat_sum[cl.write_op->req.hdr.opcode] += (
                (tv_end.tv_sec - cl.write_op->tv_begin.tv_sec)*1000000 +
                (tv_end.tv_nsec - cl.write_op->tv_begin.tv_nsec)/1000
            );
            if (cl.write_op->req.hdr.opcode == OSD_OP_READ ||
                cl.write_op->req.hdr.opcode == OSD_OP_WRITE)
            {
                stats.op_stat_bytes[cl.write_op->req.hdr.opcode] += cl.write_op->req.rw.len;
            }
            else if (cl.write_op->req.hdr.opcode == OSD_OP_SECONDARY_READ ||
                cl.write_op->req.hdr.opcode == OSD_OP_SECONDARY_WRITE)
            {
                stats.op_stat_bytes[cl.write_op->req.hdr.opcode] += cl.write_op->req.sec_rw.len;
            }
        }
    }
    cl.write_msg.msg_iov = cl.write_op->send_list.get_iovec();
    cl.write_msg.msg_iovlen = cl.write_op->send_list.get_size();
    data->callback = [this, peer_fd](ring_data_t *data) { handle_send(data->res, peer_fd); };
    my_uring_prep_sendmsg(sqe, peer_fd, &cl.write_msg, 0);
    return true;
}

void cluster_client_t::send_replies()
{
    for (int i = 0; i < write_ready_clients.size(); i++)
    {
        int peer_fd = write_ready_clients[i];
        if (!try_send(clients[peer_fd]))
        {
            write_ready_clients.erase(write_ready_clients.begin(), write_ready_clients.begin() + i);
            return;
        }
    }
    write_ready_clients.clear();
}

void cluster_client_t::handle_send(int result, int peer_fd)
{
    auto cl_it = clients.find(peer_fd);
    if (cl_it != clients.end())
    {
        auto & cl = cl_it->second;
        if (result < 0 && result != -EAGAIN)
        {
            // this is a client socket, so don't panic. just disconnect it
            printf("Client %d socket write error: %d (%s). Disconnecting client\n", peer_fd, -result, strerror(-result));
            stop_client(peer_fd);
            return;
        }
        if (result >= 0)
        {
            osd_op_t *cur_op = cl.write_op;
            while (result > 0 && cur_op->send_list.sent < cur_op->send_list.count)
            {
                iovec & iov = cur_op->send_list.buf[cur_op->send_list.sent];
                if (iov.iov_len <= result)
                {
                    result -= iov.iov_len;
                    cur_op->send_list.sent++;
                }
                else
                {
                    iov.iov_len -= result;
                    iov.iov_base += result;
                    break;
                }
            }
            if (cur_op->send_list.sent >= cur_op->send_list.count)
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
        if (cl.write_state != 0)
        {
            write_ready_clients.push_back(peer_fd);
        }
    }
}
