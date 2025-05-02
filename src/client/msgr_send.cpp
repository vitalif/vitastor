// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#define _XOPEN_SOURCE
#include <limits.h>
#include <sys/epoll.h>

#include "messenger.h"

void osd_messenger_t::outbox_push(osd_op_t *cur_op)
{
    assert(cur_op->peer_fd);
    osd_client_t *cl = clients.at(cur_op->peer_fd);
    if (cur_op->op_type == OSD_OP_OUT)
    {
        clock_gettime(CLOCK_REALTIME, &cur_op->tv_begin);
    }
    else
    {
        // Check that operation actually belongs to this client
        // FIXME: Review if this is still needed
        bool found = false;
        for (auto it = cl->received_ops.begin(); it != cl->received_ops.end(); it++)
        {
            if (*it == cur_op)
            {
                found = true;
                cl->received_ops.erase(it, it+1);
                break;
            }
        }
        if (!found)
        {
            delete cur_op;
            return;
        }
    }
    auto & to_send_list = cl->write_msg.msg_iovlen ? cl->next_send_list : cl->send_list;
    auto & to_outbox = cl->write_msg.msg_iovlen ? cl->next_outbox : cl->outbox;
    if (cur_op->op_type == OSD_OP_IN)
    {
        measure_exec(cur_op);
        to_send_list.push_back((iovec){ .iov_base = cur_op->reply.buf, .iov_len = OSD_PACKET_SIZE });
    }
    else
    {
        to_send_list.push_back((iovec){ .iov_base = cur_op->req.buf, .iov_len = OSD_PACKET_SIZE });
        cl->sent_ops[cur_op->req.hdr.id] = cur_op;
    }
    to_outbox.push_back((msgr_sendp_t){ .op = cur_op, .flags = MSGR_SENDP_HDR });
    // Bitmap
    if (cur_op->op_type == OSD_OP_IN &&
        cur_op->req.hdr.opcode == OSD_OP_SEC_READ &&
        cur_op->reply.sec_rw.attr_len > 0)
    {
        to_send_list.push_back((iovec){
            .iov_base = cur_op->bitmap,
            .iov_len = cur_op->reply.sec_rw.attr_len,
        });
        to_outbox.push_back((msgr_sendp_t){ .op = cur_op, .flags = 0 });
    }
    else if (cur_op->op_type == OSD_OP_OUT &&
        (cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE || cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE) &&
        cur_op->req.sec_rw.attr_len > 0)
    {
        to_send_list.push_back((iovec){
            .iov_base = cur_op->bitmap,
            .iov_len = cur_op->req.sec_rw.attr_len,
        });
        to_outbox.push_back((msgr_sendp_t){ .op = cur_op, .flags = 0 });
    }
    // Operation data
    if ((cur_op->op_type == OSD_OP_IN
        ? (cur_op->req.hdr.opcode == OSD_OP_READ ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_READ ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_LIST ||
        cur_op->req.hdr.opcode == OSD_OP_SHOW_CONFIG ||
        cur_op->req.hdr.opcode == OSD_OP_DESCRIBE)
        : (cur_op->req.hdr.opcode == OSD_OP_WRITE ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_STABILIZE ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_ROLLBACK ||
        cur_op->req.hdr.opcode == OSD_OP_SHOW_CONFIG)) && cur_op->iov.count > 0)
    {
        for (int i = 0; i < cur_op->iov.count; i++)
        {
            if (cur_op->iov.buf[i].iov_len > 0)
            {
                assert(cur_op->iov.buf[i].iov_base);
                to_send_list.push_back(cur_op->iov.buf[i]);
                to_outbox.push_back((msgr_sendp_t){ .op = cur_op, .flags = 0 });
            }
        }
    }
    if (cur_op->req.hdr.opcode == OSD_OP_SEC_READ_BMP)
    {
        if (cur_op->op_type == OSD_OP_IN && cur_op->reply.hdr.retval > 0)
            to_send_list.push_back((iovec){ .iov_base = cur_op->buf, .iov_len = (size_t)cur_op->reply.hdr.retval });
        else if (cur_op->op_type == OSD_OP_OUT && cur_op->req.sec_read_bmp.len > 0)
            to_send_list.push_back((iovec){ .iov_base = cur_op->buf, .iov_len = (size_t)cur_op->req.sec_read_bmp.len });
        to_outbox.push_back((msgr_sendp_t){ .op = cur_op, .flags = 0 });
    }
    if (cur_op->op_type == OSD_OP_IN)
    {
        to_outbox[to_outbox.size()-1].flags |= MSGR_SENDP_FREE;
    }
#ifdef WITH_RDMA
    if (cl->peer_state == PEER_RDMA)
    {
        try_send_rdma(cl);
        return;
    }
#endif
    if (!ringloop)
    {
        // FIXME: It's worse because it doesn't allow batching
        while (cl->outbox.size())
        {
            try_send(cl);
        }
    }
    else
    {
        if ((cl->write_msg.msg_iovlen > 0 || !try_send(cl)) && (cl->write_state == 0))
        {
            cl->write_state = CL_WRITE_READY;
            write_ready_clients.push_back(cur_op->peer_fd);
        }
        ringloop->wakeup();
    }
}

void osd_messenger_t::inc_op_stats(osd_op_stats_t & stats, uint64_t opcode, timespec & tv_begin, timespec & tv_end, uint64_t len)
{
    uint64_t usecs = (
        (tv_end.tv_sec - tv_begin.tv_sec)*1000000 +
        (tv_end.tv_nsec - tv_begin.tv_nsec)/1000
    );
    stats.op_stat_count[opcode]++;
    if (!stats.op_stat_count[opcode])
    {
        stats.op_stat_count[opcode] = 1;
        stats.op_stat_sum[opcode] = 0;
        stats.op_stat_bytes[opcode] = 0;
    }
    stats.op_stat_sum[opcode] += usecs;
    stats.op_stat_bytes[opcode] += len;
}

void osd_messenger_t::measure_exec(osd_op_t *cur_op)
{
    // Measure execution latency
    if (cur_op->req.hdr.opcode > OSD_OP_MAX)
    {
        return;
    }
    if (!cur_op->tv_end.tv_sec)
    {
        clock_gettime(CLOCK_REALTIME, &cur_op->tv_end);
    }
    uint64_t len = 0;
    if (cur_op->req.hdr.opcode == OSD_OP_READ ||
        cur_op->req.hdr.opcode == OSD_OP_WRITE ||
        cur_op->req.hdr.opcode == OSD_OP_SCRUB)
    {
        // req.rw.len is internally set to the full object size for scrubs
        len = cur_op->req.rw.len;
    }
    else if (cur_op->req.hdr.opcode == OSD_OP_SEC_READ ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE ||
        cur_op->req.hdr.opcode == OSD_OP_SEC_WRITE_STABLE)
    {
        len = cur_op->req.sec_rw.len;
    }
    inc_op_stats(stats, cur_op->req.hdr.opcode, cur_op->tv_begin, cur_op->tv_end, len);
    if (cur_op->is_recovery_related())
    {
        inc_op_stats(recovery_stats, cur_op->req.hdr.opcode, cur_op->tv_begin, cur_op->tv_end, len);
    }
}

bool osd_messenger_t::try_send(osd_client_t *cl)
{
    int peer_fd = cl->peer_fd;
    if (!cl->send_list.size() || cl->write_msg.msg_iovlen > 0)
    {
        return true;
    }
    assert(cl->peer_state != PEER_RDMA);
    if (ringloop && !use_sync_send_recv)
    {
        auto iothread = iothreads.size() ? iothreads[peer_fd % iothreads.size()] : NULL;
        io_uring_sqe sqe_local;
        ring_data_t data_local;
        sqe_local.user_data = (uint64_t)&data_local;
        io_uring_sqe* sqe = (iothread ? &sqe_local : ringloop->get_sqe());
        if (!sqe)
        {
            return false;
        }
        cl->write_msg.msg_iov = cl->send_list.data();
        cl->write_msg.msg_iovlen = cl->send_list.size() < IOV_MAX ? cl->send_list.size() : IOV_MAX;
        cl->refs++;
        ring_data_t* data = ((ring_data_t*)sqe->user_data);
        data->callback = [this, cl](ring_data_t *data) { handle_send(data->res, data->prev, data->more, cl); };
        bool use_zc = has_sendmsg_zc && min_zerocopy_send_size >= 0;
        if (use_zc && min_zerocopy_send_size > 0)
        {
            size_t avg_size = 0;
            for (size_t i = 0; i < cl->write_msg.msg_iovlen; i++)
                avg_size += cl->write_msg.msg_iov[i].iov_len;
            if (avg_size/cl->write_msg.msg_iovlen < min_zerocopy_send_size)
                use_zc = false;
        }
        if (use_zc)
        {
            my_uring_prep_sendmsg_zc(sqe, peer_fd, &cl->write_msg, MSG_WAITALL);
        }
        else
        {
            my_uring_prep_sendmsg(sqe, peer_fd, &cl->write_msg, MSG_WAITALL);
        }
        if (iothread)
        {
            iothread->add_sqe(sqe_local);
        }
    }
    else
    {
        cl->write_msg.msg_iov = cl->send_list.data();
        cl->write_msg.msg_iovlen = cl->send_list.size() < IOV_MAX ? cl->send_list.size() : IOV_MAX;
        cl->refs++;
        int result = sendmsg(peer_fd, &cl->write_msg, MSG_NOSIGNAL);
        if (result < 0)
        {
            result = -errno;
        }
        handle_send(result, false, false, cl);
    }
    return true;
}

void osd_messenger_t::send_replies()
{
    for (int i = 0; i < write_ready_clients.size(); i++)
    {
        int peer_fd = write_ready_clients[i];
        auto cl_it = clients.find(peer_fd);
        if (cl_it != clients.end() && !try_send(cl_it->second))
        {
            write_ready_clients.erase(write_ready_clients.begin(), write_ready_clients.begin() + i);
            return;
        }
    }
    write_ready_clients.clear();
}

void osd_messenger_t::handle_send(int result, bool prev, bool more, osd_client_t *cl)
{
    if (!prev)
    {
        cl->write_msg.msg_iovlen = 0;
    }
    if (!more)
    {
        cl->refs--;
    }
    if (cl->peer_state == PEER_STOPPED)
    {
        if (cl->refs <= 0)
        {
            delete cl;
        }
        return;
    }
    if (result < 0 && result != -EAGAIN && result != -EINTR)
    {
        // this is a client socket, so don't panic. just disconnect it
        fprintf(stderr, "Client %d socket write error: %d (%s). Disconnecting client\n", cl->peer_fd, -result, strerror(-result));
        stop_client(cl->peer_fd);
        return;
    }
    if (result >= 0)
    {
        if (prev)
        {
            // Second notification - only free a batch of postponed ops
            int i = 0;
            for (; i < cl->zc_free_list.size() && cl->zc_free_list[i]; i++)
                delete cl->zc_free_list[i];
            if (i > 0)
                cl->zc_free_list.erase(cl->zc_free_list.begin(), cl->zc_free_list.begin()+i+1);
            return;
        }
        int done = 0;
        while (result > 0 && done < cl->send_list.size())
        {
            iovec & iov = cl->send_list[done];
            if (iov.iov_len <= result)
            {
                if (cl->outbox[done].flags & MSGR_SENDP_FREE)
                {
                    // Reply fully sent
                    if (more)
                        cl->zc_free_list.push_back(cl->outbox[done].op);
                    else
                        delete cl->outbox[done].op;
                }
                result -= iov.iov_len;
                done++;
            }
            else
            {
                iov.iov_len -= result;
                iov.iov_base = (uint8_t*)iov.iov_base + result;
                break;
            }
        }
        if (more)
        {
            auto expected = cl->send_list.size() < IOV_MAX ? cl->send_list.size() : IOV_MAX;
            assert(done == expected);
            cl->zc_free_list.push_back(NULL); // end marker
        }
        if (done > 0)
        {
            cl->send_list.erase(cl->send_list.begin(), cl->send_list.begin()+done);
            cl->outbox.erase(cl->outbox.begin(), cl->outbox.begin()+done);
        }
        if (cl->next_send_list.size())
        {
            cl->send_list.insert(cl->send_list.end(), cl->next_send_list.begin(), cl->next_send_list.end());
            cl->outbox.insert(cl->outbox.end(), cl->next_outbox.begin(), cl->next_outbox.end());
            cl->next_send_list.clear();
            cl->next_outbox.clear();
        }
        cl->write_state = cl->outbox.size() > 0 ? CL_WRITE_READY : 0;
#ifdef WITH_RDMA
        if (cl->rdma_conn && !cl->outbox.size() && cl->peer_state == PEER_RDMA_CONNECTING)
        {
            // FIXME: Do something better than just forgetting the FD
            // FIXME: Ignore pings during RDMA state transition
            if (log_level > 0)
            {
                fprintf(stderr, "Successfully connected with client %d using RDMA\n", cl->peer_fd);
            }
            cl->peer_state = PEER_RDMA;
            tfd->set_fd_handler(cl->peer_fd, false, [this](int peer_fd, int epoll_events)
            {
                // Do not miss the disconnection!
                if (epoll_events & EPOLLRDHUP)
                {
                    handle_peer_epoll(peer_fd, epoll_events);
                }
            });
            // Add the initial receive request
            try_recv_rdma(cl);
        }
#endif
    }
    if (cl->write_state != 0)
    {
        write_ready_clients.push_back(cl->peer_fd);
    }
}
